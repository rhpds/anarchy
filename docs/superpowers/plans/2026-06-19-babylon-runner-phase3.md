# Babylon Runner Phase 3 (Production) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add observability (Prometheus metrics, health endpoints), optimize performance (polling loop, Tower client reuse/token caching, K8s secret informer cache), and achieve feature parity (controller scheduler integration) on top of the validated Phase 1+2 codebase.

**Architecture:** The existing codebase has a clean `internal/` package structure with typed payloads, context propagation, and shared HTTP infrastructure. Phase 3 adds a metrics HTTP server (separate from the Anarchy protocol), wires Prometheus instrumentation into existing components, and introduces two new packages (`internal/metrics`, `internal/secrets`) alongside a new client (`internal/clients/scheduler.go`). All changes are surgical additions — no handler or client rewrites.

**Tech Stack:** Go 1.26, `github.com/prometheus/client_golang` (new), `k8s.io/client-go` (existing), `k8s.io/api` (promote from indirect)

## Global Constraints

- **Do not rewrite existing handlers or clients** — Phase 1+2 code is validated in production. Add instrumentation, caching, and new features surgically.
- **All tests must pass:** `cd babylon-runner && go test ./...` must succeed after each task.
- **Each new package must have `*_test.go`** files with unit tests.
- **Commit messages must not mention the phase.** No `Co-Authored-By` lines.
- **`httputil/token_cache.go` already exists** and satisfies the spec's `TokenCache` interface — do NOT rewrite it. The Sandbox client already uses it. Only the Tower client needs wiring.
- **`httputil/transport.go` already exists** with connection pooling (`MaxIdleConns:100`, `MaxIdleConnsPerHost:10`, `IdleConnTimeout:90s`) — do NOT rewrite it.
- **`ControllerSchedulerMeta` already exists** in `internal/types/payload.go` — reuse it.

---

### Task 1: Prometheus Metrics and Health Server (#8, #9)

**Files:**

- Create: `internal/metrics/metrics.go`
- Create: `internal/metrics/server.go`
- Create: `internal/metrics/metrics_test.go`
- Create: `internal/metrics/server_test.go`
- Modify: `internal/runner/config.go` — add `MetricsPort` field
- Modify: `internal/runner/config_test.go` — test new config field
- Modify: `internal/runner/runner.go` — add `ready` atomic, expose `IsReady()`
- Modify: `cmd/babylon-runner/main.go` — start metrics server in goroutine

**Interfaces:**

- Consumes: nothing (first task)
- Produces:
  - `metrics.RunDuration` (`*prometheus.HistogramVec`, labels: `handler_type`, `action`)
  - `metrics.RunTotal` (`*prometheus.CounterVec`, labels: `handler_type`, `action`, `status`)
  - `metrics.PollDuration` (`*prometheus.Histogram`)
  - `metrics.TowerJobDuration` (`*prometheus.HistogramVec`, labels: `operation`)
  - `metrics.SandboxAPIDuration` (`*prometheus.HistogramVec`, labels: `operation`)
  - `metrics.ActiveRun` (`*prometheus.Gauge`)
  - `metrics.NewServer(port int, readyFn func() bool) *Server`
  - `(*Server).Start(ctx context.Context) error`
  - `(*Runner).IsReady() bool`
  - `Config.MetricsPort` (int, default 9093)

- [ ] **Step 1: Add prometheus dependency**

```bash
cd babylon-runner && go get github.com/prometheus/client_golang/prometheus github.com/prometheus/client_golang/prometheus/promauto github.com/prometheus/client_golang/prometheus/promhttp
```

- [ ] **Step 2: Write failing test for metrics registration**

Create `internal/metrics/metrics_test.go`:

```go
package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegistered(t *testing.T) {
	// Verify all metrics can be described (they're registered with promauto).
	ch := make(chan *prometheus.Desc, 20)
	RunDuration.Describe(ch)
	desc := <-ch
	if desc == nil {
		t.Error("RunDuration not registered")
	}

	RunTotal.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("RunTotal not registered")
	}

	PollDuration.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("PollDuration not registered")
	}

	ActiveRun.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("ActiveRun not registered")
	}

	TowerJobDuration.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("TowerJobDuration not registered")
	}

	SandboxAPIDuration.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("SandboxAPIDuration not registered")
	}
}
```

Run: `cd babylon-runner && go test ./internal/metrics/ -v -run TestMetricsRegistered`
Expected: FAIL — package does not exist yet.

- [ ] **Step 3: Write metrics.go with all metric definitions**

Create `internal/metrics/metrics.go`:

```go
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RunDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "babylon_runner_run_duration_seconds",
		Help:    "Duration of run execution by handler type and action",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300},
	}, []string{"handler_type", "action"})

	RunTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "babylon_runner_runs_total",
		Help: "Total runs processed by status",
	}, []string{"handler_type", "action", "status"})

	PollDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "babylon_runner_poll_duration_seconds",
		Help:    "Duration of GET /run poll requests",
		Buckets: []float64{0.01, 0.1, 1, 5, 10, 30, 35},
	})

	TowerJobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "babylon_runner_tower_job_duration_seconds",
		Help:    "Duration of Tower API operations",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30},
	}, []string{"operation"})

	SandboxAPIDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "babylon_runner_sandbox_api_duration_seconds",
		Help:    "Duration of Sandbox API operations",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60},
	}, []string{"operation"})

	ActiveRun = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "babylon_runner_active_run",
		Help: "1 if currently processing a run, 0 if idle",
	})
)
```

Run: `cd babylon-runner && go test ./internal/metrics/ -v -run TestMetricsRegistered`
Expected: PASS

- [ ] **Step 4: Write failing test for health server**

Create `internal/metrics/server_test.go`:

```go
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestServerHealthz(t *testing.T) {
	port := 19093
	s := NewServer(port, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/healthz", port))
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want 200", resp.StatusCode)
	}
}

func TestServerReadyzReady(t *testing.T) {
	port := 19094
	s := NewServer(port, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz", port))
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz status = %d, want 200", resp.StatusCode)
	}
}

func TestServerReadyzNotReady(t *testing.T) {
	port := 19095
	s := NewServer(port, func() bool { return false })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz", port))
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz status = %d, want 503", resp.StatusCode)
	}
}

func TestServerMetrics(t *testing.T) {
	port := 19096
	s := NewServer(port, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", port))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /metrics status = %d, want 200", resp.StatusCode)
	}
}

func TestServerShutdown(t *testing.T) {
	port := 19097
	s := NewServer(port, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("Start returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("server did not shut down within 2s")
	}
}
```

Run: `cd babylon-runner && go test ./internal/metrics/ -v -run TestServer`
Expected: FAIL — `NewServer` does not exist yet.

- [ ] **Step 5: Write server.go**

Create `internal/metrics/server.go`:

```go
package metrics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	server  *http.Server
	readyFn func() bool
}

func NewServer(port int, readyFn func() bool) *Server {
	mux := http.NewServeMux()
	s := &Server{
		readyFn: readyFn,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
	}

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)

	return s
}

func (s *Server) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.server.Close()
	}()
	err := s.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.readyFn != nil && s.readyFn() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte("not ready"))
}
```

Run: `cd babylon-runner && go test ./internal/metrics/ -v`
Expected: PASS

- [ ] **Step 6: Add MetricsPort to Config**

In `internal/runner/config.go`, add `MetricsPort int` to the `Config` struct and parse it in `ConfigFromEnv()`:

```go
// Add to Config struct:
MetricsPort int

// Add to ConfigFromEnv(), after ActionRetryIntervals:
cfg.MetricsPort = envInt("METRICS_PORT", 9093)
```

Add test in `internal/runner/config_test.go` for `METRICS_PORT` env var:

```go
func TestConfigFromEnv_MetricsPort(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("METRICS_PORT", "9999")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.MetricsPort != 9999 {
		t.Errorf("MetricsPort = %d, want 9999", cfg.MetricsPort)
	}
}

func TestConfigFromEnv_MetricsPortDefault(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.MetricsPort != 9093 {
		t.Errorf("MetricsPort = %d, want 9093", cfg.MetricsPort)
	}
}
```

Note: The existing config tests use a `setRequiredEnv(t)` helper or equivalent — follow the existing test pattern. If no such helper exists, set the required env vars (`ANARCHY_URL`, `RUNNER_NAME`, `RUNNER_TOKEN`, `HOSTNAME`) directly in each test.

Run: `cd babylon-runner && go test ./internal/runner/ -v -run TestConfigFromEnv_MetricsPort`
Expected: PASS

- [ ] **Step 7: Add IsReady to Runner and wire metrics server in main.go**

In `internal/runner/runner.go`:

- Add `ready atomic.Bool` to `Runner` struct (import `sync/atomic`)
- Add method `func (r *Runner) IsReady() bool { return r.ready.Load() }`
- In `getRun()`, after successful response parsing (both 200 and 204/408 cases), set `r.ready.Store(true)` — a successful HTTP round-trip to Anarchy means the runner is connected

In `cmd/babylon-runner/main.go`, before `r.Run()`:

```go
metricsServer := metrics.NewServer(cfg.MetricsPort, r.IsReady)
go func() {
    if err := metricsServer.Start(context.Background()); err != nil {
        slog.Error("metrics server failed", "error", err)
    }
}()
```

Add the import: `"github.com/rhpds/anarchy/babylon-runner/internal/metrics"`

Run: `cd babylon-runner && go test ./... -count=1`
Expected: PASS (all existing tests still pass)

- [ ] **Step 8: Commit**

```bash
cd babylon-runner
git add internal/metrics/ internal/runner/config.go internal/runner/config_test.go internal/runner/runner.go cmd/babylon-runner/main.go go.mod go.sum
git commit -m "feat(babylon-runner): add Prometheus metrics and health endpoints

Add metrics HTTP server serving /metrics, /healthz, /readyz on
configurable METRICS_PORT (default 9093). Define Prometheus metrics
for run duration, run count, poll duration, Tower/Sandbox API
operations, and active run gauge. Health probes support Kubernetes
liveness and readiness checks."
```

---

### Task 2: Polling Loop Optimization (#12)

**Files:**

- Modify: `internal/runner/runner.go` — refactor `Run()` method
- Modify: `internal/runner/runner_test.go` — add/update tests for new loop behavior

**Interfaces:**

- Consumes: `Runner.pollOnce(ctx)`, `Runner.config.PollingInterval`
- Produces: same `Run()` method signature, different loop behavior (tight loop, sleep only on error)

- [ ] **Step 1: Write test for new polling loop behavior**

Add to `internal/runner/runner_test.go`:

```go
func TestRunLoopStopsOnContextCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	// Give it time to poll at least once.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestRunLoopSleepsOnError(t *testing.T) {
	var pollCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pollCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := Config{
		AnarchyURL:      server.URL,
		RunnerName:      "r",
		PodName:         "p",
		RunnerToken:     "t",
		PollingInterval: 200 * time.Millisecond,
		RequestTimeout:  1 * time.Second,
	}
	r := New(cfg, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	r.Run(ctx)

	// With 200ms sleep between errors and 600ms window, we expect ~3 polls max.
	// Without sleep, it would be hundreds.
	count := pollCount.Load()
	if count > 5 {
		t.Errorf("poll count = %d, expected <=5 (should sleep between errors)", count)
	}
}
```

Run: `cd babylon-runner && go test ./internal/runner/ -v -run TestRunLoop`
Expected: FAIL — current ticker-based Run() won't match expected behavior.

- [ ] **Step 2: Refactor Run() to remove ticker**

Replace the `Run()` method in `internal/runner/runner.go`:

```go
func (r *Runner) Run(ctx context.Context) {
	slog.Info("babylon-runner starting",
		"runner", r.config.RunnerName,
		"pod", r.config.PodName,
		"url", r.config.AnarchyURL)

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		default:
		}
		if err := r.pollOnce(ctx); err != nil {
			slog.Error("poll error", "error", err)
			select {
			case <-ctx.Done():
				slog.Info("shutting down")
				return
			case <-time.After(r.config.PollingInterval):
			}
		}
	}
}
```

Also update the signature: the current `Run()` takes no arguments and creates its own context. The new version takes `ctx context.Context` so signal handling moves to `main.go`.

Update `cmd/babylon-runner/main.go` to handle signals:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

// ... metrics server start ...

r.Run(ctx)
```

Add imports in main.go: `"context"`, `"os"`, `"os/signal"`, `"syscall"`.

Run: `cd babylon-runner && go test ./internal/runner/ -v -run TestRunLoop`
Expected: PASS

- [ ] **Step 3: Run full test suite**

Run: `cd babylon-runner && go test ./... -count=1`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
cd babylon-runner
git add internal/runner/runner.go internal/runner/runner_test.go cmd/babylon-runner/main.go
git commit -m "feat(babylon-runner): optimize polling loop to remove redundant ticker

Remove time.Ticker from Run() — the server-side 30s long-poll timeout
is the effective idle sleep. On timeout (no run available), immediately
re-poll. On connection error, wait PollingInterval before retrying.
Move signal handling to main.go so Run() accepts a context."
```

---

### Task 3: Run Metrics Instrumentation and Instrumented HTTP Transport (#8)

**Files:**

- Create: `internal/httputil/instrument.go`
- Create: `internal/httputil/instrument_test.go`
- Modify: `internal/runner/runner.go` — instrument `pollOnce` with metrics
- Modify: `internal/clients/anarchy.go` — wrap transport with instrumented transport
- Modify: `internal/clients/tower.go` — wrap transport with instrumented transport
- Modify: `internal/clients/sandbox.go` — wrap transport with instrumented transport

**Interfaces:**

- Consumes: `metrics.RunDuration`, `metrics.RunTotal`, `metrics.PollDuration`, `metrics.ActiveRun`, `metrics.TowerJobDuration`, `metrics.SandboxAPIDuration`
- Produces: `httputil.InstrumentedTransport(next http.RoundTripper, histogram *prometheus.HistogramVec, labels ...string) http.RoundTripper`

- [ ] **Step 1: Write failing test for InstrumentedTransport**

Create `internal/httputil/instrument_test.go`:

```go
package httputil

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestInstrumentedTransport(t *testing.T) {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "test_http_duration_seconds",
		Help:    "test",
		Buckets: []float64{0.01, 0.1, 1},
	}, []string{"method"})

	transport := InstrumentedTransport(http.DefaultTransport, hist, "GET")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &http.Client{Transport: transport}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	// Verify the histogram was observed.
	ch := make(chan prometheus.Metric, 1)
	hist.With(prometheus.Labels{"method": "GET"}).Write(nil) // force collect
	hist.Collect(ch)
	m := <-ch

	var metric prometheus.Metric = m
	if metric == nil {
		t.Error("expected histogram metric to be collected")
	}
}

func TestInstrumentedTransportError(t *testing.T) {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "test_http_error_duration",
		Help:    "test",
		Buckets: []float64{0.01, 0.1, 1},
	}, []string{"method"})

	transport := InstrumentedTransport(http.DefaultTransport, hist, "GET")
	client := &http.Client{Transport: transport}

	// Request to non-existent server — should not panic.
	_, err := client.Get("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}
```

Run: `cd babylon-runner && go test ./internal/httputil/ -v -run TestInstrumentedTransport`
Expected: FAIL — `InstrumentedTransport` does not exist.

- [ ] **Step 2: Implement InstrumentedTransport**

Create `internal/httputil/instrument.go`:

```go
package httputil

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type instrumentedTransport struct {
	next      http.RoundTripper
	histogram *prometheus.HistogramVec
	labels    []string
}

// InstrumentedTransport wraps an http.RoundTripper with Prometheus histogram
// instrumentation. Each request records duration with the given label values.
func InstrumentedTransport(next http.RoundTripper, histogram *prometheus.HistogramVec, labels ...string) http.RoundTripper {
	return &instrumentedTransport{
		next:      next,
		histogram: histogram,
		labels:    labels,
	}
}

func (t *instrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	t.histogram.WithLabelValues(t.labels...).Observe(time.Since(start).Seconds())
	return resp, err
}
```

Run: `cd babylon-runner && go test ./internal/httputil/ -v -run TestInstrumentedTransport`
Expected: PASS

- [ ] **Step 3: Add metrics recording in pollOnce**

In `internal/runner/runner.go`, instrument `pollOnce`:

```go
import "github.com/rhpds/anarchy/babylon-runner/internal/metrics"
```

Add timing/counting to `pollOnce()`:

- Record `metrics.PollDuration` around `getRun()` call
- Set `metrics.ActiveRun.Set(1)` before dispatch, `metrics.ActiveRun.Set(0)` after (in defer)
- Record `metrics.RunDuration` around the dispatch+postResult block
- Increment `metrics.RunTotal` after result is known

The exact instrumentation points:

```go
func (r *Runner) pollOnce(ctx context.Context) error {
	pollStart := time.Now()
	payload, err := r.getRun(ctx)
	metrics.PollDuration.Observe(time.Since(pollStart).Seconds())
	if err != nil {
		slog.Error("poll failed", "error", err)
		return err
	}
	if payload == nil {
		return nil
	}

	metrics.ActiveRun.Set(1)
	defer metrics.ActiveRun.Set(0)

	// ... build RunContext (existing code) ...

	handlerType := payload.Handler.Type
	actionName := handlerName

	runStart := time.Now()

	func() {
		// ... existing panic recovery + dispatch ...
	}()

	metrics.RunDuration.WithLabelValues(handlerType, actionName).Observe(time.Since(runStart).Seconds())
	metrics.RunTotal.WithLabelValues(handlerType, actionName, rc.Result.Status).Inc()

	// ... existing postResult ...
}
```

- [ ] **Step 4: Wire InstrumentedTransport into clients**

In `internal/clients/anarchy.go`, wrap the HTTP transport:

```go
import "github.com/rhpds/anarchy/babylon-runner/internal/metrics"

// In NewAnarchyClient, wrap the transport:
transport := httputil.NewTransport(nil)
client := &http.Client{
    Timeout:   cfg.Timeout,
    Transport: httputil.InstrumentedTransport(transport, metrics.PollDuration),
}
```

Wait — the Anarchy client doesn't need InstrumentedTransport on `PollDuration`. The poll is measured in the runner. For per-client HTTP metrics, we don't have a spec-defined metric name for the Anarchy client specifically. The spec defines `TowerJobDuration` and `SandboxAPIDuration` as operation-level metrics, not transport-level.

**Revised approach**: Only wrap Tower and Sandbox transports:

In `internal/clients/tower.go`, in `NewTowerClient`:

```go
import (
    "github.com/rhpds/anarchy/babylon-runner/internal/httputil"
    "github.com/rhpds/anarchy/babylon-runner/internal/metrics"
)

func NewTowerClient(hostname, username, password string, tlsConfig *tls.Config) *TowerClient {
    transport := httputil.NewTransport(tlsConfig)
    return &TowerClient{
        baseURL:  "https://" + hostname,
        username: username,
        password: password,
        client: &http.Client{
            Transport: httputil.InstrumentedTransport(transport, metrics.TowerJobDuration, "http"),
        },
    }
}
```

In `internal/clients/sandbox.go`, in `NewSandboxAPIClient`:

```go
import "github.com/rhpds/anarchy/babylon-runner/internal/metrics"

// Replace the transport line:
Transport: httputil.InstrumentedTransport(httputil.NewTransport(nil), metrics.SandboxAPIDuration, "http"),
```

- [ ] **Step 5: Run full test suite**

Run: `cd babylon-runner && go test ./... -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd babylon-runner
git add internal/httputil/instrument.go internal/httputil/instrument_test.go internal/runner/runner.go internal/clients/tower.go internal/clients/sandbox.go
git commit -m "feat(babylon-runner): instrument runner and clients with Prometheus metrics

Add InstrumentedTransport HTTP round-tripper wrapper for per-request
timing. Instrument polling loop with run duration, run count, poll
duration, and active run gauge. Wire Tower and Sandbox client
transports with operation histograms."
```

---

### Task 4: Tower Client Pool and Token Caching (#6)

**Files:**

- Modify: `internal/clients/tower.go` — add `TokenCache` to `TowerClient`, add `TowerClientPool`
- Modify: `internal/clients/tower_test.go` — tests for token caching and pool
- Modify: `internal/runner/runner.go` — create and hold `TowerClientPool`
- Modify: `internal/runner/run_context.go` — add `TowerClientPool` field
- Modify: `internal/handler/tower_launch.go` — use pool in `getTowerClientForAction` and `getTowerClientForHost`
- Modify: `cmd/babylon-runner/main.go` — defer pool cleanup

**Interfaces:**

- Consumes: `httputil.TokenCache`, `httputil.NewTransport`, `Runner`, `RunContext`
- Produces:
  - `TowerClient.GetToken(ctx context.Context) (string, error)` — cached token access
  - `TowerClient.Close(ctx context.Context) error` — cleanup
  - `TowerClientPool` struct with `Get(hostname, username, password string, tlsConfig *tls.Config) *TowerClient`
  - `TowerClientPool.CloseAll(ctx context.Context)` — cleanup all cached clients

- [ ] **Step 1: Write failing test for Tower token caching**

Add to `internal/clients/tower_test.go`:

```go
func TestTowerClientTokenCache(t *testing.T) {
	var tokenCreateCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tokens/":
			tokenCreateCount.Add(1)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "cached-token",
				"id":    float64(42),
			})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	tc := NewTowerClient(host, "user", "pass", &tls.Config{InsecureSkipVerify: true})
	tc.baseURL = server.URL

	ctx := context.Background()
	tok1, err := tc.GetToken(ctx)
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if tok1 != "cached-token" {
		t.Errorf("token = %q, want %q", tok1, "cached-token")
	}

	tok2, err := tc.GetToken(ctx)
	if err != nil {
		t.Fatalf("GetToken second call: %v", err)
	}
	if tok2 != "cached-token" {
		t.Errorf("second token = %q, want %q", tok2, "cached-token")
	}

	if tokenCreateCount.Load() != 1 {
		t.Errorf("token create count = %d, want 1 (should be cached)", tokenCreateCount.Load())
	}

	tc.Close(ctx)
}

func TestTowerClientPoolReuse(t *testing.T) {
	pool := NewTowerClientPool()
	tc1 := pool.Get("host1", "user", "pass", nil)
	tc2 := pool.Get("host1", "user", "pass", nil)

	if tc1 != tc2 {
		t.Error("pool should return the same client for the same hostname")
	}

	tc3 := pool.Get("host2", "user", "pass", nil)
	if tc1 == tc3 {
		t.Error("pool should return different clients for different hostnames")
	}
}
```

Run: `cd babylon-runner && go test ./internal/clients/ -v -run TestTowerClient`
Expected: FAIL — `GetToken`, `Close`, `NewTowerClientPool` don't exist.

- [ ] **Step 2: Add TokenCache to TowerClient and create TowerClientPool**

In `internal/clients/tower.go`:

Add fields and methods to `TowerClient`:

```go
type TowerClient struct {
	baseURL    string
	username   string
	password   string
	client     *http.Client
	tokenCache *httputil.TokenCache
	tokenID    int
}
```

Update `NewTowerClient` to set up the token cache:

```go
func NewTowerClient(hostname, username, password string, tlsConfig *tls.Config) *TowerClient {
	tc := &TowerClient{
		baseURL:  "https://" + hostname,
		username: username,
		password: password,
		client: &http.Client{
			Transport: httputil.InstrumentedTransport(
				httputil.NewTransport(tlsConfig),
				metrics.TowerJobDuration, "http",
			),
		},
	}
	tc.tokenCache = httputil.NewTokenCache(
		func(ctx context.Context) (string, time.Duration, error) {
			token, id, err := tc.CreateOAuthToken()
			if err != nil {
				return "", 0, err
			}
			tc.tokenID = id
			return token, 30 * time.Minute, nil
		},
		httputil.WithCleanup(func(ctx context.Context, token string) error {
			if tc.tokenID > 0 {
				return tc.DeleteOAuthToken(tc.tokenID)
			}
			return nil
		}),
	)
	return tc
}
```

Add `GetToken` and `Close`:

```go
func (tc *TowerClient) GetToken(ctx context.Context) (string, error) {
	return tc.tokenCache.Get(ctx)
}

func (tc *TowerClient) Close(ctx context.Context) error {
	return tc.tokenCache.Close(ctx)
}
```

Add `TowerClientPool`:

```go
type TowerClientPool struct {
	mu      sync.RWMutex
	clients map[string]*TowerClient
}

func NewTowerClientPool() *TowerClientPool {
	return &TowerClientPool{
		clients: make(map[string]*TowerClient),
	}
}

func (p *TowerClientPool) Get(hostname, username, password string, tlsConfig *tls.Config) *TowerClient {
	p.mu.RLock()
	if c, ok := p.clients[hostname]; ok {
		p.mu.RUnlock()
		return c
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[hostname]; ok {
		return c
	}
	c := NewTowerClient(hostname, username, password, tlsConfig)
	p.clients[hostname] = c
	return c
}

func (p *TowerClientPool) CloseAll(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		c.Close(ctx)
	}
}
```

Add required imports: `"context"`, `"sync"`, `"time"`, `"github.com/rhpds/anarchy/babylon-runner/internal/httputil"`.

- [ ] **Step 3: Update LaunchJob to use cached token**

In `internal/clients/tower.go`, update `LaunchJob()`:

Replace the token creation/cleanup block:

```go
// OLD:
token, tokenID, err := tc.CreateOAuthToken()
if err != nil { ... }
defer func() { _ = tc.DeleteOAuthToken(tokenID) }()

// NEW:
token, err := tc.GetToken(context.TODO())
if err != nil {
    return 0, fmt.Errorf("get oauth token: %w", err)
}
```

Remove the `defer` — token cleanup is handled by `Close()` on pool shutdown.

Similarly update `GetJobStatus`, `CancelJob`, etc. that currently take `oauthToken string` parameter: these methods now need to get the token internally. **However**, changing these method signatures would be a large refactor that affects all callers (handler code). Instead, leave the method signatures unchanged — callers will use `tc.GetToken(ctx)` to get the token and pass it. This matches the existing pattern but avoids re-creating tokens.

The key change is in `handler/tower_launch.go` and `handler/check_deployer.go` where tokens are currently created inline.

- [ ] **Step 4: Wire TowerClientPool into Runner and RunContext**

In `internal/runner/runner.go`:

- Add `towerPool *clients.TowerClientPool` to `Runner` struct
- Initialize in `New()`: `towerPool: clients.NewTowerClientPool()`
- Pass to `RunContext` in `pollOnce()`

In `internal/runner/run_context.go`:

- Add `TowerClientPool *clients.TowerClientPool` to `RunContext`

In `cmd/babylon-runner/main.go`:

- Add `defer r.TowerPool().CloseAll(context.Background())` (or expose pool for cleanup)

- [ ] **Step 5: Update tower_launch.go to use the pool**

In `internal/handler/tower_launch.go`, update `getTowerClientForAction()`:

```go
// OLD:
return clients.NewTowerClient(hostname, username, password, rc.TowerTLSConfig), hostname, nil

// NEW:
return rc.TowerClientPool.Get(hostname, username, password, rc.TowerTLSConfig), hostname, nil
```

Similarly update `getTowerClientForHost()` — use `rc.TowerClientPool.Get(...)` directly, no nil guard needed (pool is always initialized in `New()`).

Update `checkDeployerJob` in `handler/check_deployer.go` — replace inline token creation:

```go
// OLD:
token, tokenID, err := tc.CreateOAuthToken()
...
defer func() { ... tc.DeleteOAuthToken(tokenID) ... }()

// NEW:
token, err := tc.GetToken(rc.Ctx)
if err != nil {
    slog.Error("checkDeployerJob: failed to get token", ...)
    rc.ContinueAction("5m")
    return nil
}
```

Remove the `defer DeleteOAuthToken` — pool manages cleanup.

Similarly update `cancelTowerJob` in `tower_launch.go`.

- [ ] **Step 6: Run full test suite**

Run: `cd babylon-runner && go test ./... -count=1`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
cd babylon-runner
git add internal/clients/tower.go internal/clients/tower_test.go internal/runner/runner.go internal/runner/run_context.go internal/handler/tower_launch.go internal/handler/check_deployer.go cmd/babylon-runner/main.go
git commit -m "feat(babylon-runner): add Tower client pool with OAuth token caching

Reuse Tower HTTP clients per hostname via TowerClientPool and cache
OAuth tokens with httputil.TokenCache. Eliminates per-operation token
create/delete cycles — tokens are cached for 30 minutes and cleaned
up on pool shutdown."
```

---

### Task 5: Kubernetes Secret Informer Cache (#17)

**Files:**

- Create: `internal/secrets/cache.go`
- Create: `internal/secrets/cache_test.go`
- Modify: `cmd/babylon-runner/main.go` — initialize cache
- Modify: `internal/runner/run_context.go` — add `SecretCache` field
- Modify: `internal/handler/tower_launch.go` — use cache in `resolveControllerCreds`

**Interfaces:**

- Consumes: `kubernetes.Interface` (client-go clientset)
- Produces:
  - `secrets.Cache` struct
  - `secrets.NewCache(clientset kubernetes.Interface, namespace string) *Cache`
  - `(*Cache).Start(ctx context.Context) error` — starts informer, waits for sync
  - `(*Cache).GetByLabel(labelKey, labelValue string) (*corev1.Secret, bool)` — local cache lookup
  - `(*Cache).Stop()` — stops informer

- [ ] **Step 1: Write failing test for SecretCache**

Create `internal/secrets/cache_test.go`:

```go
package secrets

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCacheGetByLabel(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tower-creds-host1",
			Namespace: "anarchy",
			Labels: map[string]string{
				"babylon.gpte.redhat.com/ansible-control-plane": "host1.example.com",
			},
		},
		Data: map[string][]byte{
			"user":     []byte("admin"),
			"password": []byte("secret"),
		},
	})

	c := NewCache(clientset, "anarchy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	secret, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "host1.example.com")
	if !ok {
		t.Fatal("expected to find secret")
	}
	if string(secret.Data["user"]) != "admin" {
		t.Errorf("user = %q, want %q", string(secret.Data["user"]), "admin")
	}
	if string(secret.Data["password"]) != "secret" {
		t.Errorf("password = %q, want %q", string(secret.Data["password"]), "secret")
	}
}

func TestCacheGetByLabelNotFound(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	c := NewCache(clientset, "anarchy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	_, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "nonexistent")
	if ok {
		t.Error("expected not to find secret")
	}
}

func TestCacheMultipleSecrets(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tower-creds-host1",
				Namespace: "anarchy",
				Labels: map[string]string{
					"babylon.gpte.redhat.com/ansible-control-plane": "host1.example.com",
				},
			},
			Data: map[string][]byte{"user": []byte("admin1")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tower-creds-host2",
				Namespace: "anarchy",
				Labels: map[string]string{
					"babylon.gpte.redhat.com/ansible-control-plane": "host2.example.com",
				},
			},
			Data: map[string][]byte{"user": []byte("admin2")},
		},
	)

	c := NewCache(clientset, "anarchy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	s1, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "host1.example.com")
	if !ok {
		t.Fatal("expected to find host1 secret")
	}
	if string(s1.Data["user"]) != "admin1" {
		t.Errorf("host1 user = %q, want %q", string(s1.Data["user"]), "admin1")
	}

	s2, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "host2.example.com")
	if !ok {
		t.Fatal("expected to find host2 secret")
	}
	if string(s2.Data["user"]) != "admin2" {
		t.Errorf("host2 user = %q, want %q", string(s2.Data["user"]), "admin2")
	}
}
```

Run: `cd babylon-runner && go test ./internal/secrets/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 2: Implement SecretCache**

Create `internal/secrets/cache.go`:

```go
package secrets

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type Cache struct {
	informer cache.SharedIndexInformer
	stopCh   chan struct{}
}

func NewCache(clientset kubernetes.Interface, namespace string) *Cache {
	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset, 0,
		informers.WithNamespace(namespace),
	)
	informer := factory.Core().V1().Secrets().Informer()
	return &Cache{
		informer: informer,
		stopCh:   make(chan struct{}),
	}
}

func (c *Cache) Start(ctx context.Context) error {
	go c.informer.Run(c.stopCh)

	deadline, hasDeadline := ctx.Deadline()
	timeout := 30 * time.Second
	if hasDeadline {
		timeout = time.Until(deadline)
	}

	if !cache.WaitForCacheSync(
		func() <-chan struct{} {
			ch := make(chan struct{})
			go func() {
				select {
				case <-time.After(timeout):
					close(ch)
				case <-ctx.Done():
					close(ch)
				}
			}()
			return ch
		}(),
		c.informer.HasSynced,
	) {
		return fmt.Errorf("secret informer cache sync timed out")
	}
	return nil
}

func (c *Cache) GetByLabel(labelKey, labelValue string) (*corev1.Secret, bool) {
	for _, obj := range c.informer.GetStore().List() {
		secret, ok := obj.(*corev1.Secret)
		if !ok {
			continue
		}
		if secret.Labels[labelKey] == labelValue {
			return secret, true
		}
	}
	return nil, false
}

func (c *Cache) Stop() {
	close(c.stopCh)
}
```

Run: `cd babylon-runner && go test ./internal/secrets/ -v`
Expected: PASS

- [ ] **Step 3: Promote k8s.io/api from indirect to direct dependency**

The `cache_test.go` imports `k8s.io/api/core/v1` which is currently an indirect dependency. Run:

```bash
cd babylon-runner && go mod tidy
```

- [ ] **Step 4: Wire SecretCache into main.go and RunContext**

In `internal/runner/run_context.go`, add:

```go
import "github.com/rhpds/anarchy/babylon-runner/internal/secrets"

// Add to RunContext:
SecretCache *secrets.Cache
```

In `internal/runner/runner.go`, add:

```go
// Add to Runner struct:
secretCache *secrets.Cache

// Add method:
func (r *Runner) SetSecretCache(c *secrets.Cache) { r.secretCache = c }
```

Pass in `pollOnce()`:

```go
rc := &RunContext{
    // ... existing fields ...
    SecretCache: r.secretCache,
}
```

In `cmd/babylon-runner/main.go`, after building clientset:

```go
import "github.com/rhpds/anarchy/babylon-runner/internal/secrets"

// Determine namespace.
ns := os.Getenv("ANARCHY_NAMESPACE")
if ns == "" {
    if nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
        ns = string(nsBytes)
    }
}

if clientset != nil && ns != "" {
    secretCache := secrets.NewCache(clientset, ns)
    if err := secretCache.Start(ctx); err != nil {
        slog.Warn("secret cache failed to start", "error", err)
    } else {
        r.SetSecretCache(secretCache)
        defer secretCache.Stop()
    }
}
```

- [ ] **Step 5: Update resolveControllerCreds to use cache**

In `internal/handler/tower_launch.go`, replace the entire K8s Secret fallback block in `resolveControllerCreds` with:

```go
// OLD (entire block — namespace resolution + Clientset.CoreV1().Secrets().List()):
if ns != "" && hostname != "" && rc.Clientset != nil {
    secrets, err := rc.Clientset.CoreV1().Secrets(ns).List(...)
    ...
}

// NEW — cache lookup only, no fallback:
if hostname != "" && rc.SecretCache != nil {
    if secret, ok := rc.SecretCache.GetByLabel(
        "babylon.gpte.redhat.com/ansible-control-plane", hostname); ok {
        if u := string(secret.Data["user"]); u != "" && username == "" {
            username = u
        }
        if p := string(secret.Data["password"]); p != "" && password == "" {
            password = p
        }
    }
}
```

No fallback to direct API call — the SharedInformer watches all secrets in the namespace. If the cache is nil (informer failed to start), tiers 1 and 2 (direct fields + varSecret) still resolve credentials. Remove the `os`, `metav1`, and namespace resolution code that was only needed for the direct API call.

Remove unused imports: `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"` (if no longer used elsewhere in the file), `"os"` (if no longer used).

- [ ] **Step 6: Run full test suite**

Run: `cd babylon-runner && go test ./... -count=1`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
cd babylon-runner
git add internal/secrets/ internal/runner/run_context.go internal/runner/runner.go internal/handler/tower_launch.go cmd/babylon-runner/main.go go.mod go.sum
git commit -m "feat(babylon-runner): add Kubernetes secret informer cache

Use client-go SharedInformer to cache secrets locally with watch-based
invalidation. Replaces per-run API server calls in resolveControllerCreds
with zero-cost local cache lookups."
```

---

### Task 6: Controller Scheduler Integration (#16)

**Files:**

- Create: `internal/clients/scheduler.go`
- Create: `internal/clients/scheduler_test.go`
- Modify: `internal/handler/tower_launch.go` — integrate scheduler in `getTowerClientForAction`

**Interfaces:**

- Consumes: `httputil.DoJSON`, `httputil.RetryWithContext`, `httputil.NewTransport`, `RunContext.Meta().ControllerScheduler`, `RunContext.Payload.Governor.Spec.Vars.Get("controller_scheduler_credentials")`
- Produces:
  - `clients.SchedulerClient` struct
  - `clients.NewSchedulerClient(baseURL, apiKey string, tlsConfig *tls.Config) *SchedulerClient`
  - `(*SchedulerClient).Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error)`
  - `clients.EvaluateRequest`, `clients.EvaluateResponse`, `clients.Candidate`, `clients.RankedController` types

- [ ] **Step 1: Write failing test for SchedulerClient**

Create `internal/clients/scheduler_test.go`:

```go
package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSchedulerEvaluateSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/evaluate/controllers" {
			t.Errorf("path = %s, want /api/v1/evaluate/controllers", r.URL.Path)
		}
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Errorf("X-API-Key = %q, want %q", r.Header.Get("X-API-Key"), "test-key")
		}

		var req EvaluateRequest
		json.NewDecoder(r.Body).Decode(&req)
		if len(req.Candidates) != 2 {
			t.Errorf("candidates = %d, want 2", len(req.Candidates))
		}

		json.NewEncoder(w).Encode(EvaluateResponse{
			Ranked: []RankedController{
				{Domain: "host1.example.com", Score: 0.9},
				{Domain: "host2.example.com", Score: 0.1},
			},
		})
	}))
	defer server.Close()

	client := NewSchedulerClient(server.URL, "test-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Evaluate(ctx, EvaluateRequest{
		Candidates: []Candidate{
			{Domain: "host1.example.com"},
			{Domain: "host2.example.com"},
		},
		RequireLabels: map[string]string{"env": "prod"},
		InstanceGroup: "provision",
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(resp.Ranked) != 2 {
		t.Fatalf("ranked = %d, want 2", len(resp.Ranked))
	}
	if resp.Ranked[0].Domain != "host1.example.com" {
		t.Errorf("ranked[0].domain = %q, want %q", resp.Ranked[0].Domain, "host1.example.com")
	}
}

func TestSchedulerEvaluateServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewSchedulerClient(server.URL, "test-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Evaluate(ctx, EvaluateRequest{
		Candidates: []Candidate{{Domain: "host1"}},
	})
	if err == nil {
		t.Error("expected error for 500")
	}
}

func TestSchedulerEvaluateTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewSchedulerClient(server.URL, "test-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := client.Evaluate(ctx, EvaluateRequest{
		Candidates: []Candidate{{Domain: "host1"}},
	})
	if err == nil {
		t.Error("expected error for timeout")
	}
}
```

Run: `cd babylon-runner && go test ./internal/clients/ -v -run TestScheduler`
Expected: FAIL — types and `NewSchedulerClient` don't exist.

- [ ] **Step 2: Implement SchedulerClient**

Create `internal/clients/scheduler.go`:

```go
package clients

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
)

type Candidate struct {
	Domain string `json:"domain"`
}

type EvaluateRequest struct {
	Candidates    []Candidate       `json:"candidates"`
	RequireLabels map[string]string `json:"require_labels,omitempty"`
	PreferLabels  map[string]string `json:"prefer_labels,omitempty"`
	InstanceGroup string            `json:"instance_group,omitempty"`
}

type RankedController struct {
	Domain string  `json:"domain"`
	Score  float64 `json:"score"`
}

type EvaluateResponse struct {
	Ranked []RankedController `json:"ranked"`
}

type SchedulerClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewSchedulerClient(baseURL, apiKey string, tlsConfig *tls.Config) *SchedulerClient {
	return &SchedulerClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: httputil.NewTransport(tlsConfig),
		},
	}
}

func (c *SchedulerClient) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	url := c.baseURL + "/api/v1/evaluate/controllers"
	headers := map[string]string{"X-API-Key": c.apiKey}

	var resp EvaluateResponse
	var lastErr error

	retryDelays := []time.Duration{3 * time.Second, 3 * time.Second}

	err := httputil.RetryWithContext(ctx, retryDelays, func() error {
		status, err := httputil.DoJSON(ctx, c.client, http.MethodPost, url, headers, req, &resp)
		if err != nil {
			lastErr = fmt.Errorf("POST %s: %w", url, err)
			return lastErr
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("POST %s: status %d", url, status)
			return lastErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
```

Run: `cd babylon-runner && go test ./internal/clients/ -v -run TestScheduler`
Expected: PASS

- [ ] **Step 3: Integrate scheduler in getTowerClientForAction**

In `internal/handler/tower_launch.go`, add scheduler logic before the local `SelectController` call in `getTowerClientForAction()`. Insert the scheduler check block between the `controllers` slice extraction and the local `selectController` fallback:

```go
// Try controller-scheduler first (if configured).
if meta.ControllerScheduler != nil && meta.ControllerScheduler.URL != "" {
	selected, hostname, err := trySchedulerSelection(rc, meta, controllers)
	if err != nil {
		slog.Warn("controller-scheduler failed, falling back to local selection",
			"error", err)
	} else {
		return selected, hostname, nil
	}
}

// (existing local selectController fallback follows)
```

Add the `trySchedulerSelection` and `resolveSchedulerAPIKey` helpers:

```go
func trySchedulerSelection(rc *runner.RunContext, meta *types.Meta, controllers []map[string]interface{}) (*clients.TowerClient, string, error) {
	cs := meta.ControllerScheduler

	// Build candidates from controller hostnames.
	candidates := make([]clients.Candidate, 0, len(controllers))
	for _, c := range controllers {
		if h, ok := c["hostname"].(string); ok && h != "" {
			candidates = append(candidates, clients.Candidate{Domain: h})
		}
	}
	if len(candidates) == 0 {
		return nil, "", fmt.Errorf("no valid controller hostnames for scheduler")
	}

	// Resolve API key from secret cache or direct lookup.
	apiKey, err := resolveSchedulerAPIKey(rc)
	if err != nil {
		return nil, "", fmt.Errorf("scheduler API key: %w", err)
	}

	// Determine instance group: override from meta, or action name.
	instanceGroup := cs.InstanceGroup
	if instanceGroup == "" {
		instanceGroup = rc.ActionName()
	}

	scheduler := clients.NewSchedulerClient(cs.URL, apiKey, rc.TowerTLSConfig)
	resp, err := scheduler.Evaluate(rc.Ctx, clients.EvaluateRequest{
		Candidates:    candidates,
		RequireLabels: cs.RequireLabels,
		PreferLabels:  cs.PreferLabels,
		InstanceGroup: instanceGroup,
	})
	if err != nil {
		return nil, "", err
	}
	if len(resp.Ranked) == 0 {
		return nil, "", fmt.Errorf("scheduler returned empty ranking")
	}

	// Find the ranked controller in our list and resolve creds.
	selectedHost := resp.Ranked[0].Domain
	for _, c := range controllers {
		if h, _ := c["hostname"].(string); h == selectedHost {
			username, password, err := resolveControllerCreds(rc, c)
			if err != nil {
				return nil, "", fmt.Errorf("credentials for scheduler-selected %s: %w", selectedHost, err)
			}
			return rc.TowerClientPool.Get(selectedHost, username, password, rc.TowerTLSConfig), selectedHost, nil
		}
	}
	return nil, "", fmt.Errorf("scheduler selected %q but not found in ansible_controllers", selectedHost)
}

func resolveSchedulerAPIKey(rc *runner.RunContext) (string, error) {
	// The scheduler API key arrives pre-resolved via varSecrets:
	// agnosticv __meta__.secrets defines "cluster-scheduler-api-key-governor"
	// → Anarchy operator reads the K8s secret and merges it into
	//   governor.spec.vars.controller_scheduler_credentials
	// → runner receives it already resolved in the payload.
	creds, _ := rc.Payload.Governor.Spec.Vars.Get("controller_scheduler_credentials").(map[string]interface{})
	if creds == nil {
		return "", fmt.Errorf("controller_scheduler_credentials not found in governor vars")
	}
	key, _ := creds["cluster_scheduler_api_key_governor"].(string)
	if key == "" {
		return "", fmt.Errorf("cluster_scheduler_api_key_governor is empty in controller_scheduler_credentials")
	}
	return key, nil
}
````

- [ ] **Step 4: Add integration test for scheduler path**

Add to `internal/handler/tower_launch_test.go`:

```go
func TestGetTowerClientForAction_SchedulerFallback(t *testing.T) {
	// When scheduler is configured but returns an error,
	// getTowerClientForAction should fall back to local selection.
	towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/tokens/" {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "test-token",
				"id":    float64(1),
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer towerServer.Close()

	towerHost := strings.TrimPrefix(towerServer.URL, "https://")
	towerHost = strings.TrimPrefix(towerHost, "http://")

	rc := &runner.RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{
						Meta: &types.Meta{
							AnsibleControllers: []map[string]interface{}{
								{
									"hostname": towerHost,
									"user":     "admin",
									"password": "pass",
								},
							},
							ControllerScheduler: &types.ControllerSchedulerMeta{
								URL: "http://nonexistent-scheduler:8080",
							},
						},
					},
				},
			},
			Action: &types.Action{
				Spec: types.ActionSpec{Action: "provision"},
			},
		},
		TowerTLSConfig: &tls.Config{InsecureSkipVerify: true},
	}

	// Should fall back to local selection since scheduler URL is unreachable.
	tc, hostname, err := getTowerClientForAction(rc)
	if err != nil {
		t.Fatalf("getTowerClientForAction: %v", err)
	}
	if tc == nil {
		t.Fatal("expected non-nil tower client")
	}
	if hostname != towerHost {
		t.Errorf("hostname = %q, want %q", hostname, towerHost)
	}
}
```

- [ ] **Step 5: Run full test suite**

Run: `cd babylon-runner && go test ./... -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd babylon-runner
git add internal/clients/scheduler.go internal/clients/scheduler_test.go internal/handler/tower_launch.go internal/handler/tower_launch_test.go
git commit -m "feat(babylon-runner): add controller scheduler integration

Add SchedulerClient that calls POST /api/v1/evaluate/controllers for
intelligent, label-aware controller selection. Integrates in
getTowerClientForAction with graceful fallback to local selection
on scheduler failure. API key resolved from governor vars
(operator-injected via varSecrets)."
```

---

### Post-Implementation

- [ ] **Update DEPLOY.md** — add `METRICS_PORT` env var, document /metrics, /healthz, /readyz endpoints, and Kubernetes probe configuration example.

- [ ] **Run final full test suite**: `cd babylon-runner && go test ./... -count=1 -race`

- [ ] **Update spec status**: Change Phase 3 from "In Progress" to "Completed" in `docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md`.

- [ ] **Final commit** for DEPLOY.md, spec, and plan updates.

```bash
git add docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md docs/superpowers/plans/2026-06-19-babylon-runner-phase3.md babylon-runner/DEPLOY.md
git commit -m "docs(babylon-runner): update DEPLOY.md, mark Phase 3 completed in spec

Add METRICS_PORT env var documentation, Kubernetes probe configuration
examples, and /metrics /healthz /readyz endpoint docs. Update spec
status to Completed. Include implementation plan."
```
