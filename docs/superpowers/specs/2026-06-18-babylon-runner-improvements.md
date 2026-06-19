# Babylon Runner: Structural Improvements and Corrections

## Summary

This spec addresses structural, performance, correctness, and maintainability issues identified in the `babylon-runner` Go binary (PoC by Guillaume). The state machine, handler logic, and toggle mechanism (`spec.runner: babylon-go`) remain unchanged. The focus is on code organization, production readiness, and long-term sustainability.

## Implementation Phases

| Phase       | Theme       | Changes                            | Status      |
| ----------- | ----------- | ---------------------------------- | ----------- |
| **Phase 1** | Foundation  | #1, #2, #3, #14, #15               | In Progress |
| **Phase 2** | Correctness | #4, #5, #7, #10, #13, GAP-1, GAP-3 | Not Started |
| **Phase 3** | Production  | #6, #8, #9, #12, #16, #17          | Not Started |

**Phase 1 — Foundation:** Restructure from flat `package main` to standard Go layout, add typed payloads, and establish shared HTTP infrastructure. All subsequent changes target the new layout.

**Phase 2 — Correctness:** Fix bugs and add safety mechanisms blocking for production deployment.

**Phase 3 — Production:** Observability, performance optimization, and feature parity for production readiness.

## Motivation

The PoC is functionally correct — it implements the babylon governor state machine and passes its test suite. However, deploying it alongside 203 runner pods processing ~3,250 runs/day requires:

- Standard Go project structure for maintainability
- Typed payloads instead of `map[string]interface{}` for compile-time safety
- Kubernetes-native client usage instead of raw HTTP
- Observability (Prometheus metrics)
- Graceful shutdown via `context.Context` propagation
- Data-driven configuration instead of hardcoded variable mappings

## Changes

### 1. Project Structure (Phase 1)

Restructure from flat `package main` (21 files) to standard Go layout with internal packages.

**Current:**

```text
babylon-runner/
├── main.go
├── config.go
├── types.go
├── runner.go
├── anarchy.go
├── tower.go
├── sandboxapi.go
├── k8s.go
├── jinja2.go
├── handler_provision.go
├── handler_destroy.go
├── handler_start.go
├── handler_stop.go
├── handler_status.go
├── handler_update.go
├── handler_event_create.go
├── handler_event_update.go
├── handler_event_delete.go
├── handler_check_deployer.go
├── handler_sandbox.go
├── handler_tower_launch.go
└── *_test.go (14 files)
```

**Proposed:**

```text
babylon-runner/
├── cmd/
│   └── babylon-runner/
│       └── main.go              # Entry point, handler registration, signal handling
├── internal/
│   ├── runner/
│   │   ├── runner.go            # Polling loop, dispatch, RunContext, config
│   │   └── config.go            # Config struct, env var parsing
│   ├── handler/
│   │   ├── provision.go         # provision + check-provision-queue
│   │   ├── destroy.go
│   │   ├── start.go
│   │   ├── stop.go
│   │   ├── status.go
│   │   ├── update.go
│   │   ├── event_create.go
│   │   ├── event_update.go
│   │   ├── event_delete.go
│   │   ├── check_deployer.go    # Tower job polling + failure/success routing
│   │   ├── sandbox.go           # Sandbox get/book/cleanup/start/stop
│   │   └── tower_launch.go      # Tower job assembly + launch
│   ├── clients/
│   │   ├── anarchy.go           # Anarchy API client (SubjectUpdate, ScheduleAction, etc.)
│   │   ├── tower.go             # Tower/AAP2 client (LaunchJob, GetJobStatus, OAuth)
│   │   ├── sandbox.go           # Sandbox API client (Login, BookPlacement, etc.)
│   │   └── scheduler.go         # Controller scheduler client (Evaluate)
│   ├── httputil/
│   │   ├── transport.go         # Shared http.Transport factory (TLS config, connection pooling)
│   │   ├── retry.go             # retryWithContext, pollWithContext helpers
│   │   ├── json.go              # DoJSON request/response helper
│   │   ├── instrument.go        # Prometheus-instrumented HTTP round-tripper
│   │   └── token_cache.go       # Thread-safe token cache with TTL and refresh callback
│   ├── template/
│   │   └── jinja2.go            # Existing Jinja2 resolver (migrated as-is)
│   └── types/
│       ├── payload.go           # RunPayload, Handler, typed Governor/Subject/Action structs
│       ├── result.go            # RunResult, directives (Finish, Continue, Delete)
│       ├── patch.go             # SubjectPatch, ScheduleActionRequest
│       └── helpers.go           # nowUTC, deepMergeMap, extractStringSlice
├── Makefile
├── go.mod
├── go.sum
├── Dockerfile
└── README.md
```

**Rationale:** Enables isolated testing per package, encapsulation of client internals, and potential reuse of clients in diagnostic tools.

**Testing requirement:** Every new package under `internal/` must include `*_test.go` files with unit tests. This is not optional — no package is considered complete without tests. At minimum, each package must test:

- **`runner/`** — polling loop, dispatch, config parsing (env var defaults, required var validation)
- **`handler/`** — correct API calls, state transitions, and retry scheduling per handler (mock external clients)
- **`clients/`** — one `*_test.go` per client file: anarchy (request construction, response parsing, retry behavior), tower (controller selection, job launch, status polling, OAuth lifecycle), sandbox (login, booking, placement lifecycle, token caching), scheduler (evaluate request/response, fallback on failure, timeout handling)
- **`httputil/`** — retry with context cancellation, poll timeout, JSON marshal/unmarshal, TLS config, token cache (TTL expiry, refresh, thread-safety, cleanup)
- **`template/`** — existing Jinja2 constructs (variable substitution, dotted paths, default filter)
- **`types/`** — deep merge, nested accessors, JSON serialization round-trip

The PoC already has 14 test files in the flat structure. These must be migrated to the corresponding packages and expanded to cover new functionality introduced by each change.

### 2. Typed Payloads (Phase 1)

Replace `map[string]interface{}` in `RunPayload` with typed structs for fields with known schema. Retain `map[string]interface{}` only for genuinely dynamic fields (`job_vars`, `extra_vars`).

**Current:**

```go
type RunPayload struct {
    Handler  Handler                `json:"handler"`
    Governor map[string]interface{} `json:"governor"`
    Subject  map[string]interface{} `json:"subject"`
    Action   map[string]interface{} `json:"action,omitempty"`
    Run      map[string]interface{} `json:"run"`
}
```

**Proposed:**

```go
type RunPayload struct {
    Handler  Handler  `json:"handler"`
    Governor Governor `json:"governor"`
    Subject  Subject  `json:"subject"`
    Action   *Action  `json:"action,omitempty"`
    Run      Run      `json:"run"`
}

type Governor struct {
    Metadata ObjectMeta    `json:"metadata"`
    Spec     GovernorSpec  `json:"spec"`
    Status   map[string]interface{} `json:"status,omitempty"`
}

type GovernorSpec struct {
    Vars    GovernorVars               `json:"vars"`
    Actions map[string]ActionConfig    `json:"actions"`
    Runner  string                     `json:"runner,omitempty"`
}

type GovernorVars struct {
    JobVars  map[string]interface{} `json:"job_vars"`
    Meta     *Meta                  `json:"__meta__,omitempty"`
    // Remaining fields are dynamic
    Extra    map[string]interface{} `json:"-"`
}

type Meta struct {
    Deployer            *DeployerMeta            `json:"deployer,omitempty"`
    Sandboxes           []interface{}             `json:"sandboxes,omitempty"`
    AWSsandboxed        bool                      `json:"aws_sandboxed,omitempty"`
    SandboxAPI          *SandboxAPIMeta           `json:"sandbox_api,omitempty"`
    AnsibleControllers  []ControllerConfig        `json:"ansible_controllers,omitempty"`
    ControllerScheduler *ControllerSchedulerMeta  `json:"controller_scheduler,omitempty"`
}

type ControllerSchedulerMeta struct {
    URL           string            `json:"url,omitempty"`
    RequireLabels map[string]string `json:"require_labels,omitempty"`
    PreferLabels  map[string]string `json:"prefer_labels,omitempty"`
    InstanceGroup string            `json:"instance_group,omitempty"` // override; default = action name
}

type Subject struct {
    Metadata ObjectMeta    `json:"metadata"`
    Spec     SubjectSpec   `json:"spec"`
    Status   SubjectStatus `json:"status"`
}

type SubjectSpec struct {
    Vars SubjectVars `json:"vars"`
}

type SubjectVars struct {
    CurrentState  string                 `json:"current_state"`
    DesiredState  string                 `json:"desired_state"`
    Healthy       *bool                  `json:"healthy,omitempty"`
    JobVars       map[string]interface{} `json:"job_vars"`
    CheckStatusState string             `json:"check_status_state,omitempty"`
}

type SubjectStatus struct {
    Actions    map[string]ActionStatus  `json:"actions,omitempty"`
    TowerJobs  map[string]TowerJobInfo  `json:"towerJobs,omitempty"`
    PreviousState *PreviousState        `json:"previous_state,omitempty"`
}
```

**Rationale:** A typo in `getNestedString(m, "spec", "vasr", "current_state")` compiles and silently returns `""`. A typo in `subject.Spec.Vars.CurrentState` is a compile error. The `RunContext` convenience methods (`CurrentState()`, `DesiredState()`, etc.) become trivial field accessors instead of map traversals.

### 3. Kubernetes Client — client-go (Phase 1)

Replace raw HTTP calls in `k8s.go` with `client-go`.

**Current problems:**

- Reimplements `rest.InClusterConfig()` manually (reading token file and env vars)
- `InsecureSkipVerify: true` — ignores cluster CA at `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`
- Falls back to `exec.Command("oc", "whoami")` for local dev
- Creates a new `http.Client` per call

**Proposed:** Initialize `kubernetes.Interface` in `main.go` and pass it through the `Runner` struct to handlers that need it. No dedicated `internal/k8s/` package — the runner only reads secrets, which is a single `clientset.CoreV1().Secrets(ns).List()` call. Wrapping that in a package would be a layer with no value.

```go
// cmd/babylon-runner/main.go
func main() {
    config, err := rest.InClusterConfig()
    if err != nil {
        config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
        if err != nil {
            log.Fatal(err)
        }
    }
    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        log.Fatal(err)
    }

    runner := runner.New(cfg, clientset, ...)
}
```

Handlers that need secret data receive `kubernetes.Interface` via `RunContext` and call `client-go` directly:

```go
// inside resolveControllerCreds
secrets, err := rc.Clientset.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{
    LabelSelector: label,
})
```

For tests, inject `fake.NewSimpleClientset()` with pre-populated secrets — no mocks or custom interfaces needed.

**Rationale:** Proper TLS with cluster CA, connection pooling, automatic token refresh, standard local dev experience via kubeconfig. Keeping the `kubernetes.Interface` as a passed dependency (instead of a wrapper package) is simpler and equally testable via `client-go/kubernetes/fake`.

**New dependency:** `k8s.io/client-go`

### 4. Context Propagation (Phase 2)

Thread `context.Context` through all HTTP operations and retry loops for graceful shutdown. All clients use `httputil.RetryWithContext` and `httputil.DoJSON` (see change #14) instead of inline `time.Sleep` loops.

**Affected components:**

| Component                      | Current                  | Proposed                                                   |
| ------------------------------ | ------------------------ | ---------------------------------------------------------- |
| `TowerClient` methods          | `http.NewRequest()`      | `httputil.DoJSON(ctx, ...)` with context                   |
| `SandboxAPIClient` methods     | `http.NewRequest()`      | `httputil.DoJSON(ctx, ...)` with context                   |
| `SandboxAPIClient.Login` retry | `time.Sleep(retryDelay)` | `httputil.RetryWithContext(ctx, delays, fn)`               |
| `pollSandboxRequest`           | `time.Sleep(5s)` × 120   | `httputil.PollWithContext(ctx, interval, maxAttempts, fn)` |
| `doPlacementAction` retry      | `time.Sleep(delay)`      | `httputil.RetryWithContext(ctx, delays, fn)`               |
| `AnarchyClient.doWithRetry`    | `time.Sleep(delay)`      | `httputil.RetryWithContext(ctx, delays, fn)`               |

**Rationale:** SIGTERM during a 200-second sandbox login retry (40 × 5s) currently blocks shutdown. With context, the runner shuts down within the current retry delay interval.

### 5. TLS Configuration (Phase 2)

Make TLS verification configurable instead of hardcoded `InsecureSkipVerify: true`.

**New env vars:**

| Variable           | Default | Description                        |
| ------------------ | ------- | ---------------------------------- |
| `TOWER_TLS_VERIFY` | `true`  | Verify Tower TLS certificates      |
| `TOWER_CA_CERT`    | (none)  | Path to custom CA bundle for Tower |

**K8s TLS:** Handled automatically by `client-go` (uses cluster CA from serviceaccount).

### 6. HTTP Client Reuse and Token Caching (Phase 3)

**Problem 1:** `getTowerClientForAction` creates a new `TowerClient` (with new `http.Transport`) on every call. Each `checkDeployerJob` (every 5 minutes) creates a new client, makes HTTP calls, and discards the transport.

**Problem 2:** Each `TowerClient` operation creates and deletes an OAuth token. Sequential operations on the same controller repeat the cycle.

**Problem 3 (GAP-2):** Each sandbox operation (`sandboxGet`, `sandboxBook`, `sandboxStart`, `sandboxStop`, `sandboxCleanup`) independently calls `sandboxLogin`, creating a new access token every time. A single provision handler may call `sandboxGet` then `sandboxBook`, performing two login round-trips.

**Proposed — shared `httputil.TokenCache`:**

Tower and Sandbox follow the same token lifecycle: obtain a token, reuse it until it expires, refresh when needed. Instead of implementing token management independently in each client, extract a generic `TokenCache` into `httputil/token_cache.go`:

```go
// httputil/token_cache.go
type TokenCache struct {
    mu      sync.RWMutex
    token   string
    expiry  time.Time
    refresh func(ctx context.Context) (token string, ttl time.Duration, err error)
    cleanup func(ctx context.Context, token string) error  // optional
}

type TokenCacheOption func(*TokenCache)

func WithCleanup(fn func(ctx context.Context, token string) error) TokenCacheOption {
    return func(c *TokenCache) { c.cleanup = fn }
}

func NewTokenCache(refresh func(context.Context) (string, time.Duration, error), opts ...TokenCacheOption) *TokenCache

// Get returns a valid token, refreshing if expired or missing.
// Thread-safe with double-check locking.
func (c *TokenCache) Get(ctx context.Context) (string, error) {
    c.mu.RLock()
    if c.token != "" && time.Now().Before(c.expiry) {
        defer c.mu.RUnlock()
        return c.token, nil
    }
    c.mu.RUnlock()

    c.mu.Lock()
    defer c.mu.Unlock()
    if c.token != "" && time.Now().Before(c.expiry) {
        return c.token, nil
    }
    token, ttl, err := c.refresh(ctx)
    if err != nil {
        return "", err
    }
    c.token = token
    c.expiry = time.Now().Add(ttl)
    return token, nil
}

// Close cleans up the current token (e.g., Tower DELETE /api/v2/tokens/{id}).
// No-op if no cleanup function was provided (e.g., Sandbox tokens expire on their own).
func (c *TokenCache) Close(ctx context.Context) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.cleanup != nil && c.token != "" {
        err := c.cleanup(ctx, c.token)
        c.token = ""
        return err
    }
    c.token = ""
    return nil
}
```

**How each client uses it:**

```go
// clients/tower.go — OAuth token with cleanup
tokenCache := httputil.NewTokenCache(
    func(ctx context.Context) (string, time.Duration, error) {
        // POST /api/v2/tokens/ → return token, 30*time.Minute, nil
    },
    httputil.WithCleanup(func(ctx context.Context, token string) error {
        // DELETE /api/v2/tokens/{id}/
    }),
)
defer tokenCache.Close(ctx)

// clients/sandbox.go — login token, no cleanup needed
tokenCache := httputil.NewTokenCache(
    func(ctx context.Context) (string, time.Duration, error) {
        // POST /token with credentials → return accessToken, 1*time.Hour, nil
    },
)
```

**Tower client pool** (unchanged — Tower-specific because it has multiple controllers):

```go
type TowerClientPool struct {
    mu      sync.RWMutex
    clients map[string]*TowerClient  // keyed by hostname
}

func (p *TowerClientPool) Get(hostname, username, password string) *TowerClient {
    p.mu.RLock()
    if c, ok := p.clients[hostname]; ok {
        p.mu.RUnlock()
        return c
    }
    p.mu.RUnlock()
    p.mu.Lock()
    defer p.mu.Unlock()
    c := NewTowerClient(hostname, username, password)
    p.clients[hostname] = c
    return c
}
```

Each `TowerClient` in the pool holds its own `httputil.TokenCache` instance. The pool reuses clients (and their cached tokens) across calls to the same controller.

**What stays in each client:** Auth-specific logic (how to obtain/delete tokens) lives in the refresh/cleanup callbacks. The `TokenCache` only manages the lifecycle (thread-safety, TTL, refresh-on-expiry).

### 7. Configuration for Hardcoded Constants (Phase 2)

Move hardcoded operational constants to the Config struct with env vars and sensible defaults. No `__meta__` reads — these are runner-level settings, not governor-level.

**New env vars:**

| Variable                 | Default                                                         | Description                                                                                                        |
| ------------------------ | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `SANDBOX_API_URL`        | `http://sandbox-api.babylon-sandbox-api.svc.cluster.local:8080` | Sandbox API base URL. Runner and Sandbox API run in the same cluster, accessed via K8s service layer (plain HTTP). |
| `ACTION_RETRY_INTERVALS` | `1m,5m,10m,30m,1h,2h,4h,8h,16h,1d`                              | Comma-separated list of retry intervals for failed actions.                                                        |

These are added to the existing Config struct in `internal/runner/config.go` alongside `ANARCHY_URL`, `POLLING_INTERVAL`, etc.

**What stays hardcoded (known limitation):**

- `deployer_entry_points` — already configurable via `__meta__.deployer.actions.{action}.entry_point`. No change needed.
- Initial variables in `handleEventCreate` (`cloud_provider`, `platform`, `uuid`, `guid`) remain hardcoded. In the Ansible role, these come from `defaults/main.yaml` via Jinja2 resolution (e.g., `{{ __meta__.cloud_provider | default('none') }}`). The Go runner cannot resolve these templates without a full Jinja2 engine. Making them data-driven is deferred to a future spec. Until then, adding a new initial variable requires a code change.

### 8. Prometheus Metrics (Phase 3)

Add metrics matching the existing Ansible runner's observability level.

**Proposed metrics:**

```go
var (
    runDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "babylon_runner_run_duration_seconds",
        Help:    "Duration of run execution by handler type and action",
        Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300},
    }, []string{"handler_type", "action"})

    runTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "babylon_runner_runs_total",
        Help: "Total runs processed by status",
    }, []string{"handler_type", "action", "status"})

    pollDuration = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "babylon_runner_poll_duration_seconds",
        Help:    "Duration of GET /run poll requests",
        Buckets: []float64{0.01, 0.1, 1, 5, 10, 30, 35},
    })

    towerJobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "babylon_runner_tower_job_duration_seconds",
        Help:    "Duration of Tower API operations",
        Buckets: []float64{0.1, 0.5, 1, 5, 10, 30},
    }, []string{"operation"})

    sandboxAPIDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "babylon_runner_sandbox_api_duration_seconds",
        Help:    "Duration of Sandbox API operations",
        Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60},
    }, []string{"operation"})

    activeRun = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "babylon_runner_active_run",
        Help: "1 if currently processing a run, 0 if idle",
    })
)
```

**Metrics endpoint:** HTTP server on configurable port (env `METRICS_PORT`, default `9093`) serving `/metrics`.

**New dependency:** `github.com/prometheus/client_golang`

### 9. Health Endpoint (Phase 3)

Expose `/healthz` and `/readyz` on the metrics HTTP server for Kubernetes probes.

- `/healthz` — returns 200 if the HTTP server can respond. No custom deadlock detection needed — if the process hangs, the probe times out and Kubernetes restarts the pod.
- `/readyz` — returns 200 if the runner has successfully connected to the Anarchy API at least once

### 10. Deep Merge (Phase 2)

Replace shallow `mergeMap` with recursive deep merge.

**Current** (`types.go:195`):

```go
func mergeMap(dst, src map[string]interface{}) {
    for k, v := range src {
        dst[k] = v
    }
}
```

**Proposed:**

```go
func deepMergeMap(dst, src map[string]interface{}) {
    for k, v := range src {
        if srcMap, ok := v.(map[string]interface{}); ok {
            if dstMap, ok := dst[k].(map[string]interface{}); ok {
                deepMergeMap(dstMap, srcMap)
                continue
            }
        }
        dst[k] = v
    }
}
```

**Rationale:** Ansible `combine(recursive=True)` does recursive merge. `buildJobExtraVars` merges `governor_job_vars` + `subject_job_vars` which both contain `__meta__` sub-trees. Shallow merge overwrites entire nested maps instead of merging their keys.

### 12. Polling Loop Optimization (Phase 3)

**Current:** `Runner.Run()` uses a `time.Ticker(5s)` but `client.Do()` blocks for the server's 30s long-poll timeout anyway, making the ticker redundant.

**Proposed:** Remove the ticker. Loop directly with `getRun()`, sleeping only on connection errors:

```go
func (r *Runner) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        default:
        }
        if err := r.pollOnce(ctx); err != nil {
            slog.Error("poll error", "error", err)
            select {
            case <-ctx.Done():
                return nil
            case <-time.After(r.config.PollingInterval):
            }
        }
    }
}
```

**Rationale:** Matches the Python runner behavior. The server-side 30s hold IS the idle sleep. On timeout (no run available), the runner immediately re-polls. On connection error, it waits `PollingInterval` before retrying.

### 13. Missing `guid` in Event Create (Phase 2)

**Current:** `handleEventCreate` sets `cloud_provider`, `platform`, `uuid` but not `guid`. The Ansible role sets both `uuid` and `guid`.

**Fix:** Add `guid` explicitly alongside the existing initial variables:

```go
jobVarsPatch["guid"] = guidFromSubjectName(rc)
```

Where `guid` is typically derived from the subject name or generated.

### 14. Shared HTTP Infrastructure — `internal/httputil` (Phase 1)

**Problem:** Three API clients (Anarchy, Tower, Sandbox) each implement their own HTTP transport creation, JSON request/response marshaling, retry loops with `time.Sleep`, and will each need metrics instrumentation. This duplicates ~100 lines of infrastructure code across components.

**Decision:** Share infrastructure, not abstraction. Each client has fundamentally different auth strategies (static bearer token, OAuth lifecycle, login-flow token) and retry semantics (fixed delays, polling, backoff). A generic `APIClient` would need so many parameters and interfaces that it becomes more complex than the three clients combined.

Instead, extract the common plumbing into `internal/httputil/` — each client imports the helpers but owns its auth and business logic.

**Package contents:**

```go
package httputil

// --- transport.go ---

// NewTransport creates a shared http.Transport with connection pooling.
// tlsConfig is optional — nil uses Go defaults (system CA, verify enabled).
func NewTransport(tlsConfig *tls.Config) *http.Transport {
    return &http.Transport{
        TLSClientConfig:     tlsConfig,
        MaxIdleConns:        100,
        MaxIdleConnsPerHost: 10,
        IdleConnTimeout:     90 * time.Second,
    }
}

// NewTLSConfig builds a tls.Config from env vars.
// Returns nil (use system defaults) when verify=true and no custom CA.
func NewTLSConfig(verify bool, caPath string) (*tls.Config, error) { ... }

// --- retry.go ---

// RetryWithContext executes fn with retries, respecting context cancellation.
// Waits delays[i] between attempt i and i+1. Returns last error if all fail.
func RetryWithContext(ctx context.Context, delays []time.Duration, fn func() error) error {
    var lastErr error
    for i := 0; i <= len(delays); i++ {
        if i > 0 {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(delays[i-1]):
            }
        }
        if err := fn(); err != nil {
            lastErr = err
            continue
        }
        return nil
    }
    return lastErr
}

// PollWithContext polls fn at interval until it returns nil (success),
// a permanent error, or context cancels. Used for sandbox request status polling.
func PollWithContext(ctx context.Context, interval time.Duration, maxAttempts int,
    fn func() (done bool, err error)) error { ... }

// --- json.go ---

// DoJSON executes an HTTP request with JSON body/response marshaling.
// body can be nil (no request body). result can be nil (response discarded).
// Returns the HTTP status code and any error.
func DoJSON(ctx context.Context, client *http.Client, method, url string,
    headers map[string]string, body, result interface{}) (int, error) { ... }

// --- instrument.go ---

// InstrumentedTransport wraps an http.RoundTripper with Prometheus histogram
// instrumentation. Each request records duration with the given labels.
func InstrumentedTransport(next http.RoundTripper,
    histogram *prometheus.HistogramVec, labels ...string) http.RoundTripper { ... }
```

**How each client uses it:**

| Client                 | Transport                        | Retry                                                              | JSON     | Metrics                 |
| ---------------------- | -------------------------------- | ------------------------------------------------------------------ | -------- | ----------------------- |
| `clients/anarchy.go`   | `NewTransport(nil)` (plain HTTP) | `RetryWithContext` with `[5s, 10s, 20s]`                           | `DoJSON` | `InstrumentedTransport` |
| `clients/tower.go`     | `NewTransport(tlsConfig)` (CA)   | `RetryWithContext` where needed                                    | `DoJSON` | `InstrumentedTransport` |
| `clients/sandbox.go`   | `NewTransport(nil)` (plain HTTP) | `RetryWithContext` for login/actions, `PollWithContext` for status | `DoJSON` | `InstrumentedTransport` |
| `clients/scheduler.go` | `NewTransport(tlsConfig)` (CA)   | `RetryWithContext` with `[3s, 3s]` (2 retries)                     | `DoJSON` | `InstrumentedTransport` |

All clients live in the `clients` package and import helpers from `httputil`.

**What stays in each client:**

- Auth header construction (each is different)
- Error interpretation (status code → business error mapping)
- Token lifecycle (Tower OAuth create/delete, Sandbox login flow)
- Request body assembly (domain-specific structs)

**Rationale:** Eliminates ~100 lines of duplicated transport/retry/marshal code. Fixes context propagation and metrics instrumentation in one place instead of three. Each client remains a focused ~60-80 line file with only domain logic. Adding a 4th API client in the future gets retry, metrics, and context for free.

### 15. Makefile (Phase 1)

**Current state:** The project has `dev-run.sh` (96-line shell script for local development) but no Makefile. `dev-run.sh` handles port-forwarding, dev pod creation, building (`go build -o ${TMPDIR}/babylon-runner .`), and running — mixing infrastructure setup with the build step. The Dockerfile duplicates the build command (`CGO_ENABLED=0 GOOS=linux go build -o babylon-runner .`). There is no standard entry point for common developer tasks.

**Proposed:** Add a Makefile with standard Go project targets:

```makefile
BINARY     := babylon-runner
BUILD_DIR  := bin
GOFLAGS    := -trimpath
LDFLAGS    := -s -w

.PHONY: build run test lint clean docker-build fmt vet

build:
    CGO_ENABLED=0 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY) ./cmd/babylon-runner/

run: build
    $(BUILD_DIR)/$(BINARY)

test:
    go test ./...

lint:
    golangci-lint run ./...

fmt:
    gofmt -w .

vet:
    go vet ./...

clean:
    rm -rf $(BUILD_DIR)

docker-build:
    podman build -t $(BINARY):latest .
```

**Interaction with other changes:**

- **#1 Project structure:** The build path changes from `.` to `./cmd/babylon-runner/`. The Dockerfile's `COPY *.go ./` must be updated to `COPY . .` (or explicit directory copies) to capture the `internal/` tree.
- **`dev-run.sh`:** Keeps its infrastructure responsibilities (port-forward, dev pod lifecycle) but delegates the build step to `make build` instead of inline `go build`.

**Rationale:** Standard Go projects use Makefiles as the single entry point for build, test, and lint. This eliminates duplicated build commands between `dev-run.sh` and `Dockerfile`, and gives contributors a discoverable interface (`make help` or reading the Makefile) for all project operations.

### 16. Controller Scheduler Integration (Phase 3)

**Context:** The `babylon_anarchy_governor` Ansible role (branch `controller-scheduler`) adds an optional external service that replaces the local `selectController` logic (random/balance/first-available) with intelligent, label-aware controller selection via `POST /api/v1/evaluate/controllers`. The babylon-runner must support this same mechanism to maintain feature parity.

**Current Go implementation** (`tower.go:135`, `handler_tower_launch.go:77-81`):

```go
mode := "random"
if m, ok := meta["ansible_controller_select_mode"].(string); ok && m != "" {
    mode = m
}
controller := selectController(controllers, mode)
```

The runner picks a controller locally from the `ansible_controllers` list. There is no concept of calling an external scheduler.

**Ansible implementation** (`check-controller-scheduler.yaml`):

1. If `__meta__.controller_scheduler.url` is defined, calls `POST /api/v1/evaluate/controllers` with:
   - `candidates` — controller hostnames from `ansible_controllers`
   - `require_labels` — mandatory controller capabilities (from `__meta__.controller_scheduler.require_labels`)
   - `prefer_labels` — soft preferences (from `__meta__.controller_scheduler.prefer_labels`)
   - `instance_group` — derived from `anarchy_action_config_name` in the Ansible role (maps to the action name: provision/destroy/start/stop/status/update). The scheduler uses this to route jobs to the correct AAP instance group on the controller. In the Go runner, this comes from `rc.ActionName()`. Can be overridden per governor via `__meta__.controller_scheduler.instance_group` for governors that need a non-default mapping.
2. The scheduler returns `ranked[]` — controllers sorted by score
3. If the scheduler succeeds, uses `ranked[0].domain` as the selected controller and **skips** the local selection loop
4. If the scheduler fails (timeout 10s, 2 retries with 3s delay), falls back to local `selectController`
5. API key comes from `vaultSecrets` (`controller_scheduler_credentials.cluster_scheduler_api_key_governor`)

**Proposed Go implementation:**

```go
package scheduler

type Client struct {
    httpClient *http.Client
    baseURL    string
    apiKey     string
}

type EvaluateRequest struct {
    Candidates    []Candidate       `json:"candidates"`
    RequireLabels map[string]string `json:"require_labels,omitempty"`
    PreferLabels  map[string]string `json:"prefer_labels,omitempty"`
    InstanceGroup string            `json:"instance_group,omitempty"`
}

type Candidate struct {
    Domain string `json:"domain"`
}

type EvaluateResponse struct {
    Ranked []RankedController `json:"ranked"`
}

type RankedController struct {
    Domain string  `json:"domain"`
    Score  float64 `json:"score"`
}

func (c *Client) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
    // POST /api/v1/evaluate/controllers
    // Header: X-API-Key
    // Timeout: 10s (context deadline)
    // Retries: 2 with 3s delay via httputil.RetryWithContext
}
```

**Integration in `getTowerClientForAction`:**

```go
func getTowerClientForAction(rc *RunContext) (*TowerClient, string, error) {
    // ... extract controllers from __meta__ ...

    // Try controller-scheduler first (if configured)
    schedulerURL := getNestedString(meta, "controller_scheduler", "url")
    if schedulerURL != "" {
        selected, err := trySchedulerSelection(ctx, rc, schedulerURL, controllers, actionName)
        if err != nil {
            slog.Warn("controller-scheduler failed, falling back to local selection",
                "error", err)
        } else if selected != nil {
            return selected.client, selected.hostname, nil
        }
    }

    // Fallback: local selectController (random/balance/first-available)
    controller := selectController(controllers, mode)
    // ...
}
```

**Credentials:** The API key is stored in a Kubernetes secret resolved via `vaultSecrets`. With change #3 (client-go), the runner can read this secret directly. The key path is `controller_scheduler_credentials.cluster_scheduler_api_key_governor`.

**Graceful fallback:** Matching the Ansible behavior (`ignore_errors: true`), the Go implementation must never fail the run if the scheduler is unavailable. Log a warning and fall back to local selection.

**Location:** `internal/clients/scheduler.go` — lives alongside the other API clients in the `clients` package. Uses `httputil.DoJSON` and `httputil.RetryWithContext` from change #14.

### 17. Kubernetes Secret Informer Cache (Phase 3)

**Context:** The `resolveControllerCreds` function in `tower_launch.go` calls `Clientset.CoreV1().Secrets(ns).List()` with a label selector on every run that launches a Tower job. In production with ~3,250 runs/day across 203 runner pods, this generates thousands of API server round-trips per day for data that rarely changes.

**Current Go implementation** (`internal/handler/tower_launch.go`):

```go
secrets, err := rc.Clientset.CoreV1().Secrets(ns).List(context.TODO(), metav1.ListOptions{
    LabelSelector: fmt.Sprintf("babylon.gpte.redhat.com/ansible-control-plane=%s", hostname),
})
```

Each call hits the Kubernetes API server. Secrets for Tower controllers change infrequently (credential rotation), so caching with watch-based invalidation is the idiomatic Kubernetes approach.

**Proposed Go implementation:**

Use a `SharedInformer` from `client-go/informers` to maintain an in-memory cache of secrets with the relevant labels. The informer:

1. Performs a single LIST on startup to populate the cache
2. Maintains a WATCH connection to receive updates (create/update/delete) in real-time via resourceVersion tracking
3. Provides local cache lookups with zero API server round-trips
4. Handles reconnection and re-list automatically

```go
package secrets

import (
    "fmt"
    "sync"

    corev1 "k8s.io/api/core/v1"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
)

type SecretCache struct {
    informer cache.SharedIndexInformer
    stopCh   chan struct{}
}

func NewSecretCache(clientset kubernetes.Interface, namespace string) *SecretCache {
    factory := informers.NewSharedInformerFactoryWithOptions(
        clientset, 0,
        informers.WithNamespace(namespace),
        informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
            opts.LabelSelector = "babylon.gpte.redhat.com/ansible-control-plane"
        }),
    )
    informer := factory.Core().V1().Secrets().Informer()
    // ...
}

func (sc *SecretCache) GetByHostname(hostname string) (*corev1.Secret, error) {
    // Local cache lookup — no API server call
}
```

**Scope:** This applies to three secret categories:

1. **Tower controller credentials** — `babylon.gpte.redhat.com/ansible-control-plane={hostname}` label
2. **Controller scheduler API key** — `controller_scheduler_credentials` (used by change #16)
3. **Sandbox API credentials** — if sandbox API authentication moves to K8s secrets

**Location:** `internal/secrets/cache.go` — new package for secret cache management. Initialized in `cmd/babylon-runner/main.go`, passed to `RunContext` or handlers that need secret lookups.

**Dependencies:** Change #3 (client-go, already landed in Phase 1).

## New Dependencies

| Dependency                            | Purpose               | Justification                                 |
| ------------------------------------- | --------------------- | --------------------------------------------- |
| `k8s.io/client-go`                    | Kubernetes API access | Replaces raw HTTP with standard Go K8s client |
| `github.com/prometheus/client_golang` | Metrics               | Production observability                      |

## Migration Strategy

Changes are independent and can be implemented incrementally. See **Implementation Phases** at the top of this document for the phase table and status.

**Phase 1 — Foundation** (do first to avoid rework):

- #1 Project structure — restructure flat `package main` into `internal/` packages. All subsequent changes target the new layout.
- #14 Shared HTTP infrastructure (`internal/httputil`) — must land with #1, as clients depend on it
- #15 Makefile — lands with #1, as build path changes with project structure
- #2 Typed payloads
- #3 Kubernetes client (client-go) — initialized in main, passed via struct (no `internal/k8s/` package)

**Phase 2 — Correctness** (blocking for production):

- #10 Deep merge
- #13 Missing guid
- #4 Context propagation
- #5 TLS configuration
- #7 Configuration for hardcoded constants (SANDBOX_API_URL, ACTION_RETRY_INTERVALS)
- GAP-1 Panic recovery in dispatch loop
- GAP-3 Status handler finish when deployer disabled

**Phase 3 — Production** (observability, performance, feature parity):

- #8 Prometheus metrics
- #9 Health endpoint
- #6 HTTP client reuse and token caching (includes GAP-2: sandbox API token caching)
- #12 Polling loop optimization
- #16 Controller scheduler integration — depends on #3 (client-go for secret reading), #5 (TLS), #14 (httputil)
- #17 Kubernetes secret informer cache — depends on #3 (client-go)

Each phase can be validated independently with the existing toggle mechanism (`spec.runner: babylon-go`) on a live cluster.

## Out of Scope

- **Jinja2 template engine** — deferred to a future spec. The current `jinja2.go` (148 lines, 3 constructs) covers 100% of templates the runner resolves today. A robust Jinja2-compatible engine (library selection, preprocessor, filters, caching) is a separate effort with its own scope and evaluation criteria. Until then, adding new initial variables in `handleEventCreate` requires a code change.

## Gap Analysis

Thorough review of all 21 Go source files against the spec changes revealed the following issues not covered by existing changes.

### GAP-1: No Panic Recovery (Phase 2)

**File:** `runner.go:307`

The original design doc specifies "Panic → `defer recover()` in handler, log stack trace, post failure" but this is not implemented. If any handler panics (nil pointer dereference, index out of range, type assertion failure), the entire runner pod crashes.

```go
// Current: no recovery
if err := dispatch(rc, r.handlers); err != nil {
    // only handles returned errors, not panics
}
```

**Fix:** Add `defer func() { if r := recover(); r != nil { ... } }()` in `pollOnce` wrapping the `dispatch` + `postResult` calls. On panic, log the stack trace and post a failed result back to the operator so the run is properly marked as failed.

**Severity:** High — in production with 203 pods, a panic loses the runner pod. The operator creates a new pod but the current run's result is lost (the operator eventually times it out and marks it as "lost", delaying the next retry by minutes).

### GAP-2: Sandbox API Token Not Cached (absorbed into #6)

**File:** `handler_sandbox.go:64-75`

Each sandbox operation (`sandboxGet`, `sandboxBook`, `sandboxStart`, `sandboxStop`, `sandboxCleanup`) independently calls `sandboxLogin`, creating a new access token every time. A single provision handler may call `sandboxGet` then `sandboxBook`, performing two login round-trips to the same API.

**Fix:** Resolved by change #6 — the Sandbox client uses `httputil.TokenCache` with the login flow as its refresh callback. The token is cached and reused automatically across operations within the same client instance.

### GAP-3: Status Handler Orphans Action When Deployer Disabled (Phase 2)

**File:** `handler_status.go:50-63`

When `DeployerDisabled("status")` is true AND `check_status_state` is "pending", `runStatus` sets a `startTimestamp` then returns nil without calling `FinishAction` or `ContinueAction`. The run is posted back as "successful" (the default) with no directives, but the action and `check_status_state` are left in an inconsistent state ("pending" was never resolved).

```go
func runStatus(rc *RunContext) error {
    // Set startTimestamp...
    if !rc.DeployerDisabled("status") {
        // launch tower job, continue
        return nil
    }
    return nil  // <--- deployer disabled: no FinishAction, no state update
}
```

**Fix:** When deployer is disabled for status, immediately mark the status check as successful and finish the action:

```go
if rc.DeployerDisabled("status") {
    rc.SubjectUpdate(SubjectPatch{
        Patch: PatchBody{
            Spec: &PatchSpec{
                Vars: map[string]interface{}{
                    "check_status_state": "successful",
                },
            },
            SkipUpdateProcessing: true,
        },
    })
    rc.FinishAction("successful")
    return nil
}
```

**Severity:** Low (latent) — only triggers if a governor disables the deployer for status AND defines a status action. Unlikely in current configurations but a correctness issue.

### GAP-4: Update Retry Does Not Increment Retry Count (note)

**File:** `handler_check_deployer.go:446-452`

`handleUpdateFailure` uses `actionRetryInterval(rc.ActionRetryCount())` to get the retry interval but uses `ContinueAction(interval)` instead of `ContinueActionWithVars(interval, {"action_retry_count": count + 1})`. This means the retry count never increments and every retry uses the first interval (1m).

Compare with `continueWithRetry` (used by provision/destroy/start/stop failures) which correctly increments the count.

The code comment says "matching Ansible behavior" — this needs verification against the Ansible governor's update retry logic. If Ansible does use fixed 1m retries for update, this is correct. If not, it's a bug.

**Action:** Verify against Ansible governor. If the behavior differs, add to Phase 1. If it matches, add a code comment explaining why update retries differ from other actions.

## What Does NOT Change

- State machine (provision-pending → provisioning → started ↔ stopped → destroying → destroyed)
- Handler dispatch logic (action type + name → handler function)
- Anarchy API protocol (GET /run, POST /run/{name}, PATCH /run/subject/{name}, POST /run/subject/{name}/actions)
- Toggle mechanism (`spec.runner: babylon-go` on AnarchyGovernor CR)
- Callback race workaround (`ContinueAction("0s")`) — this requires an operator-side fix, not a runner change
- Test fixtures and test structure (adapted to new package paths)
