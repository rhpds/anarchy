# Babylon Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Ansible-based AnarchyRun execution for the babylon governor with a Go binary that implements the same logic natively.

**Architecture:** Go binary `babylon-runner` polls `GET /run` from the anarchy operator API, dispatches to handler functions that mirror the Ansible task files 1:1, makes direct HTTP calls to the anarchy API (subject updates, action scheduling) and external services (Sandbox API, AAP2/Tower), then posts results back via `POST /run/{name}`. Toggled via `spec.runner: babylon-go` on the AnarchyGovernor CR.

**Tech Stack:** Go 1.22+, `net/http`, `encoding/json`, `log/slog` (structured logging)

**Reference docs (READ THESE FIRST):**
- Design spec: `docs/superpowers/specs/2026-05-15-babylon-runner-design.md`
- Runner protocol: `skills/anarchy-runner-protocol.md`
- Handler logic: `skills/babylon-governor-handlers.md`

---

## File Structure

```
babylon-runner/
├── main.go                        # Entry point, config from env, starts runner
├── main_test.go                   # Config parsing tests
├── config.go                      # Config struct + env parsing
├── config_test.go                 # Config tests
├── runner.go                      # Polling loop, dispatch, POST result
├── runner_test.go                 # Polling + dispatch tests
├── types.go                       # All shared types (RunPayload, Subject, Governor, etc.)
├── anarchy.go                     # Anarchy API client (PATCH subject, POST actions)
├── anarchy_test.go                # Anarchy client tests
├── sandboxapi.go                  # Sandbox API client (login, book, get, release, start, stop)
├── sandboxapi_test.go             # Sandbox API tests
├── tower.go                       # AAP2/Tower client (select controller, launch, status, cancel)
├── tower_test.go                  # Tower client tests
├── handler_event_create.go        # handle-event-create
├── handler_event_update.go        # handle-event-update
├── handler_event_delete.go        # handle-event-delete
├── handler_provision.go           # handle-action-provision + run-provision + provision-complete/error/failed
├── handler_destroy.go             # handle-action-destroy + run-destroy + destroy-complete
├── handler_start.go               # handle-action-start + run-start + start-complete
├── handler_stop.go                # handle-action-stop + run-stop + stop-complete
├── handler_status.go              # handle-action-status + run-status
├── handler_update.go              # handle-action-update + run-update
├── handler_check_deployer.go      # check-deployer-job (poll Tower job, route to complete/error/failed)
├── handlers_test.go               # Tests for all handlers
├── go.mod
├── go.sum
└── Dockerfile
```

All files live in `package main` — no internal packages. This is a single binary with ~20 files, each under 300 lines. No premature abstractions.

---

### Task 1: Project Scaffold, Config, and Types

**Files:**
- Create: `babylon-runner/go.mod`
- Create: `babylon-runner/main.go`
- Create: `babylon-runner/config.go`
- Create: `babylon-runner/config_test.go`
- Create: `babylon-runner/types.go`

- [ ] **Step 1: Initialize Go module**

```bash
cd anarchy && mkdir -p babylon-runner && cd babylon-runner
go mod init github.com/rhpds/anarchy/babylon-runner
```

- [ ] **Step 2: Write config test**

```go
// config_test.go
package main

import (
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("ANARCHY_URL", "http://anarchy-api:5000")
	t.Setenv("RUNNER_NAME", "babylon-go")
	t.Setenv("RUNNER_TOKEN", "secret123")
	t.Setenv("HOSTNAME", "runner-pod-abc")

	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AnarchyURL != "http://anarchy-api:5000" {
		t.Errorf("AnarchyURL = %q, want %q", cfg.AnarchyURL, "http://anarchy-api:5000")
	}
	if cfg.RunnerName != "babylon-go" {
		t.Errorf("RunnerName = %q, want %q", cfg.RunnerName, "babylon-go")
	}
	if cfg.PodName != "runner-pod-abc" {
		t.Errorf("PodName = %q, want %q", cfg.PodName, "runner-pod-abc")
	}
	if cfg.PollingInterval != 5 {
		t.Errorf("PollingInterval = %d, want 5", cfg.PollingInterval)
	}
	if cfg.RequestTimeout != 35 {
		t.Errorf("RequestTimeout = %d, want 35", cfg.RequestTimeout)
	}
}

func TestConfigFromEnvMissingRequired(t *testing.T) {
	// Clear all required vars
	t.Setenv("ANARCHY_URL", "")
	t.Setenv("RUNNER_NAME", "test")
	t.Setenv("RUNNER_TOKEN", "test")
	t.Setenv("HOSTNAME", "test")

	_, err := configFromEnv()
	if err == nil {
		t.Fatal("expected error for missing ANARCHY_URL")
	}
}

func TestAuthHeader(t *testing.T) {
	cfg := Config{
		RunnerName:  "babylon-go",
		PodName:     "pod-123",
		RunnerToken: "secret",
	}
	want := "Bearer babylon-go:pod-123:secret"
	if got := cfg.AuthHeader(); got != want {
		t.Errorf("AuthHeader() = %q, want %q", got, want)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestConfig -v
```
Expected: FAIL — `configFromEnv` and `Config` not defined

- [ ] **Step 4: Implement config**

```go
// config.go
package main

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	AnarchyURL      string
	RunnerName      string
	RunnerToken     string
	PodName         string
	PollingInterval int // seconds
	RequestTimeout  int // seconds
}

func configFromEnv() (Config, error) {
	cfg := Config{
		AnarchyURL:      os.Getenv("ANARCHY_URL"),
		RunnerName:      os.Getenv("RUNNER_NAME"),
		RunnerToken:     os.Getenv("RUNNER_TOKEN"),
		PodName:         os.Getenv("HOSTNAME"),
		PollingInterval: 5,
		RequestTimeout:  35,
	}

	if cfg.AnarchyURL == "" {
		return cfg, fmt.Errorf("ANARCHY_URL is required")
	}
	if cfg.RunnerName == "" {
		return cfg, fmt.Errorf("RUNNER_NAME is required")
	}
	if cfg.RunnerToken == "" {
		return cfg, fmt.Errorf("RUNNER_TOKEN is required")
	}
	if cfg.PodName == "" {
		return cfg, fmt.Errorf("HOSTNAME is required")
	}

	if v := os.Getenv("POLLING_INTERVAL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("POLLING_INTERVAL: %w", err)
		}
		cfg.PollingInterval = n
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("REQUEST_TIMEOUT: %w", err)
		}
		cfg.RequestTimeout = n
	}

	return cfg, nil
}

func (c Config) AuthHeader() string {
	return fmt.Sprintf("Bearer %s:%s:%s", c.RunnerName, c.PodName, c.RunnerToken)
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestConfig -v && go test -run TestAuth -v
```
Expected: PASS

- [ ] **Step 6: Write types**

These are the shared types used by all components — the run payload from GET /run, subject/governor/action/run K8s objects as generic maps, and the result struct for POST /run.

```go
// types.go
package main

import (
	"encoding/json"
	"time"
)

// RunPayload is the response from GET /run
type RunPayload struct {
	Handler  Handler                `json:"handler"`
	Governor map[string]interface{} `json:"governor"`
	Subject  map[string]interface{} `json:"subject"`
	Action   map[string]interface{} `json:"action,omitempty"`
	Run      map[string]interface{} `json:"run"`
}

type Handler struct {
	Type string                 `json:"type"` // "action", "actionCallback", "subjectEvent"
	Name string                 `json:"name,omitempty"`
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// RunResult is the body for POST /run/{name}
type RunResult struct {
	Result ResultPayload `json:"result"`
}

type ResultPayload struct {
	RC            int    `json:"rc"`
	Status        string `json:"status"` // "successful" or "failed"
	StatusMessage string `json:"statusMessage,omitempty"`
}

// SubjectPatch is the body for PATCH /run/subject/{name}
type SubjectPatch struct {
	Patch PatchBody `json:"patch"`
}

type PatchBody struct {
	Metadata             *PatchMetadata         `json:"metadata,omitempty"`
	Spec                 *PatchSpec             `json:"spec,omitempty"`
	Status               map[string]interface{} `json:"status,omitempty"`
	SkipUpdateProcessing bool                   `json:"skip_update_processing,omitempty"`
}

type PatchMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type PatchSpec struct {
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// ScheduleActionRequest is the body for POST /run/subject/{name}/actions
type ScheduleActionRequest struct {
	Action string                 `json:"action"`
	After  string                 `json:"after,omitempty"`
	Cancel []string               `json:"cancel,omitempty"`
	Vars   map[string]interface{} `json:"vars,omitempty"`
}

// --- Helper functions to navigate the untyped maps ---

// getNestedMap safely traverses nested maps: getNestedMap(m, "spec", "vars")
func getNestedMap(m map[string]interface{}, keys ...string) map[string]interface{} {
	current := m
	for _, k := range keys {
		if current == nil {
			return nil
		}
		v, ok := current[k]
		if !ok {
			return nil
		}
		next, ok := v.(map[string]interface{})
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// getNestedString safely gets a string from nested maps
func getNestedString(m map[string]interface{}, keys ...string) string {
	if len(keys) == 0 {
		return ""
	}
	parent := getNestedMap(m, keys[:len(keys)-1]...)
	if parent == nil {
		return ""
	}
	v, _ := parent[keys[len(keys)-1]].(string)
	return v
}

// getNestedBool safely gets a bool from nested maps
func getNestedBool(m map[string]interface{}, keys ...string) bool {
	if len(keys) == 0 {
		return false
	}
	parent := getNestedMap(m, keys[:len(keys)-1]...)
	if parent == nil {
		return false
	}
	v, _ := parent[keys[len(keys)-1]].(bool)
	return v
}

// setNested sets a value in a nested map, creating intermediate maps as needed
func setNested(m map[string]interface{}, value interface{}, keys ...string) {
	for i := 0; i < len(keys)-1; i++ {
		next, ok := m[keys[i]].(map[string]interface{})
		if !ok {
			next = map[string]interface{}{}
			m[keys[i]] = next
		}
		m = next
	}
	m[keys[len(keys)-1]] = value
}

// nowUTC returns current time in RFC3339 format matching anarchy's '%FT%TZ'
func nowUTC() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

// mergeMap merges src into dst (shallow)
func mergeMap(dst, src map[string]interface{}) map[string]interface{} {
	if dst == nil {
		dst = map[string]interface{}{}
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// toJSON marshals to JSON bytes, panics on error (for building request bodies)
func toJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
```

- [ ] **Step 7: Write minimal main.go**

```go
// main.go
package main

import (
	"log/slog"
	"os"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	slog.Info("starting babylon-runner",
		"runner", cfg.RunnerName,
		"pod", cfg.PodName,
		"anarchy_url", cfg.AnarchyURL,
	)

	runner := NewRunner(cfg)
	runner.Run()
}
```

- [ ] **Step 8: Verify it compiles (expect failure — NewRunner not yet defined)**

```bash
cd babylon-runner && go build ./...
```
Expected: FAIL — `NewRunner` undefined. This is fine, it will be implemented in Task 3.

- [ ] **Step 9: Commit**

```bash
git add babylon-runner/
git commit -m "feat(babylon-runner): scaffold project with config and types"
```

---

### Task 2: Anarchy API Client

**Files:**
- Create: `babylon-runner/anarchy.go`
- Create: `babylon-runner/anarchy_test.go`

The anarchy client wraps the two HTTP endpoints the runner calls during execution:
- `PATCH /run/subject/{name}` — update subject (labels, vars, status)
- `POST /run/subject/{name}/actions` — schedule an action

It also provides the `finishAction`/`continueAction`/`deleteSubject` methods that set fields on the result struct (these are NOT HTTP calls — in the Ansible runner they write to `anarchy-result.yaml`, in our Go runner they set fields on a `RunContext`).

- [ ] **Step 1: Write anarchy client test**

```go
// anarchy_test.go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnarchyClientSubjectUpdate(t *testing.T) {
	var receivedPatch SubjectPatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		if r.URL.Path != "/run/subject/test-subject" {
			t.Errorf("path = %s, want /run/subject/test-subject", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer runner:pod:token" {
			t.Errorf("auth = %s", r.Header.Get("Authorization"))
		}
		json.NewDecoder(r.Body).Decode(&receivedPatch)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": map[string]interface{}{}})
	}))
	defer server.Close()

	cfg := Config{AnarchyURL: server.URL, RunnerName: "runner", PodName: "pod", RunnerToken: "token"}
	client := NewAnarchyClient(cfg)

	err := client.SubjectUpdate("test-subject", SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{Labels: map[string]string{"state": "started"}},
			Spec:     &PatchSpec{Vars: map[string]interface{}{"current_state": "started"}},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		t.Fatalf("SubjectUpdate error: %v", err)
	}
	if receivedPatch.Patch.Metadata.Labels["state"] != "started" {
		t.Errorf("label state = %q, want started", receivedPatch.Patch.Metadata.Labels["state"])
	}
	if !receivedPatch.Patch.SkipUpdateProcessing {
		t.Error("skip_update_processing should be true")
	}
}

func TestAnarchyClientScheduleAction(t *testing.T) {
	var receivedReq ScheduleActionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/run/subject/test-subject/actions" {
			t.Errorf("path = %s, want /run/subject/test-subject/actions", r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&receivedReq)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": map[string]interface{}{}})
	}))
	defer server.Close()

	cfg := Config{AnarchyURL: server.URL, RunnerName: "runner", PodName: "pod", RunnerToken: "token"}
	client := NewAnarchyClient(cfg)

	err := client.ScheduleAction("test-subject", ScheduleActionRequest{
		Action: "provision",
		Cancel: []string{"start", "stop"},
	})
	if err != nil {
		t.Fatalf("ScheduleAction error: %v", err)
	}
	if receivedReq.Action != "provision" {
		t.Errorf("action = %q, want provision", receivedReq.Action)
	}
	if len(receivedReq.Cancel) != 2 {
		t.Errorf("cancel len = %d, want 2", len(receivedReq.Cancel))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestAnarchyClient -v
```
Expected: FAIL — `NewAnarchyClient`, `AnarchyClient` not defined

- [ ] **Step 3: Implement anarchy client**

```go
// anarchy.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type AnarchyClient struct {
	cfg    Config
	client *http.Client
}

func NewAnarchyClient(cfg Config) *AnarchyClient {
	return &AnarchyClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.RequestTimeout) * time.Second,
		},
	}
}

// SubjectUpdate patches the subject via PATCH /run/subject/{name}.
// Retries 3 times with backoff [5s, 10s, 20s].
func (a *AnarchyClient) SubjectUpdate(subjectName string, patch SubjectPatch) error {
	url := fmt.Sprintf("%s/run/subject/%s", a.cfg.AnarchyURL, subjectName)
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	delays := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= len(delays); attempt++ {
		if attempt > 0 {
			slog.Warn("retrying subject update", "attempt", attempt, "subject", subjectName)
			time.Sleep(delays[attempt-1])
		}
		req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", a.cfg.AuthHeader())
		req.Header.Set("Content-Type", "application/json")

		resp, err := a.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("subject update request: %w", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("subject update: status %d", resp.StatusCode)
	}
	return lastErr
}

// ScheduleAction creates an action via POST /run/subject/{name}/actions.
// Retries 3 times with backoff [5s, 10s, 20s].
func (a *AnarchyClient) ScheduleAction(subjectName string, req ScheduleActionRequest) error {
	url := fmt.Sprintf("%s/run/subject/%s/actions", a.cfg.AnarchyURL, subjectName)
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal action: %w", err)
	}

	delays := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= len(delays); attempt++ {
		if attempt > 0 {
			slog.Warn("retrying schedule action", "attempt", attempt, "subject", subjectName)
			time.Sleep(delays[attempt-1])
		}
		httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Authorization", a.cfg.AuthHeader())
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := a.client.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("schedule action request: %w", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("schedule action: status %d", resp.StatusCode)
	}
	return lastErr
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestAnarchyClient -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add babylon-runner/anarchy.go babylon-runner/anarchy_test.go
git commit -m "feat(babylon-runner): add anarchy API client (subject update, schedule action)"
```

---

### Task 3: Runner Polling Loop and Dispatch

**Files:**
- Create: `babylon-runner/runner.go`
- Create: `babylon-runner/runner_test.go`

The runner polls `GET /run`, dispatches to handler functions, and posts results via `POST /run/{name}`. It also holds the `RunContext` which tracks per-run state (finish/continue/delete directives).

- [ ] **Step 1: Write RunContext and dispatch test**

```go
// runner_test.go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestRunnerDispatchEvent(t *testing.T) {
	payload := RunPayload{
		Handler: Handler{Type: "subjectEvent", Name: "create"},
		Governor: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-gov"},
			"spec":     map[string]interface{}{"vars": map[string]interface{}{}},
		},
		Subject: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-subject"},
			"spec":     map[string]interface{}{"vars": map[string]interface{}{}},
		},
		Run: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-run"},
		},
	}

	var called bool
	ctx := &RunContext{Payload: payload}
	handlers := map[string]HandlerFunc{
		"event:create": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	err := dispatch(ctx, handlers)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if !called {
		t.Error("event:create handler was not called")
	}
}

func TestRunnerDispatchAction(t *testing.T) {
	payload := RunPayload{
		Handler: Handler{Type: "action"},
		Action: map[string]interface{}{
			"spec": map[string]interface{}{"action": "provision"},
		},
		Subject: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-subject"},
			"spec":     map[string]interface{}{"vars": map[string]interface{}{}},
		},
		Run: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-run"},
		},
	}

	var called bool
	ctx := &RunContext{Payload: payload}
	handlers := map[string]HandlerFunc{
		"action:provision": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	err := dispatch(ctx, handlers)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if !called {
		t.Error("action:provision handler was not called")
	}
}

func TestRunnerPollAndPost(t *testing.T) {
	var postCalled atomic.Bool
	payload := RunPayload{
		Handler: Handler{Type: "subjectEvent", Name: "create"},
		Subject: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-subject"},
			"spec":     map[string]interface{}{"vars": map[string]interface{}{}},
		},
		Run: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "test-run-001"},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/run":
			json.NewEncoder(w).Encode(payload)
		case r.Method == http.MethodPost && r.URL.Path == "/run/test-run-001":
			postCalled.Store(true)
			var result RunResult
			json.NewDecoder(r.Body).Decode(&result)
			if result.Result.Status != "successful" {
				t.Errorf("result status = %q, want successful", result.Result.Status)
			}
			json.NewEncoder(w).Encode(map[string]bool{"success": true})
		default:
			// Anarchy API calls from handler — accept them
			json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": map[string]interface{}{}})
		}
	}))
	defer server.Close()

	cfg := Config{AnarchyURL: server.URL, RunnerName: "runner", PodName: "pod", RunnerToken: "token", RequestTimeout: 5}
	runner := NewRunner(cfg)
	// Override handlers with a no-op for create
	runner.handlers["event:create"] = func(rc *RunContext) error { return nil }

	// Run a single poll cycle
	runner.pollOnce()

	if !postCalled.Load() {
		t.Error("POST /run/{name} was not called")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestRunner -v
```
Expected: FAIL — `RunContext`, `HandlerFunc`, `dispatch`, `Runner`, `NewRunner` not defined

- [ ] **Step 3: Implement runner**

```go
// runner.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// HandlerFunc processes a run. It may call RunContext methods to update the
// subject, schedule actions, or set the finish/continue/delete directives.
type HandlerFunc func(rc *RunContext) error

// RunContext holds per-run state and provides methods mirroring the Ansible modules.
type RunContext struct {
	Payload       RunPayload
	Anarchy       *AnarchyClient
	SubjectName   string
	RunName       string
	ActionName    string // e.g. "provision", "destroy" — empty for events
	GovernorVars  map[string]interface{}
	SubjectVars   map[string]interface{}
	SubjectStatus map[string]interface{}

	// Set by handlers — included in POST /run result
	finished bool
	finishState string // "successful", "failed", "error"
}

// FinishAction marks the action as finished (equivalent to anarchy_finish_action).
func (rc *RunContext) FinishAction(state string) {
	rc.finished = true
	rc.finishState = state
}

// ContinueAction reschedules the current action (equivalent to anarchy_continue_action).
// The after parameter is an interval like "5m" or an ISO timestamp.
func (rc *RunContext) ContinueAction(after string) error {
	return rc.Anarchy.ScheduleAction(rc.SubjectName, ScheduleActionRequest{
		Action: rc.ActionName,
		After:  after,
	})
}

// SubjectUpdate patches the subject.
func (rc *RunContext) SubjectUpdate(patch SubjectPatch) error {
	return rc.Anarchy.SubjectUpdate(rc.SubjectName, patch)
}

// ScheduleAction schedules a new action on the subject.
func (rc *RunContext) ScheduleAction(req ScheduleActionRequest) error {
	return rc.Anarchy.ScheduleAction(rc.SubjectName, req)
}

// CurrentState returns the subject's current_state.
func (rc *RunContext) CurrentState() string {
	return getNestedString(rc.Payload.Subject, "spec", "vars", "current_state")
}

// DesiredState returns the subject's desired_state.
func (rc *RunContext) DesiredState() string {
	return getNestedString(rc.Payload.Subject, "spec", "vars", "desired_state")
}

// JobVars returns subject.spec.vars.job_vars.
func (rc *RunContext) JobVars() map[string]interface{} {
	return getNestedMap(rc.Payload.Subject, "spec", "vars", "job_vars")
}

// GovernorJobVars returns governor-level job_vars.
func (rc *RunContext) GovernorJobVars() map[string]interface{} {
	return getNestedMap(rc.Payload.Governor, "spec", "vars", "job_vars")
}

// Meta returns __meta__ from governor vars.
func (rc *RunContext) Meta() map[string]interface{} {
	govVars := getNestedMap(rc.Payload.Governor, "spec", "vars")
	if govVars == nil {
		return nil
	}
	m, _ := govVars["__meta__"].(map[string]interface{})
	return m
}

// SandboxAPIInUse returns true if sandbox API should be used.
func (rc *RunContext) SandboxAPIInUse() bool {
	meta := rc.Meta()
	if meta == nil {
		return false
	}
	if awsSandboxed, ok := meta["aws_sandboxed"].(bool); ok && awsSandboxed {
		return true
	}
	if sandboxes, ok := meta["sandboxes"].([]interface{}); ok && len(sandboxes) > 0 {
		return true
	}
	return false
}

// DeployerDisabled checks if the deployer entry point for the given action is disabled.
func (rc *RunContext) DeployerDisabled(action string) bool {
	meta := rc.Meta()
	if meta == nil {
		return false
	}
	deployer, _ := meta["deployer"].(map[string]interface{})
	if deployer == nil {
		return false
	}
	entryPoints, _ := deployer["entry_points"].(map[string]interface{})
	if entryPoints == nil {
		return false
	}
	ep, _ := entryPoints[action].(string)
	return ep == "disabled" || ep == "none"
}

// UUID returns the service UUID from job_vars.
func (rc *RunContext) UUID() string {
	jv := rc.JobVars()
	if jv == nil {
		return ""
	}
	v, _ := jv["uuid"].(string)
	return v
}

// GUID returns the guid from job_vars.
func (rc *RunContext) GUID() string {
	jv := rc.JobVars()
	if jv == nil {
		return ""
	}
	v, _ := jv["guid"].(string)
	return v
}

// StatusActions returns status.actions map, or empty map.
func (rc *RunContext) StatusActions() map[string]interface{} {
	m := getNestedMap(rc.Payload.Subject, "status", "actions")
	if m == nil {
		return map[string]interface{}{}
	}
	return m
}

// StatusTowerJobs returns status.towerJobs map, or empty map.
func (rc *RunContext) StatusTowerJobs() map[string]interface{} {
	m := getNestedMap(rc.Payload.Subject, "status", "towerJobs")
	if m == nil {
		return map[string]interface{}{}
	}
	return m
}

// GovernorActions returns the governor's spec.actions map.
func (rc *RunContext) GovernorActions() map[string]interface{} {
	return getNestedMap(rc.Payload.Governor, "spec", "actions")
}

// Runner polls the anarchy API and dispatches runs.
type Runner struct {
	cfg      Config
	client   *http.Client
	anarchy  *AnarchyClient
	handlers map[string]HandlerFunc
}

func NewRunner(cfg Config) *Runner {
	r := &Runner{
		cfg: cfg,
		client: &http.Client{
			Timeout: time.Duration(cfg.RequestTimeout) * time.Second,
		},
		anarchy:  NewAnarchyClient(cfg),
		handlers: map[string]HandlerFunc{},
	}
	// Handlers are registered in main.go or by the caller
	return r
}

// Run starts the polling loop, stopping on SIGTERM/SIGINT.
func (r *Runner) Run() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	slog.Info("runner started, polling for runs")
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		default:
			r.pollOnce()
		}
	}
}

func (r *Runner) pollOnce() {
	payload, err := r.getRun()
	if err != nil {
		slog.Error("failed to get run", "error", err)
		time.Sleep(time.Duration(r.cfg.PollingInterval) * time.Second)
		return
	}
	if payload == nil {
		time.Sleep(time.Duration(r.cfg.PollingInterval) * time.Second)
		return
	}

	runName := getNestedString(payload.Run, "metadata", "name")
	subjectName := getNestedString(payload.Subject, "metadata", "name")

	rc := &RunContext{
		Payload:     *payload,
		Anarchy:     r.anarchy,
		SubjectName: subjectName,
		RunName:     runName,
	}

	// Set action name if this is an action run
	if payload.Action != nil {
		rc.ActionName = getNestedString(payload.Action, "spec", "action")
	}

	slog.Info("executing run", "run", runName, "subject", subjectName,
		"handler_type", payload.Handler.Type, "handler_name", payload.Handler.Name,
		"action", rc.ActionName)

	err = dispatch(rc, r.handlers)

	result := RunResult{
		Result: ResultPayload{
			RC:     0,
			Status: "successful",
		},
	}
	if err != nil {
		slog.Error("run failed", "run", runName, "error", err)
		result.Result.RC = 1
		result.Result.Status = "failed"
		result.Result.StatusMessage = err.Error()
	}

	if postErr := r.postResult(runName, result); postErr != nil {
		slog.Error("failed to post result", "run", runName, "error", postErr)
	}
}

func (r *Runner) getRun() (*RunPayload, error) {
	url := fmt.Sprintf("%s/run", r.cfg.AnarchyURL)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", r.cfg.AuthHeader())

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, nil // timeout or connection error = no run available
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("forbidden (pod may be deleting)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}

	var payload RunPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode run payload: %w", err)
	}

	return &payload, nil
}

func (r *Runner) postResult(runName string, result RunResult) error {
	url := fmt.Sprintf("%s/run/%s", r.cfg.AnarchyURL, runName)
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(r.cfg.PollingInterval) * time.Second)
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", r.cfg.AuthHeader())
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("post result: status %d", resp.StatusCode)
	}
	return lastErr
}

// dispatch routes a run to the appropriate handler.
func dispatch(rc *RunContext, handlers map[string]HandlerFunc) error {
	var key string
	switch rc.Payload.Handler.Type {
	case "subjectEvent":
		key = "event:" + rc.Payload.Handler.Name
	case "action":
		actionName := getNestedString(rc.Payload.Action, "spec", "action")
		key = "action:" + actionName
	case "actionCallback":
		actionName := getNestedString(rc.Payload.Action, "spec", "action")
		key = "action:" + actionName + ":" + rc.Payload.Handler.Name
	default:
		return fmt.Errorf("unknown handler type: %q", rc.Payload.Handler.Type)
	}

	handler, ok := handlers[key]
	if !ok {
		return fmt.Errorf("no handler registered for %q", key)
	}

	return handler(rc)
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestRunner -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add babylon-runner/runner.go babylon-runner/runner_test.go
git commit -m "feat(babylon-runner): add polling loop, dispatch, and RunContext"
```

---

### Task 4: Sandbox API Client

**Files:**
- Create: `babylon-runner/sandboxapi.go`
- Create: `babylon-runner/sandboxapi_test.go`

HTTP client for the sandbox API: login, book placement, get placement, release, start, stop, and check request status.

- [ ] **Step 1: Write sandbox API client test**

```go
// sandboxapi_test.go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSandboxAPILogin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/login" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer login-token-123" {
			t.Errorf("auth = %s", r.Header.Get("Authorization"))
		}
		json.NewEncoder(w).Encode(map[string]string{"access_token": "access-xyz"})
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	token, err := client.Login("login-token-123")
	if err != nil {
		t.Fatalf("Login error: %v", err)
	}
	if token != "access-xyz" {
		t.Errorf("token = %q, want access-xyz", token)
	}
}

func TestSandboxAPIGetPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/placements/uuid-123" {
			t.Errorf("path = %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"uuid":   "uuid-123",
			"status": "success",
			"resources": []interface{}{
				map[string]interface{}{"kind": "AwsSandbox", "name": "sb-1"},
			},
		})
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	placement, statusCode, err := client.GetPlacement("access-token", "uuid-123")
	if err != nil {
		t.Fatalf("GetPlacement error: %v", err)
	}
	if statusCode != 200 {
		t.Errorf("status = %d, want 200", statusCode)
	}
	status, _ := placement["status"].(string)
	if status != "success" {
		t.Errorf("placement status = %q, want success", status)
	}
}

func TestSandboxAPIBookPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/placements" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		if req["service_uuid"] != "uuid-123" {
			t.Errorf("service_uuid = %v", req["service_uuid"])
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"uuid":   "uuid-123",
			"status": "success",
		})
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	result, statusCode, err := client.BookPlacement("access-token", map[string]interface{}{
		"service_uuid": "uuid-123",
		"resources":    []interface{}{},
	})
	if err != nil {
		t.Fatalf("BookPlacement error: %v", err)
	}
	if statusCode != 200 {
		t.Errorf("status = %d", statusCode)
	}
	if result["status"] != "success" {
		t.Errorf("result status = %v", result["status"])
	}
}

func TestSandboxAPIStartStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"request_id": "req-abc",
			"message":    "starting",
		})
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	result, err := client.StartPlacement("token", "uuid-123")
	if err != nil {
		t.Fatalf("StartPlacement error: %v", err)
	}
	if result["request_id"] != "req-abc" {
		t.Errorf("request_id = %v", result["request_id"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestSandboxAPI -v
```
Expected: FAIL

- [ ] **Step 3: Implement sandbox API client**

```go
// sandboxapi.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type SandboxAPIClient struct {
	baseURL string
	client  *http.Client
}

func NewSandboxAPIClient(baseURL string) *SandboxAPIClient {
	return &SandboxAPIClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *SandboxAPIClient) Login(loginToken string) (string, error) {
	url := fmt.Sprintf("%s/api/v1/login", s.baseURL)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+loginToken)

	resp, err := s.doWithRetry(req, 40, 5*time.Second)
	if err != nil {
		return "", fmt.Errorf("sandbox login: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode login response: %w", err)
	}
	return result.AccessToken, nil
}

func (s *SandboxAPIClient) GetPlacement(accessToken, uuid string) (map[string]interface{}, int, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s", s.baseURL, uuid)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("get placement: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, resp.StatusCode, nil
}

func (s *SandboxAPIClient) BookPlacement(accessToken string, body map[string]interface{}) (map[string]interface{}, int, error) {
	url := fmt.Sprintf("%s/api/v1/placements", s.baseURL)
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("book placement: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	return result, resp.StatusCode, nil
}

func (s *SandboxAPIClient) ReleasePlacement(accessToken, uuid string) error {
	url := fmt.Sprintf("%s/api/v1/placements/%s", s.baseURL, uuid)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("release placement: %w", err)
	}
	resp.Body.Close()
	return nil
}

func (s *SandboxAPIClient) StartPlacement(accessToken, uuid string) (map[string]interface{}, error) {
	return s.putPlacementAction(accessToken, uuid, "start")
}

func (s *SandboxAPIClient) StopPlacement(accessToken, uuid string) (map[string]interface{}, error) {
	return s.putPlacementAction(accessToken, uuid, "stop")
}

func (s *SandboxAPIClient) putPlacementAction(accessToken, uuid, action string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s/%s", s.baseURL, uuid, action)
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := s.doWithRetry(req, 40, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("%s placement: %w", action, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, nil
}

func (s *SandboxAPIClient) GetRequestStatus(accessToken, requestID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/requests/%s/status", s.baseURL, requestID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get request status: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, nil
}

func (s *SandboxAPIClient) doWithRetry(req *http.Request, maxRetries int, delay time.Duration) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			slog.Warn("retrying sandbox API request", "attempt", attempt, "url", req.URL.Path)
			time.Sleep(delay)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		if resp.StatusCode == 404 {
			return resp, nil // caller handles 404
		}
		resp.Body.Close()
		lastErr = fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil, lastErr
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestSandboxAPI -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add babylon-runner/sandboxapi.go babylon-runner/sandboxapi_test.go
git commit -m "feat(babylon-runner): add sandbox API client"
```

---

### Task 5: Tower/AAP2 Client

**Files:**
- Create: `babylon-runner/tower.go`
- Create: `babylon-runner/tower_test.go`

REST client for Ansible Automation Platform. This is the most complex external client — it needs to create organizations, inventories, credentials, projects, execution environments, job templates, then launch and monitor jobs.

- [ ] **Step 1: Write tower client test**

```go
// tower_test.go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTowerSelectControllerRandom(t *testing.T) {
	controllers := []map[string]interface{}{
		{"hostname": "tower1.example.com"},
		{"hostname": "tower2.example.com"},
	}
	selected := selectController(controllers, "random")
	if selected == nil {
		t.Fatal("selectController returned nil")
	}
	hostname, _ := selected["hostname"].(string)
	if hostname != "tower1.example.com" && hostname != "tower2.example.com" {
		t.Errorf("unexpected hostname: %s", hostname)
	}
}

func TestTowerSelectControllerFirstAvailable(t *testing.T) {
	controllers := []map[string]interface{}{
		{"hostname": "tower1.example.com"},
		{"hostname": "tower2.example.com"},
	}
	selected := selectController(controllers, "first-available")
	hostname, _ := selected["hostname"].(string)
	if hostname != "tower1.example.com" {
		t.Errorf("hostname = %s, want tower1.example.com", hostname)
	}
}

func TestTowerLaunchJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    1,
				"token": "oauth-token-xyz",
			})
		case "/api/v2/organizations/":
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": 1})
		case "/api/v2/inventories/":
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": 1})
		case "/api/v2/projects/":
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": 1, "name": "test-project"})
		case "/api/v2/job_templates/":
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": 1, "name": "test-template"})
		case "/api/v2/job_templates/1/launch/":
			w.WriteHeader(201)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": 42, "job": 42})
		default:
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(map[string]interface{}{})
		}
	}))
	defer server.Close()

	client := NewTowerClient(server.URL[len("https://"):], "user", "pass") // strip scheme
	// For testing, override the base URL
	client.baseURL = server.URL

	jobID, err := client.LaunchJob(TowerJobConfig{
		Organization:    "babylon",
		Inventory:       "babylon default",
		ProjectSCMURL:   "https://github.com/example/repo.git",
		ProjectSCMRef:   "main",
		TemplateName:    "babylon provision uuid-123",
		Playbook:        "ansible/main.yml",
		ExtraVars:       map[string]interface{}{"key": "value"},
		Timeout:         10800,
	})
	if err != nil {
		t.Fatalf("LaunchJob error: %v", err)
	}
	if jobID == 0 {
		t.Error("jobID should not be 0")
	}
}

func TestTowerGetJobStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":     42,
			"status": "successful",
			"finished": "2026-05-15T10:30:00Z",
		})
	}))
	defer server.Close()

	client := &TowerClient{baseURL: server.URL, client: http.DefaultClient}
	status, err := client.GetJobStatus("oauth-token", 42)
	if err != nil {
		t.Fatalf("GetJobStatus error: %v", err)
	}
	s, _ := status["status"].(string)
	if s != "successful" {
		t.Errorf("status = %q, want successful", s)
	}
}

func TestTowerCancelJob(t *testing.T) {
	var cancelCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/jobs/42/cancel/" && r.Method == http.MethodPost {
			cancelCalled = true
			w.WriteHeader(202)
		}
	}))
	defer server.Close()

	client := &TowerClient{baseURL: server.URL, client: http.DefaultClient}
	err := client.CancelJob("oauth-token", 42)
	if err != nil {
		t.Fatalf("CancelJob error: %v", err)
	}
	if !cancelCalled {
		t.Error("cancel endpoint not called")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestTower -v
```
Expected: FAIL

- [ ] **Step 3: Implement tower client**

```go
// tower.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"time"
)

type TowerClient struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

type TowerJobConfig struct {
	Organization        string
	Inventory           string
	ProjectSCMURL       string
	ProjectSCMRef       string
	TemplateName        string
	Playbook            string
	ExtraVars           map[string]interface{}
	Timeout             int
	Credentials         []map[string]interface{}
	ExecutionEnvironment map[string]interface{}
	InstanceGroups       []string
}

func NewTowerClient(hostname, username, password string) *TowerClient {
	return &TowerClient{
		baseURL:  fmt.Sprintf("https://%s", hostname),
		username: username,
		password: password,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: nil, // TODO: configure TLS
			},
		},
	}
}

// selectController picks a controller from the available list based on mode.
func selectController(controllers []map[string]interface{}, mode string) map[string]interface{} {
	if len(controllers) == 0 {
		return nil
	}
	switch mode {
	case "first-available":
		return controllers[0]
	case "balance":
		// Pick the one with lowest active job count
		best := controllers[0]
		bestCount := getJobCount(best)
		for _, c := range controllers[1:] {
			count := getJobCount(c)
			if count < bestCount {
				best = c
				bestCount = count
			}
		}
		return best
	default: // "random"
		return controllers[rand.Intn(len(controllers))]
	}
}

func getJobCount(controller map[string]interface{}) int {
	count, _ := controller["active_job_count"].(float64)
	return int(count)
}

// CreateOAuthToken creates a personal access token for API access.
func (t *TowerClient) CreateOAuthToken() (string, int, error) {
	url := fmt.Sprintf("%s/api/v2/tokens/", t.baseURL)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte("{}")))
	if err != nil {
		return "", 0, err
	}
	req.SetBasicAuth(t.username, t.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("create oauth token: %w", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	token, _ := result["token"].(string)
	id, _ := result["id"].(float64)
	return token, int(id), nil
}

// DeleteOAuthToken removes an OAuth token.
func (t *TowerClient) DeleteOAuthToken(tokenID int) error {
	url := fmt.Sprintf("%s/api/v2/tokens/%d/", t.baseURL, tokenID)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(t.username, t.password)

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// LaunchJob creates all required Tower resources and launches a job.
// Returns the job ID.
func (t *TowerClient) LaunchJob(config TowerJobConfig) (int, error) {
	oauthToken, tokenID, err := t.CreateOAuthToken()
	if err != nil {
		return 0, fmt.Errorf("create oauth token: %w", err)
	}
	defer func() {
		if err := t.DeleteOAuthToken(tokenID); err != nil {
			slog.Warn("failed to delete oauth token", "tokenID", tokenID, "error", err)
		}
	}()

	// Create organization
	orgID, err := t.ensureOrganization(oauthToken, config.Organization)
	if err != nil {
		return 0, fmt.Errorf("ensure organization: %w", err)
	}

	// Create inventory
	_, err = t.ensureInventory(oauthToken, config.Inventory, orgID)
	if err != nil {
		return 0, fmt.Errorf("ensure inventory: %w", err)
	}

	// Create project
	projectID, err := t.ensureProject(oauthToken, config, orgID)
	if err != nil {
		return 0, fmt.Errorf("ensure project: %w", err)
	}

	// Create job template
	templateID, err := t.ensureJobTemplate(oauthToken, config, orgID, projectID)
	if err != nil {
		return 0, fmt.Errorf("ensure job template: %w", err)
	}

	// Launch
	jobID, err := t.launchJobTemplate(oauthToken, templateID, config)
	if err != nil {
		return 0, fmt.Errorf("launch job: %w", err)
	}

	return jobID, nil
}

// GetJobStatus returns the job details including status.
func (t *TowerClient) GetJobStatus(oauthToken string, jobID int) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v2/jobs/%d/", t.baseURL, jobID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get job status: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, nil
}

// CancelJob cancels a running job.
func (t *TowerClient) CancelJob(oauthToken string, jobID int) error {
	url := fmt.Sprintf("%s/api/v2/jobs/%d/cancel/", t.baseURL, jobID)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	resp.Body.Close()
	return nil
}

func (t *TowerClient) ensureOrganization(token, name string) (int, error) {
	return t.ensureResource(token, "/api/v2/organizations/", map[string]interface{}{
		"name": name,
	})
}

func (t *TowerClient) ensureInventory(token, name string, orgID int) (int, error) {
	return t.ensureResource(token, "/api/v2/inventories/", map[string]interface{}{
		"name":         name,
		"organization": orgID,
	})
}

func (t *TowerClient) ensureProject(token string, config TowerJobConfig, orgID int) (int, error) {
	projectName := fmt.Sprintf("%s %s (%s)", config.Organization, config.ProjectSCMURL, config.ProjectSCMRef)
	return t.ensureResource(token, "/api/v2/projects/", map[string]interface{}{
		"name":                 projectName,
		"organization":         orgID,
		"scm_type":             "git",
		"scm_url":              config.ProjectSCMURL,
		"scm_branch":           config.ProjectSCMRef,
		"scm_update_on_launch": true,
	})
}

func (t *TowerClient) ensureJobTemplate(token string, config TowerJobConfig, orgID, projectID int) (int, error) {
	extraVarsJSON, _ := json.Marshal(config.ExtraVars)
	return t.ensureResource(token, "/api/v2/job_templates/", map[string]interface{}{
		"name":                   config.TemplateName,
		"organization":           orgID,
		"project":                projectID,
		"playbook":               config.Playbook,
		"extra_vars":             string(extraVarsJSON),
		"ask_inventory_on_launch": true,
		"timeout":                config.Timeout,
	})
}

func (t *TowerClient) launchJobTemplate(token string, templateID int, config TowerJobConfig) (int, error) {
	url := fmt.Sprintf("%s/api/v2/job_templates/%d/launch/", t.baseURL, templateID)
	body, _ := json.Marshal(map[string]interface{}{
		"inventory": config.Inventory,
	})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	id, _ := result["id"].(float64)
	if id == 0 {
		id, _ = result["job"].(float64)
	}
	return int(id), nil
}

// ensureResource creates or finds a resource, returns its ID.
func (t *TowerClient) ensureResource(token, path string, data map[string]interface{}) (int, error) {
	url := t.baseURL + path
	body, _ := json.Marshal(data)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	id, _ := result["id"].(float64)
	return int(id), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestTower -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add babylon-runner/tower.go babylon-runner/tower_test.go
git commit -m "feat(babylon-runner): add Tower/AAP2 client"
```

---

### Task 6: Event Handlers (create, update, delete)

**Files:**
- Create: `babylon-runner/handler_event_create.go`
- Create: `babylon-runner/handler_event_update.go`
- Create: `babylon-runner/handler_event_delete.go`
- Create: `babylon-runner/handlers_test.go`

- [ ] **Step 1: Write event handler tests**

```go
// handlers_test.go
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// testAnarchyServer creates a mock anarchy API that records calls.
type anarchyCall struct {
	Method string
	Path   string
	Body   map[string]interface{}
}

func newTestAnarchyServer(t *testing.T) (*httptest.Server, *[]anarchyCall) {
	var calls []anarchyCall
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		calls = append(calls, anarchyCall{Method: r.Method, Path: r.URL.Path, Body: body})
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": map[string]interface{}{}})
	}))
	return server, &calls
}

func newTestRunContext(t *testing.T, server *httptest.Server) *RunContext {
	cfg := Config{AnarchyURL: server.URL, RunnerName: "r", PodName: "p", RunnerToken: "t", RequestTimeout: 5}
	return &RunContext{
		Anarchy:     NewAnarchyClient(cfg),
		SubjectName: "test-subject",
		Payload: RunPayload{
			Governor: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"job_vars": map[string]interface{}{
							"cloud_provider": "aws",
							"platform":       "RHPDS",
						},
					},
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{},
						"destroy":   map[string]interface{}{},
						"start":     map[string]interface{}{},
						"stop":      map[string]interface{}{},
					},
				},
			},
			Subject: map[string]interface{}{
				"metadata": map[string]interface{}{"name": "test-subject"},
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{},
				},
				"status": map[string]interface{}{},
			},
			Run: map[string]interface{}{
				"metadata": map[string]interface{}{"name": "test-run"},
			},
		},
	}
}

func TestHandleEventCreate(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	err := handleEventCreate(rc)
	if err != nil {
		t.Fatalf("handleEventCreate error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}
	// First call: PATCH subject (set provision-pending state)
	if (*calls)[0].Method != "PATCH" {
		t.Errorf("call 0: method = %s, want PATCH", (*calls)[0].Method)
	}
	// Second call: POST action (schedule provision)
	if (*calls)[1].Method != "POST" {
		t.Errorf("call 1: method = %s, want POST", (*calls)[1].Method)
	}
}

func TestHandleEventUpdateStartStop(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	// Subject is started, desired is stopped → should schedule stop
	rc.Payload.Subject["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"current_state": "started",
			"desired_state": "stopped",
			"job_vars":      map[string]interface{}{},
		},
	}
	rc.Payload.Subject["status"] = map[string]interface{}{
		"previous_state": map[string]interface{}{
			"job_vars": map[string]interface{}{},
		},
	}

	err := handleEventUpdate(rc)
	if err != nil {
		t.Fatalf("handleEventUpdate error: %v", err)
	}

	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}
}

func TestHandleEventDeleteWithDestroy(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	// Has a provision tower job → should schedule destroy
	rc.Payload.Subject["status"] = map[string]interface{}{
		"towerJobs": map[string]interface{}{
			"provision": map[string]interface{}{
				"deployerJob": float64(123),
			},
		},
	}

	err := handleEventDelete(rc)
	if err != nil {
		t.Fatalf("handleEventDelete error: %v", err)
	}

	// Should have scheduled a destroy action and updated state
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestHandleEvent -v
```
Expected: FAIL — handler functions not defined

- [ ] **Step 3: Implement event create handler**

```go
// handler_event_create.go
package main

import (
	"log/slog"

	"github.com/google/uuid"
)

// handleEventCreate mirrors handle-event-create.yaml.
// Sets current_state to provision-pending and schedules a provision action.
func handleEventCreate(rc *RunContext) error {
	// Only initialize if current_state is not set
	if rc.CurrentState() != "" {
		slog.Info("subject already initialized, skipping create", "subject", rc.SubjectName)
		return nil
	}

	// Generate UUID if not present
	jobVars := rc.JobVars()
	if jobVars == nil {
		jobVars = map[string]interface{}{}
	}
	if _, ok := jobVars["uuid"]; !ok {
		jobVars["uuid"] = uuid.New().String()
	}

	// Set cloud_provider and platform from governor defaults
	govJobVars := rc.GovernorJobVars()
	if _, ok := jobVars["cloud_provider"]; !ok {
		cp := "none"
		if govJobVars != nil {
			if v, ok := govJobVars["cloud_provider"].(string); ok {
				cp = v
			}
		}
		jobVars["cloud_provider"] = cp
	}
	if _, ok := jobVars["platform"]; !ok {
		platform := "RHPDS"
		if govJobVars != nil {
			if v, ok := govJobVars["platform"].(string); ok {
				platform = v
			}
		}
		jobVars["platform"] = platform
	}

	// Update subject state to provision-pending
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "provision-pending"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-pending",
					"job_vars":      jobVars,
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}

	// Schedule provision action
	return rc.ScheduleAction(ScheduleActionRequest{
		Action: "provision",
	})
}
```

- [ ] **Step 4: Implement event update handler**

```go
// handler_event_update.go
package main

import (
	"log/slog"
	"reflect"
)

// handleEventUpdate mirrors handle-event-update.yaml.
// Compares current/desired state and job_vars to determine action.
func handleEventUpdate(rc *RunContext) error {
	currentState := rc.CurrentState()
	desiredState := rc.DesiredState()

	currentJobVars := rc.JobVars()
	previousJobVars := getNestedMap(rc.Payload.Subject, "status", "previous_state", "job_vars")
	if previousJobVars == nil {
		previousJobVars = currentJobVars
	}

	// Determine action
	var action string
	if !reflect.DeepEqual(currentJobVars, previousJobVars) {
		action = "update"
	} else if desiredState == "started" && currentState == "stopped" {
		action = "start"
	} else if desiredState == "stopped" && currentState == "started" {
		action = "stop"
	}

	// Schedule action if valid and exists in governor
	if action != "" {
		govActions := rc.GovernorActions()
		if _, exists := govActions[action]; exists {
			slog.Info("scheduling action from update event", "action", action, "subject", rc.SubjectName)

			err := rc.SubjectUpdate(SubjectPatch{
				Patch: PatchBody{
					Metadata: &PatchMetadata{
						Labels: map[string]string{"state": action + "-pending"},
					},
					Spec: &PatchSpec{
						Vars: map[string]interface{}{
							"current_state": action + "-pending",
						},
					},
					SkipUpdateProcessing: true,
				},
			})
			if err != nil {
				return err
			}

			err = rc.ScheduleAction(ScheduleActionRequest{
				Action: action,
				Cancel: []string{"start", "stop"},
			})
			if err != nil {
				return err
			}
		}
	}

	// Check for status check request
	subjectVars := getNestedMap(rc.Payload.Subject, "spec", "vars")
	checkStatusState, _ := subjectVars["check_status_state"].(string)

	if checkStatusState == "pending" || checkStatusState == "" || checkStatusState == "successful" {
		// Check if a status check was requested
		checkTimestamp, _ := subjectVars["check_status_request_timestamp"].(string)
		prevCheckTimestamp := getNestedString(rc.Payload.Subject, "status", "previous_state", "check_status_request_timestamp")

		if checkTimestamp != "" && checkTimestamp != prevCheckTimestamp {
			slog.Info("scheduling status check", "subject", rc.SubjectName)

			err := rc.SubjectUpdate(SubjectPatch{
				Patch: PatchBody{
					Spec: &PatchSpec{
						Vars: map[string]interface{}{
							"check_status_state": "pending",
						},
					},
					SkipUpdateProcessing: true,
				},
			})
			if err != nil {
				return err
			}

			return rc.ScheduleAction(ScheduleActionRequest{
				Action: "status",
			})
		}
	}

	return nil
}
```

- [ ] **Step 5: Implement event delete handler**

```go
// handler_event_delete.go
package main

import (
	"log/slog"
)

// handleEventDelete mirrors handle-event-delete.yaml.
// Cancels running jobs, then schedules destroy or deletes directly.
func handleEventDelete(rc *RunContext) error {
	// TODO: Cancel incomplete tower jobs (requires Tower client in RunContext)
	// For now, skip job cancellation — will be added in Task 11

	// Check if we should destroy or delete directly
	towerJobs := rc.StatusTowerJobs()
	provisionJob := getNestedMap(towerJobs, "provision")
	hasProvisionJob := provisionJob != nil && provisionJob["deployerJob"] != nil

	govActions := rc.GovernorActions()
	_, hasDestroyAction := govActions["destroy"]

	deployerDisabled := rc.DeployerDisabled("destroy")

	if hasProvisionJob && hasDestroyAction && !deployerDisabled {
		return handleEventDeleteWithDestroy(rc)
	}
	return handleEventDeleteWithoutDestroy(rc)
}

// handleEventDeleteWithDestroy mirrors handle-event-delete-with-destroy.yaml.
func handleEventDeleteWithDestroy(rc *RunContext) error {
	slog.Info("scheduling destroy for deleted subject", "subject", rc.SubjectName)

	err := rc.ScheduleAction(ScheduleActionRequest{
		Action: "destroy",
		Cancel: []string{"start", "stop", "update"},
	})
	if err != nil {
		return err
	}

	return rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-pending",
					"desired_state": "destroyed",
				},
			},
		},
	})
}

// handleEventDeleteWithoutDestroy mirrors handle-event-delete-without-destroy.yaml.
func handleEventDeleteWithoutDestroy(rc *RunContext) error {
	slog.Info("deleting subject without destroy", "subject", rc.SubjectName)

	// Sandbox cleanup if applicable
	if rc.SandboxAPIInUse() && rc.UUID() != "" {
		meta := rc.Meta()
		catchAll := true
		if meta != nil {
			if v, ok := meta["sandbox_api_destroy_catch_all"].(bool); ok {
				catchAll = v
			}
		}
		if catchAll {
			// TODO: sandbox cleanup — will be connected in Task 7
			slog.Info("sandbox cleanup needed but not yet implemented", "subject", rc.SubjectName)
		}
	}

	// Update state
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-complete",
					"desired_state": "destroyed",
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Mark for deletion
	rc.FinishAction("successful")
	// TODO: Subject delete with remove_finalizers — needs to be in result
	return nil
}
```

- [ ] **Step 6: Add uuid dependency**

```bash
cd babylon-runner && go get github.com/google/uuid
```

- [ ] **Step 7: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestHandleEvent -v
```
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add babylon-runner/
git commit -m "feat(babylon-runner): add event handlers (create, update, delete)"
```

---

### Task 7: Action Provision Handler

**Files:**
- Create: `babylon-runner/handler_provision.go`

This is the most complex handler. It routes between provision-pending, provision-queued, and provisioning states. It integrates sandbox API booking, Tower job launching, and handles the complete/error/failed callbacks.

- [ ] **Step 1: Write provision handler test**

Add to `handlers_test.go`:

```go
func TestHandleProvisionPending(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.ActionName = "provision"
	rc.Payload.Subject["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"current_state": "provision-pending",
			"job_vars":      map[string]interface{}{"uuid": "test-uuid"},
		},
	}
	rc.Payload.Subject["status"] = map[string]interface{}{}
	rc.Payload.Action = map[string]interface{}{
		"spec": map[string]interface{}{"action": "provision"},
	}
	// Governor with deployer disabled (simplest path — no Tower)
	rc.Payload.Governor["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{
					"entry_points": map[string]interface{}{
						"provision": "disabled",
					},
				},
				"sandboxes": []interface{}{
					map[string]interface{}{"kind": "AwsSandbox"},
				},
			},
		},
	}

	err := handleProvision(rc)
	if err != nil {
		t.Fatalf("handleProvision error: %v", err)
	}

	// With deployer disabled and sandbox API in use:
	// Should set startTimestamp, then set state=started + healthy=true + finish action
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestHandleProvision -v
```
Expected: FAIL

- [ ] **Step 3: Implement provision handler**

```go
// handler_provision.go
package main

import (
	"fmt"
	"log/slog"
)

// handleProvision mirrors handle-action-provision.yaml — routes based on current_state.
func handleProvision(rc *RunContext) error {
	switch rc.CurrentState() {
	case "provision-pending":
		return runProvision(rc)
	case "provision-queued":
		return checkProvisionQueue(rc)
	case "provisioning":
		if !rc.DeployerDisabled("provision") {
			return checkDeployerJob(rc, "provision")
		}
		return nil
	default:
		slog.Warn("unexpected state for provision", "state", rc.CurrentState())
		return nil
	}
}

// runProvision mirrors run-provision.yaml.
func runProvision(rc *RunContext) error {
	// Set startTimestamp if not set
	actions := rc.StatusActions()
	provisionActions, _ := actions["provision"].(map[string]interface{})
	if provisionActions == nil || provisionActions["startTimestamp"] == nil {
		err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{
							"startTimestamp": nowUTC(),
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		})
		if err != nil {
			return fmt.Errorf("set provision startTimestamp: %w", err)
		}
	}

	// Get sandbox if sandbox API in use
	if rc.SandboxAPIInUse() {
		// TODO: sandbox_get — will be implemented when sandbox operations are connected
		slog.Info("sandbox get needed", "subject", rc.SubjectName)
	}

	if !rc.DeployerDisabled("provision") {
		// Launch Tower job
		// TODO: check-run-tower-job — will be implemented in Task 11
		slog.Info("tower job launch needed for provision", "subject", rc.SubjectName)

		// Schedule check
		return rc.ContinueAction("5m")
	}

	// Deployer disabled + sandbox API in use → mark as started immediately
	if rc.SandboxAPIInUse() {
		now := nowUTC()
		err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Metadata: &PatchMetadata{
					Labels: map[string]string{"state": "started"},
				},
				Spec: &PatchSpec{
					Vars: map[string]interface{}{
						"current_state": "started",
						"healthy":       true,
					},
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{
							"completeTimestamp": now,
							"status":           "successful",
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		})
		if err != nil {
			return err
		}
		rc.FinishAction("successful")
		return nil
	}

	return nil
}

// handleProvisionComplete mirrors handle-action-provision-complete.yaml.
func handleProvisionComplete(rc *RunContext, provisionData, messageBody interface{}, messages interface{}) error {
	now := nowUTC()
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "started"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state":          "started",
					"healthy":                true,
					"provision_data":         provisionData,
					"provision_message_body": messageBody,
					"provision_messages":     messages,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "successful",
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	rc.FinishAction("successful")
	return nil
}

// handleProvisionError mirrors handle-action-provision-error.yaml.
func handleProvisionError(rc *RunContext) error {
	now := nowUTC()
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "provision-error"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-error",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "error",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "error",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}
	rc.FinishAction("error")
	return nil
}

// handleProvisionFailed mirrors handle-action-provision-failed.yaml.
func handleProvisionFailed(rc *RunContext) error {
	now := nowUTC()
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "provision-failed"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-failed",
					"healthy":       false,
					"job_vars": mergeMap(rc.JobVars(), map[string]interface{}{
						"agnosticd_collect_forensics": true,
					}),
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "failed",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "failed",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}
	rc.FinishAction("failed")
	return nil
}

// checkProvisionQueue mirrors check-provision-queue.yaml.
// Checks if a queued sandbox placement is ready.
func checkProvisionQueue(rc *RunContext) error {
	// TODO: Implement when sandbox API client is connected to RunContext
	slog.Info("check provision queue not yet implemented", "subject", rc.SubjectName)
	return rc.ContinueAction("30s")
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestHandleProvision -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add babylon-runner/handler_provision.go
git commit -m "feat(babylon-runner): add provision handler (pending, complete, error, failed)"
```

---

### Task 8: Action Destroy Handler

**Files:**
- Create: `babylon-runner/handler_destroy.go`

- [ ] **Step 1: Write destroy handler test**

Add to `handlers_test.go`:

```go
func TestHandleDestroyWithCatchAll(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.ActionName = "destroy"
	rc.Payload.Subject["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"current_state": "destroy-error",
			"job_vars":      map[string]interface{}{"uuid": "test-uuid"},
		},
	}
	rc.Payload.Action = map[string]interface{}{
		"spec": map[string]interface{}{"action": "destroy"},
	}
	rc.Payload.Governor["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"__meta__": map[string]interface{}{
				"sandboxes":                   []interface{}{map[string]interface{}{"kind": "AwsSandbox"}},
				"sandbox_api_destroy_catch_all": true,
			},
		},
	}

	err := handleDestroy(rc)
	if err != nil {
		t.Fatalf("handleDestroy error: %v", err)
	}

	// Should have made calls (cleanup + delete)
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestHandleDestroy -v
```
Expected: FAIL

- [ ] **Step 3: Implement destroy handler**

```go
// handler_destroy.go
package main

import (
	"fmt"
	"log/slog"
)

// handleDestroy mirrors handle-action-destroy.yaml.
func handleDestroy(rc *RunContext) error {
	currentState := rc.CurrentState()

	// Set startTimestamp if needed
	if currentState == "destroy-pending" {
		actions := rc.StatusActions()
		destroyActions, _ := actions["destroy"].(map[string]interface{})
		if destroyActions == nil || destroyActions["startTimestamp"] == nil {
			err := rc.SubjectUpdate(SubjectPatch{
				Patch: PatchBody{
					Status: map[string]interface{}{
						"actions": map[string]interface{}{
							"destroy": map[string]interface{}{
								"startTimestamp": nowUTC(),
							},
						},
					},
					SkipUpdateProcessing: true,
				},
			})
			if err != nil {
				return fmt.Errorf("set destroy startTimestamp: %w", err)
			}
		}
	}

	// Catch-all: if sandbox API + catch_all + error state or deployer disabled → cleanup and delete
	meta := rc.Meta()
	catchAll := true
	if meta != nil {
		if v, ok := meta["sandbox_api_destroy_catch_all"].(bool); ok {
			catchAll = v
		}
	}

	isErrorState := currentState == "destroy-error" || currentState == "destroy-failed" || currentState == "destroy-canceled"
	if rc.SandboxAPIInUse() && catchAll && (isErrorState || rc.DeployerDisabled("destroy")) {
		slog.Info("destroy catch-all: cleanup and delete", "subject", rc.SubjectName, "state", currentState)
		// TODO: sandbox_cleanup
		// Delete subject
		rc.FinishAction("successful")
		return nil
	}

	if currentState != "destroying" && !rc.DeployerDisabled("destroy") {
		return runDestroy(rc)
	}

	if currentState == "destroying" && !rc.DeployerDisabled("destroy") {
		return checkDeployerJob(rc, "destroy")
	}

	return nil
}

// runDestroy mirrors run-destroy.yaml.
func runDestroy(rc *RunContext) error {
	if rc.SandboxAPIInUse() {
		// TODO: sandbox_get
		slog.Info("sandbox get needed for destroy", "subject", rc.SubjectName)
	}

	// TODO: Cancel running provision Tower job if exists
	// TODO: Launch destroy Tower job via check-run-tower-job

	slog.Info("tower job launch needed for destroy", "subject", rc.SubjectName)
	return rc.ContinueAction("5m")
}

// handleDestroyComplete mirrors handle-action-destroy-complete.yaml.
func handleDestroyComplete(rc *RunContext) error {
	if rc.SandboxAPIInUse() {
		// TODO: sandbox_cleanup
		slog.Info("sandbox cleanup needed for destroy complete", "subject", rc.SubjectName)
	}

	now := nowUTC()
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "destroy-complete"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-complete",
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "successful",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}

	// Delete subject
	rc.FinishAction("successful")
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestHandleDestroy -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add babylon-runner/handler_destroy.go babylon-runner/handlers_test.go
git commit -m "feat(babylon-runner): add destroy handler (catch-all, complete)"
```

---

### Task 9: Action Start/Stop Handlers

**Files:**
- Create: `babylon-runner/handler_start.go`
- Create: `babylon-runner/handler_stop.go`

- [ ] **Step 1: Write start/stop handler tests**

Add to `handlers_test.go`:

```go
func TestHandleStartDeployerDisabled(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.ActionName = "start"
	rc.Payload.Subject["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"current_state": "stopped",
			"job_vars":      map[string]interface{}{"uuid": "test-uuid"},
		},
	}
	rc.Payload.Action = map[string]interface{}{
		"spec": map[string]interface{}{"action": "start"},
	}
	rc.Payload.Governor["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{
					"entry_points": map[string]interface{}{"start": "disabled"},
				},
				"sandboxes": []interface{}{map[string]interface{}{"kind": "AwsSandbox"}},
			},
		},
	}

	err := handleStart(rc)
	if err != nil {
		t.Fatalf("handleStart error: %v", err)
	}
}

func TestHandleStopDeployerDisabled(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.ActionName = "stop"
	rc.Payload.Subject["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"current_state": "started",
			"job_vars":      map[string]interface{}{"uuid": "test-uuid"},
		},
	}
	rc.Payload.Action = map[string]interface{}{
		"spec": map[string]interface{}{"action": "stop"},
	}
	rc.Payload.Governor["spec"] = map[string]interface{}{
		"vars": map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{
					"entry_points": map[string]interface{}{"stop": "disabled"},
				},
				"sandboxes": []interface{}{map[string]interface{}{"kind": "AwsSandbox"}},
				"sandbox_api": map[string]interface{}{
					"actions": map[string]interface{}{
						"stop": map[string]interface{}{"enable": true},
					},
				},
			},
		},
	}

	err := handleStop(rc)
	if err != nil {
		t.Fatalf("handleStop error: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run "TestHandleStart|TestHandleStop" -v
```
Expected: FAIL

- [ ] **Step 3: Implement start handler**

```go
// handler_start.go
package main

import (
	"log/slog"
)

// handleStart mirrors handle-action-start.yaml.
func handleStart(rc *RunContext) error {
	if rc.CurrentState() != "starting" {
		return runStart(rc)
	}
	if !rc.DeployerDisabled("start") {
		return checkDeployerJob(rc, "start")
	}
	return nil
}

// runStart mirrors run-start.yaml.
func runStart(rc *RunContext) error {
	// Set startTimestamp
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"start": map[string]interface{}{
						"startTimestamp": nowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}

	// Sandbox API start if enabled
	if rc.SandboxAPIInUse() && rc.sandboxActionEnabled("start") {
		// TODO: sandbox_api_login + sandbox_api_start
		slog.Info("sandbox API start needed", "subject", rc.SubjectName)

		if rc.DeployerDisabled("start") {
			// No deployer — sandbox start is the whole operation
			err := rc.SubjectUpdate(SubjectPatch{
				Patch: PatchBody{
					Metadata: &PatchMetadata{
						Labels: map[string]string{"state": "started"},
					},
					Spec: &PatchSpec{
						Vars: map[string]interface{}{"current_state": "started"},
					},
					Status: map[string]interface{}{
						"actions": map[string]interface{}{
							"start": map[string]interface{}{
								"completeTimestamp": nowUTC(),
							},
						},
					},
					SkipUpdateProcessing: true,
				},
			})
			if err != nil {
				return err
			}
			rc.FinishAction("successful")
			return nil
		}
	}

	if !rc.DeployerDisabled("start") {
		if rc.SandboxAPIInUse() {
			// TODO: sandbox_get
			slog.Info("sandbox get needed for start", "subject", rc.SubjectName)
		}
		// TODO: Launch Tower start job
		slog.Info("tower job launch needed for start", "subject", rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	return nil
}

// handleStartComplete mirrors handle-action-start-complete.yaml.
func handleStartComplete(rc *RunContext) error {
	now := nowUTC()
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "started"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{"current_state": "started"},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"start": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"start": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "successful",
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	rc.FinishAction("successful")
	return nil
}

// sandboxActionEnabled checks if a sandbox API action is enabled in __meta__.
func (rc *RunContext) sandboxActionEnabled(action string) bool {
	meta := rc.Meta()
	if meta == nil {
		return true // default enabled
	}
	sandboxAPI, _ := meta["sandbox_api"].(map[string]interface{})
	if sandboxAPI == nil {
		return true
	}
	actions, _ := sandboxAPI["actions"].(map[string]interface{})
	if actions == nil {
		return true
	}
	actionCfg, _ := actions[action].(map[string]interface{})
	if actionCfg == nil {
		return true
	}
	enable, ok := actionCfg["enable"].(bool)
	if !ok {
		return true
	}
	return enable
}
```

- [ ] **Step 4: Implement stop handler**

```go
// handler_stop.go
package main

import (
	"log/slog"
)

// handleStop mirrors handle-action-stop.yaml.
func handleStop(rc *RunContext) error {
	if rc.CurrentState() != "stopping" {
		return runStop(rc)
	}
	if !rc.DeployerDisabled("stop") {
		return checkDeployerJob(rc, "stop")
	}
	return nil
}

// runStop mirrors run-stop.yaml.
func runStop(rc *RunContext) error {
	// Set startTimestamp
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"startTimestamp": nowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}

	if !rc.DeployerDisabled("stop") {
		if rc.SandboxAPIInUse() {
			// TODO: sandbox_get
			slog.Info("sandbox get needed for stop", "subject", rc.SubjectName)
		}
		// TODO: Launch Tower stop job
		slog.Info("tower job launch needed for stop", "subject", rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	// Deployer disabled + sandbox API → run sandbox stop
	if rc.SandboxAPIInUse() && rc.sandboxActionEnabled("stop") {
		// TODO: sandbox_api_login + sandbox_api_stop
		slog.Info("sandbox API stop needed", "subject", rc.SubjectName)

		err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Metadata: &PatchMetadata{
					Labels: map[string]string{"state": "stopped"},
				},
				Spec: &PatchSpec{
					Vars: map[string]interface{}{"current_state": "stopped"},
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"stop": map[string]interface{}{
							"completeTimestamp": nowUTC(),
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		})
		if err != nil {
			return err
		}
		rc.FinishAction("successful")
	}

	return nil
}

// handleStopComplete mirrors handle-action-stop-complete.yaml.
func handleStopComplete(rc *RunContext) error {
	now := nowUTC()

	// Update completion timestamps
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "successful",
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Sandbox API stop if enabled
	if rc.SandboxAPIInUse() && rc.sandboxActionEnabled("stop") {
		// TODO: sandbox_api_login + sandbox_api_stop
		slog.Info("sandbox API stop on complete", "subject", rc.SubjectName)
	}

	// Set stopped state
	err = rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "stopped"},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{"current_state": "stopped"},
			},
		},
	})
	if err != nil {
		return err
	}

	rc.FinishAction("successful")
	return nil
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd babylon-runner && go test -run "TestHandleStart|TestHandleStop" -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add babylon-runner/handler_start.go babylon-runner/handler_stop.go babylon-runner/handlers_test.go
git commit -m "feat(babylon-runner): add start/stop handlers"
```

---

### Task 10: Action Status and Update Handlers

**Files:**
- Create: `babylon-runner/handler_status.go`
- Create: `babylon-runner/handler_update.go`

- [ ] **Step 1: Implement status handler**

```go
// handler_status.go
package main

import (
	"log/slog"
)

// handleStatus mirrors handle-action-status.yaml.
func handleStatus(rc *RunContext) error {
	subjectVars := getNestedMap(rc.Payload.Subject, "spec", "vars")
	checkStatusState, _ := subjectVars["check_status_state"].(string)

	if checkStatusState == "pending" {
		return runStatus(rc)
	}
	if checkStatusState == "running" && !rc.DeployerDisabled("status") {
		return checkDeployerJob(rc, "status")
	}
	return nil
}

// runStatus mirrors run-status.yaml.
func runStatus(rc *RunContext) error {
	// Set startTimestamp
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"status": map[string]interface{}{
						"startTimestamp": nowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}

	if !rc.DeployerDisabled("status") {
		if rc.SandboxAPIInUse() {
			// TODO: sandbox_get
			slog.Info("sandbox get needed for status", "subject", rc.SubjectName)
		}
		// TODO: Launch Tower status job
		slog.Info("tower job launch needed for status", "subject", rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	return nil
}
```

- [ ] **Step 2: Implement update handler**

```go
// handler_update.go
package main

import (
	"log/slog"
)

// handleUpdate mirrors handle-action-update.yaml.
func handleUpdate(rc *RunContext) error {
	if rc.CurrentState() != "updating" {
		return runUpdate(rc)
	}
	return checkDeployerJob(rc, "update")
}

// runUpdate mirrors run-update.yaml.
func runUpdate(rc *RunContext) error {
	if rc.SandboxAPIInUse() {
		// TODO: sandbox_get
		slog.Info("sandbox get needed for update", "subject", rc.SubjectName)
	}

	// TODO: Launch Tower update job
	slog.Info("tower job launch needed for update", "subject", rc.SubjectName)
	return rc.ContinueAction("5m")
}
```

- [ ] **Step 3: Run tests**

```bash
cd babylon-runner && go test ./... -v
```
Expected: PASS (all tests)

- [ ] **Step 4: Commit**

```bash
git add babylon-runner/handler_status.go babylon-runner/handler_update.go
git commit -m "feat(babylon-runner): add status and update handlers"
```

---

### Task 11: Check Deployer Job

**Files:**
- Create: `babylon-runner/handler_check_deployer.go`

This is the shared function that polls a Tower job's status and routes to the appropriate complete/error/failed handler.

- [ ] **Step 1: Write check deployer job test**

Add to `handlers_test.go`:

```go
func TestCheckDeployerJobSuccessful(t *testing.T) {
	// Mock Tower server returning successful job
	towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": 1, "token": "tok"})
		default:
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":     42,
				"status": "successful",
			})
		}
	}))
	defer towerServer.Close()

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.ActionName = "provision"
	rc.Payload.Subject["status"] = map[string]interface{}{
		"towerJobs": map[string]interface{}{
			"provision": map[string]interface{}{
				"deployerJob": float64(42),
				"towerHost":   towerServer.URL[len("https://"):],
			},
		},
	}
	rc.Payload.Action = map[string]interface{}{
		"spec": map[string]interface{}{"action": "provision"},
	}
	// Store tower server URL for testing
	rc.towerBaseURL = towerServer.URL

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	// Should have called provision-complete (subject update + finish)
	if len(*calls) < 1 {
		t.Fatalf("expected calls for provision complete, got %d", len(*calls))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd babylon-runner && go test -run TestCheckDeployerJob -v
```
Expected: FAIL

- [ ] **Step 3: Implement check deployer job**

```go
// handler_check_deployer.go
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// checkDeployerJob mirrors check-deployer-job.yaml.
// Polls the Tower job status and routes to the appropriate completion handler.
func checkDeployerJob(rc *RunContext, action string) error {
	towerJobs := rc.StatusTowerJobs()
	jobInfo := getNestedMap(towerJobs, action)
	if jobInfo == nil {
		return fmt.Errorf("no tower job info for action %s", action)
	}

	jobID, _ := jobInfo["deployerJob"].(float64)
	if jobID == 0 {
		return fmt.Errorf("no deployerJob ID for action %s", action)
	}
	towerHost, _ := jobInfo["towerHost"].(string)

	// Get Tower credentials from governor vars
	meta := rc.Meta()
	var towerClient *TowerClient

	if rc.towerBaseURL != "" {
		// Testing override
		towerClient = &TowerClient{
			baseURL: rc.towerBaseURL,
			client:  &http.Client{Timeout: 30 * time.Second},
		}
	} else if meta != nil {
		// TODO: Extract controller credentials from __meta__.ansible_controllers
		towerClient = NewTowerClient(towerHost, "", "")
	}

	if towerClient == nil {
		return fmt.Errorf("cannot create tower client for host %s", towerHost)
	}

	// Create OAuth token for API access
	oauthToken, tokenID, err := towerClient.CreateOAuthToken()
	if err != nil {
		// Tower unreachable — retry later, don't change state
		slog.Warn("tower unreachable, will retry", "host", towerHost, "error", err)
		return rc.ContinueAction("5m")
	}
	defer func() {
		if err := towerClient.DeleteOAuthToken(tokenID); err != nil {
			slog.Warn("failed to cleanup oauth token", "error", err)
		}
	}()

	// Get job status
	jobStatus, err := towerClient.GetJobStatus(oauthToken, int(jobID))
	if err != nil {
		slog.Warn("failed to get job status, will retry", "jobID", int(jobID), "error", err)
		return rc.ContinueAction("5m")
	}

	status, _ := jobStatus["status"].(string)
	slog.Info("deployer job status", "action", action, "jobID", int(jobID), "status", status)

	switch status {
	case "canceled", "error", "failed":
		return handleDeployerJobFailure(rc, action, status)
	case "successful":
		return handleDeployerJobSuccess(rc, action, jobStatus)
	default:
		// Still running — check again later
		return rc.ContinueAction("5m")
	}
}

func handleDeployerJobFailure(rc *RunContext, action, status string) error {
	switch action {
	case "provision":
		if status == "failed" {
			return handleProvisionFailed(rc)
		}
		return handleProvisionError(rc)
	case "destroy":
		now := nowUTC()
		rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Metadata: &PatchMetadata{
					Labels: map[string]string{"state": "destroy-" + status},
				},
				Spec: &PatchSpec{
					Vars: map[string]interface{}{
						"current_state": "destroy-" + status,
					},
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"destroy": map[string]interface{}{
							"completeTimestamp": now,
							"state":            status,
						},
					},
					"towerJobs": map[string]interface{}{
						"destroy": map[string]interface{}{
							"completeTimestamp": now,
							"jobStatus":        status,
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		})
		rc.FinishAction(status)
		return nil
	case "start":
		return handleGenericActionFailure(rc, "start", status)
	case "stop":
		return handleGenericActionFailure(rc, "stop", status)
	case "status":
		return handleGenericActionFailure(rc, "status", status)
	case "update":
		return handleGenericActionFailure(rc, "update", status)
	default:
		return fmt.Errorf("unknown action for failure handling: %s", action)
	}
}

func handleDeployerJobSuccess(rc *RunContext, action string, jobStatus map[string]interface{}) error {
	// TODO: extract provision_data, provision_message_body, provision_messages from job artifacts

	switch action {
	case "provision":
		return handleProvisionComplete(rc, nil, nil, nil)
	case "destroy":
		return handleDestroyComplete(rc)
	case "start":
		return handleStartComplete(rc)
	case "stop":
		return handleStopComplete(rc)
	case "status":
		return handleStatusComplete(rc)
	case "update":
		return handleUpdateComplete(rc)
	default:
		return fmt.Errorf("unknown action for success handling: %s", action)
	}
}

func handleGenericActionFailure(rc *RunContext, action, status string) error {
	now := nowUTC()
	rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": action + "-" + status},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": action + "-" + status,
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					action: map[string]interface{}{
						"completeTimestamp": now,
						"state":            status,
					},
				},
				"towerJobs": map[string]interface{}{
					action: map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        status,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	rc.FinishAction(status)
	return nil
}

// handleStatusComplete handles successful status check completion.
func handleStatusComplete(rc *RunContext) error {
	now := nowUTC()
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"check_status_state": "successful",
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"status": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "successful",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}
	rc.FinishAction("successful")
	return nil
}

// handleUpdateComplete handles successful update completion.
func handleUpdateComplete(rc *RunContext) error {
	now := nowUTC()
	// Restore previous state (update doesn't change the state)
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"update": map[string]interface{}{
						"completeTimestamp": now,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"update": map[string]interface{}{
						"completeTimestamp": now,
						"jobStatus":        "successful",
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	rc.FinishAction("successful")
	return nil
}
```

- [ ] **Step 4: Add `towerBaseURL` field to RunContext for testing**

In `runner.go`, add to `RunContext`:

```go
// Testing override for tower base URL
towerBaseURL string
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd babylon-runner && go test -run TestCheckDeployerJob -v
```
Expected: PASS

- [ ] **Step 6: Run all tests**

```bash
cd babylon-runner && go test ./... -v
```
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add babylon-runner/
git commit -m "feat(babylon-runner): add check deployer job and completion handlers"
```

---

### Task 12: Handler Registration and Main Wiring

**Files:**
- Modify: `babylon-runner/main.go`

Register all handlers in the runner and ensure the binary compiles and all tests pass.

- [ ] **Step 1: Update main.go to register handlers**

```go
// main.go
package main

import (
	"log/slog"
	"os"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	slog.Info("starting babylon-runner",
		"runner", cfg.RunnerName,
		"pod", cfg.PodName,
		"anarchy_url", cfg.AnarchyURL,
	)

	runner := NewRunner(cfg)
	registerHandlers(runner)
	runner.Run()
}

func registerHandlers(r *Runner) {
	// Event handlers
	r.handlers["event:create"] = handleEventCreate
	r.handlers["event:update"] = handleEventUpdate
	r.handlers["event:delete"] = handleEventDelete

	// Action handlers
	r.handlers["action:provision"] = handleProvision
	r.handlers["action:destroy"] = handleDestroy
	r.handlers["action:start"] = handleStart
	r.handlers["action:stop"] = handleStop
	r.handlers["action:status"] = handleStatus
	r.handlers["action:update"] = handleUpdate

	// Callback handlers (provision complete from Tower callback)
	r.handlers["action:provision:complete"] = func(rc *RunContext) error {
		return handleProvisionComplete(rc, nil, nil, nil)
	}
}
```

- [ ] **Step 2: Build and verify compilation**

```bash
cd babylon-runner && go build ./...
```
Expected: successful build

- [ ] **Step 3: Run all tests**

```bash
cd babylon-runner && go test ./... -v
```
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add babylon-runner/main.go
git commit -m "feat(babylon-runner): wire up handler registration in main"
```

---

### Task 13: Dockerfile

**Files:**
- Create: `babylon-runner/Dockerfile`

- [ ] **Step 1: Create Dockerfile**

```dockerfile
FROM golang:1.22 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o babylon-runner .

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/babylon-runner /babylon-runner
ENTRYPOINT ["/babylon-runner"]
```

- [ ] **Step 2: Test the build**

```bash
cd babylon-runner && docker build -t babylon-runner:dev .
```
Expected: successful build

- [ ] **Step 3: Commit**

```bash
git add babylon-runner/Dockerfile
git commit -m "feat(babylon-runner): add Dockerfile"
```

---

## Remaining Work (TODOs in the code)

These TODOs are marked in the handler code and will be addressed in follow-up tasks:

1. **Connect SandboxAPIClient to RunContext** — Wire up `sandbox_get`, `sandbox_book`, `sandbox_cleanup`, `sandbox_api_start`, `sandbox_api_stop` calls in the handlers. Currently logged as "needed but not implemented".

2. **Connect TowerClient to RunContext** — Wire up `check-run-tower-job` (full Tower resource creation + job launch) in `runProvision`, `runDestroy`, `runStart`, `runStop`, `runStatus`, `runUpdate`. Currently falls through to `ContinueAction`.

3. **Extract provision info from Tower job** — In `handleDeployerJobSuccess`, extract `provision_data`, `provision_message_body`, `provision_messages` from Tower job artifacts.

4. **Subject delete with remove_finalizers** — Include `deleteSubject` directive in the POST /run result. Currently `FinishAction` is called but subject deletion is not signaled.

5. **Cancel Tower jobs in event delete** — Loop through incomplete tower jobs and cancel them before scheduling destroy.

6. **Operator-side changes** — Add `spec.runner` field to AnarchyGovernor CRD and route runs to the appropriate runner pool.

Each of these is a focused follow-up task that builds on the working skeleton.
