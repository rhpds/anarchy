# Babylon Runner Phase 2 (Correctness) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply correctness fixes and safety mechanisms to the babylon-runner Go binary, enabling production deployment. All changes are surgical modifications to existing, validated code.

**Architecture:** Phase 1 is complete — the project has a standard Go layout under `babylon-runner/internal/` with typed payloads, `client-go`, shared HTTP infrastructure, and all handlers tested on a real cluster. Phase 2 threads `context.Context` through all operations, adds TLS configuration, makes hardcoded constants configurable, replaces shallow merge with deep merge, adds the missing `guid` field, adds panic recovery, and fixes the status handler's orphaned action when deployer is disabled.

**Tech Stack:** Go 1.22+, `k8s.io/client-go`, standard library (`crypto/tls`, `context`, `runtime/debug`)

## Global Constraints

- All changes target existing files in `babylon-runner/internal/`. No new packages.
- All existing tests must continue to pass after each task (`go test ./internal/...`).
- New behaviour must have corresponding `*_test.go` coverage.
- No `Co-Authored-By` in commit messages. No phase mention in commit messages.
- The `MergeMap` function is renamed to `DeepMergeMap` — all 8 call sites must be updated.

---

### Task 1: Deep Merge (#10)

Replace the shallow `MergeMap` with a recursive `DeepMergeMap` that merges nested `map[string]interface{}` values instead of overwriting them. This matches Ansible's `combine(recursive=True)` behaviour used in `buildJobExtraVars`.

**Files:**

- Modify: `babylon-runner/internal/types/helpers.go:73-79` — replace `MergeMap` with `DeepMergeMap`
- Modify: `babylon-runner/internal/types/helpers_test.go:81-88` — update test + add deep merge cases
- Modify: `babylon-runner/internal/handler/tower_launch.go:161,165,169,207` — rename calls
- Modify: `babylon-runner/internal/handler/sandbox.go:124,599` — rename calls
- Modify: `babylon-runner/internal/handler/provision.go:252,407` — rename calls

**Interfaces:**

- Produces: `func DeepMergeMap(dst, src map[string]interface{})` — same signature as `MergeMap` but recursive

- [ ] **Step 1: Write the failing test for deep merge**

Add test cases to `babylon-runner/internal/types/helpers_test.go` that exercise nested map merging:

```go
func TestDeepMergeMap(t *testing.T) {
	t.Run("shallow keys", func(t *testing.T) {
		dst := map[string]interface{}{"a": 1, "b": 2}
		src := map[string]interface{}{"b": 3, "c": 4}
		DeepMergeMap(dst, src)
		if dst["a"] != 1 || dst["b"] != 3 || dst["c"] != 4 {
			t.Errorf("unexpected result: %v", dst)
		}
	})

	t.Run("nested map merge", func(t *testing.T) {
		dst := map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{"timeout": 300},
				"sandbox":  "keep-this",
			},
		}
		src := map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{"retries": 3},
				"new_key":  "added",
			},
		}
		DeepMergeMap(dst, src)
		meta := dst["__meta__"].(map[string]interface{})
		deployer := meta["deployer"].(map[string]interface{})
		if deployer["timeout"] != 300 {
			t.Error("deep merge lost existing nested key 'timeout'")
		}
		if deployer["retries"] != 3 {
			t.Error("deep merge did not add 'retries'")
		}
		if meta["sandbox"] != "keep-this" {
			t.Error("deep merge lost sibling key 'sandbox'")
		}
		if meta["new_key"] != "added" {
			t.Error("deep merge did not add 'new_key'")
		}
	})

	t.Run("src overwrites non-map with map", func(t *testing.T) {
		dst := map[string]interface{}{"key": "string-value"}
		src := map[string]interface{}{"key": map[string]interface{}{"nested": true}}
		DeepMergeMap(dst, src)
		if _, ok := dst["key"].(map[string]interface{}); !ok {
			t.Error("expected map to replace string")
		}
	})

	t.Run("src overwrites map with non-map", func(t *testing.T) {
		dst := map[string]interface{}{"key": map[string]interface{}{"nested": true}}
		src := map[string]interface{}{"key": "string-value"}
		DeepMergeMap(dst, src)
		if dst["key"] != "string-value" {
			t.Errorf("expected string, got %v", dst["key"])
		}
	})

	t.Run("nil src value", func(t *testing.T) {
		dst := map[string]interface{}{"a": 1}
		src := map[string]interface{}{"a": nil}
		DeepMergeMap(dst, src)
		if dst["a"] != nil {
			t.Errorf("expected nil, got %v", dst["a"])
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd babylon-runner && go test ./internal/types/ -run TestDeepMergeMap -v`
Expected: FAIL — `DeepMergeMap` not defined

- [ ] **Step 3: Implement DeepMergeMap and remove MergeMap**

Replace the function in `babylon-runner/internal/types/helpers.go`:

```go
// DeepMergeMap recursively merges src into dst. When both dst[k] and src[k]
// are map[string]interface{}, their contents are merged recursively.
// Otherwise src[k] overwrites dst[k].
func DeepMergeMap(dst, src map[string]interface{}) {
	for k, v := range src {
		if srcMap, ok := v.(map[string]interface{}); ok {
			if dstMap, ok := dst[k].(map[string]interface{}); ok {
				DeepMergeMap(dstMap, srcMap)
				continue
			}
		}
		dst[k] = v
	}
}
```

Remove the old `TestMergeMap` test (or rename it to `TestDeepMergeMap` — it should already be covered by the "shallow keys" sub-test above).

- [ ] **Step 4: Rename all call sites from MergeMap to DeepMergeMap**

Update these 8 locations (search: `types.MergeMap`):

- `internal/handler/tower_launch.go`: lines 161, 165, 169, 207
- `internal/handler/sandbox.go`: lines 124, 599
- `internal/handler/provision.go`: lines 252, 407

Each is a simple rename: `types.MergeMap(` → `types.DeepMergeMap(`

- [ ] **Step 5: Run all tests to verify nothing breaks**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add babylon-runner/internal/types/helpers.go babylon-runner/internal/types/helpers_test.go \
       babylon-runner/internal/handler/tower_launch.go babylon-runner/internal/handler/sandbox.go \
       babylon-runner/internal/handler/provision.go
git commit -m "feat(babylon-runner): replace shallow MergeMap with recursive DeepMergeMap

Matches Ansible's combine(recursive=True) behaviour. buildJobExtraVars
merges governor and subject job_vars which both contain __meta__
sub-trees — shallow merge was overwriting entire nested maps."
```

---

### Task 2: Missing guid in Event Create (#13)

Add `guid` to the initial variables set by `handleEventCreate`, matching the Ansible role which sets both `uuid` and `guid`. The `guid` uses the same value as `uuid` (matching the Ansible `defaults/main.yaml` where `guid` defaults to `uuid`).

**Files:**

- Modify: `babylon-runner/internal/handler/event_create.go:49-53` — add guid field
- Modify: `babylon-runner/internal/handler/handler_test.go` or create new test — add test for guid

**Interfaces:**

- Consumes: `rc.JobVars()` to check for existing guid; `rc.SubjectUpdate(...)` to patch

- [ ] **Step 1: Write the failing test**

Find the existing test for `handleEventCreate` in `babylon-runner/internal/handler/handler_test.go` (or `integration_test.go`). Add a test case that verifies `guid` is set in the subject update patch. If no dedicated test exists for event_create, create one in `handler_test.go`:

```go
func TestHandleEventCreate_SetsGUID(t *testing.T) {
	var patchBody types.SubjectPatch

	anarchyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch:
			json.NewDecoder(r.Body).Decode(&patchBody)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer anarchyServer.Close()

	rc := testRunContext(anarchyServer.URL, types.RunPayload{
		Handler: types.Handler{Type: "subjectEvent", Name: "create"},
		Governor: types.Governor{
			Spec: types.GovernorSpec{
				Vars: types.GovernorVars{
					JobVars: map[string]interface{}{
						"cloud_provider": "aws",
					},
				},
			},
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subj"},
			Spec: types.SubjectSpec{
				Vars: types.SubjectVars{
					CurrentState: "", // triggers initialization
				},
			},
		},
		Run: types.Run{Metadata: types.ObjectMeta{Name: "test-run"}},
	})

	if err := handleEventCreate(rc); err != nil {
		t.Fatalf("handleEventCreate: %v", err)
	}

	jobVars, ok := patchBody.Patch.Spec.Vars["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("expected job_vars in patch")
	}
	guid, ok := jobVars["guid"].(string)
	if !ok || guid == "" {
		t.Error("guid not set in job_vars patch")
	}
	uuid, ok := jobVars["uuid"].(string)
	if !ok || uuid == "" {
		t.Fatal("uuid not set in job_vars patch")
	}
	if guid != uuid {
		t.Errorf("guid (%q) should equal uuid (%q)", guid, uuid)
	}
}
```

Note: Adapt the `testRunContext` helper based on what already exists in the test file. The key assertion is that `guid` exists in the patch and equals `uuid`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd babylon-runner && go test ./internal/handler/ -run TestHandleEventCreate_SetsGUID -v`
Expected: FAIL — `guid` key missing from patch

- [ ] **Step 3: Add guid to jobVarsPatch**

In `babylon-runner/internal/handler/event_create.go`, add `guid` to the patch map at line 53. The `guid` uses the same value as `uuid` (matching Ansible defaults):

```go
		jobVarsPatch := map[string]interface{}{
			"cloud_provider": cloudProvider,
			"platform":       platform,
			"uuid":           subjectUUID,
			"guid":           subjectUUID,
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd babylon-runner && go test ./internal/handler/ -run TestHandleEventCreate_SetsGUID -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add babylon-runner/internal/handler/event_create.go babylon-runner/internal/handler/handler_test.go
git commit -m "fix(babylon-runner): set guid in handleEventCreate

The Ansible role sets both uuid and guid during event creation.
The Go runner was only setting uuid, leaving guid empty. Downstream
sandbox operations use guid for placement booking."
```

---

### Task 3: Context Propagation (#4)

Thread `context.Context` from the poll loop through `RunContext` into all handler operations. Currently 9 call sites use `context.TODO()`, which prevents graceful shutdown from cancelling in-flight HTTP calls.

**Files:**

- Modify: `babylon-runner/internal/runner/run_context.go` — add `Ctx context.Context` field; update `SubjectUpdate` and `ScheduleAction` delegates
- Modify: `babylon-runner/internal/runner/runner.go:94-102` — set `Ctx` when creating RunContext
- Modify: `babylon-runner/internal/handler/provision.go:344` — replace `context.TODO()` with `rc.Ctx`
- Modify: `babylon-runner/internal/handler/sandbox.go:84,207,251,272,319` — replace `context.TODO()` with `rc.Ctx`
- Modify: `babylon-runner/internal/handler/tower_launch.go:126` — replace `context.TODO()` with `rc.Ctx`
- Modify: `babylon-runner/internal/runner/runner_test.go` — update tests that create `RunContext` directly

**Interfaces:**

- Produces: `RunContext.Ctx` field of type `context.Context`

- [ ] **Step 1: Add Ctx field to RunContext**

In `babylon-runner/internal/runner/run_context.go`, add the field to the struct:

```go
type RunContext struct {
	Ctx                  context.Context
	Payload              types.RunPayload
	Result               types.RunResult
	AnarchyClient        *clients.AnarchyClient
	Clientset            kubernetes.Interface
	TowerBaseURL         string
	SandboxBaseURL       string
	DefaultSandboxAPIURL string
	SandboxClientOpts    []clients.SandboxAPIOption
}
```

- [ ] **Step 2: Update the delegates to use rc.Ctx**

In `babylon-runner/internal/runner/run_context.go`, replace `context.TODO()` in both delegates:

```go
func (rc *RunContext) SubjectUpdate(patch types.SubjectPatch) error {
	return rc.AnarchyClient.SubjectUpdate(rc.Ctx, rc.SubjectName(), patch)
}

func (rc *RunContext) ScheduleAction(req types.ScheduleActionRequest) error {
	return rc.AnarchyClient.ScheduleAction(rc.Ctx, rc.SubjectName(), req)
}
```

- [ ] **Step 3: Set Ctx when creating RunContext in pollOnce**

In `babylon-runner/internal/runner/runner.go`, update the RunContext creation (around line 94):

```go
	rc := &RunContext{
		Ctx:                  ctx,
		Payload:              *payload,
		AnarchyClient:        r.anarchy,
		Clientset:            r.clientset,
		DefaultSandboxAPIURL: r.config.SandboxAPIURL,
		Result: types.RunResult{
			Status: "successful",
		},
	}
```

- [ ] **Step 4: Replace context.TODO() in handlers**

Replace each `context.TODO()` with `rc.Ctx` in the following files:

**`internal/handler/provision.go`** (1 occurrence):

```go
// Line ~344: change
ctx := context.TODO()
// to
ctx := rc.Ctx
```

**`internal/handler/sandbox.go`** (5 occurrences):
Each function that does `ctx := context.TODO()` should change to `ctx := rc.Ctx`:

- `sandboxGet` (line ~84)
- `sandboxBook` (line ~207)
- `sandboxCleanup` (line ~251)
- `sandboxStart` (line ~272)
- `sandboxStop` (line ~319)

**`internal/handler/tower_launch.go`** (1 occurrence):

```go
// Line ~126: change
secrets, err := rc.Clientset.CoreV1().Secrets(ns).List(context.TODO(), metav1.ListOptions{
// to
secrets, err := rc.Clientset.CoreV1().Secrets(ns).List(rc.Ctx, metav1.ListOptions{
```

Remove unused `"context"` imports from handler files if no other `context` usage remains.

- [ ] **Step 5: Update existing tests that create RunContext**

In `babylon-runner/internal/runner/runner_test.go`, add `Ctx: context.Background()` to all `RunContext` literals. Also update any handler tests that construct `RunContext` directly.

Search for `&RunContext{` across all test files and add the `Ctx` field. For the `pollOnce` tests, `ctx` is already passed correctly since `pollOnce(ctx)` sets it.

In `runner.go`'s `pollOnce`, the `ctx` is already the function parameter — no change needed since we set `Ctx: ctx` in step 3.

- [ ] **Step 6: Run all tests**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add babylon-runner/internal/runner/run_context.go babylon-runner/internal/runner/runner.go \
       babylon-runner/internal/handler/provision.go babylon-runner/internal/handler/sandbox.go \
       babylon-runner/internal/handler/tower_launch.go babylon-runner/internal/runner/runner_test.go \
       babylon-runner/internal/handler/
git commit -m "feat(babylon-runner): propagate context.Context through RunContext

Threads the poll loop context into all handler operations via
RunContext.Ctx. Replaces 9 context.TODO() call sites. Enables
graceful shutdown to cancel in-flight HTTP calls and retry loops
instead of blocking until they complete."
```

---

### Task 4: TLS Configuration (#5)

Make Tower TLS verification configurable via environment variables instead of hardcoding `InsecureSkipVerify: true`. The httputil package already provides `NewTLSConfig` and `NewTransport` — the Tower client just needs to use them.

**Files:**

- Modify: `babylon-runner/internal/runner/config.go` — add `TowerTLSVerify bool` and `TowerCACert string` fields
- Modify: `babylon-runner/internal/runner/config_test.go` — add tests for new env vars
- Modify: `babylon-runner/internal/clients/tower.go:118-129` — accept TLS config parameter
- Modify: `babylon-runner/internal/clients/tower_test.go` — update NewTowerClient calls
- Modify: `babylon-runner/internal/handler/tower_launch.go` — pass TLS config when creating TowerClient
- Modify: `babylon-runner/internal/runner/run_context.go` — add TowerTLSConfig field

**Interfaces:**

- Consumes: `httputil.NewTLSConfig(verify, caPath)`, `httputil.NewTransport(tlsConfig)`
- Produces: `Config.TowerTLSVerify`, `Config.TowerCACert`, `RunContext.TowerTLSConfig`

- [ ] **Step 1: Write config test for new env vars**

Add to `babylon-runner/internal/runner/config_test.go`:

```go
func TestConfigTowerTLS(t *testing.T) {
	setRequiredEnvs(t)

	t.Run("defaults to verify=true", func(t *testing.T) {
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if !cfg.TowerTLSVerify {
			t.Error("TowerTLSVerify should default to true")
		}
	})

	t.Run("TOWER_TLS_VERIFY=false", func(t *testing.T) {
		t.Setenv("TOWER_TLS_VERIFY", "false")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.TowerTLSVerify {
			t.Error("TowerTLSVerify should be false")
		}
	})

	t.Run("TOWER_CA_CERT set", func(t *testing.T) {
		t.Setenv("TOWER_CA_CERT", "/etc/pki/ca.crt")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.TowerCACert != "/etc/pki/ca.crt" {
			t.Errorf("TowerCACert = %q, want /etc/pki/ca.crt", cfg.TowerCACert)
		}
	})
}
```

Note: Use or create a `setRequiredEnvs(t)` helper that sets `ANARCHY_URL`, `RUNNER_NAME`, `RUNNER_TOKEN`, `HOSTNAME` for the test.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd babylon-runner && go test ./internal/runner/ -run TestConfigTowerTLS -v`
Expected: FAIL — `TowerTLSVerify` field not defined

- [ ] **Step 3: Add TLS fields to Config struct and parsing**

In `babylon-runner/internal/runner/config.go`, add fields and parsing:

```go
type Config struct {
	AnarchyURL      string
	RunnerName      string
	RunnerToken     string
	PodName         string
	PollingInterval time.Duration
	RequestTimeout  time.Duration
	SandboxAPIURL   string
	TowerTLSVerify  bool
	TowerCACert     string
}
```

In `ConfigFromEnv()`, add after the SandboxAPIURL parsing:

```go
	cfg.TowerTLSVerify = envBool("TOWER_TLS_VERIFY", true)
	cfg.TowerCACert = os.Getenv("TOWER_CA_CERT")
```

Add the `envBool` helper:

```go
func envBool(key string, defaultVal bool) bool {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return defaultVal
	}
	return v
}
```

- [ ] **Step 4: Add TowerTLSConfig to RunContext**

In `babylon-runner/internal/runner/run_context.go`, add:

```go
type RunContext struct {
	Ctx                  context.Context
	Payload              types.RunPayload
	Result               types.RunResult
	AnarchyClient        *clients.AnarchyClient
	Clientset            kubernetes.Interface
	TowerBaseURL         string
	SandboxBaseURL       string
	DefaultSandboxAPIURL string
	SandboxClientOpts    []clients.SandboxAPIOption
	TowerTLSConfig       *tls.Config
}
```

Add `"crypto/tls"` to imports.

- [ ] **Step 5: Update main.go / runner.go to build and pass TLS config**

In `babylon-runner/internal/runner/runner.go`, update `New` to accept TLS config and pass it into RunContext. Alternatively, build the TLS config in `cmd/babylon-runner/main.go` and pass it to `New`.

The simplest approach: add `TowerTLSConfig *tls.Config` to the `Runner` struct, set it in `New`, and propagate it to RunContext in `pollOnce`.

In `runner.go`:

```go
type Runner struct {
	config         Config
	client         *http.Client
	anarchy        *clients.AnarchyClient
	clientset      kubernetes.Interface
	handlers       map[string]HandlerFunc
	postRetryDelay time.Duration
	towerTLSConfig *tls.Config
}
```

In `cmd/babylon-runner/main.go`, after config parsing:

```go
	towerTLSConfig, err := httputil.NewTLSConfig(cfg.TowerTLSVerify, cfg.TowerCACert)
	if err != nil {
		log.Fatalf("tower TLS config: %v", err)
	}
```

Pass it to `runner.New()`. Update `New` signature to accept it:

```go
func New(cfg Config, clientset kubernetes.Interface, towerTLSConfig *tls.Config) *Runner {
```

In `pollOnce`, set it on the RunContext:

```go
	rc := &RunContext{
		...
		TowerTLSConfig:       r.towerTLSConfig,
	}
```

- [ ] **Step 6: Update TowerClient to accept TLS config**

In `babylon-runner/internal/clients/tower.go`, change `NewTowerClient`:

```go
func NewTowerClient(hostname, username, password string, tlsConfig *tls.Config) *TowerClient {
	return &TowerClient{
		baseURL:  "https://" + hostname,
		username: username,
		password: password,
		client: &http.Client{
			Transport: httputil.NewTransport(tlsConfig),
		},
	}
}
```

Add imports for `"crypto/tls"` and `httputil`.

- [ ] **Step 7: Update tower_launch.go to pass TLS config**

Find where `NewTowerClient` is called in `tower_launch.go` and pass `rc.TowerTLSConfig`:

```go
tc := clients.NewTowerClient(hostname, username, password, rc.TowerTLSConfig)
```

- [ ] **Step 8: Update all tests**

Update `NewTowerClient` calls in test files to pass `nil` as the TLS config (uses Go defaults):

- `internal/clients/tower_test.go`
- `internal/handler/tower_launch_test.go`
- `internal/handler/check_deployer_test.go`
- Any other test that calls `NewTowerClient`

Update `runner.New(cfg, clientset)` calls to `runner.New(cfg, clientset, nil)` in tests.

- [ ] **Step 9: Run all tests**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 10: Commit**

```bash
git add babylon-runner/internal/runner/config.go babylon-runner/internal/runner/config_test.go \
       babylon-runner/internal/runner/run_context.go babylon-runner/internal/runner/runner.go \
       babylon-runner/internal/runner/runner_test.go babylon-runner/internal/clients/tower.go \
       babylon-runner/internal/clients/tower_test.go babylon-runner/internal/handler/tower_launch.go \
       babylon-runner/internal/handler/tower_launch_test.go babylon-runner/internal/handler/check_deployer_test.go \
       babylon-runner/cmd/babylon-runner/main.go
git commit -m "feat(babylon-runner): make Tower TLS verification configurable

Adds TOWER_TLS_VERIFY (default true) and TOWER_CA_CERT env vars.
Tower client now uses httputil.NewTransport with configurable TLS
instead of hardcoded InsecureSkipVerify. For environments with
self-signed certs, set TOWER_TLS_VERIFY=false."
```

---

### Task 5: Configuration for Hardcoded Constants (#7)

Make `ACTION_RETRY_INTERVALS` configurable via environment variable. The `SANDBOX_API_URL` is already configurable (done in Phase 1). The retry intervals are currently a hardcoded slice in `check_deployer.go`.

**Files:**

- Modify: `babylon-runner/internal/runner/config.go` — add `DefaultActionRetryIntervals` var and `ActionRetryIntervals []string` field
- Modify: `babylon-runner/internal/runner/config_test.go` — add test for `ACTION_RETRY_INTERVALS`
- Modify: `babylon-runner/internal/runner/run_context.go` — add `ActionRetryIntervals` field
- Modify: `babylon-runner/internal/runner/runner.go` — pass intervals to RunContext
- Modify: `babylon-runner/internal/handler/check_deployer.go:90-101` — read intervals from RunContext

**Interfaces:**

- Produces: `Config.ActionRetryIntervals`, `RunContext.ActionRetryIntervals`

- [ ] **Step 1: Write config test**

Add to `babylon-runner/internal/runner/config_test.go`:

```go
func TestConfigActionRetryIntervals(t *testing.T) {
	setRequiredEnvs(t)

	t.Run("default intervals", func(t *testing.T) {
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		expected := []string{"1m", "5m", "10m", "30m", "1h", "2h", "4h", "8h", "16h", "1d"}
		if len(cfg.ActionRetryIntervals) != len(expected) {
			t.Fatalf("len = %d, want %d", len(cfg.ActionRetryIntervals), len(expected))
		}
		for i, v := range expected {
			if cfg.ActionRetryIntervals[i] != v {
				t.Errorf("[%d] = %q, want %q", i, cfg.ActionRetryIntervals[i], v)
			}
		}
	})

	t.Run("custom intervals", func(t *testing.T) {
		t.Setenv("ACTION_RETRY_INTERVALS", "30s,2m,10m")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if len(cfg.ActionRetryIntervals) != 3 {
			t.Fatalf("len = %d, want 3", len(cfg.ActionRetryIntervals))
		}
		if cfg.ActionRetryIntervals[0] != "30s" {
			t.Errorf("[0] = %q, want 30s", cfg.ActionRetryIntervals[0])
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd babylon-runner && go test ./internal/runner/ -run TestConfigActionRetryIntervals -v`
Expected: FAIL — `ActionRetryIntervals` field not defined

- [ ] **Step 3: Add default var, field, and parsing**

In `babylon-runner/internal/runner/config.go`, add the default var following the same pattern as `DefaultSandboxAPIURL`:

```go
// DefaultActionRetryIntervals is the default retry schedule for failed actions.
var DefaultActionRetryIntervals = []string{
	"1m", "5m", "10m", "30m", "1h", "2h", "4h", "8h", "16h", "1d",
}
```

Add the field to Config:

```go
type Config struct {
	// ... existing fields ...
	ActionRetryIntervals []string
}
```

In `ConfigFromEnv()`, add parsing:

```go
	cfg.ActionRetryIntervals = envStringSlice("ACTION_RETRY_INTERVALS", DefaultActionRetryIntervals)
```

Add helper:

```go
func envStringSlice(key string, defaultVal []string) []string {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return defaultVal
	}
	return result
}
```

Add `"strings"` to imports.

- [ ] **Step 4: Add ActionRetryIntervals to RunContext and propagate**

In `babylon-runner/internal/runner/run_context.go`, add field:

```go
type RunContext struct {
	// ... existing fields ...
	ActionRetryIntervals []string
}
```

In `babylon-runner/internal/runner/runner.go` `pollOnce`, set it:

```go
	rc := &RunContext{
		// ... existing fields ...
		ActionRetryIntervals: r.config.ActionRetryIntervals,
	}
```

- [ ] **Step 5: Update check_deployer.go to use RunContext intervals**

In `babylon-runner/internal/handler/check_deployer.go`, change `actionRetryInterval` to accept the intervals slice:

```go
func actionRetryInterval(retryCount int, intervals []string) string {
	if retryCount < len(intervals) {
		return intervals[retryCount]
	}
	return intervals[len(intervals)-1]
}
```

Update all call sites in the same file:

- `continueWithRetry` (line ~106): `interval := actionRetryInterval(count, rc.ActionRetryIntervals)`
- `handleUpdateFailure` (line ~443): `interval := actionRetryInterval(rc.ActionRetryCount(), rc.ActionRetryIntervals)`

Remove the package-level `actionRetryIntervals` var.

- [ ] **Step 6: Update check_deployer_test.go**

Update the `actionRetryInterval` test to pass the intervals slice:

```go
func TestActionRetryInterval(t *testing.T) {
	intervals := []string{"1m", "5m", "10m", "30m", "1h", "2h", "4h", "8h", "16h", "1d"}
	// ... existing test logic but call actionRetryInterval(i, intervals) ...
}
```

Also update any test that creates a `RunContext` for check_deployer handlers to include `ActionRetryIntervals`.

- [ ] **Step 7: Run all tests**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add babylon-runner/internal/runner/config.go babylon-runner/internal/runner/config_test.go \
       babylon-runner/internal/runner/run_context.go babylon-runner/internal/runner/runner.go \
       babylon-runner/internal/handler/check_deployer.go babylon-runner/internal/handler/check_deployer_test.go
git commit -m "feat(babylon-runner): make action retry intervals configurable

Adds ACTION_RETRY_INTERVALS env var (comma-separated duration strings).
Defaults to the existing 1m,5m,10m,30m,1h,2h,4h,8h,16h,1d schedule.
Intervals are passed through RunContext instead of a package-level var."
```

---

### Task 6: Panic Recovery (GAP-1)

Add `defer recover()` in `pollOnce` wrapping the dispatch + postResult calls. On panic, log the stack trace and post a failed result so the operator marks the run properly instead of timing out.

**Files:**

- Modify: `babylon-runner/internal/runner/runner.go:84-126` — add recovery wrapper
- Modify: `babylon-runner/internal/runner/runner_test.go` — add panic recovery test

**Interfaces:**

- No new public interfaces

- [ ] **Step 1: Write the panic recovery test**

Add to `babylon-runner/internal/runner/runner_test.go`:

```go
func TestPollOnceRecoversPanic(t *testing.T) {
	var postBody struct {
		Result types.RunResult `json:"result"`
	}
	var postCalled atomic.Int32
	runName := "test-run-panic"

	payload := types.RunPayload{
		Handler: types.Handler{
			Type: "subjectEvent",
			Name: "create",
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subject"},
		},
		Run: types.Run{
			Metadata: types.ObjectMeta{Name: runName},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/run":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(payload)
		case r.Method == http.MethodPost && r.URL.Path == fmt.Sprintf("/run/%s", runName):
			postCalled.Add(1)
			json.NewDecoder(r.Body).Decode(&postBody)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := Config{
		AnarchyURL:      server.URL,
		RunnerName:      "runner",
		PodName:         "pod",
		RunnerToken:     "token",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}

	runner := New(cfg, nil, nil)
	runner.handlers["event:create"] = func(rc *RunContext) error {
		panic("nil pointer dereference simulation")
	}

	err := runner.pollOnce(context.Background())
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postCalled.Load() != 1 {
		t.Errorf("POST call count = %d, want 1", postCalled.Load())
	}
	if postBody.Result.Status != "failed" {
		t.Errorf("result status = %q, want %q", postBody.Result.Status, "failed")
	}
	if !strings.Contains(postBody.Result.StatusMessage, "panic") {
		t.Errorf("status message = %q, should contain 'panic'", postBody.Result.StatusMessage)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd babylon-runner && go test ./internal/runner/ -run TestPollOnceRecoversPanic -v`
Expected: FAIL — panic crashes the test process

- [ ] **Step 3: Add panic recovery to pollOnce**

In `babylon-runner/internal/runner/runner.go`, wrap the dispatch call in `pollOnce`. Add the recovery between the RunContext creation and the dispatch call:

```go
func (r *Runner) pollOnce(ctx context.Context) error {
	payload, err := r.getRun(ctx)
	if err != nil {
		slog.Error("poll failed", "error", err)
		return err
	}
	if payload == nil {
		return nil
	}

	rc := &RunContext{
		Ctx:                  ctx,
		Payload:              *payload,
		AnarchyClient:        r.anarchy,
		Clientset:            r.clientset,
		DefaultSandboxAPIURL: r.config.SandboxAPIURL,
		TowerTLSConfig:       r.towerTLSConfig,
		ActionRetryIntervals: r.config.ActionRetryIntervals,
		Result: types.RunResult{
			Status: "successful",
		},
	}

	subject := rc.SubjectName()
	handlerName := payload.Handler.Name
	if handlerName == "" && payload.Action != nil {
		handlerName = payload.Action.Spec.Action
	}
	slog.Info("dispatching run",
		"run", rc.RunName(),
		"subject", subject,
		"handler", payload.Handler.Type+":"+handlerName)

	func() {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				slog.Error("handler panicked",
					"run", rc.RunName(),
					"subject", subject,
					"panic", r,
					"stack", stack)
				rc.Result.Status = "failed"
				rc.Result.StatusMessage = fmt.Sprintf("panic: %v", r)
			}
		}()

		if err := Dispatch(rc, r.handlers); err != nil {
			slog.Error("handler failed", "run", rc.RunName(), "subject", subject, "error", err)
			rc.Result.Status = "failed"
			rc.Result.StatusMessage = err.Error()
		}
	}()

	if err := r.postResult(ctx, rc.RunName(), rc.Result); err != nil {
		slog.Warn("post result rejected (will be re-dispatched)", "run", rc.RunName(), "subject", subject, "error", err)
	} else {
		slog.Info("run complete", "run", rc.RunName(), "subject", subject, "status", rc.Result.Status)
	}
	return nil
}
```

Add `"runtime/debug"` to imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd babylon-runner && go test ./internal/runner/ -run TestPollOnceRecoversPanic -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add babylon-runner/internal/runner/runner.go babylon-runner/internal/runner/runner_test.go
git commit -m "feat(babylon-runner): add panic recovery in dispatch loop

Wraps handler dispatch with defer/recover. On panic, logs the stack
trace and posts a failed result so the operator marks the run properly
instead of timing out after losing the runner pod."
```

---

### Task 7: Status Handler Orphan Fix (GAP-3)

When `DeployerDisabled("status")` is true and `check_status_state` is "pending", `runStatus` sets `startTimestamp` then returns without finishing the action. Fix: immediately mark status check as successful and finish the action.

**Files:**

- Modify: `babylon-runner/internal/handler/status.go:49-75` — add deployer-disabled path
- Modify: `babylon-runner/internal/handler/statusupdate_test.go` or add new test — test the fix

**Interfaces:**

- Consumes: `rc.SubjectUpdate(...)`, `rc.FinishAction(...)`

- [ ] **Step 1: Write the failing test**

Add to `babylon-runner/internal/handler/statusupdate_test.go` (or the appropriate test file):

```go
func TestHandleStatus_DeployerDisabled_FinishesAction(t *testing.T) {
	var patches []types.SubjectPatch

	anarchyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			var p types.SubjectPatch
			json.NewDecoder(r.Body).Decode(&p)
			patches = append(patches, p)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer anarchyServer.Close()

	rc := testRunContext(anarchyServer.URL, types.RunPayload{
		Handler: types.Handler{Type: "action", Name: "run"},
		Governor: types.Governor{
			Spec: types.GovernorSpec{
				Vars: types.GovernorVars{
					Meta: &types.Meta{
						Deployer: &types.DeployerMeta{
							Actions: map[string]types.DeployerActionConfig{
								"status": {Disabled: true},
							},
						},
					},
				},
			},
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subj"},
			Spec: types.SubjectSpec{
				Vars: types.SubjectVars{
					CurrentState:     "started",
					CheckStatusState: "pending",
					JobVars:          map[string]interface{}{"uuid": "test"},
				},
			},
		},
		Action: &types.Action{
			Metadata: types.ObjectMeta{Name: "test-action"},
			Spec:     types.ActionSpec{Action: "status"},
		},
		Run: types.Run{Metadata: types.ObjectMeta{Name: "test-run"}},
	})

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus: %v", err)
	}

	if rc.Result.FinishAction == nil {
		t.Fatal("FinishAction should be set when deployer is disabled")
	}
	if rc.Result.FinishAction.State != "successful" {
		t.Errorf("FinishAction.State = %q, want successful", rc.Result.FinishAction.State)
	}

	// Verify check_status_state was set to "successful" in the patch
	found := false
	for _, p := range patches {
		if p.Patch.Spec != nil {
			if css, ok := p.Patch.Spec.Vars["check_status_state"]; ok && css == "successful" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected check_status_state=successful in subject update")
	}
}
```

Note: Adapt the `testRunContext` helper based on what already exists in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd babylon-runner && go test ./internal/handler/ -run TestHandleStatus_DeployerDisabled_FinishesAction -v`
Expected: FAIL — `FinishAction` is nil

- [ ] **Step 3: Fix the deployer-disabled path in runStatus**

In `babylon-runner/internal/handler/status.go`, replace the empty return at the end of `runStatus`:

```go
func runStatus(rc *runner.RunContext) error {
	// Set startTimestamp (always, matching Ansible).
	ts := types.NowUTC()
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"status": map[string]interface{}{
						"startTimestamp": ts,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	if !rc.DeployerDisabled("status") {
		// ... existing tower launch code (unchanged) ...
		return nil
	}

	// Deployer disabled: mark status check as successful immediately.
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Spec: &types.PatchSpec{
				Vars: map[string]interface{}{
					"check_status_state": "successful",
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}
	rc.FinishAction("successful")
	return nil
}
```

Also fix `handleStatus` for the "running" + deployer-disabled case (line 23-25). When deployer is disabled and state is "running", it should also finish:

```go
	if checkStatusState == "running" {
		if !rc.DeployerDisabled("status") {
			return checkDeployerJob(rc, "status")
		}
		// Deployer disabled but state stuck at "running" — finish it.
		if err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
				Spec: &types.PatchSpec{
					Vars: map[string]interface{}{
						"check_status_state": "successful",
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
		rc.FinishAction("successful")
		return nil
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd babylon-runner && go test ./internal/handler/ -run TestHandleStatus_DeployerDisabled -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `cd babylon-runner && go test ./internal/...`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add babylon-runner/internal/handler/status.go babylon-runner/internal/handler/statusupdate_test.go
git commit -m "fix(babylon-runner): finish status action when deployer is disabled

When DeployerDisabled('status') is true, runStatus was returning nil
without finishing the action or updating check_status_state. This left
the action in an inconsistent state. Now sets check_status_state to
'successful' and calls FinishAction."
```

---

### Task 8: Update Spec Status and Run Final Validation

Update the spec's phase table to mark Phase 2 as completed, run final validation, and verify with `dev-run.sh` against a real cluster.

**Files:**

- Modify: `docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md:12` — update Phase 2 status

- [ ] **Step 1: Run full test suite**

Run: `cd babylon-runner && go test ./internal/... -v -count=1`
Expected: All PASS (no cached results)

- [ ] **Step 2: Run go vet**

Run: `cd babylon-runner && go vet ./internal/...`
Expected: No issues

- [ ] **Step 3: Build and run with dev-run.sh**

Run: `cd babylon-runner && ./dev-run.sh`

This builds the binary and runs it against a real cluster (requires kubeconfig and port-forwarding). Verify the runner starts, polls successfully, and can process at least one run without errors. The user will provide cluster access if needed.

- [ ] **Step 4: Update spec status**

In `docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md`, change line 12:

```
| **Phase 2** | Correctness | #4, #5, #7, #10, #13, GAP-1, GAP-3 | Completed   |
```

Also update Phase 1 status to "Completed" if not already.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md
git commit -m "docs: mark babylon-runner Phase 2 (Correctness) as completed"
```
