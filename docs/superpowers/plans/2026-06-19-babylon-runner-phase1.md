# Phase 1: Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the babylon-runner from a flat `package main` (21 Go files) into a standard Go project layout with internal packages, typed payloads, shared HTTP infrastructure, and a proper Kubernetes client — establishing the foundation for Phase 2 (Correctness) and Phase 3 (Production).

**Architecture:** Bottom-up package creation alongside existing flat files. New packages (`internal/types`, `internal/httputil`, `internal/template`, `internal/clients`, `internal/runner`, `internal/handler`) are created and tested independently. The final task deletes old flat files, creates `cmd/babylon-runner/main.go`, and updates build artifacts. During Tasks 1–7, old files coexist with new packages (different Go packages, no conflicts). Task 8 performs the switchover.

**Tech Stack:** Go 1.25, `k8s.io/client-go` (Kubernetes API), `github.com/google/uuid` (existing dependency), `httptest` (test HTTP servers).

## Global Constraints

- Go module path: `github.com/rhpds/anarchy/babylon-runner`
- All new packages live under `internal/` (not importable outside the module)
- No Prometheus dependency in Phase 1 — `httputil/instrument.go` deferred to Phase 3
- No `context.Context` threading through handler call chains in Phase 1 — clients use `context.TODO()` where callers don't provide context yet. Phase 2 (#4) threads real contexts top-to-bottom.
- Commit messages must NOT mention "Phase 1" and must NOT include Co-Authored-By
- Every package must have `*_test.go` files — no package is complete without tests
- `go test ./internal/...` must pass after each task; `go test ./...` must pass after Task 8
- Keep `mergeMap` as shallow merge (deep merge is Phase 2, change #10)

---

### Task 1: Project Skeleton, Makefile, and Dependencies

**Files:**

- Create: `babylon-runner/Makefile`
- Modify: `babylon-runner/go.mod`

**Interfaces:**

- Consumes: nothing
- Produces: Directory structure, Makefile targets (`build`, `test`, `lint`, `fmt`, `vet`, `clean`, `docker-build`), go.mod with `k8s.io/client-go` dependency

- [ ] **Step 1: Create directory structure**

```bash
cd babylon-runner
mkdir -p cmd/babylon-runner internal/{types,httputil,template,clients,runner,handler}
```

- [ ] **Step 2: Create the Makefile**

Create `babylon-runner/Makefile`:

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

- [ ] **Step 3: Update go.mod with client-go dependency**

Run from `babylon-runner/`:

```bash
go get k8s.io/client-go@latest
go get k8s.io/apimachinery@latest
go mod tidy
```

This adds `k8s.io/client-go` and its transitive dependencies. The `go.sum` file will be updated automatically.

- [ ] **Step 4: Verify the existing tests still pass**

```bash
go test ./... -count=1
```

Expected: All existing tests pass (the old flat structure is untouched).

---

### Task 2: `internal/types` — Typed Payloads, Results, Patches, and Helpers

**Files:**

- Create: `babylon-runner/internal/types/payload.go`
- Create: `babylon-runner/internal/types/result.go`
- Create: `babylon-runner/internal/types/patch.go`
- Create: `babylon-runner/internal/types/helpers.go`
- Create: `babylon-runner/internal/types/payload_test.go`
- Create: `babylon-runner/internal/types/helpers_test.go`

**Interfaces:**

- Consumes: nothing
- Produces:
  - `types.RunPayload`, `types.Handler`, `types.Governor`, `types.GovernorSpec`, `types.GovernorVars` (with `Extra`, `Get`, `GetString`, `AllVars`), `types.Meta`, `types.DeployerMeta`, `types.DeployerActionConfig`, `types.TowerMeta`, `types.ControllerSchedulerMeta`, `types.Subject`, `types.SubjectSpec`, `types.SubjectVars` (with `Extra`, `Get`, `GetString`, `AllVars`), `types.SubjectStatus`, `types.Action`, `types.ActionSpec`, `types.Run`, `types.ObjectMeta`
  - `types.RunResult`, `types.ResultPayload`, `types.FinishActionDirective`, `types.ContinueActionDirective`, `types.DeleteSubjectDirective`
  - `types.SubjectPatch`, `types.PatchBody`, `types.PatchMetadata`, `types.PatchSpec`, `types.ScheduleActionRequest`
  - `types.GetNestedMap()`, `types.GetNestedString()`, `types.GetNestedBool()`, `types.SetNested()`, `types.NowUTC()`, `types.MergeMap()`, `types.ExtractStringSlice()`

- [ ] **Step 1: Write tests for typed payload JSON round-trip**

Create `babylon-runner/internal/types/payload_test.go`.

The `testPayloadJSON` constant below is derived from a real production
AnarchySubject/Governor pair (`enterprise.event-driven-ansible.prod` in
`babylon-anarchy-0`), restructured into the payload format the runner
receives from `GET /run`. All sensitive data (passwords, tokens, emails,
hostnames, AWS account IDs, SSH keys) has been replaced with dummy values.

Key structural note: in the K8s resource, `__meta__` lives inside
`spec.vars.job_vars.__meta__`, but the Anarchy API restructures the
payload so the runner receives `__meta__` at `spec.vars.__meta__`. The
typed structs match the **payload** structure, not the K8s resource.

```go
package types

import (
	"encoding/json"
	"testing"
)

// testPayloadJSON is a realistic run payload based on production data
// (enterprise.event-driven-ansible.prod). All sensitive values have been
// replaced with dummy data. This mirrors the structure GET /run returns
// to the runner — notably __meta__ is at the governor's spec.vars level,
// not nested inside job_vars.
const testPayloadJSON = `{
	"handler": {"type": "action", "name": "provision"},
	"governor": {
		"metadata": {
			"name": "enterprise.event-driven-ansible.prod",
			"namespace": "babylon-anarchy-0"
		},
		"spec": {
			"vars": {
				"job_vars": {
					"cloud_provider": "ec2",
					"env_type": "ocp4-cluster",
					"platform": "RHPDS",
					"purpose": "PROD",
					"aws_region": "us-east-2",
					"student_name": "lab-user"
				},
				"__meta__": {
					"deployer": {
						"type": "agnosticd",
						"scm_url": "https://github.com/redhat-cop/agnosticd.git",
						"scm_ref": "demo-event-driven-ansible-1.0.2",
						"actions": {
							"provision": {"entry_point": "ansible/main.yml"},
							"destroy": {"entry_point": "ansible/destroy.yml"},
							"stop": {"entry_point": "ansible/lifecycle.yml"},
							"start": {"entry_point": "ansible/lifecycle.yml"},
							"status": {"entry_point": "ansible/lifecycle.yml"}
						}
					},
					"aws_sandboxed": true,
					"tower": {
						"organization": "RHPDS",
						"timeout": 7200
					},
					"sandbox_api": {
						"actions": {
							"start": {"enable": true},
							"stop": {"enable": true}
						}
					}
				},
				"sandbox_api": {
					"url": "https://sandbox-api.example.com"
				}
			},
			"actions": {
				"provision": {"roles": [{"name": "check-deployer"}]},
				"destroy": {"roles": [{"name": "check-deployer"}]},
				"start": {"roles": [{"name": "check-deployer"}]},
				"stop": {"roles": [{"name": "check-deployer"}]},
				"status": {"roles": [{"name": "check-deployer"}]}
			},
			"runner": "babylon-go"
		}
	},
	"subject": {
		"metadata": {
			"name": "enterprise.event-driven-ansible.prod-ab12c",
			"namespace": "babylon-anarchy-0",
			"labels": {
				"anarchy.gpte.redhat.com/governor": "enterprise.event-driven-ansible.prod",
				"state": "started"
			},
			"annotations": {
				"poolboy.gpte.redhat.com/resource-claim-name": "enterprise.event-driven-ansible.prod-claim1",
				"poolboy.gpte.redhat.com/resource-requester-email": "user@example.com"
			}
		},
		"spec": {
			"vars": {
				"current_state": "started",
				"desired_state": "started",
				"healthy": true,
				"job_vars": {
					"cloud_provider": "ec2",
					"guid": "ab12c",
					"uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
					"sandbox_name": "sandbox1234",
					"sandbox_zone": "sandbox1234.example.com",
					"sandbox_account": "123456789012",
					"platform": "RHPDS"
				},
				"provision_data": {
					"ocp_console_url": "https://console.cluster.example.com",
					"ssh_password": "REDACTED"
				},
				"action_schedule": {
					"stop": "8h",
					"destroy": "6d"
				},
				"check_status_request_timestamp": "2026-06-18T22:00:00Z"
			}
		},
		"status": {
			"towerJobs": {
				"provision": {
					"completeTimestamp": "2026-06-18T23:06:52Z",
					"deployerJob": 285742,
					"jobStatus": "successful",
					"towerHost": "controller.example.com"
				}
			},
			"actions": {
				"provision": {
					"state": "successful",
					"completeTimestamp": "2026-06-18T23:06:52Z"
				}
			}
		}
	},
	"action": {
		"metadata": {"name": "enterprise.event-driven-ansible.prod-ab12c-provision"},
		"spec": {
			"action": "provision",
			"vars": {"action_retry_count": 0}
		}
	},
	"run": {
		"metadata": {"name": "enterprise.event-driven-ansible.prod-ab12c-provision-run1"}
	}
}`

// NewTestPayload returns a parsed RunPayload from testPayloadJSON.
// Panics on parse error — safe for use in tests only.
func NewTestPayload() RunPayload {
	var p RunPayload
	if err := json.Unmarshal([]byte(testPayloadJSON), &p); err != nil {
		panic("NewTestPayload: " + err.Error())
	}
	return p
}

func TestRunPayloadUnmarshal(t *testing.T) {
	payload := NewTestPayload()

	// Handler
	if payload.Handler.Type != "action" {
		t.Errorf("handler type = %q, want %q", payload.Handler.Type, "action")
	}
	if payload.Handler.Name != "provision" {
		t.Errorf("handler name = %q, want %q", payload.Handler.Name, "provision")
	}

	// Governor metadata
	if payload.Governor.Metadata.Name != "enterprise.event-driven-ansible.prod" {
		t.Errorf("governor name = %q", payload.Governor.Metadata.Name)
	}
	if payload.Governor.Metadata.Namespace != "babylon-anarchy-0" {
		t.Errorf("governor namespace = %q", payload.Governor.Metadata.Namespace)
	}
	if payload.Governor.Spec.Runner != "babylon-go" {
		t.Errorf("runner = %q, want %q", payload.Governor.Spec.Runner, "babylon-go")
	}

	// Governor job_vars
	if payload.Governor.Spec.Vars.JobVars["cloud_provider"] != "ec2" {
		t.Errorf("governor cloud_provider = %v", payload.Governor.Spec.Vars.JobVars["cloud_provider"])
	}
	if payload.Governor.Spec.Vars.JobVars["env_type"] != "ocp4-cluster" {
		t.Errorf("governor env_type = %v", payload.Governor.Spec.Vars.JobVars["env_type"])
	}

	// Meta (__meta__ at spec.vars level, NOT inside job_vars)
	meta := payload.Governor.Spec.Vars.Meta
	if meta == nil {
		t.Fatal("meta is nil")
	}
	if !meta.AWSSandboxed {
		t.Error("aws_sandboxed = false, want true")
	}

	// Deployer
	if meta.Deployer == nil {
		t.Fatal("deployer is nil")
	}
	if meta.Deployer.Type != "agnosticd" {
		t.Errorf("deployer type = %q, want %q", meta.Deployer.Type, "agnosticd")
	}
	if meta.Deployer.SCMRef != "demo-event-driven-ansible-1.0.2" {
		t.Errorf("scm_ref = %q", meta.Deployer.SCMRef)
	}
	if cfg, ok := meta.Deployer.Actions["provision"]; !ok {
		t.Error("deployer actions missing 'provision'")
	} else if cfg.EntryPoint != "ansible/main.yml" {
		t.Errorf("entry_point = %q, want %q", cfg.EntryPoint, "ansible/main.yml")
	}
	if _, ok := meta.Deployer.Actions["destroy"]; !ok {
		t.Error("deployer actions missing 'destroy'")
	}

	// Tower meta
	if meta.Tower == nil {
		t.Fatal("tower meta is nil")
	}
	if meta.Tower.Organization != "RHPDS" {
		t.Errorf("tower org = %q, want %q", meta.Tower.Organization, "RHPDS")
	}
	if meta.Tower.Timeout != 7200 {
		t.Errorf("tower timeout = %d, want 7200", meta.Tower.Timeout)
	}

	// Governor sandbox_api (top-level, separate from __meta__.sandbox_api)
	if payload.Governor.Spec.Vars.SandboxAPI == nil {
		t.Fatal("governor sandbox_api is nil")
	}
	if payload.Governor.Spec.Vars.SandboxAPI["url"] != "https://sandbox-api.example.com" {
		t.Errorf("sandbox_api url = %v", payload.Governor.Spec.Vars.SandboxAPI["url"])
	}

	// Subject metadata
	if payload.Subject.Metadata.Name != "enterprise.event-driven-ansible.prod-ab12c" {
		t.Errorf("subject name = %q", payload.Subject.Metadata.Name)
	}
	if payload.Subject.Metadata.Labels["state"] != "started" {
		t.Errorf("subject label state = %q", payload.Subject.Metadata.Labels["state"])
	}

	// Subject vars
	if payload.Subject.Spec.Vars.CurrentState != "started" {
		t.Errorf("current_state = %q", payload.Subject.Spec.Vars.CurrentState)
	}
	if payload.Subject.Spec.Vars.DesiredState != "started" {
		t.Errorf("desired_state = %q", payload.Subject.Spec.Vars.DesiredState)
	}
	if payload.Subject.Spec.Vars.Healthy == nil || !*payload.Subject.Spec.Vars.Healthy {
		t.Error("healthy should be true")
	}
	if payload.Subject.Spec.Vars.JobVars["guid"] != "ab12c" {
		t.Errorf("subject guid = %v", payload.Subject.Spec.Vars.JobVars["guid"])
	}
	if payload.Subject.Spec.Vars.JobVars["uuid"] != "a1b2c3d4-e5f6-7890-abcd-ef1234567890" {
		t.Errorf("subject uuid = %v", payload.Subject.Spec.Vars.JobVars["uuid"])
	}

	// Subject vars Extra fields (not in typed struct — captured in Extra)
	provisionData := payload.Subject.Spec.Vars.Get("provision_data")
	if provisionData == nil {
		t.Fatal("provision_data should be captured in Extra")
	}
	pd, ok := provisionData.(map[string]interface{})
	if !ok {
		t.Fatalf("provision_data type = %T, want map", provisionData)
	}
	if pd["ocp_console_url"] != "https://console.cluster.example.com" {
		t.Errorf("ocp_console_url = %v", pd["ocp_console_url"])
	}
	if payload.Subject.Spec.Vars.GetString("check_status_request_timestamp") != "2026-06-18T22:00:00Z" {
		t.Errorf("check_status_request_timestamp = %q", payload.Subject.Spec.Vars.GetString("check_status_request_timestamp"))
	}

	// Subject status
	provisionJob := payload.Subject.Status.TowerJobs["provision"]
	if provisionJob == nil {
		t.Fatal("towerJobs.provision is nil")
	}

	// Action
	if payload.Action == nil {
		t.Fatal("action is nil")
	}
	if payload.Action.Spec.Action != "provision" {
		t.Errorf("action spec = %q, want %q", payload.Action.Spec.Action, "provision")
	}

	// Run
	if payload.Run.Metadata.Name != "enterprise.event-driven-ansible.prod-ab12c-provision-run1" {
		t.Errorf("run name = %q", payload.Run.Metadata.Name)
	}
}

func TestGovernorVarsExtraFields(t *testing.T) {
	raw := `{
		"job_vars": {"key": "val"},
		"__meta__": {"aws_sandboxed": true},
		"sandbox_api": {"url": "http://sandbox"},
		"scm_ref_var": "agnosticd_scm_ref",
		"unknown_field": 42
	}`

	var vars GovernorVars
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if vars.JobVars["key"] != "val" {
		t.Error("job_vars not parsed")
	}
	if vars.Meta == nil || !vars.Meta.AWSSandboxed {
		t.Error("__meta__ not parsed")
	}
	if vars.SandboxAPI["url"] != "http://sandbox" {
		t.Error("sandbox_api not parsed")
	}
	if vars.GetString("scm_ref_var") != "agnosticd_scm_ref" {
		t.Errorf("extra field scm_ref_var = %q, want %q", vars.GetString("scm_ref_var"), "agnosticd_scm_ref")
	}
	if vars.Get("unknown_field") == nil {
		t.Error("unknown extra field not captured")
	}
}

func TestGovernorVarsMarshalRoundTrip(t *testing.T) {
	original := GovernorVars{
		JobVars:    map[string]interface{}{"region": "us-east-1"},
		Meta:       &Meta{AWSSandboxed: true},
		SandboxAPI: map[string]interface{}{"url": "http://sandbox"},
		Extra:      map[string]interface{}{"scm_ref_var": "agnosticd_scm_ref"},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var roundTripped GovernorVars
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if roundTripped.GetString("scm_ref_var") != "agnosticd_scm_ref" {
		t.Error("extra field lost during round-trip")
	}
}

func TestGovernorVarsAllVars(t *testing.T) {
	vars := GovernorVars{
		JobVars:    map[string]interface{}{"region": "us-east-1"},
		Meta:       &Meta{AWSSandboxed: true},
		SandboxAPI: map[string]interface{}{"url": "http://sandbox"},
		Extra:      map[string]interface{}{"scm_ref_var": "agnosticd_scm_ref"},
	}

	all := vars.AllVars()
	if all["job_vars"] == nil {
		t.Error("job_vars missing from AllVars")
	}
	if all["__meta__"] == nil {
		t.Error("__meta__ missing from AllVars")
	}
	if all["sandbox_api"] == nil {
		t.Error("sandbox_api missing from AllVars")
	}
	if all["scm_ref_var"] != "agnosticd_scm_ref" {
		t.Error("extra field missing from AllVars")
	}
}

func TestSubjectVarsExtraFields(t *testing.T) {
	raw := `{
		"current_state": "started",
		"desired_state": "started",
		"healthy": true,
		"job_vars": {"guid": "abc"},
		"provision_data": {"console": "https://console.example.com"},
		"action_schedule": {"stop": "8h"},
		"check_status_request_timestamp": "2026-06-18T22:00:00Z",
		"provision_messages": ["provisioned successfully"]
	}`

	var vars SubjectVars
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Typed fields
	if vars.CurrentState != "started" {
		t.Errorf("current_state = %q", vars.CurrentState)
	}
	if vars.JobVars["guid"] != "abc" {
		t.Error("job_vars not parsed")
	}

	// Extra fields
	if vars.Get("provision_data") == nil {
		t.Error("provision_data not captured in Extra")
	}
	if vars.Get("action_schedule") == nil {
		t.Error("action_schedule not captured in Extra")
	}
	if vars.GetString("check_status_request_timestamp") != "2026-06-18T22:00:00Z" {
		t.Error("check_status_request_timestamp not captured")
	}
	if vars.Get("provision_messages") == nil {
		t.Error("provision_messages not captured in Extra")
	}
}

func TestSubjectVarsMarshalRoundTrip(t *testing.T) {
	healthy := true
	original := SubjectVars{
		CurrentState: "started",
		DesiredState: "started",
		Healthy:      &healthy,
		JobVars:      map[string]interface{}{"guid": "abc"},
		Extra: map[string]interface{}{
			"provision_data": map[string]interface{}{"console": "https://console.example.com"},
			"action_schedule": map[string]interface{}{"stop": "8h"},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var roundTripped SubjectVars
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if roundTripped.CurrentState != "started" {
		t.Error("typed field lost during round-trip")
	}
	if roundTripped.Get("provision_data") == nil {
		t.Error("extra field lost during round-trip")
	}
}

func TestSubjectVarsAllVars(t *testing.T) {
	healthy := true
	vars := SubjectVars{
		CurrentState: "started",
		DesiredState: "started",
		Healthy:      &healthy,
		JobVars:      map[string]interface{}{"guid": "abc"},
		Extra: map[string]interface{}{
			"provision_data": map[string]interface{}{"console": "url"},
			"action_schedule": map[string]interface{}{"stop": "8h"},
		},
	}

	all := vars.AllVars()

	// Typed fields present
	if all["current_state"] != "started" {
		t.Error("current_state missing from AllVars")
	}
	if all["desired_state"] != "started" {
		t.Error("desired_state missing from AllVars")
	}
	if all["healthy"] != true {
		t.Error("healthy missing from AllVars")
	}
	if all["job_vars"] == nil {
		t.Error("job_vars missing from AllVars")
	}

	// Extra fields present
	if all["provision_data"] == nil {
		t.Error("provision_data missing from AllVars")
	}
	if all["action_schedule"] == nil {
		t.Error("action_schedule missing from AllVars")
	}
}

func TestRunPayloadMarshal(t *testing.T) {
	payload := RunPayload{
		Handler: Handler{Type: "action", Name: "provision"},
		Subject: Subject{
			Metadata: ObjectMeta{Name: "subj-1"},
			Spec: SubjectSpec{
				Vars: SubjectVars{
					CurrentState: "started",
					DesiredState: "started",
				},
			},
		},
		Run: Run{Metadata: ObjectMeta{Name: "run-1"}},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded RunPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.Subject.Spec.Vars.CurrentState != "started" {
		t.Errorf("current_state = %q, want %q", decoded.Subject.Spec.Vars.CurrentState, "started")
	}
	if decoded.Action != nil {
		t.Error("action should be nil when omitted")
	}
}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
cd babylon-runner && go test ./internal/types/ -v -count=1
```

Expected: Compilation errors — types not yet defined.

- [ ] **Step 3: Implement `payload.go`**

Create `babylon-runner/internal/types/payload.go`:

```go
package types

import "encoding/json"

// ObjectMeta holds Kubernetes-style metadata fields.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	DeletionTimestamp *string           `json:"deletionTimestamp,omitempty"`
}

// Handler identifies which handler to invoke for a run.
type Handler struct {
	Type string                 `json:"type"`
	Name string                 `json:"name"`
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// RunPayload is the response body from GET /run.
type RunPayload struct {
	Handler  Handler  `json:"handler"`
	Governor Governor `json:"governor"`
	Subject  Subject  `json:"subject"`
	Action   *Action  `json:"action,omitempty"`
	Run      Run      `json:"run"`
}

// Governor represents the AnarchyGovernor attached to a run.
type Governor struct {
	Metadata ObjectMeta   `json:"metadata"`
	Spec     GovernorSpec `json:"spec"`
}

// GovernorSpec holds governor specification fields.
type GovernorSpec struct {
	Vars    GovernorVars                      `json:"vars"`
	Actions map[string]map[string]interface{} `json:"actions,omitempty"`
	Runner  string                            `json:"runner,omitempty"`
}

// GovernorVars holds governor variables with typed access for known fields
// and dynamic access for unknown fields via Get/GetString.
type GovernorVars struct {
	JobVars    map[string]interface{} `json:"job_vars,omitempty"`
	Meta       *Meta                  `json:"__meta__,omitempty"`
	SandboxAPI map[string]interface{} `json:"sandbox_api,omitempty"`
	Extra      map[string]interface{} `json:"-"`
}

func (v *GovernorVars) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	known := map[string]bool{"job_vars": true, "__meta__": true, "sandbox_api": true}

	if jv, ok := raw["job_vars"]; ok {
		if err := json.Unmarshal(jv, &v.JobVars); err != nil {
			return err
		}
	}
	if m, ok := raw["__meta__"]; ok {
		v.Meta = &Meta{}
		if err := json.Unmarshal(m, v.Meta); err != nil {
			return err
		}
	}
	if sa, ok := raw["sandbox_api"]; ok {
		if err := json.Unmarshal(sa, &v.SandboxAPI); err != nil {
			return err
		}
	}

	v.Extra = make(map[string]interface{})
	for k, rm := range raw {
		if !known[k] {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err != nil {
				return err
			}
			v.Extra[k] = val
		}
	}
	return nil
}

func (v GovernorVars) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	if v.JobVars != nil {
		m["job_vars"] = v.JobVars
	}
	if v.Meta != nil {
		m["__meta__"] = v.Meta
	}
	if v.SandboxAPI != nil {
		m["sandbox_api"] = v.SandboxAPI
	}
	return json.Marshal(m)
}

// Get returns a dynamic field value from the governor vars that is not
// one of the typed fields (job_vars, __meta__, sandbox_api).
func (v *GovernorVars) Get(key string) interface{} {
	if v.Extra != nil {
		return v.Extra[key]
	}
	return nil
}

// GetString returns a dynamic field as a string, or "" if missing or not a string.
func (v *GovernorVars) GetString(key string) string {
	if s, ok := v.Get(key).(string); ok {
		return s
	}
	return ""
}

// AllVars reconstructs the complete flat map from typed fields + Extra.
// Used by J2VarContext to maintain the same merge behavior as the
// current code (which merges ALL spec.vars, not just job_vars).
func (v *GovernorVars) AllVars() map[string]interface{} {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	if v.JobVars != nil {
		m["job_vars"] = v.JobVars
	}
	if v.Meta != nil {
		m["__meta__"] = v.Meta
	}
	if v.SandboxAPI != nil {
		m["sandbox_api"] = v.SandboxAPI
	}
	return m
}

// Meta holds the __meta__ governor configuration.
type Meta struct {
	Deployer                    *DeployerMeta            `json:"deployer,omitempty"`
	Sandboxes                   []interface{}            `json:"sandboxes,omitempty"`
	AWSSandboxed                bool                     `json:"aws_sandboxed,omitempty"`
	SandboxAPI                  map[string]interface{}   `json:"sandbox_api,omitempty"`
	AnsibleControllers          []map[string]interface{} `json:"ansible_controllers,omitempty"`
	AnsibleControllerSelectMode string                   `json:"ansible_controller_select_mode,omitempty"`
	ControllerScheduler         *ControllerSchedulerMeta `json:"controller_scheduler,omitempty"`
	Tower                       *TowerMeta               `json:"tower,omitempty"`
}

// DeployerMeta configures the deployer (e.g. agnosticd) for Tower job launches.
type DeployerMeta struct {
	Type              string                          `json:"type,omitempty"`
	Actions           map[string]DeployerActionConfig `json:"actions,omitempty"`
	SCMUrl            string                          `json:"scm_url,omitempty"`
	SCMRef            string                          `json:"scm_ref,omitempty"`
	SCMType           string                          `json:"scm_type,omitempty"`
	SCMCredential     string                          `json:"scm_credential,omitempty"`
	SCMUpdateOnLaunch *bool                           `json:"scm_update_on_launch,omitempty"`
	SCMCacheTimeout   int                             `json:"scm_cache_timeout,omitempty"`
	SCMClean          *bool                           `json:"scm_clean,omitempty"`
}

// DeployerActionConfig holds per-action deployer overrides.
type DeployerActionConfig struct {
	EntryPoint string `json:"entry_point,omitempty"`
	SCMRef     string `json:"scm_ref,omitempty"`
	Disabled   bool   `json:"disabled,omitempty"`
}

// TowerMeta holds Tower/AAP configuration from __meta__.tower.
type TowerMeta struct {
	Organization string `json:"organization,omitempty"`
	Timeout      int    `json:"timeout,omitempty"`
	Inventory    string `json:"inventory,omitempty"`
}

// ControllerSchedulerMeta configures external controller selection.
type ControllerSchedulerMeta struct {
	URL           string            `json:"url,omitempty"`
	RequireLabels map[string]string `json:"require_labels,omitempty"`
	PreferLabels  map[string]string `json:"prefer_labels,omitempty"`
	InstanceGroup string            `json:"instance_group,omitempty"`
}

// Subject represents the AnarchySubject attached to a run.
type Subject struct {
	Metadata ObjectMeta    `json:"metadata"`
	Spec     SubjectSpec   `json:"spec"`
	Status   SubjectStatus `json:"status"`
}

// SubjectSpec holds subject specification fields.
type SubjectSpec struct {
	Vars SubjectVars `json:"vars"`
}

// SubjectVars holds subject variables with typed access for known fields
// and dynamic access for unknown fields via Get/GetString.
// Uses the same Extra pattern as GovernorVars to avoid losing fields
// like provision_data, action_schedule, provision_messages during unmarshal.
type SubjectVars struct {
	CurrentState     string                 `json:"current_state,omitempty"`
	DesiredState     string                 `json:"desired_state,omitempty"`
	Healthy          *bool                  `json:"healthy,omitempty"`
	JobVars          map[string]interface{} `json:"job_vars,omitempty"`
	CheckStatusState string                 `json:"check_status_state,omitempty"`
	Extra            map[string]interface{} `json:"-"`
}

func (v *SubjectVars) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	known := map[string]bool{
		"current_state": true, "desired_state": true,
		"healthy": true, "job_vars": true, "check_status_state": true,
	}

	if cs, ok := raw["current_state"]; ok {
		if err := json.Unmarshal(cs, &v.CurrentState); err != nil {
			return err
		}
	}
	if ds, ok := raw["desired_state"]; ok {
		if err := json.Unmarshal(ds, &v.DesiredState); err != nil {
			return err
		}
	}
	if h, ok := raw["healthy"]; ok {
		var healthy bool
		if err := json.Unmarshal(h, &healthy); err != nil {
			return err
		}
		v.Healthy = &healthy
	}
	if jv, ok := raw["job_vars"]; ok {
		if err := json.Unmarshal(jv, &v.JobVars); err != nil {
			return err
		}
	}
	if css, ok := raw["check_status_state"]; ok {
		if err := json.Unmarshal(css, &v.CheckStatusState); err != nil {
			return err
		}
	}

	v.Extra = make(map[string]interface{})
	for k, rm := range raw {
		if !known[k] {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err != nil {
				return err
			}
			v.Extra[k] = val
		}
	}
	return nil
}

func (v SubjectVars) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	if v.CurrentState != "" {
		m["current_state"] = v.CurrentState
	}
	if v.DesiredState != "" {
		m["desired_state"] = v.DesiredState
	}
	if v.Healthy != nil {
		m["healthy"] = *v.Healthy
	}
	if v.JobVars != nil {
		m["job_vars"] = v.JobVars
	}
	if v.CheckStatusState != "" {
		m["check_status_state"] = v.CheckStatusState
	}
	return json.Marshal(m)
}

// Get returns a dynamic field value from subject vars.
func (v *SubjectVars) Get(key string) interface{} {
	if v.Extra != nil {
		return v.Extra[key]
	}
	return nil
}

// GetString returns a dynamic field as a string, or "" if missing.
func (v *SubjectVars) GetString(key string) string {
	if s, ok := v.Get(key).(string); ok {
		return s
	}
	return ""
}

// AllVars reconstructs the complete flat map from typed fields + Extra.
// Used by J2VarContext to maintain the same merge behavior as the
// current code (which merges ALL spec.vars, not just job_vars).
func (v *SubjectVars) AllVars() map[string]interface{} {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	if v.CurrentState != "" {
		m["current_state"] = v.CurrentState
	}
	if v.DesiredState != "" {
		m["desired_state"] = v.DesiredState
	}
	if v.Healthy != nil {
		m["healthy"] = *v.Healthy
	}
	if v.JobVars != nil {
		m["job_vars"] = v.JobVars
	}
	if v.CheckStatusState != "" {
		m["check_status_state"] = v.CheckStatusState
	}
	return m
}

// SubjectStatus holds subject status fields. Inner structures remain
// as maps because they are deeply nested and accessed dynamically.
type SubjectStatus struct {
	Actions       map[string]interface{} `json:"actions,omitempty"`
	TowerJobs     map[string]interface{} `json:"towerJobs,omitempty"`
	PreviousState map[string]interface{} `json:"previous_state,omitempty"`
}

// Action represents the AnarchyAction attached to a run (nil for events).
type Action struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     ActionSpec `json:"spec"`
}

// ActionSpec holds action specification fields.
type ActionSpec struct {
	Action string                 `json:"action"`
	Vars   map[string]interface{} `json:"vars,omitempty"`
}

// Run represents the AnarchyRun being executed.
type Run struct {
	Metadata ObjectMeta `json:"metadata"`
}
```

- [ ] **Step 4: Implement `result.go`**

Create `babylon-runner/internal/types/result.go`:

```go
package types

// RunResult is the body for POST /run/{name}.
type RunResult struct {
	RC            int                      `json:"rc"`
	Status        string                   `json:"status"`
	StatusMessage string                   `json:"statusMessage,omitempty"`
	FinishAction  *FinishActionDirective   `json:"finishAction,omitempty"`
	ContinueAction *ContinueActionDirective `json:"continueAction,omitempty"`
	DeleteSubject *DeleteSubjectDirective  `json:"deleteSubject,omitempty"`
}

// FinishActionDirective signals the operator to mark the action as finished.
type FinishActionDirective struct {
	State string `json:"state"`
}

// ContinueActionDirective signals the operator to reschedule the action.
type ContinueActionDirective struct {
	After string                 `json:"after"`
	Vars  map[string]interface{} `json:"vars,omitempty"`
}

// DeleteSubjectDirective signals the operator to delete the subject.
type DeleteSubjectDirective struct {
	RemoveFinalizers bool `json:"removeFinalizers"`
}
```

- [ ] **Step 5: Implement `patch.go`**

Create `babylon-runner/internal/types/patch.go`:

```go
package types

// SubjectPatch is the body for PATCH /run/subject/{name}.
type SubjectPatch struct {
	Patch PatchBody `json:"patch"`
}

// PatchBody contains the fields to patch on a subject.
type PatchBody struct {
	Metadata             *PatchMetadata         `json:"metadata,omitempty"`
	Spec                 *PatchSpec             `json:"spec,omitempty"`
	Status               map[string]interface{} `json:"status,omitempty"`
	SkipUpdateProcessing bool                   `json:"skip_update_processing,omitempty"`
}

// PatchMetadata patches metadata labels and annotations.
type PatchMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PatchSpec patches spec-level vars.
type PatchSpec struct {
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// ScheduleActionRequest is the body for POST /run/subject/{name}/actions.
type ScheduleActionRequest struct {
	Action string `json:"action"`
	After  string `json:"after,omitempty"`
}
```

- [ ] **Step 6: Implement `helpers.go`**

Create `babylon-runner/internal/types/helpers.go`. Migrate helpers from the existing `types.go` with exported names:

```go
package types

import (
	"fmt"
	"time"
)

// GetNestedMap safely traverses nested maps by keys.
// Returns nil if any key is missing or the value is not a map.
func GetNestedMap(m map[string]interface{}, keys ...string) map[string]interface{} {
	current := m
	for _, k := range keys {
		next, ok := current[k].(map[string]interface{})
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// GetNestedString safely extracts a string from nested maps.
// Returns "" if the path is missing or the value is not a string.
func GetNestedString(m map[string]interface{}, keys ...string) string {
	if len(keys) == 0 {
		return ""
	}
	parent := GetNestedMap(m, keys[:len(keys)-1]...)
	if parent == nil {
		return ""
	}
	s, _ := parent[keys[len(keys)-1]].(string)
	return s
}

// GetNestedBool safely extracts a bool from nested maps.
// Returns false if the path is missing or the value is not a bool.
func GetNestedBool(m map[string]interface{}, keys ...string) bool {
	if len(keys) == 0 {
		return false
	}
	parent := GetNestedMap(m, keys[:len(keys)-1]...)
	if parent == nil {
		return false
	}
	b, _ := parent[keys[len(keys)-1]].(bool)
	return b
}

// SetNested sets a value at the given key path in a nested map,
// creating intermediate maps as needed.
func SetNested(m map[string]interface{}, value interface{}, keys ...string) {
	if len(keys) == 0 {
		return
	}
	current := m
	for _, k := range keys[:len(keys)-1] {
		next, ok := current[k].(map[string]interface{})
		if !ok {
			next = make(map[string]interface{})
			current[k] = next
		}
		current = next
	}
	current[keys[len(keys)-1]] = value
}

// NowUTC returns the current time as an RFC3339 string in UTC.
func NowUTC() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// MergeMap copies all key-value pairs from src into dst (shallow merge).
// Existing keys in dst are overwritten.
func MergeMap(dst, src map[string]interface{}) {
	for k, v := range src {
		dst[k] = v
	}
}

// ExtractStringSlice extracts a []string from a map value that may be
// []interface{} (as produced by JSON unmarshaling).
func ExtractStringSlice(m map[string]interface{}, key string) []string {
	raw, ok := m[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	default:
		return nil
	}
}

// AfterTimestamp converts a Go duration string (e.g., "5m", "1h") to an
// absolute RFC3339 timestamp. If the duration string is already an absolute
// timestamp (contains "T"), it is returned as-is. Returns the current time
// on parse error.
func AfterTimestamp(after string) string {
	if after == "" {
		return NowUTC()
	}
	for _, ch := range after {
		if ch == 'T' {
			return after
		}
	}
	d, err := time.ParseDuration(after)
	if err != nil {
		return NowUTC()
	}
	return time.Now().UTC().Add(d).Format(time.RFC3339)
}

// StringFromMap extracts a string value from a map, returning "" if missing
// or not a string.
func StringFromMap(m map[string]interface{}, key string) string {
	s, _ := m[key].(string)
	return s
}

// FloatFromMap extracts a float64 value from a map, returning 0 if missing.
func FloatFromMap(m map[string]interface{}, key string) float64 {
	f, _ := m[key].(float64)
	return f
}

// FirstString returns the first non-empty string from the arguments.
func FirstString(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// DeepCopyMap returns a deep copy of a map[string]interface{}.
func DeepCopyMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[k] = DeepCopyMap(val)
		case []interface{}:
			dst[k] = DeepCopySlice(val)
		default:
			dst[k] = v
		}
	}
	return dst
}

// DeepCopySlice returns a deep copy of a []interface{}.
func DeepCopySlice(src []interface{}) []interface{} {
	if src == nil {
		return nil
	}
	dst := make([]interface{}, len(src))
	for i, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[i] = DeepCopyMap(val)
		case []interface{}:
			dst[i] = DeepCopySlice(val)
		default:
			dst[i] = v
		}
	}
	return dst
}

// FormatTimestamp returns a formatted timestamp string.
// Used for startTimestamp fields.
func FormatTimestamp() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}
```

- [ ] **Step 7: Write and run helper tests**

Create `babylon-runner/internal/types/helpers_test.go`. Migrate tests from the existing `types_test.go`, adapting function names (lowercase → uppercase):

```go
package types

import (
	"testing"
	"time"
)

func TestGetNestedMap(t *testing.T) {
	m := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"key": "value",
			},
		},
	}

	t.Run("valid path", func(t *testing.T) {
		result := GetNestedMap(m, "level1", "level2")
		if result == nil {
			t.Fatal("expected non-nil map")
		}
		if result["key"] != "value" {
			t.Errorf("got %v, want %q", result["key"], "value")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		result := GetNestedMap(m, "level1", "missing")
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("nil map", func(t *testing.T) {
		result := GetNestedMap(nil, "key")
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}

func TestGetNestedString(t *testing.T) {
	m := map[string]interface{}{
		"spec": map[string]interface{}{
			"vars": map[string]interface{}{
				"current_state": "started",
			},
		},
	}

	if got := GetNestedString(m, "spec", "vars", "current_state"); got != "started" {
		t.Errorf("got %q, want %q", got, "started")
	}
	if got := GetNestedString(m, "spec", "vars", "missing"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestGetNestedBool(t *testing.T) {
	m := map[string]interface{}{
		"config": map[string]interface{}{
			"enabled": true,
		},
	}
	if got := GetNestedBool(m, "config", "enabled"); !got {
		t.Error("expected true")
	}
	if got := GetNestedBool(m, "config", "missing"); got {
		t.Error("expected false for missing key")
	}
}

func TestSetNested(t *testing.T) {
	m := make(map[string]interface{})
	SetNested(m, "value", "a", "b", "c")
	if got := GetNestedString(m, "a", "b", "c"); got != "value" {
		t.Errorf("got %q, want %q", got, "value")
	}
}

func TestMergeMap(t *testing.T) {
	dst := map[string]interface{}{"a": 1, "b": 2}
	src := map[string]interface{}{"b": 3, "c": 4}
	MergeMap(dst, src)
	if dst["a"] != 1 || dst["b"] != 3 || dst["c"] != 4 {
		t.Errorf("unexpected merge result: %v", dst)
	}
}

func TestExtractStringSlice(t *testing.T) {
	m := map[string]interface{}{
		"tags": []interface{}{"a", "b", "c"},
	}
	got := ExtractStringSlice(m, "tags")
	if len(got) != 3 || got[0] != "a" {
		t.Errorf("got %v, want [a b c]", got)
	}
}

func TestNowUTC(t *testing.T) {
	ts := NowUTC()
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatalf("invalid RFC3339: %v", err)
	}
	if parsed.Location() != time.UTC {
		t.Error("not UTC")
	}
}

func TestAfterTimestamp(t *testing.T) {
	t.Run("duration string", func(t *testing.T) {
		ts := AfterTimestamp("5m")
		parsed, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
		diff := time.Until(parsed)
		if diff < 4*time.Minute || diff > 6*time.Minute {
			t.Errorf("expected ~5m from now, got %v", diff)
		}
	})

	t.Run("empty string", func(t *testing.T) {
		ts := AfterTimestamp("")
		if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
	})

	t.Run("zero duration", func(t *testing.T) {
		ts := AfterTimestamp("0s")
		if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
	})
}

func TestDeepCopyMap(t *testing.T) {
	src := map[string]interface{}{
		"nested": map[string]interface{}{"key": "val"},
		"list":   []interface{}{1, 2, 3},
	}
	dst := DeepCopyMap(src)
	dst["nested"].(map[string]interface{})["key"] = "changed"
	if src["nested"].(map[string]interface{})["key"] != "val" {
		t.Error("deep copy is not deep — original was mutated")
	}
}

func TestFirstString(t *testing.T) {
	if got := FirstString("", "", "hello"); got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
	if got := FirstString("first", "second"); got != "first" {
		t.Errorf("got %q, want %q", got, "first")
	}
}
```

- [ ] **Step 8: Run all types tests**

```bash
cd babylon-runner && go test ./internal/types/ -v -count=1
```

Expected: All tests PASS.

---

### Task 3: `internal/httputil` — HTTP Transport, Retry, JSON, and Token Cache

**Files:**

- Create: `babylon-runner/internal/httputil/transport.go`
- Create: `babylon-runner/internal/httputil/retry.go`
- Create: `babylon-runner/internal/httputil/json.go`
- Create: `babylon-runner/internal/httputil/token_cache.go`
- Create: `babylon-runner/internal/httputil/transport_test.go`
- Create: `babylon-runner/internal/httputil/retry_test.go`
- Create: `babylon-runner/internal/httputil/json_test.go`
- Create: `babylon-runner/internal/httputil/token_cache_test.go`

**Interfaces:**

- Consumes: nothing
- Produces:
  - `httputil.NewTransport(tlsConfig *tls.Config) *http.Transport`
  - `httputil.NewTLSConfig(verify bool, caPath string) (*tls.Config, error)`
  - `httputil.RetryWithContext(ctx context.Context, delays []time.Duration, fn func() error) error`
  - `httputil.PollWithContext(ctx context.Context, interval time.Duration, maxAttempts int, fn func() (done bool, err error)) error`
  - `httputil.DoJSON(ctx context.Context, client *http.Client, method, url string, headers map[string]string, body, result interface{}) (int, error)`
  - `httputil.NewTokenCache(refresh func(context.Context) (string, time.Duration, error), opts ...TokenCacheOption) *TokenCache`
  - `httputil.WithCleanup(fn func(context.Context, string) error) TokenCacheOption`
  - `(*TokenCache).Get(ctx context.Context) (string, error)`
  - `(*TokenCache).Close(ctx context.Context) error`

- [ ] **Step 1: Write retry tests**

Create `babylon-runner/internal/httputil/retry_test.go`:

```go
package httputil

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithContext_SuccessOnFirst(t *testing.T) {
	calls := 0
	err := RetryWithContext(context.Background(), []time.Duration{time.Millisecond}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestRetryWithContext_SuccessOnRetry(t *testing.T) {
	calls := 0
	err := RetryWithContext(context.Background(), []time.Duration{time.Millisecond, time.Millisecond}, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetryWithContext_AllFail(t *testing.T) {
	calls := 0
	err := RetryWithContext(context.Background(), []time.Duration{time.Millisecond}, func() error {
		calls++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2 (initial + 1 retry)", calls)
	}
}

func TestRetryWithContext_CancelledDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := RetryWithContext(ctx, []time.Duration{time.Second}, func() error {
		calls++
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestPollWithContext_ImmediateSuccess(t *testing.T) {
	calls := 0
	err := PollWithContext(context.Background(), time.Millisecond, 10, func() (bool, error) {
		calls++
		return true, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestPollWithContext_EventualSuccess(t *testing.T) {
	calls := 0
	err := PollWithContext(context.Background(), time.Millisecond, 10, func() (bool, error) {
		calls++
		return calls >= 3, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestPollWithContext_MaxAttemptsExhausted(t *testing.T) {
	calls := 0
	err := PollWithContext(context.Background(), time.Millisecond, 3, func() (bool, error) {
		calls++
		return false, nil
	})
	if err == nil {
		t.Fatal("expected error after max attempts")
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}
```

- [ ] **Step 2: Run retry tests — verify they fail**

```bash
cd babylon-runner && go test ./internal/httputil/ -run TestRetry -v -count=1
```

Expected: Compilation errors.

- [ ] **Step 3: Implement `retry.go`**

Create `babylon-runner/internal/httputil/retry.go`:

```go
package httputil

import (
	"context"
	"fmt"
	"time"
)

// RetryWithContext executes fn with retries, respecting context cancellation.
// It makes 1 + len(delays) attempts. Between attempt i and i+1, it waits
// delays[i]. Returns the last error if all attempts fail, or ctx.Err() if
// cancelled during a delay.
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

// PollWithContext polls fn at interval until it returns done=true, a non-nil
// error, or context cancels. Returns an error if maxAttempts is exhausted
// without fn returning done=true.
func PollWithContext(ctx context.Context, interval time.Duration, maxAttempts int, fn func() (done bool, err error)) error {
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
		}
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return fmt.Errorf("poll exhausted after %d attempts", maxAttempts)
}
```

- [ ] **Step 4: Run retry tests — verify they pass**

```bash
cd babylon-runner && go test ./internal/httputil/ -run "TestRetry|TestPoll" -v -count=1
```

Expected: All PASS.

- [ ] **Step 5: Write JSON helper tests and implement `json.go`**

Create `babylon-runner/internal/httputil/json_test.go`:

```go
package httputil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDoJSON_PostAndDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing Content-Type header")
		}
		if r.Header.Get("X-Custom") != "test" {
			t.Error("missing custom header")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"result": "ok"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	var result map[string]interface{}
	status, err := DoJSON(context.Background(), client, http.MethodPost, server.URL,
		map[string]string{"X-Custom": "test"},
		map[string]string{"key": "val"}, &result)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if result["result"] != "ok" {
		t.Errorf("result = %v", result)
	}
}

func TestDoJSON_NilBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	var result map[string]interface{}
	status, err := DoJSON(context.Background(), client, http.MethodGet, server.URL, nil, nil, &result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
}

func TestDoJSON_NilResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &http.Client{}
	status, err := DoJSON(context.Background(), client, http.MethodDelete, server.URL, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 204 {
		t.Errorf("status = %d, want 204", status)
	}
}

func TestDoJSON_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "boom"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	status, err := DoJSON(context.Background(), client, http.MethodGet, server.URL, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 500 {
		t.Errorf("status = %d, want 500", status)
	}
}
```

Create `babylon-runner/internal/httputil/json.go`:

```go
package httputil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// DoJSON executes an HTTP request with optional JSON body and response
// decoding. body can be nil (no request body). result can be nil (response
// body is discarded). Returns the HTTP status code. Non-2xx status codes
// are NOT treated as errors — the caller decides what status codes are
// acceptable. An error is returned only for transport/marshaling failures.
func DoJSON(ctx context.Context, client *http.Client, method, url string,
	headers map[string]string, body, result interface{}) (int, error) {

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if result != nil && resp.ContentLength != 0 {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp.StatusCode, fmt.Errorf("read response body: %w", err)
		}
		if len(respBody) > 0 {
			if err := json.Unmarshal(respBody, result); err != nil {
				return resp.StatusCode, fmt.Errorf("unmarshal response: %w", err)
			}
		}
	} else {
		io.Copy(io.Discard, resp.Body)
	}

	return resp.StatusCode, nil
}
```

- [ ] **Step 6: Write transport tests and implement `transport.go`**

Create `babylon-runner/internal/httputil/transport_test.go`:

```go
package httputil

import (
	"crypto/tls"
	"testing"
)

func TestNewTransport_NilTLS(t *testing.T) {
	tr := NewTransport(nil)
	if tr.TLSClientConfig != nil {
		t.Error("expected nil TLS config for default transport")
	}
	if tr.MaxIdleConns != 100 {
		t.Errorf("MaxIdleConns = %d, want 100", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != 10 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 10", tr.MaxIdleConnsPerHost)
	}
}

func TestNewTransport_CustomTLS(t *testing.T) {
	cfg := &tls.Config{InsecureSkipVerify: true}
	tr := NewTransport(cfg)
	if tr.TLSClientConfig != cfg {
		t.Error("TLS config not set")
	}
}

func TestNewTLSConfig_VerifyTrue(t *testing.T) {
	cfg, err := NewTLSConfig(true, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config when verify=true and no CA")
	}
}

func TestNewTLSConfig_VerifyFalse(t *testing.T) {
	cfg, err := NewTLSConfig(false, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true")
	}
}
```

Create `babylon-runner/internal/httputil/transport.go`:

```go
package httputil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"
)

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

// NewTLSConfig builds a tls.Config from the given parameters.
// Returns nil when verify=true and no custom CA (use Go system defaults).
// Returns a config with InsecureSkipVerify=true when verify=false.
// Loads a custom CA bundle from caPath when provided.
func NewTLSConfig(verify bool, caPath string) (*tls.Config, error) {
	if !verify {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	if caPath == "" {
		return nil, nil
	}
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA cert from %s", caPath)
	}
	return &tls.Config{RootCAs: pool}, nil
}
```

- [ ] **Step 7: Write token cache tests and implement `token_cache.go`**

Create `babylon-runner/internal/httputil/token_cache_test.go`:

```go
package httputil

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTokenCache_Get_CachesToken(t *testing.T) {
	calls := 0
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		calls++
		return "token-1", time.Hour, nil
	})

	tok1, err := tc.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tok2, err := tc.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tok1 != "token-1" || tok2 != "token-1" {
		t.Errorf("tokens = %q, %q, want %q", tok1, tok2, "token-1")
	}
	if calls != 1 {
		t.Errorf("refresh called %d times, want 1", calls)
	}
}

func TestTokenCache_Get_RefreshesExpired(t *testing.T) {
	calls := 0
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		calls++
		return fmt.Sprintf("token-%d", calls), 1 * time.Millisecond, nil
	})

	tok1, _ := tc.Get(context.Background())
	time.Sleep(5 * time.Millisecond)
	tok2, _ := tc.Get(context.Background())

	if tok1 == tok2 {
		t.Error("expected different tokens after expiry")
	}
	if calls != 2 {
		t.Errorf("refresh called %d times, want 2", calls)
	}
}

func TestTokenCache_Get_RefreshError(t *testing.T) {
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		return "", 0, errors.New("auth failed")
	})

	_, err := tc.Get(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenCache_Get_ThreadSafe(t *testing.T) {
	calls := 0
	var mu sync.Mutex
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return "token", time.Hour, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := tc.Get(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if calls > 2 {
		t.Errorf("refresh called %d times, expected <=2 (double-check lock)", calls)
	}
}

func TestTokenCache_Close_WithCleanup(t *testing.T) {
	cleaned := ""
	tc := NewTokenCache(
		func(ctx context.Context) (string, time.Duration, error) {
			return "tok-to-clean", time.Hour, nil
		},
		WithCleanup(func(ctx context.Context, token string) error {
			cleaned = token
			return nil
		}),
	)

	tc.Get(context.Background())
	if err := tc.Close(context.Background()); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if cleaned != "tok-to-clean" {
		t.Errorf("cleanup received %q, want %q", cleaned, "tok-to-clean")
	}
}

func TestTokenCache_Close_WithoutCleanup(t *testing.T) {
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		return "tok", time.Hour, nil
	})

	tc.Get(context.Background())
	if err := tc.Close(context.Background()); err != nil {
		t.Fatalf("close error: %v", err)
	}
}
```

Note: add `"fmt"` to the import in the test file since `fmt.Sprintf` is used.

Create `babylon-runner/internal/httputil/token_cache.go`:

```go
package httputil

import (
	"context"
	"sync"
	"time"
)

// TokenCache provides thread-safe token caching with TTL and automatic
// refresh. Used by Tower (OAuth) and Sandbox (login) clients.
type TokenCache struct {
	mu      sync.RWMutex
	token   string
	expiry  time.Time
	refresh func(ctx context.Context) (token string, ttl time.Duration, err error)
	cleanup func(ctx context.Context, token string) error
}

// TokenCacheOption configures a TokenCache.
type TokenCacheOption func(*TokenCache)

// WithCleanup sets a cleanup function called by Close to release the token
// (e.g., Tower DELETE /api/v2/tokens/{id}). If not set, Close is a no-op.
func WithCleanup(fn func(ctx context.Context, token string) error) TokenCacheOption {
	return func(c *TokenCache) { c.cleanup = fn }
}

// NewTokenCache creates a token cache with the given refresh callback.
func NewTokenCache(refresh func(context.Context) (string, time.Duration, error), opts ...TokenCacheOption) *TokenCache {
	c := &TokenCache{refresh: refresh}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

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

// Close cleans up the current token. No-op if no cleanup function was
// provided or if no token is cached.
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

- [ ] **Step 8: Run all httputil tests**

```bash
cd babylon-runner && go test ./internal/httputil/ -v -count=1
```

Expected: All tests PASS.

---

### Task 4: `internal/template` — Jinja2 Resolver

**Files:**

- Create: `babylon-runner/internal/template/jinja2.go`
- Create: `babylon-runner/internal/template/jinja2_test.go`

**Interfaces:**

- Consumes: nothing
- Produces:
  - `template.ResolveJ2(data interface{}, vars map[string]interface{}) interface{}`
  - `template.J2VarContext(subjectAllVars, governorAllVars map[string]interface{}) map[string]interface{}`

- [ ] **Step 1: Migrate `jinja2.go` to `internal/template/`**

Copy the existing `babylon-runner/jinja2.go` to `babylon-runner/internal/template/jinja2.go` with these changes:

1. Change `package main` to `package template`
2. Export `resolveJ2` → `ResolveJ2` and `j2VarContext` → `J2VarContext`
3. Decouple `J2VarContext` from `RunContext`: instead of taking `*RunContext`, accept two flat maps directly. **IMPORTANT:** The current code merges ALL of `spec.vars` (including `current_state`, `provision_data`, etc.), not just `job_vars`. The new signature preserves this behavior by accepting the output of `AllVars()`:

```go
// Before (in old jinja2.go — merges ALL spec.vars, not just job_vars):
func j2VarContext(rc *RunContext) map[string]interface{} {
    ctx := make(map[string]interface{})
    if sv := getNestedMap(rc.Payload.Subject, "spec", "vars"); sv != nil {
        mergeMap(ctx, sv)
    }
    if gv := getNestedMap(rc.Payload.Governor, "spec", "vars"); gv != nil {
        mergeMap(ctx, gv)
    }
    return ctx
}

// After (in internal/template/jinja2.go):
// J2VarContext builds the Jinja2 variable context from subject and
// governor vars — the same flat namespace the Ansible runner uses.
// Callers pass SubjectVars.AllVars() and GovernorVars.AllVars() to
// preserve the full merge behavior (all spec.vars, not just job_vars).
func J2VarContext(subjectAllVars, governorAllVars map[string]interface{}) map[string]interface{} {
    vars := make(map[string]interface{})
    for k, v := range subjectAllVars {
        vars[k] = v
    }
    for k, v := range governorAllVars {
        vars[k] = v
    }
    return vars
}
```

Callers in handler/ will call: `template.J2VarContext(rc.SubjectAllVars(), rc.GovernorAllVars())`.

Keep all unexported functions (`resolveJ2String`, `evalJ2Expr`, `lookupDotted`, `extractDefaultArg`) unchanged except for the package declaration.

- [ ] **Step 2: Migrate `jinja2_test.go` to `internal/template/`**

Copy the existing `babylon-runner/jinja2_test.go` to `babylon-runner/internal/template/jinja2_test.go`:

1. Change `package main` to `package template`
2. Update the `j2VarContext` test to use the new signature — pass two maps instead of a `RunContext`:

```go
// Before:
rc := &RunContext{Payload: RunPayload{Subject: map[string]interface{}{...}, Governor: map[string]interface{}{...}}}
vars := j2VarContext(rc)

// After (pass full vars maps, not just job_vars):
subjectAllVars := map[string]interface{}{
    "current_state": "started",
    "job_vars": map[string]interface{}{"uuid": "abc"},
    "provision_data": map[string]interface{}{"console": "url"},
}
govAllVars := map[string]interface{}{
    "job_vars": map[string]interface{}{"region": "us-east-1"},
    "__meta__": map[string]interface{}{"aws_sandboxed": true},
}
vars := J2VarContext(subjectAllVars, govAllVars)
// vars now contains: current_state, job_vars (governor wins), provision_data, __meta__
```

3. Export function references: `resolveJ2` → `ResolveJ2`

- [ ] **Step 3: Run template tests**

```bash
cd babylon-runner && go test ./internal/template/ -v -count=1
```

Expected: All tests PASS.

---

### Task 5: `internal/clients` — Anarchy, Tower, and Sandbox API Clients

**Files:**

- Create: `babylon-runner/internal/clients/anarchy.go`
- Create: `babylon-runner/internal/clients/tower.go`
- Create: `babylon-runner/internal/clients/sandbox.go`
- Create: `babylon-runner/internal/clients/anarchy_test.go`
- Create: `babylon-runner/internal/clients/tower_test.go`
- Create: `babylon-runner/internal/clients/sandbox_test.go`

**Interfaces:**

- Consumes: `httputil.NewTransport`, `httputil.RetryWithContext`, `httputil.DoJSON` from Task 3; `types.SubjectPatch`, `types.ScheduleActionRequest` from Task 2
- Produces:
  - `clients.NewAnarchyClient(cfg AnarchyClientConfig) *AnarchyClient`
  - `(*AnarchyClient).SubjectUpdate(ctx, subjectName, patch) error`
  - `(*AnarchyClient).ScheduleAction(ctx, subjectName, req) error`
  - `clients.NewTowerClient(hostname, username, password string) *TowerClient`
  - `(*TowerClient).CreateOAuthToken() (string, int, error)`
  - `(*TowerClient).DeleteOAuthToken(tokenID int) error`
  - `(*TowerClient).GetJobStatus(token string, jobID int) (map[string]interface{}, error)`
  - `(*TowerClient).CancelJob(token string, jobID int) error`
  - `(*TowerClient).LaunchJob(config TowerJobConfig) (int, error)`
  - `clients.SelectController(controllers []map[string]interface{}, mode string) map[string]interface{}`
  - `clients.NewSandboxAPIClient(baseURL string) *SandboxAPIClient`
  - `(*SandboxAPIClient).Login(loginToken string) (string, error)`
  - `(*SandboxAPIClient).GetPlacement(accessToken, uuid string) (map[string]interface{}, int, error)`
  - `(*SandboxAPIClient).BookPlacement(accessToken string, reqBody map[string]interface{}) (map[string]interface{}, int, error)`
  - Plus: `ReleasePlacement`, `StartPlacement`, `StopPlacement`, `GetRequestStatus`

- [ ] **Step 1: Migrate `anarchy.go` to `internal/clients/anarchy.go`**

Copy the existing `babylon-runner/anarchy.go` with these changes:

1. Change `package main` to `package clients`
2. Import `httputil` and `types` packages:
   ```go
   import (
       "github.com/rhpds/anarchy/babylon-runner/internal/httputil"
       "github.com/rhpds/anarchy/babylon-runner/internal/types"
   )
   ```
3. Define a local config struct to avoid importing runner/:
   ```go
   type AnarchyClientConfig struct {
       BaseURL    string
       AuthHeader string
       Timeout    time.Duration
   }
   ```
4. Update `NewAnarchyClient` to accept `AnarchyClientConfig` instead of `Config`
5. Replace inline retry logic in `doWithRetry` with `httputil.RetryWithContext`:
   ```go
   func (a *AnarchyClient) doWithRetry(ctx context.Context, method, url string, body interface{}) error {
       return httputil.RetryWithContext(ctx, a.retryDelays, func() error {
           status, err := httputil.DoJSON(ctx, a.client, method, url,
               map[string]string{"Authorization": a.authHeader}, body, nil)
           if err != nil {
               return err
           }
           if status >= 400 {
               return fmt.Errorf("%s %s: status %d", method, url, status)
           }
           return nil
       })
   }
   ```
6. Update method signatures to use `types.SubjectPatch` and `types.ScheduleActionRequest`
7. Add `retryDelays []time.Duration` field to `AnarchyClient` (default: `[]time.Duration{5*time.Second, 10*time.Second, 20*time.Second}`)

- [ ] **Step 2: Migrate `anarchy_test.go` to `internal/clients/anarchy_test.go`**

Copy existing tests with:

1. `package clients` (or `package clients_test` for black-box)
2. Update fixture construction to use `AnarchyClientConfig` instead of `Config`
3. Update type references to use `types.SubjectPatch`, etc.
4. Verify retry behavior works with the new `httputil.RetryWithContext`

- [ ] **Step 3: Run anarchy client tests**

```bash
cd babylon-runner && go test ./internal/clients/ -run TestAnarchy -v -count=1
```

Expected: All PASS.

- [ ] **Step 4: Migrate `tower.go` to `internal/clients/tower.go`**

Copy the existing `babylon-runner/tower.go` with these changes:

1. Change `package main` to `package clients`
2. Export `selectController` → `SelectController`, `getJobCount` → `GetJobCount`
3. The `TowerClient` struct keeps its existing HTTP client for now — Phase 1 does not change the Tower client's internal HTTP handling beyond making it available in the new package. The `httputil.DoJSON` migration happens incrementally: methods that are straightforward to convert should use `DoJSON`, while complex flows (LaunchJob with its multi-step create-search-associate pattern) can retain inline HTTP for now and be migrated in Phase 2 with context propagation.
4. Export `TowerJobConfig`, `EEConfig` (already exported)
5. Export `InsertUnvaultString` if used by handlers
6. Keep `InsecureSkipVerify: true` for now — Phase 2 (#5) makes TLS configurable

- [ ] **Step 5: Migrate `tower_test.go` to `internal/clients/tower_test.go`**

Copy existing tests, update package declaration and any internal references.

- [ ] **Step 6: Migrate `sandboxapi.go` to `internal/clients/sandbox.go`**

Copy the existing `babylon-runner/sandboxapi.go` with these changes:

1. Change `package main` to `package clients`
2. Update `doPlacementAction` retry to use `httputil.RetryWithContext`:
   ```go
   func (s *SandboxAPIClient) doPlacementAction(accessToken, url string) (map[string]interface{}, error) {
       var result map[string]interface{}
       err := httputil.RetryWithContext(context.TODO(), s.retryDelays, func() error {
           status, err := httputil.DoJSON(context.TODO(), s.client, http.MethodPut, url,
               map[string]string{"Authorization": "Bearer " + accessToken}, nil, &result)
           if err != nil { return err }
           if status >= 400 { return fmt.Errorf("placement action %s: status %d", url, status) }
           return nil
       })
       return result, err
   }
   ```
3. Update `Login` retry to use `httputil.RetryWithContext`:
   ```go
   func (s *SandboxAPIClient) Login(loginToken string) (string, error) {
       var accessToken string
       err := httputil.RetryWithContext(context.TODO(), s.loginRetryDelays, func() error {
           // ... existing login logic with DoJSON ...
       })
       return accessToken, err
   }
   ```
4. Keep `DefaultSandboxAPIURL` constant

- [ ] **Step 7: Migrate `sandboxapi_test.go` to `internal/clients/sandbox_test.go`**

Copy existing tests, update package declaration.

- [ ] **Step 8: Run all client tests**

```bash
cd babylon-runner && go test ./internal/clients/ -v -count=1
```

Expected: All tests PASS.

---

### Task 6: `internal/runner` — Runner, RunContext, and Config

**Files:**

- Create: `babylon-runner/internal/runner/config.go`
- Create: `babylon-runner/internal/runner/runner.go`
- Create: `babylon-runner/internal/runner/config_test.go`
- Create: `babylon-runner/internal/runner/runner_test.go`

**Interfaces:**

- Consumes: `types.RunPayload`, `types.RunResult`, `types.ResultPayload` from Task 2; `clients.AnarchyClient`, `clients.AnarchyClientConfig` from Task 5
- Produces:
  - `runner.Config` struct with `AuthHeader() string`
  - `runner.ConfigFromEnv() (Config, error)`
  - `runner.HandlerFunc` type: `func(rc *RunContext) error`
  - `runner.RunContext` with convenience methods: `CurrentState()`, `DesiredState()`, `JobVars()`, `GovernorJobVars()`, `SubjectAllVars()`, `GovernorAllVars()`, `Meta()`, `SubjectName()`, `RunName()`, `ActionName()`, `UUID()`, `GUID()`, `SandboxAPIInUse()`, `DeployerDisabled(action)`, `StatusActions()`, `StatusTowerJobs()`, `ActionRetryCount()`, `IsBeingDeleted()`, `GovernorActions()`, `FinishAction(state)`, `DeleteSubject(removeFinalizers)`, `ContinueAction(after)`, `ContinueActionWithVars(after, vars)`, `SubjectUpdate(patch)`, `ScheduleAction(req)`
  - `runner.New(cfg Config, clientset kubernetes.Interface) *Runner`
  - `(*Runner).SetHandlers(handlers map[string]HandlerFunc)`
  - `(*Runner).Run()`

- [ ] **Step 1: Migrate `config.go` to `internal/runner/config.go`**

Copy the existing `babylon-runner/config.go` with:

1. `package runner`
2. Export: `Config`, `ConfigFromEnv` (was `configFromEnv`), `AuthHeader`
3. All fields and behavior stay the same

```go
package runner

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	AnarchyURL      string
	RunnerName      string
	RunnerToken     string
	PodName         string
	PollingInterval time.Duration
	RequestTimeout  time.Duration
}

func (c Config) AuthHeader() string {
	return fmt.Sprintf("Bearer %s:%s:%s", c.RunnerName, c.PodName, c.RunnerToken)
}

func ConfigFromEnv() (Config, error) {
	cfg := Config{}
	cfg.AnarchyURL = os.Getenv("ANARCHY_URL")
	if cfg.AnarchyURL == "" {
		return cfg, fmt.Errorf("ANARCHY_URL is required")
	}
	cfg.RunnerName = os.Getenv("RUNNER_NAME")
	if cfg.RunnerName == "" {
		return cfg, fmt.Errorf("RUNNER_NAME is required")
	}
	cfg.RunnerToken = os.Getenv("RUNNER_TOKEN")
	if cfg.RunnerToken == "" {
		return cfg, fmt.Errorf("RUNNER_TOKEN is required")
	}
	cfg.PodName = os.Getenv("HOSTNAME")
	if cfg.PodName == "" {
		return cfg, fmt.Errorf("HOSTNAME is required")
	}
	cfg.PollingInterval = time.Duration(envInt("POLLING_INTERVAL", 5)) * time.Second
	cfg.RequestTimeout = time.Duration(envInt("REQUEST_TIMEOUT", 35)) * time.Second
	return cfg, nil
}

func envInt(key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}
```

- [ ] **Step 2: Migrate `runner.go` to `internal/runner/runner.go`**

This is the most complex migration. The key changes:

1. `package runner`
2. Import `types`, `clients`, `httputil` packages
3. `RunContext` uses typed payload fields for convenience methods:

```go
package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
	"k8s.io/client-go/kubernetes"
)

// HandlerFunc is the signature for all run handlers.
type HandlerFunc func(rc *RunContext) error

// RunContext holds the per-run state and provides convenience methods.
type RunContext struct {
	Payload        types.RunPayload
	Result         types.RunResult
	AnarchyClient  *clients.AnarchyClient
	Clientset      kubernetes.Interface
	SandboxBaseURL string // overridden in tests
}

// --- Convenience accessors (typed payloads make these trivial) ---

func (rc *RunContext) SubjectName() string    { return rc.Payload.Subject.Metadata.Name }
func (rc *RunContext) RunName() string         { return rc.Payload.Run.Metadata.Name }
func (rc *RunContext) CurrentState() string    { return rc.Payload.Subject.Spec.Vars.CurrentState }
func (rc *RunContext) DesiredState() string    { return rc.Payload.Subject.Spec.Vars.DesiredState }
func (rc *RunContext) CheckStatusState() string { return rc.Payload.Subject.Spec.Vars.CheckStatusState }

func (rc *RunContext) ActionName() string {
	if rc.Payload.Action != nil {
		return rc.Payload.Action.Spec.Action
	}
	return ""
}

func (rc *RunContext) ActionVars() map[string]interface{} {
	if rc.Payload.Action != nil {
		return rc.Payload.Action.Spec.Vars
	}
	return nil
}

func (rc *RunContext) JobVars() map[string]interface{} {
	return rc.Payload.Subject.Spec.Vars.JobVars
}

func (rc *RunContext) GovernorJobVars() map[string]interface{} {
	return rc.Payload.Governor.Spec.Vars.JobVars
}

func (rc *RunContext) SubjectAllVars() map[string]interface{} {
	return rc.Payload.Subject.Spec.Vars.AllVars()
}

func (rc *RunContext) GovernorAllVars() map[string]interface{} {
	return rc.Payload.Governor.Spec.Vars.AllVars()
}

func (rc *RunContext) Meta() *types.Meta {
	if rc.Payload.Governor.Spec.Vars.Meta != nil {
		return rc.Payload.Governor.Spec.Vars.Meta
	}
	return &types.Meta{}
}

func (rc *RunContext) SandboxAPIInUse() bool {
	return rc.Meta().AWSSandboxed
}

func (rc *RunContext) DeployerDisabled(action string) bool {
	meta := rc.Meta()
	if meta.Deployer == nil {
		return false
	}
	cfg, ok := meta.Deployer.Actions[action]
	if !ok {
		return false
	}
	return cfg.Disabled
}

func (rc *RunContext) UUID() string {
	return types.StringFromMap(rc.JobVars(), "uuid")
}

func (rc *RunContext) GUID() string {
	return types.StringFromMap(rc.JobVars(), "guid")
}

func (rc *RunContext) StatusActions() map[string]interface{} {
	return rc.Payload.Subject.Status.Actions
}

func (rc *RunContext) StatusTowerJobs() map[string]interface{} {
	return rc.Payload.Subject.Status.TowerJobs
}

func (rc *RunContext) GovernorActions() map[string]interface{} {
	return rc.Payload.Governor.Spec.Actions
}

func (rc *RunContext) ActionRetryCount() int {
	if rc.Payload.Action == nil || rc.Payload.Action.Spec.Vars == nil {
		return 0
	}
	count, _ := rc.Payload.Action.Spec.Vars["action_retry_count"].(float64)
	return int(count)
}

func (rc *RunContext) IsBeingDeleted() bool {
	return rc.Payload.Subject.Metadata.DeletionTimestamp != nil
}

// --- Directives ---

func (rc *RunContext) FinishAction(state string) {
	rc.Result.FinishAction = &types.FinishActionDirective{State: state}
}

func (rc *RunContext) DeleteSubject(removeFinalizers bool) {
	rc.Result.DeleteSubject = &types.DeleteSubjectDirective{RemoveFinalizers: removeFinalizers}
}

func (rc *RunContext) ContinueAction(after string) {
	rc.Result.ContinueAction = &types.ContinueActionDirective{
		After: types.AfterTimestamp(after),
	}
}

func (rc *RunContext) ContinueActionWithVars(after string, vars map[string]interface{}) {
	rc.Result.ContinueAction = &types.ContinueActionDirective{
		After: types.AfterTimestamp(after),
		Vars:  vars,
	}
}

// --- Delegates ---

func (rc *RunContext) SubjectUpdate(patch types.SubjectPatch) error {
	return rc.AnarchyClient.SubjectUpdate(context.TODO(), rc.SubjectName(), patch)
}

func (rc *RunContext) ScheduleAction(req types.ScheduleActionRequest) error {
	return rc.AnarchyClient.ScheduleAction(context.TODO(), rc.SubjectName(), req)
}

// --- Runner ---

type Runner struct {
	config    Config
	client    *http.Client
	anarchy   *clients.AnarchyClient
	clientset kubernetes.Interface
	handlers  map[string]HandlerFunc
}

func New(cfg Config, clientset kubernetes.Interface) *Runner {
	anarchyCfg := clients.AnarchyClientConfig{
		BaseURL:    cfg.AnarchyURL,
		AuthHeader: cfg.AuthHeader(),
		Timeout:    cfg.RequestTimeout,
	}
	return &Runner{
		config:    cfg,
		client:    &http.Client{Timeout: cfg.RequestTimeout},
		anarchy:   clients.NewAnarchyClient(anarchyCfg),
		clientset: clientset,
		handlers:  make(map[string]HandlerFunc),
	}
}

func (r *Runner) SetHandlers(handlers map[string]HandlerFunc) {
	r.handlers = handlers
}

func (r *Runner) Run() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	slog.Info("babylon-runner starting",
		"runner", r.config.RunnerName,
		"pod", r.config.PodName,
		"url", r.config.AnarchyURL)

	ticker := time.NewTicker(r.config.PollingInterval)
	defer ticker.Stop()

	r.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		case <-ticker.C:
			r.pollOnce(ctx)
		}
	}
}

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
		Payload:       *payload,
		AnarchyClient: r.anarchy,
		Clientset:     r.clientset,
	}

	slog.Info("dispatching run",
		"run", rc.RunName(),
		"handler", payload.Handler.Type+":"+payload.Handler.Name)

	if err := Dispatch(rc, r.handlers); err != nil {
		slog.Error("handler failed", "run", rc.RunName(), "error", err)
		rc.Result.Status = "failed"
		rc.Result.StatusMessage = err.Error()
	}

	if err := r.postResult(ctx, rc.RunName(), rc.Result); err != nil {
		slog.Error("post result failed", "run", rc.RunName(), "error", err)
	}
	return nil
}

func (r *Runner) getRun(ctx context.Context) (*types.RunPayload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		r.config.AnarchyURL+"/run", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", r.config.AuthHeader())

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var payload types.RunPayload
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("decode run payload: %w", err)
		}
		return &payload, nil
	case http.StatusNoContent, http.StatusRequestTimeout:
		return nil, nil
	case http.StatusForbidden:
		return nil, fmt.Errorf("authentication failed (403)")
	default:
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
}

func (r *Runner) postResult(ctx context.Context, runName string, result types.RunResult) error {
	url := fmt.Sprintf("%s/run/%s", r.config.AnarchyURL, runName)
	maxRetries := 10
	baseDelay := 5 * time.Second
	maxDelay := 60 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(attempt)
			if delay > maxDelay {
				delay = maxDelay
			}
			slog.Warn("retrying POST result", "attempt", attempt, "delay", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		body, _ := json.Marshal(result)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
			strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", r.config.AuthHeader())

		resp, err := r.client.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		slog.Warn("POST result failed", "status", resp.StatusCode)
	}
	return fmt.Errorf("POST result failed after %d retries", maxRetries)
}

// Dispatch routes a run to the appropriate handler based on handler type and name.
func Dispatch(rc *RunContext, handlers map[string]HandlerFunc) error {
	var key string
	switch rc.Payload.Handler.Type {
	case "subjectEvent":
		key = "event:" + rc.Payload.Handler.Name
	case "action":
		key = "action:" + rc.ActionName()
	case "actionCallback":
		key = "action:" + rc.ActionName() + ":" + rc.Payload.Handler.Name
	default:
		return fmt.Errorf("unknown handler type: %s", rc.Payload.Handler.Type)
	}

	handler, ok := handlers[key]
	if !ok {
		return fmt.Errorf("no handler registered for %s", key)
	}
	return handler(rc)
}
```

- [ ] **Step 3: Migrate `config_test.go` to `internal/runner/config_test.go`**

Copy existing tests with `package runner` and update `configFromEnv` → `ConfigFromEnv`.

- [ ] **Step 4: Write runner tests**

Create `babylon-runner/internal/runner/runner_test.go`. Migrate tests from the existing `runner_test.go` with these changes:

1. `package runner`
2. Use `types.RunPayload` with typed fields instead of `map[string]interface{}`
3. Use `types.Handler`, `types.Subject`, `types.Run` etc.
4. Test `Dispatch` routing with typed payloads
5. Test `RunContext` convenience methods with typed fields

Key test patterns to migrate:

- Polling loop tests using `httptest.NewServer`
- Dispatch routing tests (event, action, actionCallback)
- RunContext directive tests (FinishAction, ContinueAction, DeleteSubject)
- POST result retry tests

For `RunContext` convenience method tests, add:

```go
func TestRunContext_ConvenienceMethods(t *testing.T) {
	rc := &RunContext{
		Payload: types.RunPayload{
			Handler: types.Handler{Type: "action", Name: "provision"},
			Governor: types.Governor{
				Metadata: types.ObjectMeta{Name: "test-gov"},
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{
						JobVars: map[string]interface{}{"region": "us-east-1"},
						Meta: &types.Meta{
							AWSSandboxed: true,
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
						CurrentState: "started",
						DesiredState: "stopped",
						JobVars:      map[string]interface{}{"uuid": "abc-123", "guid": "xyz-456"},
					},
				},
			},
			Action: &types.Action{
				Metadata: types.ObjectMeta{Name: "test-action"},
				Spec:     types.ActionSpec{Action: "provision"},
			},
			Run: types.Run{Metadata: types.ObjectMeta{Name: "test-run"}},
		},
	}

	if rc.SubjectName() != "test-subj" { t.Errorf("SubjectName = %q", rc.SubjectName()) }
	if rc.RunName() != "test-run" { t.Errorf("RunName = %q", rc.RunName()) }
	if rc.CurrentState() != "started" { t.Errorf("CurrentState = %q", rc.CurrentState()) }
	if rc.DesiredState() != "stopped" { t.Errorf("DesiredState = %q", rc.DesiredState()) }
	if rc.ActionName() != "provision" { t.Errorf("ActionName = %q", rc.ActionName()) }
	if rc.UUID() != "abc-123" { t.Errorf("UUID = %q", rc.UUID()) }
	if rc.GUID() != "xyz-456" { t.Errorf("GUID = %q", rc.GUID()) }
	if !rc.SandboxAPIInUse() { t.Error("SandboxAPIInUse = false") }
	if !rc.DeployerDisabled("status") { t.Error("DeployerDisabled(status) = false") }
	if rc.DeployerDisabled("provision") { t.Error("DeployerDisabled(provision) = true") }
}
```

- [ ] **Step 5: Run all runner tests**

```bash
cd babylon-runner && go test ./internal/runner/ -v -count=1
```

Expected: All tests PASS.

---

### Task 7: `internal/handler` — All Handlers

**Files:**

- Create: `babylon-runner/internal/handler/register.go`
- Create: `babylon-runner/internal/handler/provision.go`
- Create: `babylon-runner/internal/handler/destroy.go`
- Create: `babylon-runner/internal/handler/start.go`
- Create: `babylon-runner/internal/handler/stop.go`
- Create: `babylon-runner/internal/handler/status.go`
- Create: `babylon-runner/internal/handler/update.go`
- Create: `babylon-runner/internal/handler/event_create.go`
- Create: `babylon-runner/internal/handler/event_update.go`
- Create: `babylon-runner/internal/handler/event_delete.go`
- Create: `babylon-runner/internal/handler/check_deployer.go`
- Create: `babylon-runner/internal/handler/sandbox.go`
- Create: `babylon-runner/internal/handler/tower_launch.go`
- Create: `babylon-runner/internal/handler/helpers_test.go`
- Create: `babylon-runner/internal/handler/handler_test.go`
- Create: `babylon-runner/internal/handler/sandbox_test.go`
- Create: `babylon-runner/internal/handler/tower_launch_test.go`
- Create: `babylon-runner/internal/handler/check_deployer_test.go`
- Create: `babylon-runner/internal/handler/integration_test.go`

**Interfaces:**

- Consumes: `runner.RunContext`, `runner.HandlerFunc` from Task 6; `clients.*` from Task 5; `types.*` from Task 2; `template.*` from Task 4
- Produces:
  - `handler.Register() map[string]runner.HandlerFunc` — returns the complete handler map for runner registration

- [ ] **Step 1: Create `register.go`**

```go
package handler

import (
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
)

// Register returns the complete map of handler keys to handler functions.
func Register() map[string]runner.HandlerFunc {
	return map[string]runner.HandlerFunc{
		"event:create": handleEventCreate,
		"event:update": handleEventUpdate,
		"event:delete": handleEventDelete,

		"action:provision":                       handleProvision,
		"action:provision:checkProvisionQueue":    checkProvisionQueue,
		"action:destroy":                         handleDestroy,
		"action:start":                           handleStart,
		"action:stop":                            handleStop,
		"action:status":                          handleStatus,
		"action:update":                          handleUpdate,

		"action:provision:checkDeployerJob":       checkDeployerProvision,
		"action:destroy:checkDeployerJob":         checkDeployerDestroy,
		"action:start:checkDeployerJob":           checkDeployerStart,
		"action:stop:checkDeployerJob":            checkDeployerStop,
		"action:status:checkDeployerJob":          checkDeployerStatus,
		"action:update:checkDeployerJob":          checkDeployerUpdate,
	}
}
```

Note: The check handler keys must match exactly what the original `registerHandlers` in `main.go` registers. Read the original `main.go` carefully to capture all keys.

- [ ] **Step 2: Migrate handler files**

For each handler file, apply these transformations:

1. `package main` → `package handler`
2. Add imports:
   ```go
   import (
       "github.com/rhpds/anarchy/babylon-runner/internal/clients"
       "github.com/rhpds/anarchy/babylon-runner/internal/runner"
       "github.com/rhpds/anarchy/babylon-runner/internal/template"
       "github.com/rhpds/anarchy/babylon-runner/internal/types"
   )
   ```
3. Replace `*RunContext` → `*runner.RunContext` in all function signatures
4. Replace map-based payload access with typed access:

**Common transformations (apply to all handlers):**

| Old pattern                                                                 | New pattern                                          |
| --------------------------------------------------------------------------- | ---------------------------------------------------- |
| `rc.Payload.Subject["metadata"]["name"].(string)`                           | `rc.SubjectName()`                                   |
| `rc.Payload.Run["metadata"]["name"].(string)`                               | `rc.RunName()`                                       |
| `rc.Payload.Action["metadata"]["name"].(string)`                            | `rc.Payload.Action.Metadata.Name`                    |
| `rc.Payload.Action["spec"]["action"].(string)`                              | `rc.ActionName()`                                    |
| `getNestedString(rc.Payload.Subject, "spec", "vars", "current_state")`      | `rc.CurrentState()`                                  |
| `getNestedString(rc.Payload.Subject, "spec", "vars", "desired_state")`      | `rc.DesiredState()`                                  |
| `getNestedString(rc.Payload.Subject, "spec", "vars", "check_status_state")` | `rc.CheckStatusState()`                              |
| `getNestedMap(rc.Payload.Subject, "spec", "vars", "job_vars")`              | `rc.JobVars()`                                       |
| `getNestedMap(rc.Payload.Governor, "spec", "vars", "job_vars")`             | `rc.GovernorJobVars()`                               |
| `getNestedMap(rc.Payload.Governor, "spec", "vars", "__meta__")`             | use `rc.Meta()` (returns `*types.Meta`)              |
| `meta["deployer"].(map[string]interface{})`                                 | `rc.Meta().Deployer` (returns `*types.DeployerMeta`) |
| `meta["ansible_controllers"]`                                               | `rc.Meta().AnsibleControllers`                       |
| `meta["tower"]`                                                             | `rc.Meta().Tower` (returns `*types.TowerMeta`)       |
| `meta["aws_sandboxed"]`                                                     | `rc.Meta().AWSSandboxed`                             |
| `rc.Payload.Subject["metadata"]["deletionTimestamp"] != nil`                | `rc.IsBeingDeleted()`                                |
| `getNestedMap(rc.Payload.Subject, "status", "towerJobs")`                   | `rc.StatusTowerJobs()`                               |
| `getNestedMap(rc.Payload.Subject, "status", "actions")`                     | `rc.StatusActions()`                                 |
| `getNestedMap(rc.Payload.Governor, "spec", "actions")`                      | `rc.GovernorActions()`                               |
| `getNestedMap(rc.Payload.Action, "spec", "vars")`                           | `rc.ActionVars()`                                    |
| `getNestedMap(rc.Payload.Governor, "spec", "vars", "sandbox_api")`          | `rc.Payload.Governor.Spec.Vars.SandboxAPI`           |
| `j2VarContext(rc)`                                                          | `template.J2VarContext(rc.SubjectAllVars(), rc.GovernorAllVars())` |
| `subjectVars["check_status_request_timestamp"]`                             | `rc.Payload.Subject.Spec.Vars.GetString("check_status_request_timestamp")` |
| `subjectVars["provision_data"]`                                             | `rc.Payload.Subject.Spec.Vars.Get("provision_data")` |
| `SubjectPatch{...}`                                                         | `types.SubjectPatch{...}`                            |
| `ScheduleActionRequest{...}`                                                | `types.ScheduleActionRequest{...}`                   |
| `getNestedString(...)` on dynamic maps                                      | `types.GetNestedString(...)`                         |
| `getNestedMap(...)` on dynamic maps                                         | `types.GetNestedMap(...)`                            |
| `getNestedBool(...)` on dynamic maps                                        | `types.GetNestedBool(...)`                           |
| `setNested(...)`                                                            | `types.SetNested(...)`                               |
| `nowUTC()`                                                                  | `types.NowUTC()`                                     |
| `mergeMap(...)`                                                             | `types.MergeMap(...)`                                |
| `extractStringSlice(...)`                                                   | `types.ExtractStringSlice(...)`                      |
| `afterTimestamp(...)`                                                       | `types.AfterTimestamp(...)`                          |

**Deployer access (in `tower_launch.go`, `check_deployer.go`):**

| Old pattern                                  | New pattern                                |
| -------------------------------------------- | ------------------------------------------ |
| `deployer["type"].(string)`                  | `meta.Deployer.Type`                       |
| `deployer["scm_url"].(string)`               | `meta.Deployer.SCMUrl`                     |
| `deployer["scm_ref"].(string)`               | `meta.Deployer.SCMRef`                     |
| `deployer["actions"][action]["entry_point"]` | `meta.Deployer.Actions[action].EntryPoint` |

**Tower meta access:**

| Old pattern                     | New pattern                                     |
| ------------------------------- | ----------------------------------------------- |
| `meta["tower"]["organization"]` | `rc.Meta().Tower.Organization` (with nil check) |
| `meta["tower"]["timeout"]`      | `rc.Meta().Tower.Timeout`                       |

**Kubernetes secret reading (in `tower_launch.go:resolveControllerCreds`):**

Replace `k8sSecretData(namespace, labelSelector)` with direct `client-go` call:

```go
func resolveControllerCreds(rc *runner.RunContext, controller map[string]interface{}) (string, string, error) {
    // ... try direct creds and varSecret first (unchanged logic) ...

    // K8s secret fallback using client-go
    hostname := types.StringFromMap(controller, "hostname")
    ns := rc.Payload.Subject.Metadata.Namespace
    secrets, err := rc.Clientset.CoreV1().Secrets(ns).List(context.TODO(), metav1.ListOptions{
        LabelSelector: fmt.Sprintf("babylon.gpte.redhat.com/ansible-control-plane=%s", hostname),
    })
    if err != nil {
        return "", "", fmt.Errorf("list secrets: %w", err)
    }
    if len(secrets.Items) == 0 {
        return "", "", fmt.Errorf("no secret found for controller %s", hostname)
    }
    secret := secrets.Items[0]
    user := string(secret.Data["user"])
    pass := string(secret.Data["password"])
    return user, pass, nil
}
```

Import `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"` in tower_launch.go.

**Sandbox handler (`sandbox.go`):**

Replace `j2VarContext(rc)` calls with:

```go
vars := template.J2VarContext(rc.SubjectAllVars(), rc.GovernorAllVars())
resolved := template.ResolveJ2(data, vars)
```

Replace `SandboxAPIClient` creation with `clients.NewSandboxAPIClient(url)`.

**Jinja2 resolver usage:**

Replace `resolveJ2(data, vars)` with `template.ResolveJ2(data, vars)`.

- [ ] **Step 3: Migrate test helper utilities to `helpers_test.go`**

Create `babylon-runner/internal/handler/helpers_test.go` with the test infrastructure from the existing `handlers_test.go`:

1. `anarchyCall` struct for recording HTTP requests
2. `newTestAnarchyServer(t)` — returns `(*httptest.Server, *[]anarchyCall)`
3. `newTestRunContext(t, server)` — creates a `*runner.RunContext` with typed payload fields:

```go
func newTestRunContext(t *testing.T, server *httptest.Server) *runner.RunContext {
    t.Helper()
    anarchyCfg := clients.AnarchyClientConfig{
        BaseURL:    server.URL,
        AuthHeader: "Bearer test:pod:token",
        Timeout:    5 * time.Second,
    }
    return &runner.RunContext{
        Payload: types.RunPayload{
            Handler:  types.Handler{Type: "action", Name: "provision"},
            Governor: types.Governor{
                Metadata: types.ObjectMeta{Name: "test-gov", Namespace: "test-ns"},
                Spec: types.GovernorSpec{
                    Vars: types.GovernorVars{
                        JobVars: map[string]interface{}{},
                        Meta:    &types.Meta{},
                    },
                    Actions: map[string]map[string]interface{}{
                        "provision": {"roles": []interface{}{map[string]interface{}{"name": "check-deployer"}}},
                        "destroy":   {"roles": []interface{}{map[string]interface{}{"name": "check-deployer"}}},
                        "start":     {"roles": []interface{}{map[string]interface{}{"name": "check-deployer"}}},
                        "stop":      {"roles": []interface{}{map[string]interface{}{"name": "check-deployer"}}},
                        "update":    {"roles": []interface{}{map[string]interface{}{"name": "check-deployer"}}},
                    },
                },
            },
            Subject: types.Subject{
                Metadata: types.ObjectMeta{Name: "test-subj", Namespace: "test-ns"},
                Spec: types.SubjectSpec{
                    Vars: types.SubjectVars{JobVars: map[string]interface{}{}},
                },
                Status: types.SubjectStatus{
                    TowerJobs: map[string]interface{}{},
                    Actions:   map[string]interface{}{},
                },
            },
            Action: &types.Action{
                Metadata: types.ObjectMeta{Name: "test-action"},
                Spec:     types.ActionSpec{Action: "provision", Vars: map[string]interface{}{}},
            },
            Run: types.Run{Metadata: types.ObjectMeta{Name: "test-run"}},
        },
        AnarchyClient: clients.NewAnarchyClient(anarchyCfg),
    }
}
```

4. `newTestTowerServer(t)` — mock Tower API server
5. `withTowerServer(rc, towerServer)` — configures Tower meta on the RunContext
6. `assertAfterTimestamp(t, got, expected)` — timestamp tolerance checker

- [ ] **Step 4: Migrate all handler test files**

Migrate each test file from the flat structure:

| Old file                                        | New file                              |
| ----------------------------------------------- | ------------------------------------- |
| `handlers_test.go` (test helpers + basic tests) | `helpers_test.go` + `handler_test.go` |
| `handler_completion_test.go`                    | `check_deployer_test.go`              |
| `handler_integration_test.go`                   | `integration_test.go`                 |
| `handler_sandbox_test.go`                       | `sandbox_test.go`                     |
| `handler_startstop_test.go`                     | `startstop_test.go`                   |
| `handler_statusupdate_test.go`                  | `statusupdate_test.go`                |
| `handler_tower_launch_test.go`                  | `tower_launch_test.go`                |

For each test file:

1. `package handler`
2. Update payload construction to use typed structs (same transformation patterns as Step 2)
3. Update type references to use `types.`, `runner.`, `clients.` prefixes
4. Update helper function calls to use exported names
5. Use `fake.NewSimpleClientset()` from `k8s.io/client-go/kubernetes/fake` for tests that exercise `resolveControllerCreds`:

```go
import (
    "k8s.io/client-go/kubernetes/fake"
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// In test setup:
secret := &corev1.Secret{
    ObjectMeta: metav1.ObjectMeta{
        Name:      "tower-creds",
        Namespace: "test-ns",
        Labels:    map[string]string{"babylon.gpte.redhat.com/ansible-control-plane": "tower.example.com"},
    },
    Data: map[string][]byte{
        "user":     []byte("admin"),
        "password": []byte("secret"),
    },
}
rc.Clientset = fake.NewSimpleClientset(secret)
```

- [ ] **Step 5: Run all handler tests**

```bash
cd babylon-runner && go test ./internal/handler/ -v -count=1
```

Expected: All tests PASS.

- [ ] **Step 6: Run all internal package tests together**

```bash
cd babylon-runner && go test ./internal/... -v -count=1
```

Expected: All tests PASS across all packages.

---

### Task 8: Entry Point, Build, and Documentation

**Files:**

- Create: `babylon-runner/cmd/babylon-runner/main.go`
- Modify: `babylon-runner/Dockerfile`
- Modify: `babylon-runner/dev-run.sh`
- Modify: `babylon-runner/DEPLOY.md`
- Delete: All old flat `.go` files in `babylon-runner/` root (21 source + 14 test files)
- Modify: `docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md` (update Phase 1 status)

**Interfaces:**

- Consumes: `runner.New`, `runner.ConfigFromEnv` from Task 6; `handler.Register` from Task 7
- Produces: Working binary at `cmd/babylon-runner/main.go`; updated Dockerfile, dev-run.sh, DEPLOY.md

- [ ] **Step 1: Create `cmd/babylon-runner/main.go`**

```go
package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/rhpds/anarchy/babylon-runner/internal/handler"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := runner.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	clientset, err := buildClientset()
	if err != nil {
		slog.Warn("kubernetes client not available", "error", err)
	}

	r := runner.New(cfg, clientset)
	r.SetHandlers(handler.Register())
	r.Run()
}

func buildClientset() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(config)
}
```

- [ ] **Step 2: Delete old flat Go files**

Remove all `.go` files from the `babylon-runner/` root directory (these have been migrated to `internal/`):

```bash
cd babylon-runner
rm -f main.go runner.go config.go types.go anarchy.go tower.go sandboxapi.go k8s.go jinja2.go
rm -f handler_provision.go handler_destroy.go handler_start.go handler_stop.go
rm -f handler_status.go handler_update.go handler_event_create.go handler_event_update.go
rm -f handler_event_delete.go handler_check_deployer.go handler_sandbox.go handler_tower_launch.go
rm -f runner_test.go config_test.go types_test.go anarchy_test.go tower_test.go
rm -f sandboxapi_test.go jinja2_test.go handlers_test.go handler_completion_test.go
rm -f handler_integration_test.go handler_sandbox_test.go handler_startstop_test.go
rm -f handler_statusupdate_test.go handler_tower_launch_test.go
```

- [ ] **Step 3: Update Dockerfile**

Replace the existing `babylon-runner/Dockerfile`:

```dockerfile
FROM golang:1.25.9 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags '-s -w' -o babylon-runner ./cmd/babylon-runner/

FROM registry.access.redhat.com/ubi9-micro
COPY --from=builder /app/babylon-runner /babylon-runner
ENTRYPOINT ["/babylon-runner"]
```

Key change: `COPY *.go ./` → `COPY . .` to capture `internal/` and `cmd/` directories. Build path changes from `.` to `./cmd/babylon-runner/`.

- [ ] **Step 4: Update `dev-run.sh`**

Update the build line in `dev-run.sh` to use `make build`:

```bash
# Old:
go build -o "${TMPDIR:-/tmp}/babylon-runner" . || exit 1

# New:
make build || exit 1
```

And update the run line:

```bash
# Old:
"${TMPDIR:-/tmp}/babylon-runner"

# New:
bin/babylon-runner
```

- [ ] **Step 5: Update `DEPLOY.md`**

Update the unit test command to reflect the new structure:

````markdown
### 1. Run unit tests

\```bash
cd anarchy/babylon-runner
make test
\```

Or to run tests for a specific package:

\```bash
go test ./internal/types/ -v
go test ./internal/runner/ -v
go test ./internal/handler/ -v
\```
````

Update the local dev build command:

````markdown
### 2. Test locally against a real cluster

\```bash
make build
export ANARCHY_URL=http://localhost:5000

# ... rest unchanged ...

bin/babylon-runner
\```
````

- [ ] **Step 6: Run full test suite**

```bash
cd babylon-runner && go test ./... -v -count=1
```

Expected: ALL tests pass. No compilation errors. No old files remaining.

- [ ] **Step 7: Verify the build**

```bash
cd babylon-runner && make build
ls -la bin/babylon-runner
```

Expected: Binary exists at `bin/babylon-runner`.

- [ ] **Step 8: Smoke test against dev cluster**

Run the Go runner against the dev cluster to verify it starts, authenticates,
and polls successfully. No changes to Anarchy are needed — the run dispatch
queue is global (not filtered by runner pool), so the Go runner with
`RUNNER_NAME=default` competes for the same pending runs as the Python runners.

```bash
export KUBECONFIG=~/secrets/ocp-babydev.infra-us-east.kubeconfig
cd babylon-runner && ./dev-run.sh
```

The script will:
1. Port-forward `svc/anarchy` from `babylon-anarchy-test` to `localhost:5000`
2. Create a temporary dev pod with runner labels and a generated token
3. Build the binary (`make build` after Step 4 update) and start polling

**Expected behavior:**
- Runner starts and logs `Starting babylon-runner...`
- Polls `GET /run` every 1s (dev default `POLLING_INTERVAL=1`)
- If no pending runs: receives timeout (empty response), re-polls — this is
  normal and already proves the runner is functional (auth works, protocol works)
- If a pending run exists: picks it up, executes, and posts results

Press Ctrl+C to stop. Cleanup (pod deletion, port-forward kill) is automatic.

**If the runner fails to authenticate:** The API returns 403. Check that the dev
pod was created (`oc get pod babylon-runner-godev -n babylon-anarchy-test`) and
that `RUNNER_TOKEN` matches between the pod env and the binary env.

**If you want to trigger a run to see full execution:** Create a test subject
from one of the existing test governors:

```bash
oc apply -n babylon-anarchy-test -f - <<'EOF'
apiVersion: anarchy.gpte.redhat.com/v1
kind: AnarchySubject
metadata:
  name: smoke-test-go-runner
  namespace: babylon-anarchy-test
spec:
  governor: tests.babylon-empty-config.dev
  vars:
    desired_state: started
EOF
```

This will trigger a provision action → run → the Go runner picks it up.
Clean up after: `oc delete anarchysubject smoke-test-go-runner -n babylon-anarchy-test`

- [ ] **Step 9: Update spec status**

In `docs/superpowers/specs/2026-06-18-babylon-runner-improvements.md`, update the Phase 1 status from "Not Started" to "Completed":

```markdown
| **Phase 1** | Foundation | #1, #2, #3, #14, #15 | Completed |
```
