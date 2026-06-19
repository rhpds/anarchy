package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// --- Sandbox test helpers ---

// sandboxServerConfig controls the mock sandbox server behavior.
type sandboxServerConfig struct {
	placementUUID      string
	placementStatus    string // "success", "queued", "error", "not-found"
	placementResources []interface{}
	bookStatusCode     int    // HTTP status for POST /placements
	bookResponseStatus string // status field in book response
}

// newTestSandboxServer creates a mock Sandbox API server that handles
// login, placement get, book, release, start, stop, and request status.
// The behavior is controlled via the cfg parameter.
func newTestSandboxServer(t *testing.T, cfg sandboxServerConfig) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "test-access-token"})

		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/placements/"):
			if cfg.placementStatus == "not-found" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			resp := map[string]interface{}{
				"uuid":   cfg.placementUUID,
				"status": cfg.placementStatus,
			}
			if cfg.placementResources != nil {
				resp["resources"] = cfg.placementResources
			}
			json.NewEncoder(w).Encode(resp)

		case r.Method == "POST" && r.URL.Path == "/api/v1/placements":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(cfg.bookStatusCode)
			resp := map[string]interface{}{
				"uuid":   cfg.placementUUID,
				"status": cfg.bookResponseStatus,
			}
			if cfg.placementResources != nil {
				resp["resources"] = cfg.placementResources
			}
			json.NewEncoder(w).Encode(resp)

		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/v1/placements/"):
			w.WriteHeader(http.StatusOK)

		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/start"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"request_id": "req-start-001",
			})

		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/stop"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"request_id": "req-stop-001",
			})

		case r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/requests/") && strings.HasSuffix(r.URL.Path, "/status"):
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "success",
			})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// testResources returns a standard set of sandbox resources with credentials.
func testResources() []interface{} {
	return []interface{}{
		map[string]interface{}{
			"name": "sandbox-001",
			"kind": "AwsSandbox",
			"credentials": []interface{}{
				map[string]interface{}{
					"kind":                  "aws_iam_key",
					"aws_access_key_id":     "AKIA-TEST-KEY",
					"aws_secret_access_key": "test-secret-key",
				},
			},
			"hosted_zone_id": "Z12345",
			"account_id":     "123456789012",
			"zone":           "sandbox001.example.com",
		},
	}
}

// configureSandboxRC sets up a RunContext for sandbox API usage.
func configureSandboxRC(t *testing.T, rc *runner.RunContext, sandboxURL string) {
	t.Helper()
	rc.SandboxBaseURL = sandboxURL
	rc.SandboxClientOpts = []clients.SandboxAPIOption{clients.WithNoRetries()}
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{AWSSandboxed: true}
	rc.Payload.Governor.Spec.Vars.SandboxAPI = map[string]interface{}{
		"sandbox_api_login_token": "test-login-token",
	}
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"uuid": "test-uuid",
		"guid": "test-guid",
	}
}

// --- Integration Tests ---

func TestIntegrationProvisionWithTowerJob(t *testing.T) {
	// Test: provision-pending, deployer enabled, no sandbox.
	// Should: set startTimestamp, launch tower job, continue 5m.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}
	withTowerServer(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "provision-pending"

	if err := handleProvision(rc); err != nil {
		t.Fatalf("handleProvision returned error: %v", err)
	}

	// Expect at least 2 calls: set startTimestamp, patch towerJobs. ContinueAction sets a directive.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// First call: PATCH to set startTimestamp.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch0 := c0.Body["patch"].(map[string]interface{})
	status0 := patch0["status"].(map[string]interface{})
	actions0 := status0["actions"].(map[string]interface{})
	prov0 := actions0["provision"].(map[string]interface{})
	if prov0["startTimestamp"] == nil {
		t.Error("expected startTimestamp to be set")
	}

	// Find the tower jobs PATCH (the one with towerJobs in status).
	var towerPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		if _, ok := st["towerJobs"]; ok {
			towerPatch = p
			break
		}
	}
	if towerPatch == nil {
		t.Fatal("expected a PATCH call with towerJobs in status")
	}

	// Verify towerJobs.provision.
	statusPatch := towerPatch["status"].(map[string]interface{})
	towerJobs := statusPatch["towerJobs"].(map[string]interface{})
	provJob := towerJobs["provision"].(map[string]interface{})
	if provJob["deployerJob"] == nil {
		t.Error("expected deployerJob in towerJobs.provision")
	}
	if provJob["startTimestamp"] == nil {
		t.Error("expected startTimestamp in towerJobs.provision")
	}
	if provJob["towerHost"] == nil {
		t.Error("expected towerHost in towerJobs.provision")
	}

	// Verify labels and current_state set to "provisioning".
	meta := towerPatch["metadata"].(map[string]interface{})
	labels := meta["labels"].(map[string]interface{})
	if labels["state"] != "provisioning" {
		t.Errorf("state label = %v, want provisioning", labels["state"])
	}
	spec := towerPatch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "provisioning" {
		t.Errorf("current_state = %v, want provisioning", vars["current_state"])
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestIntegrationProvisionWithSandboxAndTower(t *testing.T) {
	// Test: provision-pending, sandbox API in use, deployer enabled.
	// Sandbox returns success with resources. Should extract vars, launch tower job.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:      "test-uuid",
		placementStatus:    "success",
		placementResources: testResources(),
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}
	configureSandboxRC(t, rc, sandboxServer.URL)
	withTowerServerAndMeta(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "provision-pending"

	if err := handleProvision(rc); err != nil {
		t.Fatalf("handleProvision returned error: %v", err)
	}

	// Should have multiple calls: startTimestamp, sandbox vars update, tower launch.
	if len(*calls) < 3 {
		t.Fatalf("expected at least 3 calls, got %d", len(*calls))
	}

	// Find the sandbox vars update PATCH (has job_vars with sandbox_name).
	var sandboxPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		jv, _ := v["job_vars"].(map[string]interface{})
		if jv != nil {
			if _, ok := jv["sandbox_name"]; ok {
				sandboxPatch = p
				break
			}
		}
	}
	if sandboxPatch == nil {
		t.Fatal("expected a PATCH call with sandbox vars (sandbox_name) in job_vars")
	}

	// Verify sandbox vars were extracted into job_vars (non-cred fields only).
	specPatch := sandboxPatch["spec"].(map[string]interface{})
	varsPatch := specPatch["vars"].(map[string]interface{})
	jv := varsPatch["job_vars"].(map[string]interface{})
	if jv["sandbox_name"] != "sandbox-001" {
		t.Errorf("sandbox_name = %v, want sandbox-001", jv["sandbox_name"])
	}
	if jv["sandbox_zone"] != "sandbox001.example.com" {
		t.Errorf("sandbox_zone = %v, want sandbox001.example.com", jv["sandbox_zone"])
	}
	// Credentials should NOT be in subject job_vars (creds=false).
	if _, found := jv["aws_access_key_id"]; found {
		t.Error("aws_access_key_id should not be in subject job_vars (creds=false)")
	}

	// Verify sandbox labels.
	if sandboxPatch["metadata"] != nil {
		meta := sandboxPatch["metadata"].(map[string]interface{})
		labels, _ := meta["labels"].(map[string]interface{})
		if labels != nil && labels["sandbox"] != "sandbox-001" {
			t.Errorf("sandbox label = %v, want sandbox-001", labels["sandbox"])
		}
	}

	// Verify a tower job was launched (PATCH with towerJobs).
	var hasTowerPatch bool
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		if _, ok := st["towerJobs"]; ok {
			hasTowerPatch = true
			break
		}
	}
	if !hasTowerPatch {
		t.Error("expected a PATCH with towerJobs after sandbox vars update")
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestIntegrationProvisionSandboxQueued(t *testing.T) {
	// Test: provision-pending, sandbox API in use, booking returns 202 (queued).
	// Should: update state to provision-queued, continue 30s.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	// Sandbox returns 404 on GET (no existing placement), then 202 on book.
	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:      "test-uuid",
		placementStatus:    "not-found",
		bookStatusCode:     202,
		bookResponseStatus: "queued",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}
	configureSandboxRC(t, rc, sandboxServer.URL)
	withTowerServerAndMeta(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "provision-pending"

	if err := handleProvision(rc); err != nil {
		t.Fatalf("handleProvision returned error: %v", err)
	}

	// Find the PATCH that sets provision-queued.
	var queuedPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		if v["current_state"] == "provision-queued" {
			queuedPatch = p
			break
		}
	}
	if queuedPatch == nil {
		t.Fatal("expected a PATCH with current_state = provision-queued")
	}

	// Verify labels.
	meta := queuedPatch["metadata"].(map[string]interface{})
	labels := meta["labels"].(map[string]interface{})
	if labels["state"] != "provision-queued" {
		t.Errorf("state label = %v, want provision-queued", labels["state"])
	}

	// Verify sandboxAPIJobs status.
	status := queuedPatch["status"].(map[string]interface{})
	sandboxJobs := status["sandboxAPIJobs"].(map[string]interface{})
	provJob := sandboxJobs["provision"].(map[string]interface{})
	if provJob["placementStatus"] != "queued" {
		t.Errorf("placementStatus = %v, want queued", provJob["placementStatus"])
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "30s")
}

func TestIntegrationCheckProvisionQueueSuccess(t *testing.T) {
	// Test: provision-queued, sandbox placement becomes "success" with resources.
	// Should: extract sandbox vars, update to provision-pending, restart provision.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:      "test-uuid",
		placementStatus:    "success",
		placementResources: testResources(),
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}
	configureSandboxRC(t, rc, sandboxServer.URL)
	withTowerServerAndMeta(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "provision-queued"

	if err := handleProvision(rc); err != nil {
		t.Fatalf("handleProvision returned error: %v", err)
	}

	// Should have multiple calls: update with sandbox vars + provision-pending,
	// then restart provision (set startTimestamp, sandbox get, launch tower, continue).
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Find the PATCH that sets provision-pending with sandbox vars.
	var pendingPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		if v["current_state"] == "provision-pending" {
			pendingPatch = p
			break
		}
	}
	if pendingPatch == nil {
		t.Fatal("expected a PATCH setting current_state = provision-pending")
	}

	// Verify sandbox vars merged into job_vars (non-cred fields only).
	specPatch := pendingPatch["spec"].(map[string]interface{})
	varsPatch := specPatch["vars"].(map[string]interface{})
	jv, _ := varsPatch["job_vars"].(map[string]interface{})
	if jv != nil {
		if jv["sandbox_name"] != "sandbox-001" {
			t.Errorf("sandbox_name = %v, want sandbox-001", jv["sandbox_name"])
		}
		// Credentials should NOT be in subject job_vars (creds=false).
		if _, found := jv["aws_access_key_id"]; found {
			t.Error("aws_access_key_id should not be in subject job_vars (creds=false)")
		}
	}

	// Verify labels include sandbox name.
	metaPatch := pendingPatch["metadata"].(map[string]interface{})
	labelsPatch := metaPatch["labels"].(map[string]interface{})
	if labelsPatch["sandbox"] != "sandbox-001" {
		t.Errorf("sandbox label = %v, want sandbox-001", labelsPatch["sandbox"])
	}

	// Verify sandboxAPIJobs dequeued.
	statusPatch := pendingPatch["status"].(map[string]interface{})
	sandboxJobs := statusPatch["sandboxAPIJobs"].(map[string]interface{})
	provJob := sandboxJobs["provision"].(map[string]interface{})
	if provJob["dequeuedTimestamp"] == nil {
		t.Error("expected dequeuedTimestamp in sandboxAPIJobs.provision")
	}
}

func TestIntegrationCheckProvisionQueueStillQueued(t *testing.T) {
	// Test: provision-queued, sandbox placement still "queued".
	// Should: continue polling with 30s.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:   "test-uuid",
		placementStatus: "queued",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}
	configureSandboxRC(t, rc, sandboxServer.URL)
	rc.Payload.Subject.Spec.Vars.CurrentState = "provision-queued"

	if err := handleProvision(rc); err != nil {
		t.Fatalf("handleProvision returned error: %v", err)
	}

	// Should have: PATCH with queued status.
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	// Verify the PATCH updates sandboxAPIJobs status.
	var statusPatchBody map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		if _, ok := st["sandboxAPIJobs"]; ok {
			statusPatchBody = p
			break
		}
	}
	if statusPatchBody == nil {
		t.Fatal("expected a PATCH with sandboxAPIJobs in status")
	}

	st := statusPatchBody["status"].(map[string]interface{})
	sj := st["sandboxAPIJobs"].(map[string]interface{})
	pj := sj["provision"].(map[string]interface{})
	if pj["placementStatus"] != "queued" {
		t.Errorf("placementStatus = %v, want queued", pj["placementStatus"])
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "30s")
}

func TestIntegrationDestroyWithTowerJob(t *testing.T) {
	// Test: destroy-pending, deployer enabled, no existing provision job.
	// Should: launch tower job, continue 5m.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-destroy-action"},
		Spec:     types.ActionSpec{Action: "destroy"},
	}
	withTowerServer(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "destroy-pending"

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	// Should have: set startTimestamp, launch tower job (patch towerJobs).
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Find the towerJobs PATCH.
	var towerPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		if _, ok := st["towerJobs"]; ok {
			towerPatch = p
			break
		}
	}
	if towerPatch == nil {
		t.Fatal("expected a PATCH call with towerJobs in status")
	}

	// Verify towerJobs.destroy has correct fields.
	statusPatch := towerPatch["status"].(map[string]interface{})
	towerJobsMap := statusPatch["towerJobs"].(map[string]interface{})
	destroyJob := towerJobsMap["destroy"].(map[string]interface{})
	if destroyJob["deployerJob"] == nil {
		t.Error("expected deployerJob in towerJobs.destroy")
	}
	if destroyJob["towerHost"] == nil {
		t.Error("expected towerHost in towerJobs.destroy")
	}

	// Verify current_state = "destroying".
	sp := towerPatch["spec"].(map[string]interface{})
	v := sp["vars"].(map[string]interface{})
	if v["current_state"] != "destroying" {
		t.Errorf("current_state = %v, want destroying", v["current_state"])
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestIntegrationDestroyCompleteWithSandboxCleanup(t *testing.T) {
	// Test: destroy complete with sandbox API in use.
	// Should: release placement, update subject, call DeleteSubject(true).
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:   "test-uuid",
		placementStatus: "success",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	configureSandboxRC(t, rc, sandboxServer.URL)

	if err := handleDestroyComplete(rc); err != nil {
		t.Fatalf("handleDestroyComplete returned error: %v", err)
	}

	// Should have 1 PATCH call (subject update).
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	// Verify the PATCH sets destroy-complete.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-complete" {
		t.Errorf("current_state = %v, want destroy-complete", vars["current_state"])
	}

	// Verify DeleteSubject was called.
	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}
	if rc.Result.DeleteSubject != nil && !rc.Result.DeleteSubject.RemoveFinalizers {
		t.Error("expected RemoveFinalizers to be true")
	}

	// Verify FinishAction was called.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
}

func TestIntegrationStartWithSandboxDeployerDisabled(t *testing.T) {
	// Test: sandbox API in use, deployer disabled for start, sandbox action enabled.
	// Should: call sandbox start, poll request, mark started, FinishAction.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:   "test-uuid",
		placementStatus: "success",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	configureSandboxRC(t, rc, sandboxServer.URL)
	rc.Payload.Governor.Spec.Vars.Meta.Deployer = &types.DeployerMeta{
		Actions: map[string]types.DeployerActionConfig{
			"start": {Disabled: true},
		},
	}

	if err := handleStart(rc); err != nil {
		t.Fatalf("handleStart returned error: %v", err)
	}

	// Should have calls: set startTimestamp, sandbox start status update, mark started.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Find the PATCH that marks state as started.
	var startedPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		if v["current_state"] == "started" {
			startedPatch = p
			break
		}
	}
	if startedPatch == nil {
		t.Fatal("expected a PATCH with current_state = started")
	}

	// Verify labels.
	pMeta := startedPatch["metadata"].(map[string]interface{})
	pLabels := pMeta["labels"].(map[string]interface{})
	if pLabels["state"] != "started" {
		t.Errorf("state label = %v, want started", pLabels["state"])
	}

	// Verify FinishAction was called.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Error("expected FinishAction with state successful")
	}
}

func TestIntegrationStopWithTowerJob(t *testing.T) {
	// Test: state not stopping, deployer enabled, sandbox in use.
	// Should: get sandbox vars, launch tower job, continue 5m.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:      "test-uuid",
		placementStatus:    "success",
		placementResources: testResources(),
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-stop-action"},
		Spec:     types.ActionSpec{Action: "stop"},
	}
	configureSandboxRC(t, rc, sandboxServer.URL)
	withTowerServerAndMeta(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"

	if err := handleStop(rc); err != nil {
		t.Fatalf("handleStop returned error: %v", err)
	}

	// Should have multiple calls: set startTimestamp, sandbox vars update, tower launch, continue.
	if len(*calls) < 3 {
		t.Fatalf("expected at least 3 calls, got %d", len(*calls))
	}

	// Find the towerJobs PATCH.
	var towerPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		if _, ok := st["towerJobs"]; ok {
			towerPatch = p
			break
		}
	}
	if towerPatch == nil {
		t.Fatal("expected a PATCH call with towerJobs in status")
	}

	// Verify tower job was for stop action.
	statusPatch := towerPatch["status"].(map[string]interface{})
	towerJobsMap := statusPatch["towerJobs"].(map[string]interface{})
	if _, ok := towerJobsMap["stop"]; !ok {
		t.Error("expected towerJobs to contain stop entry")
	}

	// Verify current_state = "stopping".
	sp := towerPatch["spec"].(map[string]interface{})
	v := sp["vars"].(map[string]interface{})
	if v["current_state"] != "stopping" {
		t.Errorf("current_state = %v, want stopping", v["current_state"])
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestIntegrationStatusWithTowerJob(t *testing.T) {
	// Test: check_status_state pending, deployer enabled.
	// Should: launch tower job with extraSpecVars check_status_state=running, continue 5m.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-status-action"},
		Spec:     types.ActionSpec{Action: "status"},
	}
	withTowerServer(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "pending"

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	// Should have: set startTimestamp, tower launch PATCH.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Find the towerJobs PATCH and verify check_status_state.
	var towerPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		if _, ok := st["towerJobs"]; ok {
			towerPatch = p
			break
		}
	}
	if towerPatch == nil {
		t.Fatal("expected a PATCH call with towerJobs in status")
	}

	// Verify spec.vars includes check_status_state = "running".
	sp := towerPatch["spec"].(map[string]interface{})
	v := sp["vars"].(map[string]interface{})
	if v["check_status_state"] != "running" {
		t.Errorf("check_status_state = %v, want running", v["check_status_state"])
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestIntegrationEventDeleteWithCancelJobs(t *testing.T) {
	// Test: subject has incomplete tower jobs. Should cancel all, then route to
	// delete with destroy (since provision job exists).
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	host := towerServerHost(t, towerServer)
	withTowerServer(rc, towerServer)

	// Set incomplete tower jobs (no completeTimestamp) with towerHost
	// matching the test TLS server so getTowerClientForHost can find it.
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob":       float64(42),
			"towerHost":         host,
			"startTimestamp":    "2024-01-01T00:00:00Z",
			"completeTimestamp": nil,
		},
		"start": map[string]interface{}{
			"deployerJob":       float64(43),
			"towerHost":         host,
			"startTimestamp":    "2024-01-01T00:00:00Z",
			"completeTimestamp": nil,
		},
	}

	if err := handleEventDelete(rc); err != nil {
		t.Fatalf("handleEventDelete returned error: %v", err)
	}

	// Should have: POST destroy action, PATCH with destroy-pending.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Verify a destroy action was scheduled.
	var destroyScheduled bool
	for _, c := range *calls {
		if c.Method == http.MethodPost && c.Path == "/run/subject/test-subject/actions" {
			if c.Body["action"] == "destroy" {
				destroyScheduled = true
				break
			}
		}
	}
	if !destroyScheduled {
		t.Error("expected destroy action to be scheduled")
	}

	// Verify subject was updated with destroy-pending.
	var patchedState string
	for _, c := range *calls {
		if c.Method == http.MethodPatch {
			p, _ := c.Body["patch"].(map[string]interface{})
			if p == nil {
				continue
			}
			sp, _ := p["spec"].(map[string]interface{})
			if sp == nil {
				continue
			}
			v, _ := sp["vars"].(map[string]interface{})
			if v != nil && v["current_state"] == "destroy-pending" {
				patchedState = "destroy-pending"
				break
			}
		}
	}
	if patchedState != "destroy-pending" {
		t.Errorf("expected current_state = destroy-pending, but did not find it")
	}
}

func TestIntegrationExtractProvisionDataFromJob(t *testing.T) {
	// Test: tower job returns successful with artifacts containing
	// provision_data, provision_message_body, provision_messages.

	// Tower TLS server that returns successful job with artifacts.
	towerServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1), "token": "test-token"})
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			// GET job status with artifacts.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":     float64(42),
				"status": "successful",
				"artifacts": map[string]interface{}{
					"provision_data": map[string]interface{}{
						"ssh_host": "10.0.0.1",
						"ssh_user": "ec2-user",
					},
					"provision_message_body": "Your environment is ready.",
					"provision_messages": []interface{}{
						"SSH access available",
						"Console URL: https://console.example.com",
					},
				},
			})
		}
	}))
	defer towerServer.Close()

	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}

	// Set up AnsibleControllers with the TLS server host so
	// getTowerClientForHost can find it.
	host := towerServerHost(t, towerServer)
	withTowerServer(rc, towerServer)

	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   host,
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob returned error: %v", err)
	}

	// Should have called handleProvisionComplete with the artifacts data.
	if rc.Result.FinishAction == nil {
		t.Fatal("expected FinishAction to have been called")
	}
	if rc.Result.FinishAction.State != "successful" {
		t.Error("expected FinishAction with state successful")
	}

	// Find the PATCH that contains provision_data in spec vars.
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	var completePatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		if _, ok := v["provision_data"]; ok {
			completePatch = p
			break
		}
	}
	if completePatch == nil {
		t.Fatal("expected a PATCH with provision_data in spec.vars")
	}

	sp := completePatch["spec"].(map[string]interface{})
	v := sp["vars"].(map[string]interface{})

	// Verify provision_data.
	pd, ok := v["provision_data"].(map[string]interface{})
	if !ok {
		t.Fatal("provision_data is not a map")
	}
	if pd["ssh_host"] != "10.0.0.1" {
		t.Errorf("ssh_host = %v, want 10.0.0.1", pd["ssh_host"])
	}
	if pd["ssh_user"] != "ec2-user" {
		t.Errorf("ssh_user = %v, want ec2-user", pd["ssh_user"])
	}

	// Verify provision_message_body.
	if v["provision_message_body"] != "Your environment is ready." {
		t.Errorf("provision_message_body = %v, want 'Your environment is ready.'", v["provision_message_body"])
	}

	// Verify provision_messages.
	messages, ok := v["provision_messages"].([]interface{})
	if !ok {
		t.Fatal("messages is not a slice")
	}
	if len(messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(messages))
	}

	// Verify current_state = started.
	if v["current_state"] != "started" {
		t.Errorf("current_state = %v, want started", v["current_state"])
	}
}

func TestIntegrationExtractSandboxVars(t *testing.T) {
	// Test: extractSandboxVars + extractSandboxLabels with AwsSandbox + OcpSandbox placement.
	placement := map[string]interface{}{
		"uuid":   "placement-001",
		"status": "success",
		"resources": []interface{}{
			map[string]interface{}{
				"name":           "sandbox-alpha",
				"kind":           "AwsSandbox",
				"hosted_zone_id": "Z-ALPHA",
				"account_id":     "111111111111",
				"zone":           "alpha.example.com",
				"credentials": []interface{}{
					map[string]interface{}{
						"kind":                  "aws_iam_key",
						"aws_access_key_id":     "AKIA-ALPHA",
						"aws_secret_access_key": "secret-alpha",
					},
				},
			},
			map[string]interface{}{
				"name":           "ocp-beta",
				"kind":           "OcpSandbox",
				"namespace":      "user-ns",
				"ocp_cluster":    "cluster-beta",
				"api_url":        "https://api.beta.example.com:6443",
				"ingress_domain": "apps.beta.example.com",
				"console_url":    "https://console.beta.example.com",
				"credentials": []interface{}{
					map[string]interface{}{
						"kind":  "ServiceAccount",
						"token": "sa-token-beta",
					},
				},
				"annotations": map[string]interface{}{
					"var": "openshift",
				},
			},
		},
	}

	// With creds=true, both resources should produce their specific vars.
	vars := extractSandboxVars(placement, true)

	// AwsSandbox (main) vars at top level.
	if vars["sandbox_name"] != "sandbox-alpha" {
		t.Errorf("sandbox_name = %v, want sandbox-alpha", vars["sandbox_name"])
	}
	if vars["sandbox_hosted_zone_id"] != "Z-ALPHA" {
		t.Errorf("sandbox_hosted_zone_id = %v, want Z-ALPHA", vars["sandbox_hosted_zone_id"])
	}
	if vars["aws_access_key_id"] != "AKIA-ALPHA" {
		t.Errorf("aws_access_key_id = %v, want AKIA-ALPHA", vars["aws_access_key_id"])
	}
	if vars["subdomain_base_suffix"] != ".alpha.example.com" {
		t.Errorf("subdomain_base_suffix = %v, want .alpha.example.com", vars["subdomain_base_suffix"])
	}

	// OcpSandbox under "openshift" var annotation.
	ocpVars, ok := vars["openshift"].(map[string]interface{})
	if !ok {
		t.Fatalf("openshift var not found or wrong type: %v", vars["openshift"])
	}
	if ocpVars["sandbox_openshift_name"] != "ocp-beta" {
		t.Errorf("openshift.sandbox_openshift_name = %v, want ocp-beta", ocpVars["sandbox_openshift_name"])
	}
	if ocpVars["sandbox_openshift_api_token"] != "sa-token-beta" {
		t.Errorf("openshift.sandbox_openshift_api_token = %v, want sa-token-beta", ocpVars["sandbox_openshift_api_token"])
	}

	// sandboxes deep copy present with creds=true.
	sandboxes, ok := vars["sandboxes"].([]interface{})
	if !ok || len(sandboxes) != 2 {
		t.Errorf("expected sandboxes with 2 elements, got %v", vars["sandboxes"])
	}

	// Labels.
	labelsResult := extractSandboxLabels(placement)
	if labelsResult["sandbox"] != "sandbox-alpha" {
		t.Errorf("sandbox label = %v, want sandbox-alpha", labelsResult["sandbox"])
	}
	if labelsResult["AwsSandbox"] != "sandbox-alpha" {
		t.Errorf("AwsSandbox label = %v, want sandbox-alpha", labelsResult["AwsSandbox"])
	}
	if labelsResult["OcpSandbox"] != "ocp-beta" {
		t.Errorf("OcpSandbox label = %v, want ocp-beta", labelsResult["OcpSandbox"])
	}

	// With creds=false, credential vars should be absent.
	noCreds := extractSandboxVars(placement, false)
	if _, found := noCreds["aws_access_key_id"]; found {
		t.Error("aws_access_key_id should not be present with creds=false")
	}
	if _, found := noCreds["sandboxes"]; found {
		t.Error("sandboxes should not be present with creds=false")
	}
	// Non-cred vars still present.
	if noCreds["sandbox_name"] != "sandbox-alpha" {
		t.Errorf("sandbox_name = %v, want sandbox-alpha (creds=false)", noCreds["sandbox_name"])
	}
}

func TestIntegrationDestroyWithCancelProvisionJob(t *testing.T) {
	// Test: destroy-pending with an incomplete provision tower job.
	// Should: cancel provision job, then launch destroy tower job.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-destroy-action"},
		Spec:     types.ActionSpec{Action: "destroy"},
	}
	host := towerServerHost(t, towerServer)
	withTowerServer(rc, towerServer)
	rc.Payload.Subject.Spec.Vars.CurrentState = "destroy-pending"

	// Set an incomplete provision tower job with towerHost matching the test server.
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob":       float64(99),
			"towerHost":         host,
			"startTimestamp":    "2024-01-01T00:00:00Z",
			"completeTimestamp": nil,
		},
	}

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	// Should have: set startTimestamp, tower job launch.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Verify a destroy tower job was launched (PATCH with towerJobs.destroy).
	var hasTowerPatch bool
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		st, _ := p["status"].(map[string]interface{})
		if st == nil {
			continue
		}
		tj, _ := st["towerJobs"].(map[string]interface{})
		if tj == nil {
			continue
		}
		if _, ok := tj["destroy"]; ok {
			hasTowerPatch = true
			break
		}
	}
	if !hasTowerPatch {
		t.Error("expected a PATCH with towerJobs.destroy")
	}

	// ContinueAction sets a directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction directive to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestIntegrationProvisionDeployerDisabledSandbox(t *testing.T) {
	// Test: provision-pending, deployer disabled, sandbox API in use.
	// Should: call sandbox get, mark started immediately, FinishAction.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:      "test-uuid",
		placementStatus:    "success",
		placementResources: testResources(),
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-provision-action"},
		Spec:     types.ActionSpec{Action: "provision"},
	}
	configureSandboxRC(t, rc, sandboxServer.URL)
	rc.Payload.Subject.Spec.Vars.CurrentState = "provision-pending"
	rc.Payload.Governor.Spec.Vars.Meta.Deployer = &types.DeployerMeta{
		Actions: map[string]types.DeployerActionConfig{
			"provision": {Disabled: true},
		},
	}

	if err := handleProvision(rc); err != nil {
		t.Fatalf("handleProvision returned error: %v", err)
	}

	// Should have calls: startTimestamp, sandbox vars update, mark started.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Find the PATCH that marks state as started.
	var startedPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		if v["current_state"] == "started" {
			startedPatch = p
			break
		}
	}
	if startedPatch == nil {
		t.Fatal("expected a PATCH with current_state = started")
	}

	// Verify labels.
	pMeta := startedPatch["metadata"].(map[string]interface{})
	pLabels := pMeta["labels"].(map[string]interface{})
	if pLabels["state"] != "started" {
		t.Errorf("state label = %v, want started", pLabels["state"])
	}

	// Verify healthy = true.
	sp := startedPatch["spec"].(map[string]interface{})
	v := sp["vars"].(map[string]interface{})
	if v["healthy"] != true {
		t.Errorf("healthy = %v, want true", v["healthy"])
	}

	// Verify FinishAction was called.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Error("expected FinishAction with state successful")
	}
}

func TestIntegrationEventDeleteWithoutDestroy(t *testing.T) {
	// Test: no provision job, sandbox API in use.
	// Should: cleanup sandbox, mark destroy-complete, DeleteSubject, FinishAction.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:   "test-uuid",
		placementStatus: "success",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	configureSandboxRC(t, rc, sandboxServer.URL)

	if err := handleEventDelete(rc); err != nil {
		t.Fatalf("handleEventDelete returned error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	// Verify PATCH with destroy-complete.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-complete" {
		t.Errorf("current_state = %v, want destroy-complete", vars["current_state"])
	}
	if vars["desired_state"] != "destroyed" {
		t.Errorf("desired_state = %v, want destroyed", vars["desired_state"])
	}

	// Verify DeleteSubject was called.
	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}

	// Verify FinishAction was called.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
}

func TestIntegrationDestroyErrorCatchAll(t *testing.T) {
	// Test: destroy-error state with sandbox catch-all enabled.
	// Should: cleanup sandbox, DeleteSubject, FinishAction.
	server, _ := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:   "test-uuid",
		placementStatus: "success",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	configureSandboxRC(t, rc, sandboxServer.URL)
	rc.Payload.Subject.Spec.Vars.CurrentState = "destroy-error"
	rc.Payload.Governor.Spec.Vars.Meta.SandboxAPI = map[string]interface{}{
		"actions": map[string]interface{}{
			"destroy": map[string]interface{}{
				"catch_all": true,
			},
		},
	}

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	// Verify DeleteSubject was called.
	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}

	// Verify FinishAction was called with successful.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Error("expected FinishAction with state successful")
	}
}

func TestIntegrationStopDeployerDisabledSandbox(t *testing.T) {
	// Test: deployer disabled, sandbox API in use, sandbox action enabled.
	// Should: call sandbox stop, mark stopped, FinishAction.
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	sandboxServer := newTestSandboxServer(t, sandboxServerConfig{
		placementUUID:   "test-uuid",
		placementStatus: "success",
	})
	defer sandboxServer.Close()

	rc := newTestRunContext(t, server)
	configureSandboxRC(t, rc, sandboxServer.URL)
	rc.Payload.Governor.Spec.Vars.Meta.Deployer = &types.DeployerMeta{
		Actions: map[string]types.DeployerActionConfig{
			"stop": {Disabled: true},
		},
	}
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"

	if err := handleStop(rc); err != nil {
		t.Fatalf("handleStop returned error: %v", err)
	}

	// Should have calls: set startTimestamp, sandbox stop status, mark stopped.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Find the PATCH that marks state as stopped.
	var stoppedPatch map[string]interface{}
	for _, c := range *calls {
		if c.Method != http.MethodPatch {
			continue
		}
		p, _ := c.Body["patch"].(map[string]interface{})
		if p == nil {
			continue
		}
		sp, _ := p["spec"].(map[string]interface{})
		if sp == nil {
			continue
		}
		v, _ := sp["vars"].(map[string]interface{})
		if v == nil {
			continue
		}
		if v["current_state"] == "stopped" {
			stoppedPatch = p
			break
		}
	}
	if stoppedPatch == nil {
		t.Fatal("expected a PATCH with current_state = stopped")
	}

	// Verify labels.
	pMeta := stoppedPatch["metadata"].(map[string]interface{})
	pLabels := pMeta["labels"].(map[string]interface{})
	if pLabels["state"] != "stopped" {
		t.Errorf("state label = %v, want stopped", pLabels["state"])
	}

	// Verify FinishAction was called.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Error("expected FinishAction with state successful")
	}
}
