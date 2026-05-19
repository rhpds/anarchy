package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// anarchyCall records an HTTP request made to the test Anarchy server.
type anarchyCall struct {
	Method string
	Path   string
	Body   map[string]interface{}
}

// newTestAnarchyServer creates an httptest.Server that records all requests.
// It returns the server and a pointer to the call log slice.
func newTestAnarchyServer(t *testing.T) (*httptest.Server, *[]anarchyCall) {
	t.Helper()
	var (
		calls []anarchyCall
		mu    sync.Mutex
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		if r.Body != nil {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				// Not all requests have JSON bodies; ignore decode errors.
				body = nil
			}
		}

		mu.Lock()
		calls = append(calls, anarchyCall{
			Method: r.Method,
			Path:   r.URL.Path,
			Body:   body,
		})
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))

	return server, &calls
}

// newTestRunContext creates a RunContext connected to the given test server.
// The governor has provision, destroy, start, stop, and update actions.
// The subject is empty (no current_state, no job_vars).
func newTestRunContext(t *testing.T, server *httptest.Server) *RunContext {
	t.Helper()
	cfg := Config{
		AnarchyURL:     server.URL,
		RunnerName:     "runner",
		PodName:        "pod",
		RunnerToken:    "token",
		RequestTimeout: 5,
	}

	return &RunContext{
		Payload: RunPayload{
			Governor: map[string]interface{}{
				"spec": map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{},
						"destroy":   map[string]interface{}{},
						"start":     map[string]interface{}{},
						"stop":      map[string]interface{}{},
						"update":    map[string]interface{}{},
					},
					"vars": map[string]interface{}{
						"job_vars": map[string]interface{}{
							"cloud_provider": "ec2",
						},
					},
				},
			},
			Subject: map[string]interface{}{
				"metadata": map[string]interface{}{
					"name": "test-subject",
				},
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{},
				},
				"status": map[string]interface{}{},
			},
		},
		Anarchy:     NewAnarchyClient(cfg),
		SubjectName: "test-subject",
	}
}

// newTestTowerServer creates a mock Tower server that handles the full
// LaunchJob workflow (token, org, inventory, project, template, launch).
// Returns the server URL.
func newTestTowerServer(t *testing.T) *httptest.Server {
	t.Helper()
	idCounter := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			idCounter++
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    float64(idCounter),
				"token": "test-token",
			})
		case "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			// GET job status (for checkDeployerJob)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":     float64(42),
				"status": "successful",
			})
		}
	}))
}

// withTowerServer configures the RunContext with a mock tower server
// and the necessary __meta__ configuration.
func withTowerServer(rc *RunContext, towerServer *httptest.Server) {
	rc.TowerBaseURL = towerServer.URL
	setNested(rc.Payload.Governor, map[string]interface{}{
		"deployer": map[string]interface{}{
			"scm_url": "https://github.com/example/repo.git",
			"scm_ref": "main",
		},
	}, "spec", "vars", "__meta__")
}

// --- handleEventCreate tests ---

func TestHandleEventCreate(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	if err := handleEventCreate(rc); err != nil {
		t.Fatalf("handleEventCreate returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: PATCH subject update.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	if c0.Path != "/run/subject/test-subject" {
		t.Errorf("call[0] path = %s, want /run/subject/test-subject", c0.Path)
	}

	// Verify the patch contains provision-pending state.
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "provision-pending" {
		t.Errorf("current_state = %v, want provision-pending", vars["current_state"])
	}

	// Verify job_vars were merged (cloud_provider from governor).
	jv := vars["job_vars"].(map[string]interface{})
	if jv["cloud_provider"] != "ec2" {
		t.Errorf("cloud_provider = %v, want ec2", jv["cloud_provider"])
	}
	// UUID should be set.
	if _, ok := jv["uuid"]; !ok {
		t.Error("expected uuid in job_vars")
	}
	// Platform default.
	if jv["platform"] != "RHPDS" {
		t.Errorf("platform = %v, want RHPDS", jv["platform"])
	}

	// Second call: POST schedule action.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPost {
		t.Errorf("call[1] method = %s, want POST", c1.Method)
	}
	if c1.Path != "/run/subject/test-subject/actions" {
		t.Errorf("call[1] path = %s, want /run/subject/test-subject/actions", c1.Path)
	}
	if c1.Body["action"] != "provision" {
		t.Errorf("action = %v, want provision", c1.Body["action"])
	}
}

func TestHandleEventCreateAlreadyInitialized(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set current_state so the handler considers it already initialized.
	setNested(rc.Payload.Subject, "started", "spec", "vars", "current_state")

	if err := handleEventCreate(rc); err != nil {
		t.Fatalf("handleEventCreate returned error: %v", err)
	}

	if len(*calls) != 0 {
		t.Errorf("expected 0 calls for already initialized subject, got %d", len(*calls))
	}
}

// --- handleEventUpdate tests ---

func TestHandleEventUpdateStartStop(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Subject is started, desired is stopped.
	setNested(rc.Payload.Subject, "started", "spec", "vars", "current_state")
	setNested(rc.Payload.Subject, "stopped", "spec", "vars", "desired_state")

	// Set matching job_vars so the handler does not detect a job_vars diff.
	jv := map[string]interface{}{"guid": "abc"}
	setNested(rc.Payload.Subject, jv, "spec", "vars", "job_vars")
	setNested(rc.Payload.Subject, jv, "status", "previous_state", "job_vars")

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: PATCH with stop-pending.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "stop-pending" {
		t.Errorf("current_state = %v, want stop-pending", vars["current_state"])
	}

	// Second call: POST schedule stop action.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPost {
		t.Errorf("call[1] method = %s, want POST", c1.Method)
	}
	if c1.Body["action"] != "stop" {
		t.Errorf("action = %v, want stop", c1.Body["action"])
	}
}

func TestHandleEventUpdateNoChange(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Same state, same job_vars → no action needed.
	setNested(rc.Payload.Subject, "started", "spec", "vars", "current_state")
	setNested(rc.Payload.Subject, "started", "spec", "vars", "desired_state")

	jv := map[string]interface{}{"guid": "abc"}
	setNested(rc.Payload.Subject, jv, "spec", "vars", "job_vars")
	setNested(rc.Payload.Subject, jv, "status", "previous_state", "job_vars")

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	if len(*calls) != 0 {
		t.Errorf("expected 0 calls, got %d", len(*calls))
	}
}

// --- handleEventDelete tests ---

func TestHandleEventDeleteWithDestroy(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set a provision tower job so the handler considers destroy needed.
	setNested(rc.Payload.Subject, map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": "job-123",
		},
	}, "status", "towerJobs")

	if err := handleEventDelete(rc); err != nil {
		t.Fatalf("handleEventDelete returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: POST schedule destroy action.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPost {
		t.Errorf("call[0] method = %s, want POST", c0.Method)
	}
	if c0.Body["action"] != "destroy" {
		t.Errorf("action = %v, want destroy", c0.Body["action"])
	}

	// Second call: PATCH subject update.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPatch {
		t.Errorf("call[1] method = %s, want PATCH", c1.Method)
	}
	patch := c1.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-pending" {
		t.Errorf("current_state = %v, want destroy-pending", vars["current_state"])
	}
	if vars["desired_state"] != "destroyed" {
		t.Errorf("desired_state = %v, want destroyed", vars["desired_state"])
	}
}

func TestHandleEventDeleteWithoutDestroy(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// No provision tower job → should delete without destroy.

	if err := handleEventDelete(rc); err != nil {
		t.Fatalf("handleEventDelete returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH), got %d", len(*calls))
	}

	// Single call: PATCH with destroy-complete.
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

	// DeleteSubject should have been called.
	if rc.deleteSubjectDirective == nil {
		t.Error("expected DeleteSubject to be called")
	}

	// FinishAction should have been called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}

// --- handleDestroy tests ---

func TestHandleDestroyWithCatchAll(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set state to destroy-error and enable sandbox API with catch-all.
	setNested(rc.Payload.Subject, "destroy-error", "spec", "vars", "current_state")
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "aws_sandboxed")
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "sandbox_api_destroy_catch_all")

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	// Verify DeleteSubject was called.
	if rc.deleteSubjectDirective == nil {
		t.Error("expected DeleteSubject to be called")
	}

	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}

func TestHandleDestroyPending(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.ActionName = "destroy"
	withTowerServer(rc, towerServer)

	// Set state to destroy-pending with no sandbox API.
	setNested(rc.Payload.Subject, "destroy-pending", "spec", "vars", "current_state")

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	// Should set startTimestamp, launch tower job (PATCH with towerJobs), and schedule continuation.
	if len(*calls) < 3 {
		t.Fatalf("expected at least 3 calls, got %d", len(*calls))
	}

	// First call: PATCH to set startTimestamp.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	destroy := actions["destroy"].(map[string]interface{})
	if destroy["startTimestamp"] == nil {
		t.Error("expected startTimestamp to be set")
	}

	// Last call: POST to schedule action continuation.
	lastCall := (*calls)[len(*calls)-1]
	if lastCall.Method != http.MethodPost {
		t.Errorf("last call method = %s, want POST", lastCall.Method)
	}
	if lastCall.Path != "/run/subject/test-subject/actions" {
		t.Errorf("last call path = %s, want /run/subject/test-subject/actions", lastCall.Path)
	}
	if lastCall.Body["action"] != "destroy" {
		t.Errorf("action = %v, want destroy", lastCall.Body["action"])
	}
	if lastCall.Body["after"] != "5m" {
		t.Errorf("after = %v, want 5m", lastCall.Body["after"])
	}
}

func TestHandleDestroyComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	if err := handleDestroyComplete(rc); err != nil {
		t.Fatalf("handleDestroyComplete returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// Verify the PATCH update.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}

	patch := c0.Body["patch"].(map[string]interface{})

	// Verify labels.
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "destroy-complete" {
		t.Errorf("state label = %v, want destroy-complete", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-complete" {
		t.Errorf("current_state = %v, want destroy-complete", vars["current_state"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	destroy := actions["destroy"].(map[string]interface{})
	if destroy["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.destroy")
	}
	if destroy["status"] != "successful" {
		t.Errorf("destroy status = %v, want successful", destroy["status"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	destroyJob := towerJobs["destroy"].(map[string]interface{})
	if destroyJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.destroy")
	}
	if destroyJob["jobStatus"] != "successful" {
		t.Errorf("jobStatus = %v, want successful", destroyJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify DeleteSubject was called.
	if rc.deleteSubjectDirective == nil {
		t.Error("expected DeleteSubject to be called")
	}

	// Verify FinishAction was called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}

// --- checkDeployerJob tests ---

func TestCheckDeployerJobSuccessful(t *testing.T) {
	// Create a mock Tower server that returns a successful job
	towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1), "token": "test-token"})
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			// GET job status
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(42), "status": "successful"})
		}
	}))
	defer towerServer.Close()

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.ActionName = "provision"
	rc.TowerBaseURL = towerServer.URL
	setNested(rc.Payload.Subject, map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   "tower.example.com",
		},
	}, "status", "towerJobs")

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	// Should have called handleProvisionComplete → subject update
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 anarchy API call, got %d", len(*calls))
	}
	if !rc.finished {
		t.Error("expected FinishAction to have been called")
	}
}

func TestCheckDeployerJobFailed(t *testing.T) {
	towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1), "token": "test-token"})
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(42), "status": "failed"})
		}
	}))
	defer towerServer.Close()

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.ActionName = "provision"
	rc.TowerBaseURL = towerServer.URL
	setNested(rc.Payload.Subject, map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   "tower.example.com",
		},
	}, "status", "towerJobs")

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	// Should have called handleProvisionFailed → subject update
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}
	if !rc.finished {
		t.Error("expected FinishAction to have been called")
	}
}

func TestCheckDeployerJobStillRunning(t *testing.T) {
	towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1), "token": "test-token"})
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(42), "status": "running"})
		}
	}))
	defer towerServer.Close()

	anarchyServer, _ := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.ActionName = "provision"
	rc.TowerBaseURL = towerServer.URL
	setNested(rc.Payload.Subject, map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   "tower.example.com",
		},
	}, "status", "towerJobs")

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	// Should NOT have called FinishAction (job still running)
	if rc.finished {
		t.Error("expected FinishAction NOT to be called while job is running")
	}
}
