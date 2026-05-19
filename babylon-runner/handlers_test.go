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

	// FinishAction should have been called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}
