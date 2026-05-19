package main

import (
	"net/http"
	"testing"
)

// --- handleStatus tests ---

func TestHandleStatusPending(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	withTowerServer(rc, towerServer)

	// Set action name.
	rc.ActionName = "status"

	// Set check_status_state to pending.
	setNested(rc.Payload.Subject, "pending", "spec", "vars", "check_status_state")

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	// Should have: set startTimestamp, launch tower job (patch with towerJobs), continue action.
	if len(*calls) < 3 {
		t.Fatalf("expected at least 3 calls, got %d", len(*calls))
	}

	// First call: PATCH to set startTimestamp.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	if c0.Path != "/run/subject/test-subject" {
		t.Errorf("call[0] path = %s, want /run/subject/test-subject", c0.Path)
	}

	patch := c0.Body["patch"].(map[string]interface{})
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	statusAction := actions["status"].(map[string]interface{})
	if statusAction["startTimestamp"] == nil {
		t.Error("expected startTimestamp to be set")
	}

	// Verify skip_update_processing is true.
	if patch["skip_update_processing"] != true {
		t.Errorf("skip_update_processing = %v, want true", patch["skip_update_processing"])
	}

	// Last call: POST to continue action.
	lastCall := (*calls)[len(*calls)-1]
	if lastCall.Method != http.MethodPost {
		t.Errorf("last call method = %s, want POST", lastCall.Method)
	}
	if lastCall.Path != "/run/subject/test-subject/actions" {
		t.Errorf("last call path = %s, want /run/subject/test-subject/actions", lastCall.Path)
	}
	if lastCall.Body["action"] != "status" {
		t.Errorf("action = %v, want status", lastCall.Body["action"])
	}
	if lastCall.Body["after"] != "5m" {
		t.Errorf("after = %v, want 5m", lastCall.Body["after"])
	}
}

func TestHandleStatusRunning(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set check_status_state to running.
	setNested(rc.Payload.Subject, "running", "spec", "vars", "check_status_state")

	// Set action name so checkDeployerJob has context.
	rc.ActionName = "status"

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (checkDeployerJob stub), got %d", len(*calls))
	}

	// checkDeployerJob stub calls ContinueAction.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPost {
		t.Errorf("call[0] method = %s, want POST", c0.Method)
	}
	if c0.Body["action"] != "status" {
		t.Errorf("action = %v, want status", c0.Body["action"])
	}
}

// --- handleUpdate tests ---

func TestHandleUpdateNotUpdating(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	withTowerServer(rc, towerServer)

	// Set current_state to started (not updating).
	setNested(rc.Payload.Subject, "started", "spec", "vars", "current_state")

	// Set action name.
	rc.ActionName = "update"

	if err := handleUpdate(rc); err != nil {
		t.Fatalf("handleUpdate returned error: %v", err)
	}

	// Should have: launch tower job (patch), continue action.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// Last call: POST to continue action.
	lastCall := (*calls)[len(*calls)-1]
	if lastCall.Method != http.MethodPost {
		t.Errorf("last call method = %s, want POST", lastCall.Method)
	}
	if lastCall.Body["action"] != "update" {
		t.Errorf("action = %v, want update", lastCall.Body["action"])
	}
	if lastCall.Body["after"] != "5m" {
		t.Errorf("after = %v, want 5m", lastCall.Body["after"])
	}
}

func TestHandleUpdateUpdating(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set current_state to updating.
	setNested(rc.Payload.Subject, "updating", "spec", "vars", "current_state")

	// Set action name.
	rc.ActionName = "update"

	if err := handleUpdate(rc); err != nil {
		t.Fatalf("handleUpdate returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (checkDeployerJob stub), got %d", len(*calls))
	}

	// checkDeployerJob stub calls ContinueAction.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPost {
		t.Errorf("call[0] method = %s, want POST", c0.Method)
	}
	if c0.Body["action"] != "update" {
		t.Errorf("action = %v, want update", c0.Body["action"])
	}
}
