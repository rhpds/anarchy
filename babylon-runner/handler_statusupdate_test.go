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

	// Should have: set startTimestamp, launch tower job (patch with towerJobs). ContinueAction sets a directive.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
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

	// ContinueAction now sets a directive instead of making a POST call.
	if rc.continueActionDirective == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.continueActionDirective.After, "5m")
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

	// checkDeployerJob stub calls ContinueAction, which now sets a directive (no API call).
	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls (ContinueAction sets directive), got %d", len(*calls))
	}

	// Verify continueActionDirective was set.
	if rc.continueActionDirective == nil {
		t.Fatal("expected continueActionDirective to be set")
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

	// Should have: launch tower job (patch). ContinueAction sets a directive.
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	// ContinueAction now sets a directive instead of making a POST call.
	if rc.continueActionDirective == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.continueActionDirective.After, "5m")
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

	// checkDeployerJob stub calls ContinueAction, which now sets a directive (no API call).
	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls (ContinueAction sets directive), got %d", len(*calls))
	}

	// Verify continueActionDirective was set.
	if rc.continueActionDirective == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
}
