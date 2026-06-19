package handler

import (
	"net/http"
	"testing"

	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// --- handleStatus tests ---

func TestHandleStatusPending(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-status-action"},
		Spec:     types.ActionSpec{Action: "status"},
	}
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"uuid": "test-uuid",
	}
	withTowerServer(rc, towerServer)

	// Set check_status_state to pending.
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "pending"

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	// Should have: set startTimestamp, launch tower job (patch with towerJobs).
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

	// ContinueAction directive should be set.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestHandleStatusRunning(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "status"},
	}

	// Set check_status_state to running.
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "running"

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	// checkDeployerJob: no tower job info -> ContinueAction directive (no API call).
	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls (ContinueAction sets directive), got %d", len(*calls))
	}

	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
}

func TestHandleStatus_DeployerDisabled_Pending_FinishesAction(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-status-action"},
		Spec:     types.ActionSpec{Action: "status"},
	}
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"uuid": "test-uuid",
	}
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "pending"

	// Set deployer disabled for status action.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		Deployer: &types.DeployerMeta{
			Actions: map[string]types.DeployerActionConfig{
				"status": {Disabled: true},
			},
		},
	}

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	// Should have set startTimestamp and check_status_state=successful.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
	}

	// First call: PATCH to set startTimestamp.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}

	// Second call: PATCH to set check_status_state=successful.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPatch {
		t.Errorf("call[1] method = %s, want PATCH", c1.Method)
	}

	patch := c1.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["check_status_state"] != "successful" {
		t.Errorf("check_status_state = %v, want successful", vars["check_status_state"])
	}

	// FinishAction should be set.
	if rc.Result.FinishAction == nil {
		t.Fatal("FinishAction should be set when deployer is disabled")
	}
	if rc.Result.FinishAction.State != "successful" {
		t.Errorf("FinishAction.State = %q, want successful", rc.Result.FinishAction.State)
	}
}

func TestHandleStatus_DeployerDisabled_Running_FinishesAction(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-status-action"},
		Spec:     types.ActionSpec{Action: "status"},
	}
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"uuid": "test-uuid",
	}
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "running"

	// Set deployer disabled for status action.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		Deployer: &types.DeployerMeta{
			Actions: map[string]types.DeployerActionConfig{
				"status": {Disabled: true},
			},
		},
	}

	if err := handleStatus(rc); err != nil {
		t.Fatalf("handleStatus returned error: %v", err)
	}

	// Should have set check_status_state=successful.
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}

	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["check_status_state"] != "successful" {
		t.Errorf("check_status_state = %v, want successful", vars["check_status_state"])
	}

	// FinishAction should be set.
	if rc.Result.FinishAction == nil {
		t.Fatal("FinishAction should be set when deployer is disabled")
	}
	if rc.Result.FinishAction.State != "successful" {
		t.Errorf("FinishAction.State = %q, want successful", rc.Result.FinishAction.State)
	}
}

// --- handleUpdate tests ---

func TestHandleUpdateNotUpdating(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-update-action"},
		Spec:     types.ActionSpec{Action: "update"},
	}
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"uuid": "test-uuid",
	}
	withTowerServer(rc, towerServer)

	// Set current_state to started (not updating).
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"

	if err := handleUpdate(rc); err != nil {
		t.Fatalf("handleUpdate returned error: %v", err)
	}

	// Should have: launch tower job (patch). ContinueAction sets a directive.
	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestHandleUpdateUpdating(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "update"},
	}

	// Set current_state to updating.
	rc.Payload.Subject.Spec.Vars.CurrentState = "updating"

	if err := handleUpdate(rc); err != nil {
		t.Fatalf("handleUpdate returned error: %v", err)
	}

	// checkDeployerJob: no tower job info -> ContinueAction directive (no API call).
	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls (ContinueAction sets directive), got %d", len(*calls))
	}

	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
}
