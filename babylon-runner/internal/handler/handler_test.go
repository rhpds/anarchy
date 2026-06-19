package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

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

func TestHandleEventCreateSetsGUID(t *testing.T) {
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

	// Verify guid is set and equals uuid in job_vars.
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	jv := vars["job_vars"].(map[string]interface{})

	uuid, ok := jv["uuid"].(string)
	if !ok || uuid == "" {
		t.Fatal("uuid not set in job_vars")
	}

	guid, ok := jv["guid"].(string)
	if !ok || guid == "" {
		t.Error("guid not set in job_vars patch")
	}

	if guid != uuid {
		t.Errorf("guid (%q) should equal uuid (%q)", guid, uuid)
	}
}

func TestHandleEventCreatePreservesExistingGUID(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Pre-set a guid in the subject job_vars.
	existingGUID := "existing-guid-123"
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"guid": existingGUID,
	}

	if err := handleEventCreate(rc); err != nil {
		t.Fatalf("handleEventCreate returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: PATCH subject update.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	jv := vars["job_vars"].(map[string]interface{})

	// guid should NOT be in the patch (already exists).
	if _, ok := jv["guid"]; ok {
		t.Error("guid should not be patched when it already exists")
	}

	// uuid should still be in the patch.
	if _, ok := jv["uuid"]; !ok {
		t.Error("uuid should be in the patch")
	}
}

func TestHandleEventCreateAlreadyInitialized(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set current_state so the handler considers it already initialized.
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"

	if err := handleEventCreate(rc); err != nil {
		t.Fatalf("handleEventCreate returned error: %v", err)
	}

	// Should still schedule provision action even when already initialized.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (schedule provision), got %d", len(*calls))
	}
	c := (*calls)[0]
	if c.Method != http.MethodPost {
		t.Errorf("call method = %s, want POST", c.Method)
	}
	if c.Body["action"] != "provision" {
		t.Errorf("scheduled action = %v, want provision", c.Body["action"])
	}
}

// --- handleEventUpdate tests ---

func TestHandleEventUpdateStartStop(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Subject is started, desired is stopped.
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"
	rc.Payload.Subject.Spec.Vars.DesiredState = "stopped"

	// Set matching job_vars so the handler does not detect a job_vars diff.
	jv := map[string]interface{}{"guid": "abc"}
	rc.Payload.Subject.Spec.Vars.JobVars = jv
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars": map[string]interface{}{"guid": "abc"},
	}

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

func TestHandleEventUpdateLegacyStatusPath(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Add status action to governor.
	rc.Payload.Governor.Spec.Actions["status"] = map[string]interface{}{}

	// Set same state/job_vars so no start/stop/update action triggers.
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"
	jv := map[string]interface{}{"guid": "abc"}
	rc.Payload.Subject.Spec.Vars.JobVars = jv
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars": map[string]interface{}{"guid": "abc"},
	}

	// Legacy path: check_status_state = "pending", no timestamp.
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "pending"

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	// Should have 2 calls: PATCH to set check_status_state, POST to schedule status.
	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: PATCH check_status_state to "pending".
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["check_status_state"] != "pending" {
		t.Errorf("check_status_state = %v, want pending", vars["check_status_state"])
	}

	// Second call: POST schedule status action.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPost {
		t.Errorf("call[1] method = %s, want POST", c1.Method)
	}
	if c1.Body["action"] != "status" {
		t.Errorf("action = %v, want status", c1.Body["action"])
	}
}

func TestHandleEventUpdateTimestampStatusPath(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Add status action to governor.
	rc.Payload.Governor.Spec.Actions["status"] = map[string]interface{}{}

	// Set same state/job_vars so no start/stop/update action triggers.
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"
	jv := map[string]interface{}{"guid": "abc"}
	rc.Payload.Subject.Spec.Vars.JobVars = jv
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars":                          map[string]interface{}{"guid": "abc"},
		"check_status_request_timestamp": "2023-12-31T00:00:00Z",
	}

	// Timestamp path: new timestamp, state empty or "successful".
	rc.Payload.Subject.Spec.Vars.Extra = map[string]interface{}{
		"check_status_request_timestamp": "2024-01-01T00:00:00Z",
	}

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	// Should have 2 calls: PATCH + POST schedule status.
	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	c1 := (*calls)[1]
	if c1.Body["action"] != "status" {
		t.Errorf("action = %v, want status", c1.Body["action"])
	}
}

func TestHandleEventUpdateNoStatusWithoutGovernorAction(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// No status action in governor (default newTestRunContext has none).
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"
	jv := map[string]interface{}{"guid": "abc"}
	rc.Payload.Subject.Spec.Vars.JobVars = jv
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars": map[string]interface{}{"guid": "abc"},
	}

	// Legacy path: check_status_state = "pending" -- but no governor status action.
	rc.Payload.Subject.Spec.Vars.CheckStatusState = "pending"

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	// Should have 0 calls -- no status action in governor.
	if len(*calls) != 0 {
		t.Errorf("expected 0 calls, got %d", len(*calls))
	}
}

func TestHandleEventUpdateNoChange(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Same state, same job_vars -> no action needed.
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"
	jv := map[string]interface{}{"guid": "abc"}
	rc.Payload.Subject.Spec.Vars.JobVars = jv
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars": map[string]interface{}{"guid": "abc"},
	}

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	if len(*calls) != 0 {
		t.Errorf("expected 0 calls, got %d", len(*calls))
	}
}

func TestHandleEventUpdateStartAction(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Subject is stopped, desired is started.
	rc.Payload.Subject.Spec.Vars.CurrentState = "stopped"
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"

	// Set matching job_vars so only start/stop logic triggers.
	jv := map[string]interface{}{"guid": "abc"}
	rc.Payload.Subject.Spec.Vars.JobVars = jv
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars": map[string]interface{}{"guid": "abc"},
	}

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: PATCH with start-pending.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "start-pending" {
		t.Errorf("current_state = %v, want start-pending", vars["current_state"])
	}

	// Verify label.
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "start-pending" {
		t.Errorf("state label = %v, want start-pending", labels["state"])
	}

	// Second call: POST schedule start action.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPost {
		t.Errorf("call[1] method = %s, want POST", c1.Method)
	}
	if c1.Body["action"] != "start" {
		t.Errorf("action = %v, want start", c1.Body["action"])
	}

	// Verify cancel list includes start and stop.
	cancelRaw, ok := c1.Body["cancel"].([]interface{})
	if !ok {
		t.Fatal("expected cancel to be a slice")
	}
	cancelSet := make(map[string]bool)
	for _, v := range cancelRaw {
		cancelSet[v.(string)] = true
	}
	if !cancelSet["start"] || !cancelSet["stop"] {
		t.Errorf("cancel = %v, want [start, stop]", cancelRaw)
	}
}

func TestHandleEventUpdateJobVarsChanged(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Same state, but different job_vars -> update action.
	rc.Payload.Subject.Spec.Vars.CurrentState = "started"
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{"guid": "abc", "new_param": "value"}
	rc.Payload.Subject.Status.PreviousState = map[string]interface{}{
		"job_vars": map[string]interface{}{"guid": "abc"},
	}

	if err := handleEventUpdate(rc); err != nil {
		t.Fatalf("handleEventUpdate returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: PATCH with update-pending.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "update-pending" {
		t.Errorf("current_state = %v, want update-pending", vars["current_state"])
	}

	// Second call: POST schedule update action.
	c1 := (*calls)[1]
	if c1.Body["action"] != "update" {
		t.Errorf("action = %v, want update", c1.Body["action"])
	}

	// Verify cancel list for update action.
	cancelRaw, ok := c1.Body["cancel"].([]interface{})
	if !ok {
		t.Fatal("expected cancel to be a slice")
	}
	cancelSet := make(map[string]bool)
	for _, v := range cancelRaw {
		cancelSet[v.(string)] = true
	}
	if !cancelSet["start"] || !cancelSet["stop"] {
		t.Errorf("cancel = %v, want [start, stop]", cancelRaw)
	}
}

// --- handleEventDelete tests ---

func TestHandleEventDeleteWithDestroy(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set a provision tower job so the handler considers destroy needed.
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": "job-123",
		},
	}

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
	// No provision tower job -> should delete without destroy.

	if err := handleEventDelete(rc); err != nil {
		t.Fatalf("handleEventDelete returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH), got %d", len(*calls))
	}

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
	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}

	// FinishAction should have been called.
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
}

func TestHandleEventDeleteDeployerDisabled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set a provision tower job.
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": "job-123",
		},
	}

	// Disable deployer for destroy.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		Deployer: &types.DeployerMeta{
			Actions: map[string]types.DeployerActionConfig{
				"destroy": {Disabled: true},
			},
		},
	}

	if err := handleEventDelete(rc); err != nil {
		t.Fatalf("handleEventDelete returned error: %v", err)
	}

	// Should go to handleEventDeleteWithoutDestroy: 1 PATCH call.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH), got %d", len(*calls))
	}

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

	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
}

// --- handleDestroy tests ---

func TestHandleDestroyWithCatchAll(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Set state to destroy-error and enable sandbox API with catch-all.
	rc.Payload.Subject.Spec.Vars.CurrentState = "destroy-error"
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		AWSSandboxed: true,
		SandboxAPI: map[string]interface{}{
			"actions": map[string]interface{}{
				"destroy": map[string]interface{}{
					"catch_all": true,
				},
			},
		},
	}

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
}

func TestHandleDestroyPending(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	towerServer := newTestTowerServer(t)
	defer towerServer.Close()

	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{
		Metadata: types.ObjectMeta{Name: "test-destroy-action"},
		Spec:     types.ActionSpec{Action: "destroy"},
	}
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"uuid": "test-uuid",
	}
	withTowerServer(rc, towerServer)

	// Set state to destroy-pending with no sandbox API.
	rc.Payload.Subject.Spec.Vars.CurrentState = "destroy-pending"

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	// Should set startTimestamp, launch tower job. ContinueAction sets a directive.
	if len(*calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(*calls))
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

	// ContinueAction directive should be set.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
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
	actionsMap := status["actions"].(map[string]interface{})
	destroyAction := actionsMap["destroy"].(map[string]interface{})
	if destroyAction["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.destroy")
	}
	if destroyAction["state"] != "successful" {
		t.Errorf("destroy state = %v, want successful", destroyAction["state"])
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

	if rc.Result.DeleteSubject == nil {
		t.Error("expected DeleteSubject to be called")
	}
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
}

func TestHandleDestroyDeployerDisabledNoCatchAll(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Deployer disabled, but no sandbox API -> catch_all path doesn't trigger.
	rc.Payload.Subject.Spec.Vars.CurrentState = "destroy-pending"
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		Deployer: &types.DeployerMeta{
			Actions: map[string]types.DeployerActionConfig{
				"destroy": {Disabled: true},
			},
		},
	}

	if err := handleDestroy(rc); err != nil {
		t.Fatalf("handleDestroy returned error: %v", err)
	}

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called (deployer disabled, no catch_all)")
	}
	if rc.Result.DeleteSubject != nil {
		t.Error("expected DeleteSubject NOT to be called")
	}
}

// --- checkDeployerJob tests ---

func TestCheckDeployerJobSuccessful(t *testing.T) {
	towerServer := newTestTowerServer(t)
	defer towerServer.Close()
	host := towerServerHost(t, towerServer)

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		AnsibleControllers: []map[string]interface{}{
			{"hostname": host, "user": "admin", "password": "secret"},
		},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   host,
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 anarchy API call, got %d", len(*calls))
	}
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to have been called")
	}
}

func TestCheckDeployerJobFailed(t *testing.T) {
	towerServer := newTestTowerServerWithStatus(t, "failed")
	defer towerServer.Close()
	host := towerServerHost(t, towerServer)

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		AnsibleControllers: []map[string]interface{}{
			{"hostname": host, "user": "admin", "password": "secret"},
		},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   host,
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to have been called")
	}
}

func TestCheckDeployerJobStillRunning(t *testing.T) {
	towerServer := newTestTowerServerWithStatus(t, "running")
	defer towerServer.Close()
	host := towerServerHost(t, towerServer)

	anarchyServer, _ := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		AnsibleControllers: []map[string]interface{}{
			{"hostname": host, "user": "admin", "password": "secret"},
		},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   host,
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called while job is running")
	}
}

func TestCheckDeployerJobNoTowerJobInfo(t *testing.T) {
	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	// No towerJobs in status -> should ContinueAction 5m.

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls (ContinueAction sets directive), got %d", len(*calls))
	}
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestCheckDeployerJobMissingDeployerJobField(t *testing.T) {
	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"towerHost": "tower.example.com",
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls, got %d", len(*calls))
	}
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestCheckDeployerJobMissingTowerHost(t *testing.T) {
	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) != 0 {
		t.Fatalf("expected 0 calls, got %d", len(*calls))
	}
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected ContinueAction to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "5m")
}

func TestCheckDeployerJobCanceled(t *testing.T) {
	towerServer := newTestTowerServerWithStatus(t, "canceled")
	defer towerServer.Close()
	host := towerServerHost(t, towerServer)

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		AnsibleControllers: []map[string]interface{}{
			{"hostname": host, "user": "admin", "password": "secret"},
		},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   host,
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "provision-canceled" {
		t.Errorf("state label = %v, want provision-canceled", labels["state"])
	}

	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction.State != "canceled" {
		t.Errorf("FinishAction state = %v, want canceled", rc.Result.FinishAction.State)
	}
}

func TestCheckDeployerJobError(t *testing.T) {
	towerServer := newTestTowerServerWithStatus(t, "error")
	defer towerServer.Close()
	host := towerServerHost(t, towerServer)

	anarchyServer, calls := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.Payload.Action = &types.Action{
		Spec: types.ActionSpec{Action: "provision"},
	}
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		AnsibleControllers: []map[string]interface{}{
			{"hostname": host, "user": "admin", "password": "secret"},
		},
	}
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob": float64(42),
			"towerHost":   host,
		},
	}

	err := checkDeployerJob(rc, "provision")
	if err != nil {
		t.Fatalf("checkDeployerJob error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "provision-error" {
		t.Errorf("state label = %v, want provision-error", labels["state"])
	}

	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction.State != "error" {
		t.Errorf("FinishAction state = %v, want error", rc.Result.FinishAction.State)
	}
}

// --- helper: newTestTowerServerWithStatus ---

// newTestTowerServerWithStatus creates a mock Tower TLS server that
// returns the given job status for GET requests.
func newTestTowerServerWithStatus(t *testing.T, jobStatus string) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1), "token": "test-token"})
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		default:
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(42), "status": jobStatus})
		}
	}))
}

