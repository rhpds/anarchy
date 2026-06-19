package handler

import (
	"net/http"
	"testing"

	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// --- handleDestroyFailure tests ---

// TestHandleDestroyFailureError tests that destroy error retries with backoff.
func TestHandleDestroyFailureError(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "destroy"}}

	if err := handleDestroyFailure(rc, "error"); err != nil {
		t.Fatalf("handleDestroyFailure returned error: %v", err)
	}

	// Should have 1 call: PATCH state update. ContinueAction now sets a directive.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// First call: PATCH with destroy-error state.
	c0 := (*calls)[0]
	if c0.Method != http.MethodPatch {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "destroy-error" {
		t.Errorf("state label = %v, want destroy-error", labels["state"])
	}
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-error" {
		t.Errorf("current_state = %v, want destroy-error", vars["current_state"])
	}

	// Verify continueActionDirective was set (retry).
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}

	// Should NOT have finished (retry via ContinueAction).
	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called (destroy always retries)")
	}
}

// TestHandleDestroyFailureCanceled tests that destroy canceled retries with fixed 1m.
func TestHandleDestroyFailureCanceled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "destroy"}}

	if err := handleDestroyFailure(rc, "canceled"); err != nil {
		t.Fatalf("handleDestroyFailure returned error: %v", err)
	}

	// Should have 1 call: PATCH state update. ContinueAction now sets a directive.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// First call: PATCH with destroy-canceled state.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "destroy-canceled" {
		t.Errorf("state label = %v, want destroy-canceled", labels["state"])
	}

	// Verify continueActionDirective with 1m interval.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "1m")

	// Should NOT have finished (destroy always retries).
	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called (destroy always retries)")
	}
}

// --- handleStatusComplete tests ---

func TestHandleStatusComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleStatusComplete(rc, nil, nil); err != nil {
		t.Fatalf("handleStatusComplete returned error: %v", err)
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

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["check_status_state"] != "successful" {
		t.Errorf("check_status_state = %v, want successful", vars["check_status_state"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	statusAction := actions["status"].(map[string]interface{})
	if statusAction["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.status")
	}
	if statusAction["state"] != "successful" {
		t.Errorf("status state = %v, want successful", statusAction["state"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "successful".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Errorf("finishActionDirective = %v, want successful", rc.Result.FinishAction)
	}
}

// --- handleUpdateComplete tests ---

func TestHandleUpdateComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleUpdateComplete(rc); err != nil {
		t.Fatalf("handleUpdateComplete returned error: %v", err)
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

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	update := actions["update"].(map[string]interface{})
	if update["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.update")
	}
	if update["state"] != "successful" {
		t.Errorf("update state = %v, want successful", update["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	updateJob := towerJobs["update"].(map[string]interface{})
	if updateJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.update")
	}
	if updateJob["jobStatus"] != "successful" {
		t.Errorf("jobStatus = %v, want successful", updateJob["jobStatus"])
	}

	// Verify FinishAction was called with "successful".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Errorf("finishActionDirective = %v, want successful", rc.Result.FinishAction)
	}
}

// --- handleStartFailure / handleStopFailure tests ---

func TestHandleStartFailureWithError(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	// desired_state not set -> finishes immediately.

	if err := handleStartFailure(rc, "error"); err != nil {
		t.Fatalf("handleStartFailure returned error: %v", err)
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
	if labels["state"] != "start-error" {
		t.Errorf("state label = %v, want start-error", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "start-error" {
		t.Errorf("current_state = %v, want start-error", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	start := actions["start"].(map[string]interface{})
	if start["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.start")
	}
	if start["state"] != "error" {
		t.Errorf("start state = %v, want error", start["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	startJob := towerJobs["start"].(map[string]interface{})
	if startJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.start")
	}
	if startJob["jobStatus"] != "error" {
		t.Errorf("jobStatus = %v, want error", startJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "error".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.Result.FinishAction)
	}
}

func TestHandleStopFailureWithFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	// desired_state not set -> finishes immediately.

	if err := handleStopFailure(rc, "failed"); err != nil {
		t.Fatalf("handleStopFailure returned error: %v", err)
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
	if labels["state"] != "stop-failed" {
		t.Errorf("state label = %v, want stop-failed", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "stop-failed" {
		t.Errorf("current_state = %v, want stop-failed", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	stop := actions["stop"].(map[string]interface{})
	if stop["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.stop")
	}
	if stop["state"] != "failed" {
		t.Errorf("stop state = %v, want failed", stop["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	stopJob := towerJobs["stop"].(map[string]interface{})
	if stopJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.stop")
	}
	if stopJob["jobStatus"] != "failed" {
		t.Errorf("jobStatus = %v, want failed", stopJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "failed".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.Result.FinishAction)
	}
}

// --- handleProvisionError tests ---

func TestHandleProvisionError(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleProvisionError(rc); err != nil {
		t.Fatalf("handleProvisionError returned error: %v", err)
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
	if labels["state"] != "provision-error" {
		t.Errorf("state label = %v, want provision-error", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "provision-error" {
		t.Errorf("current_state = %v, want provision-error", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	provision := actions["provision"].(map[string]interface{})
	if provision["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.provision")
	}
	if provision["state"] != "error" {
		t.Errorf("provision state = %v, want error", provision["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	provisionJob := towerJobs["provision"].(map[string]interface{})
	if provisionJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.provision")
	}

	// Verify FinishAction was called with "error".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.Result.FinishAction)
	}
}

// --- handleProvisionFailed tests ---

func TestHandleProvisionFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleProvisionFailed(rc); err != nil {
		t.Fatalf("handleProvisionFailed returned error: %v", err)
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
	if labels["state"] != "provision-failed" {
		t.Errorf("state label = %v, want provision-failed", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "provision-failed" {
		t.Errorf("current_state = %v, want provision-failed", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify job_vars includes agnosticd_collect_forensics = true.
	jobVars, ok := vars["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("expected job_vars to be a map")
	}
	if jobVars["agnosticd_collect_forensics"] != true {
		t.Errorf("agnosticd_collect_forensics = %v, want true", jobVars["agnosticd_collect_forensics"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	provision := actions["provision"].(map[string]interface{})
	if provision["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.provision")
	}
	if provision["state"] != "failed" {
		t.Errorf("provision state = %v, want failed", provision["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	provisionJob := towerJobs["provision"].(map[string]interface{})
	if provisionJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.provision")
	}

	// Verify FinishAction was called with "failed".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.Result.FinishAction)
	}
}

func TestHandleProvisionFailedPreservesExistingJobVars(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set existing job_vars in the subject.
	rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
		"cloud_provider": "ec2",
		"region":         "us-east-1",
	}

	if err := handleProvisionFailed(rc); err != nil {
		t.Fatalf("handleProvisionFailed returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// Verify the PATCH update.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})

	// Verify job_vars preserves existing vars and adds forensics flag.
	jobVars, ok := vars["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("expected job_vars to be a map")
	}
	if jobVars["cloud_provider"] != "ec2" {
		t.Errorf("cloud_provider = %v, want ec2", jobVars["cloud_provider"])
	}
	if jobVars["region"] != "us-east-1" {
		t.Errorf("region = %v, want us-east-1", jobVars["region"])
	}
	if jobVars["agnosticd_collect_forensics"] != true {
		t.Errorf("agnosticd_collect_forensics = %v, want true", jobVars["agnosticd_collect_forensics"])
	}
}

// --- handleProvisionComplete tests ---

func TestHandleProvisionComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	provisionData := map[string]interface{}{"key": "value"}
	messageBody := "Provision successful"
	messages := []string{"msg1", "msg2"}

	if err := handleProvisionComplete(rc, provisionData, messageBody, messages); err != nil {
		t.Fatalf("handleProvisionComplete returned error: %v", err)
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
	if labels["state"] != "started" {
		t.Errorf("state label = %v, want started", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "started" {
		t.Errorf("current_state = %v, want started", vars["current_state"])
	}
	if vars["healthy"] != true {
		t.Errorf("healthy = %v, want true", vars["healthy"])
	}

	// Verify provision_data is set.
	pd, ok := vars["provision_data"].(map[string]interface{})
	if !ok {
		t.Fatal("expected provision_data to be a map")
	}
	if pd["key"] != "value" {
		t.Errorf("provision_data[key] = %v, want value", pd["key"])
	}

	// Verify provision_message_body is set.
	if vars["provision_message_body"] != "Provision successful" {
		t.Errorf("provision_message_body = %v, want Provision successful", vars["provision_message_body"])
	}

	// Verify provision_messages is set.
	switch msgs := vars["provision_messages"].(type) {
	case []string:
		if len(msgs) != 2 {
			t.Errorf("len(provision_messages) = %d, want 2", len(msgs))
		}
	case []interface{}:
		if len(msgs) != 2 {
			t.Errorf("len(provision_messages) = %d, want 2", len(msgs))
		}
	default:
		t.Fatalf("expected provision_messages to be a slice, got %T", vars["provision_messages"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	provision := actions["provision"].(map[string]interface{})
	if provision["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.provision")
	}
	if provision["state"] != "successful" {
		t.Errorf("provision state = %v, want successful", provision["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	provisionJob := towerJobs["provision"].(map[string]interface{})
	if provisionJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.provision")
	}

	// Verify FinishAction was called with "successful".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "successful" {
		t.Errorf("finishActionDirective = %v, want successful", rc.Result.FinishAction)
	}
}

func TestHandleProvisionCompleteWithNilData(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Call with all nil data.
	if err := handleProvisionComplete(rc, nil, nil, nil); err != nil {
		t.Fatalf("handleProvisionComplete returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// Verify the PATCH update.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})

	// Verify nil data fields are not present in vars.
	if _, ok := vars["provision_data"]; ok {
		t.Error("expected provision_data to be absent when nil")
	}
	if _, ok := vars["provision_message_body"]; ok {
		t.Error("expected provision_message_body to be absent when nil")
	}
	if _, ok := vars["provision_messages"]; ok {
		t.Error("expected provision_messages to be absent when nil")
	}

	// Verify current_state and healthy are still set.
	if vars["current_state"] != "started" {
		t.Errorf("current_state = %v, want started", vars["current_state"])
	}
	if vars["healthy"] != true {
		t.Errorf("healthy = %v, want true", vars["healthy"])
	}
}

// --- sandboxActionEnabled tests ---

func TestSandboxActionEnabledNoMeta(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// No meta -> should default to true.
	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no meta")
	}
}

func TestSandboxActionEnabledNoSandboxAPI(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set meta but no sandbox_api -> should default to true.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		Deployer: &types.DeployerMeta{
			SCMUrl: "https://github.com/example/repo.git",
		},
	}

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no sandbox_api")
	}
}

func TestSandboxActionEnabledNoActions(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set sandbox_api but no actions -> should default to true.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		SandboxAPI: map[string]interface{}{
			"some_config": "value",
		},
	}

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no actions config")
	}
}

func TestSandboxActionEnabledNoActionConfig(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set actions but no config for "start" -> should default to true.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		SandboxAPI: map[string]interface{}{
			"actions": map[string]interface{}{
				"stop": map[string]interface{}{
					"enable": false,
				},
			},
		},
	}

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no config for action")
	}
}

func TestSandboxActionEnabledTrue(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set enable = true -> should return true.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		SandboxAPI: map[string]interface{}{
			"actions": map[string]interface{}{
				"start": map[string]interface{}{
					"enable": true,
				},
			},
		},
	}

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when enable=true")
	}
}

func TestSandboxActionEnabledFalse(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set enable = false -> should return false.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		SandboxAPI: map[string]interface{}{
			"actions": map[string]interface{}{
				"start": map[string]interface{}{
					"enable": false,
				},
			},
		},
	}

	if sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return false when enable=false")
	}
}

func TestSandboxActionEnabledNonBool(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set enable to a non-bool (string) -> should default to true.
	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		SandboxAPI: map[string]interface{}{
			"actions": map[string]interface{}{
				"start": map[string]interface{}{
					"enable": "yes",
				},
			},
		},
	}

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when enable is non-bool")
	}
}

// --- handleProvisionCanceled tests ---

func TestHandleProvisionCanceled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleProvisionCanceled(rc); err != nil {
		t.Fatalf("handleProvisionCanceled returned error: %v", err)
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
	if labels["state"] != "provision-canceled" {
		t.Errorf("state label = %v, want provision-canceled", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "provision-canceled" {
		t.Errorf("current_state = %v, want provision-canceled", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status timestamps.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	provision := actions["provision"].(map[string]interface{})
	if provision["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.provision")
	}
	if provision["state"] != "canceled" {
		t.Errorf("provision state = %v, want canceled", provision["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	provisionJob := towerJobs["provision"].(map[string]interface{})
	if provisionJob["jobStatus"] != "canceled" {
		t.Errorf("jobStatus = %v, want canceled", provisionJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "canceled".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "canceled" {
		t.Errorf("finishActionDirective = %v, want canceled", rc.Result.FinishAction)
	}
}

// --- handleStartFailure retry/redirect tests ---

func TestHandleStartFailureRetryWhenDesiredStarted(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "start"}}

	// desired_state = started -> should retry with backoff.
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"

	if err := handleStartFailure(rc, "error"); err != nil {
		t.Fatalf("handleStartFailure returned error: %v", err)
	}

	// Should have 1 call: PATCH. ContinueAction now sets a directive.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// Verify state set to start-error.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "start-error" {
		t.Errorf("current_state = %v, want start-error", vars["current_state"])
	}

	// Verify continueActionDirective with retry (not finish).
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	// First retry interval should be "1m".
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "1m")

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called (should retry)")
	}
}

func TestHandleStartFailureCanceledRetry(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "start"}}

	// desired_state = started, canceled -> fixed 1m retry.
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"

	if err := handleStartFailure(rc, "canceled"); err != nil {
		t.Fatalf("handleStartFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	// Verify state label.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "start-canceled" {
		t.Errorf("state label = %v, want start-canceled", labels["state"])
	}

	// Verify fixed 1m retry via directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "1m")

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestHandleStartFailureRedirectToStop(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "start"}}

	// desired_state = stopped -> should schedule stop instead of retrying.
	rc.Payload.Subject.Spec.Vars.DesiredState = "stopped"

	if err := handleStartFailure(rc, "failed"); err != nil {
		t.Fatalf("handleStartFailure returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls (PATCH + schedule stop), got %d", len(*calls))
	}

	// Verify stop action was scheduled.
	c1 := (*calls)[1]
	if c1.Method != http.MethodPost {
		t.Errorf("call[1] method = %s, want POST", c1.Method)
	}
	if c1.Body["action"] != "stop" {
		t.Errorf("action = %v, want stop", c1.Body["action"])
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

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestHandleStartFailureWhileBeingDeleted(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "start"}}

	// desired_state = started but being deleted -> should finish.
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"
	ts := "2024-01-01T00:00:00Z"
	rc.Payload.Subject.Metadata.DeletionTimestamp = &ts

	if err := handleStartFailure(rc, "error"); err != nil {
		t.Fatalf("handleStartFailure returned error: %v", err)
	}

	// Should have 1 call (PATCH) then finish -- no retry.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH only), got %d", len(*calls))
	}

	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called when being deleted")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.Result.FinishAction)
	}
}

func TestHandleStartFailureForensicsOnFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "start"}}

	if err := handleStartFailure(rc, "failed"); err != nil {
		t.Fatalf("handleStartFailure returned error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	// Verify agnosticd_collect_forensics set on failed.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	jv, ok := vars["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("expected job_vars in spec.vars")
	}
	if jv["agnosticd_collect_forensics"] != true {
		t.Errorf("agnosticd_collect_forensics = %v, want true", jv["agnosticd_collect_forensics"])
	}
}

// --- handleStopFailure retry/redirect tests ---

func TestHandleStopFailureRetryWhenDesiredStopped(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "stop"}}

	// desired_state = stopped -> should retry with backoff.
	rc.Payload.Subject.Spec.Vars.DesiredState = "stopped"

	if err := handleStopFailure(rc, "error"); err != nil {
		t.Fatalf("handleStopFailure returned error: %v", err)
	}

	// Should have 1 call: PATCH. ContinueAction now sets a directive.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "stop-error" {
		t.Errorf("current_state = %v, want stop-error", vars["current_state"])
	}

	// Verify continueActionDirective with retry.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "1m")

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called (should retry)")
	}
}

func TestHandleStopFailureCanceledRetry(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "stop"}}

	// desired_state = stopped, canceled -> fixed 1m retry.
	rc.Payload.Subject.Spec.Vars.DesiredState = "stopped"

	if err := handleStopFailure(rc, "canceled"); err != nil {
		t.Fatalf("handleStopFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "stop-canceled" {
		t.Errorf("state label = %v, want stop-canceled", labels["state"])
	}

	// Verify fixed 1m retry via directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "1m")

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestHandleStopFailureRedirectToStart(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "stop"}}

	// desired_state = started -> should schedule start instead.
	rc.Payload.Subject.Spec.Vars.DesiredState = "started"

	if err := handleStopFailure(rc, "error"); err != nil {
		t.Fatalf("handleStopFailure returned error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls (PATCH + schedule start), got %d", len(*calls))
	}

	c1 := (*calls)[1]
	if c1.Body["action"] != "start" {
		t.Errorf("action = %v, want start", c1.Body["action"])
	}

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

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestHandleStopFailureWhileBeingDeleted(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "stop"}}

	rc.Payload.Subject.Spec.Vars.DesiredState = "stopped"
	ts := "2024-01-01T00:00:00Z"
	rc.Payload.Subject.Metadata.DeletionTimestamp = &ts

	if err := handleStopFailure(rc, "failed"); err != nil {
		t.Fatalf("handleStopFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH only), got %d", len(*calls))
	}

	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called when being deleted")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.Result.FinishAction)
	}
}

func TestHandleStopFailureForensicsOnFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "stop"}}

	if err := handleStopFailure(rc, "failed"); err != nil {
		t.Fatalf("handleStopFailure returned error: %v", err)
	}

	if len(*calls) < 1 {
		t.Fatalf("expected at least 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	jv, ok := vars["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("expected job_vars in spec.vars")
	}
	if jv["agnosticd_collect_forensics"] != true {
		t.Errorf("agnosticd_collect_forensics = %v, want true", jv["agnosticd_collect_forensics"])
	}
}

// --- handleStatusFailure tests ---

func TestHandleStatusFailureError(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleStatusFailure(rc, "error"); err != nil {
		t.Fatalf("handleStatusFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})

	// Status does NOT set metadata/labels.
	if _, ok := patch["metadata"]; ok {
		t.Error("expected no metadata in status failure patch")
	}

	// Verify spec vars set check_status_state.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["check_status_state"] != "error" {
		t.Errorf("check_status_state = %v, want error", vars["check_status_state"])
	}

	// Verify status timestamps.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	statusAction := actions["status"].(map[string]interface{})
	if statusAction["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.status")
	}
	if statusAction["state"] != "error" {
		t.Errorf("status action state = %v, want error", statusAction["state"])
	}

	// Verify FinishAction with "error".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.Result.FinishAction)
	}
}

func TestHandleStatusFailureCanceledFinishesAsFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleStatusFailure(rc, "canceled"); err != nil {
		t.Fatalf("handleStatusFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["check_status_state"] != "canceled" {
		t.Errorf("check_status_state = %v, want canceled", vars["check_status_state"])
	}

	// Key Ansible behavior: canceled status finishes as "failed".
	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed (canceled status -> failed)", rc.Result.FinishAction)
	}
}

// --- handleUpdateFailure tests ---

func TestHandleUpdateFailureRetry(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "update"}}

	if err := handleUpdateFailure(rc, "error"); err != nil {
		t.Fatalf("handleUpdateFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH), got %d", len(*calls))
	}

	// Verify state.
	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	metadata := patch["metadata"].(map[string]interface{})
	labels := metadata["labels"].(map[string]interface{})
	if labels["state"] != "update-error" {
		t.Errorf("state label = %v, want update-error", labels["state"])
	}
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify continueActionDirective (retry at current interval, no increment).
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestHandleUpdateFailureCanceled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "update"}}

	if err := handleUpdateFailure(rc, "canceled"); err != nil {
		t.Fatalf("handleUpdateFailure returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH), got %d", len(*calls))
	}

	// Verify fixed 1m retry for canceled via directive.
	if rc.Result.ContinueAction == nil {
		t.Fatal("expected continueActionDirective to be set")
	}
	assertAfterTimestamp(t, rc.Result.ContinueAction.After, "1m")

	if rc.Result.FinishAction != nil {
		t.Error("expected FinishAction NOT to be called")
	}
}

func TestHandleUpdateFailureWhileBeingDeleted(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	rc.Payload.Action = &types.Action{Spec: types.ActionSpec{Action: "update"}}

	ts := "2024-01-01T00:00:00Z"
	rc.Payload.Subject.Metadata.DeletionTimestamp = &ts

	if err := handleUpdateFailure(rc, "failed"); err != nil {
		t.Fatalf("handleUpdateFailure returned error: %v", err)
	}

	// Being deleted -> finish, no retry.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call (PATCH only), got %d", len(*calls))
	}

	if rc.Result.FinishAction == nil {
		t.Error("expected FinishAction to be called when being deleted")
	}
	if rc.Result.FinishAction == nil || rc.Result.FinishAction.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.Result.FinishAction)
	}
}

// --- actionRetryInterval tests ---

func TestActionRetryIntervals(t *testing.T) {
	intervals := []string{"1m", "5m", "10m", "30m", "1h", "2h", "4h", "8h", "16h", "1d"}

	for i, want := range intervals {
		got := actionRetryInterval(i, intervals)
		if got != want {
			t.Errorf("actionRetryInterval(%d) = %q, want %q", i, got, want)
		}
	}

	// Beyond the list: should cap at last interval.
	if got := actionRetryInterval(10, intervals); got != "1d" {
		t.Errorf("actionRetryInterval(10) = %q, want 1d (cap)", got)
	}
	if got := actionRetryInterval(100, intervals); got != "1d" {
		t.Errorf("actionRetryInterval(100) = %q, want 1d (cap)", got)
	}
}

// --- handleStatusComplete with data tests ---

func TestHandleStatusCompleteWithData(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	statusData := map[string]interface{}{"cloud_state": "running"}
	statusMessages := []interface{}{"all good"}

	if err := handleStatusComplete(rc, statusData, statusMessages); err != nil {
		t.Fatalf("handleStatusComplete returned error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	c0 := (*calls)[0]
	patch := c0.Body["patch"].(map[string]interface{})
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})

	if vars["check_status_state"] != "successful" {
		t.Errorf("check_status_state = %v, want successful", vars["check_status_state"])
	}

	sd, ok := vars["status_data"].(map[string]interface{})
	if !ok {
		t.Fatal("expected status_data to be a map")
	}
	if sd["cloud_state"] != "running" {
		t.Errorf("status_data[cloud_state] = %v, want running", sd["cloud_state"])
	}

	sm, ok := vars["status_messages"].([]interface{})
	if !ok {
		t.Fatal("expected status_messages to be a slice")
	}
	if len(sm) != 1 || sm[0] != "all good" {
		t.Errorf("status_messages = %v, want [all good]", sm)
	}
}
