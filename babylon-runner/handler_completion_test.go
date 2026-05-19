package main

import (
	"net/http"
	"testing"
)

// --- handleDestroyError tests ---

func TestHandleDestroyError(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleDestroyError(rc); err != nil {
		t.Fatalf("handleDestroyError returned error: %v", err)
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
	if labels["state"] != "destroy-error" {
		t.Errorf("state label = %v, want destroy-error", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-error" {
		t.Errorf("current_state = %v, want destroy-error", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	destroy := actions["destroy"].(map[string]interface{})
	if destroy["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.destroy")
	}
	if destroy["state"] != "error" {
		t.Errorf("destroy state = %v, want error", destroy["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	destroyJob := towerJobs["destroy"].(map[string]interface{})
	if destroyJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.destroy")
	}
	if destroyJob["jobStatus"] != "error" {
		t.Errorf("jobStatus = %v, want error", destroyJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "error".
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.finishActionDirective)
	}
}

// --- handleDestroyFailed tests ---

func TestHandleDestroyFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleDestroyFailed(rc); err != nil {
		t.Fatalf("handleDestroyFailed returned error: %v", err)
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
	if labels["state"] != "destroy-failed" {
		t.Errorf("state label = %v, want destroy-failed", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-failed" {
		t.Errorf("current_state = %v, want destroy-failed", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	destroy := actions["destroy"].(map[string]interface{})
	if destroy["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.destroy")
	}
	if destroy["state"] != "failed" {
		t.Errorf("destroy state = %v, want failed", destroy["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	destroyJob := towerJobs["destroy"].(map[string]interface{})
	if destroyJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.destroy")
	}
	if destroyJob["jobStatus"] != "failed" {
		t.Errorf("jobStatus = %v, want failed", destroyJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "failed".
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.finishActionDirective)
	}
}

// --- handleDestroyCanceled tests ---

func TestHandleDestroyCanceled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleDestroyCanceled(rc); err != nil {
		t.Fatalf("handleDestroyCanceled returned error: %v", err)
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
	if labels["state"] != "destroy-canceled" {
		t.Errorf("state label = %v, want destroy-canceled", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "destroy-canceled" {
		t.Errorf("current_state = %v, want destroy-canceled", vars["current_state"])
	}
	if vars["healthy"] != false {
		t.Errorf("healthy = %v, want false", vars["healthy"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	destroy := actions["destroy"].(map[string]interface{})
	if destroy["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in actions.destroy")
	}
	if destroy["state"] != "canceled" {
		t.Errorf("destroy state = %v, want canceled", destroy["state"])
	}

	towerJobs := status["towerJobs"].(map[string]interface{})
	destroyJob := towerJobs["destroy"].(map[string]interface{})
	if destroyJob["completeTimestamp"] == nil {
		t.Error("expected completeTimestamp in towerJobs.destroy")
	}
	if destroyJob["jobStatus"] != "canceled" {
		t.Errorf("jobStatus = %v, want canceled", destroyJob["jobStatus"])
	}

	// Verify skip_update_processing.
	if patch["skip_update_processing"] != true {
		t.Error("expected skip_update_processing to be true")
	}

	// Verify FinishAction was called with "canceled".
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "canceled" {
		t.Errorf("finishActionDirective = %v, want canceled", rc.finishActionDirective)
	}
}

// --- handleStatusComplete tests ---

func TestHandleStatusComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	if err := handleStatusComplete(rc); err != nil {
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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "successful" {
		t.Errorf("finishActionDirective = %v, want successful", rc.finishActionDirective)
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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "successful" {
		t.Errorf("finishActionDirective = %v, want successful", rc.finishActionDirective)
	}
}

// --- handleStartFailure / handleStopFailure tests ---

func TestHandleStartFailureWithError(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	// desired_state not set → finishes immediately.

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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.finishActionDirective)
	}
}

func TestHandleStopFailureWithFailed(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)
	// desired_state not set → finishes immediately.

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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.finishActionDirective)
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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "error" {
		t.Errorf("finishActionDirective = %v, want error", rc.finishActionDirective)
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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "failed" {
		t.Errorf("finishActionDirective = %v, want failed", rc.finishActionDirective)
	}
}

func TestHandleProvisionFailedPreservesExistingJobVars(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set existing job_vars in the subject.
	setNested(rc.Payload.Subject, map[string]interface{}{
		"cloud_provider": "ec2",
		"region":         "us-east-1",
	}, "spec", "vars", "job_vars")

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
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
	if rc.finishActionDirective == nil || rc.finishActionDirective.State != "successful" {
		t.Errorf("finishActionDirective = %v, want successful", rc.finishActionDirective)
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

	// No meta → should default to true.
	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no meta")
	}
}

func TestSandboxActionEnabledNoSandboxAPI(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set meta but no sandbox_api → should default to true.
	setNested(rc.Payload.Governor, map[string]interface{}{
		"deployer": map[string]interface{}{
			"scm_url": "https://github.com/example/repo.git",
		},
	}, "spec", "vars", "__meta__")

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no sandbox_api")
	}
}

func TestSandboxActionEnabledNoActions(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set sandbox_api but no actions → should default to true.
	setNested(rc.Payload.Governor, map[string]interface{}{
		"sandbox_api": map[string]interface{}{
			"some_config": "value",
		},
	}, "spec", "vars", "__meta__")

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no actions config")
	}
}

func TestSandboxActionEnabledNoActionConfig(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set actions but no config for "start" → should default to true.
	setNested(rc.Payload.Governor, map[string]interface{}{
		"sandbox_api": map[string]interface{}{
			"actions": map[string]interface{}{
				"stop": map[string]interface{}{
					"enable": false,
				},
			},
		},
	}, "spec", "vars", "__meta__")

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when no config for action")
	}
}

func TestSandboxActionEnabledTrue(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set enable = true → should return true.
	setNested(rc.Payload.Governor, map[string]interface{}{
		"sandbox_api": map[string]interface{}{
			"actions": map[string]interface{}{
				"start": map[string]interface{}{
					"enable": true,
				},
			},
		},
	}, "spec", "vars", "__meta__")

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when enable=true")
	}
}

func TestSandboxActionEnabledFalse(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set enable = false → should return false.
	setNested(rc.Payload.Governor, map[string]interface{}{
		"sandbox_api": map[string]interface{}{
			"actions": map[string]interface{}{
				"start": map[string]interface{}{
					"enable": false,
				},
			},
		},
	}, "spec", "vars", "__meta__")

	if sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return false when enable=false")
	}
}

func TestSandboxActionEnabledNonBool(t *testing.T) {
	server, _ := newTestAnarchyServer(t)
	defer server.Close()
	rc := newTestRunContext(t, server)

	// Set enable to a non-bool (string) → should default to true.
	setNested(rc.Payload.Governor, map[string]interface{}{
		"sandbox_api": map[string]interface{}{
			"actions": map[string]interface{}{
				"start": map[string]interface{}{
					"enable": "yes",
				},
			},
		},
	}, "spec", "vars", "__meta__")

	if !sandboxActionEnabled(rc, "start") {
		t.Error("expected sandboxActionEnabled to return true when enable is non-bool")
	}
}
