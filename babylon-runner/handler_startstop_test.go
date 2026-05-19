package main

import (
	"testing"
)

// TestHandleStartDeployerDisabled tests start when deployer is disabled.
// Sandbox API in use, deployer disabled, sandbox action enabled -> should finish with started state.
func TestHandleStartDeployerDisabled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Configure: sandbox API in use, deployer disabled for start.
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "aws_sandboxed")
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "deployer", "actions", "start", "disable")

	if err := handleStart(rc); err != nil {
		t.Fatalf("handleStart returned error: %v", err)
	}

	// Should have 2 calls: set startTimestamp, then mark started.
	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: set startTimestamp.
	c0 := (*calls)[0]
	if c0.Method != "PATCH" {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch0 := c0.Body["patch"].(map[string]interface{})
	status0 := patch0["status"].(map[string]interface{})
	actions0 := status0["actions"].(map[string]interface{})
	start0 := actions0["start"].(map[string]interface{})
	if _, ok := start0["startTimestamp"]; !ok {
		t.Error("expected startTimestamp in call[0]")
	}

	// Second call: mark started.
	c1 := (*calls)[1]
	if c1.Method != "PATCH" {
		t.Errorf("call[1] method = %s, want PATCH", c1.Method)
	}
	patch1 := c1.Body["patch"].(map[string]interface{})

	// Verify metadata labels.
	meta1 := patch1["metadata"].(map[string]interface{})
	labels1 := meta1["labels"].(map[string]interface{})
	if labels1["state"] != "started" {
		t.Errorf("state label = %v, want started", labels1["state"])
	}

	// Verify spec vars.
	spec1 := patch1["spec"].(map[string]interface{})
	vars1 := spec1["vars"].(map[string]interface{})
	if vars1["current_state"] != "started" {
		t.Errorf("current_state = %v, want started", vars1["current_state"])
	}

	// Verify status.
	status1 := patch1["status"].(map[string]interface{})
	actions1 := status1["actions"].(map[string]interface{})
	start1 := actions1["start"].(map[string]interface{})
	if _, ok := start1["completeTimestamp"]; !ok {
		t.Error("expected completeTimestamp in call[1]")
	}

	// FinishAction should have been called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}

// TestHandleStopDeployerDisabled tests stop when deployer is disabled.
// Sandbox API in use, deployer disabled, sandbox action enabled -> should finish with stopped state.
func TestHandleStopDeployerDisabled(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Configure: sandbox API in use, deployer disabled for stop.
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "aws_sandboxed")
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "deployer", "actions", "stop", "disable")

	if err := handleStop(rc); err != nil {
		t.Fatalf("handleStop returned error: %v", err)
	}

	// Should have 2 calls: set startTimestamp, then mark stopped.
	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: set startTimestamp.
	c0 := (*calls)[0]
	if c0.Method != "PATCH" {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch0 := c0.Body["patch"].(map[string]interface{})
	status0 := patch0["status"].(map[string]interface{})
	actions0 := status0["actions"].(map[string]interface{})
	stop0 := actions0["stop"].(map[string]interface{})
	if _, ok := stop0["startTimestamp"]; !ok {
		t.Error("expected startTimestamp in call[0]")
	}

	// Second call: mark stopped.
	c1 := (*calls)[1]
	if c1.Method != "PATCH" {
		t.Errorf("call[1] method = %s, want PATCH", c1.Method)
	}
	patch1 := c1.Body["patch"].(map[string]interface{})

	// Verify metadata labels.
	meta1 := patch1["metadata"].(map[string]interface{})
	labels1 := meta1["labels"].(map[string]interface{})
	if labels1["state"] != "stopped" {
		t.Errorf("state label = %v, want stopped", labels1["state"])
	}

	// Verify spec vars.
	spec1 := patch1["spec"].(map[string]interface{})
	vars1 := spec1["vars"].(map[string]interface{})
	if vars1["current_state"] != "stopped" {
		t.Errorf("current_state = %v, want stopped", vars1["current_state"])
	}

	// Verify status.
	status1 := patch1["status"].(map[string]interface{})
	actions1 := status1["actions"].(map[string]interface{})
	stop1 := actions1["stop"].(map[string]interface{})
	if _, ok := stop1["completeTimestamp"]; !ok {
		t.Error("expected completeTimestamp in call[1]")
	}

	// FinishAction should have been called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}

// TestHandleStartComplete tests the start complete callback.
func TestHandleStartComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	if err := handleStartComplete(rc); err != nil {
		t.Fatalf("handleStartComplete returned error: %v", err)
	}

	// Should have 1 call.
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}

	c := (*calls)[0]
	if c.Method != "PATCH" {
		t.Errorf("method = %s, want PATCH", c.Method)
	}

	patch := c.Body["patch"].(map[string]interface{})

	// Verify metadata labels.
	meta := patch["metadata"].(map[string]interface{})
	labels := meta["labels"].(map[string]interface{})
	if labels["state"] != "started" {
		t.Errorf("state label = %v, want started", labels["state"])
	}

	// Verify spec vars.
	spec := patch["spec"].(map[string]interface{})
	vars := spec["vars"].(map[string]interface{})
	if vars["current_state"] != "started" {
		t.Errorf("current_state = %v, want started", vars["current_state"])
	}

	// Verify status.
	status := patch["status"].(map[string]interface{})
	actions := status["actions"].(map[string]interface{})
	start := actions["start"].(map[string]interface{})
	if _, ok := start["completeTimestamp"]; !ok {
		t.Error("expected completeTimestamp")
	}
	if start["state"] != "successful" {
		t.Errorf("start state = %v, want successful", start["state"])
	}

	// Verify towerJobs.
	towerJobs := status["towerJobs"].(map[string]interface{})
	startJob := towerJobs["start"].(map[string]interface{})
	if _, ok := startJob["completeTimestamp"]; !ok {
		t.Error("expected towerJobs.start.completeTimestamp")
	}
	if startJob["jobStatus"] != "successful" {
		t.Errorf("jobStatus = %v, want successful", startJob["jobStatus"])
	}

	// FinishAction should have been called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}

// TestHandleStopComplete tests the stop complete callback.
func TestHandleStopComplete(t *testing.T) {
	server, calls := newTestAnarchyServer(t)
	defer server.Close()

	rc := newTestRunContext(t, server)

	// Configure sandbox API in use to trigger the log message.
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "aws_sandboxed")

	if err := handleStopComplete(rc); err != nil {
		t.Fatalf("handleStopComplete returned error: %v", err)
	}

	// Should have 2 calls: update tower jobs, then update state.
	if len(*calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(*calls))
	}

	// First call: update tower jobs and action status.
	c0 := (*calls)[0]
	if c0.Method != "PATCH" {
		t.Errorf("call[0] method = %s, want PATCH", c0.Method)
	}
	patch0 := c0.Body["patch"].(map[string]interface{})
	status0 := patch0["status"].(map[string]interface{})

	actions0 := status0["actions"].(map[string]interface{})
	stop0 := actions0["stop"].(map[string]interface{})
	if _, ok := stop0["completeTimestamp"]; !ok {
		t.Error("expected completeTimestamp in call[0]")
	}
	if stop0["state"] != "successful" {
		t.Errorf("stop state = %v, want successful", stop0["state"])
	}

	towerJobs0 := status0["towerJobs"].(map[string]interface{})
	stopJob0 := towerJobs0["stop"].(map[string]interface{})
	if _, ok := stopJob0["completeTimestamp"]; !ok {
		t.Error("expected towerJobs.stop.completeTimestamp in call[0]")
	}
	if stopJob0["jobStatus"] != "successful" {
		t.Errorf("jobStatus = %v, want successful", stopJob0["jobStatus"])
	}

	// Second call: update state to stopped.
	c1 := (*calls)[1]
	if c1.Method != "PATCH" {
		t.Errorf("call[1] method = %s, want PATCH", c1.Method)
	}
	patch1 := c1.Body["patch"].(map[string]interface{})

	// Verify metadata labels.
	meta1 := patch1["metadata"].(map[string]interface{})
	labels1 := meta1["labels"].(map[string]interface{})
	if labels1["state"] != "stopped" {
		t.Errorf("state label = %v, want stopped", labels1["state"])
	}

	// Verify spec vars.
	spec1 := patch1["spec"].(map[string]interface{})
	vars1 := spec1["vars"].(map[string]interface{})
	if vars1["current_state"] != "stopped" {
		t.Errorf("current_state = %v, want stopped", vars1["current_state"])
	}

	// FinishAction should have been called.
	if !rc.finished {
		t.Error("expected FinishAction to be called")
	}
}
