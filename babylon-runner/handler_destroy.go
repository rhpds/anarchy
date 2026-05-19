package main

import (
	"log/slog"
)

// handleDestroy routes a destroy action based on the current state.
func handleDestroy(rc *RunContext) error {
	currentState := rc.CurrentState()
	actions := rc.StatusActions()
	destroy := getNestedMap(actions, "destroy")

	// Set startTimestamp if not already set and we're in destroy-pending.
	if (destroy == nil || destroy["startTimestamp"] == nil) && currentState == "destroy-pending" {
		ts := nowUTC()
		if err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"destroy": map[string]interface{}{
							"startTimestamp": ts,
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
	}

	// Sandbox API destroy catch-all: cleanup and delete if in error state.
	if rc.SandboxAPIInUse() && sandboxDestroyCatchAll(rc) {
		errorStates := map[string]bool{
			"destroy-error":    true,
			"destroy-failed":   true,
			"destroy-canceled": true,
		}
		if errorStates[currentState] || rc.DeployerDisabled("destroy") {
			slog.Info("handleDestroy: destroy catch-all triggered", "subject", rc.SubjectName, "state", currentState)
			if err := sandboxCleanup(rc); err != nil {
				slog.Error("handleDestroy: sandbox cleanup error", "subject", rc.SubjectName, "error", err)
			}
			rc.DeleteSubject(true)
			rc.FinishAction("successful")
			return nil
		}
	}

	// Run destroy if not yet in "destroying" state.
	if currentState != "destroying" && !rc.DeployerDisabled("destroy") {
		return runDestroy(rc)
	}

	// Check deployer job if already destroying.
	if currentState == "destroying" && !rc.DeployerDisabled("destroy") {
		return checkDeployerJob(rc, "destroy")
	}

	return nil
}

// runDestroy initiates the destroy workflow.
func runDestroy(rc *RunContext) error {
	// Sandbox API integration: get placement for destroy vars.
	var dynamicJobVars map[string]interface{}
	if rc.SandboxAPIInUse() {
		result, err := sandboxGet(rc, "destroy")
		if err != nil {
			slog.Error("runDestroy: sandbox get error", "subject", rc.SubjectName, "error", err)
		} else if result != nil {
			if result.Status == "error" {
				slog.Error("runDestroy: sandbox placement in error state", "subject", rc.SubjectName)
			}
			dynamicJobVars = result.DynamicVars
		}
	}

	// Cancel running provision Tower job if exists.
	cancelTowerJob(rc, "provision")

	// Launch Tower job for destroy.
	if err := launchTowerJob(rc, "destroy", "destroying", nil, dynamicJobVars); err != nil {
		slog.Error("runDestroy: tower launch failed", "subject", rc.SubjectName, "error", err)
		return err
	}

	return rc.ContinueAction("5m")
}

// handleDestroyComplete finalizes a successful destroy.
func handleDestroyComplete(rc *RunContext) error {
	// Sandbox API cleanup: release placement.
	if rc.SandboxAPIInUse() {
		if err := sandboxCleanup(rc); err != nil {
			slog.Error("handleDestroyComplete: sandbox cleanup error", "subject", rc.SubjectName, "error", err)
		}
	}

	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "destroy-complete",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-complete",
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"status":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         "successful",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	rc.DeleteSubject(true)
	rc.FinishAction("successful")
	return nil
}

// handleDestroyError marks a destroy as errored (transient failure).
func handleDestroyError(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "destroy-error",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-error",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"status":            "error",
					},
				},
				"towerJobs": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         "error",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("error")
	return nil
}

// handleDestroyFailed marks a destroy as permanently failed.
func handleDestroyFailed(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "destroy-failed",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-failed",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"status":            "failed",
					},
				},
				"towerJobs": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         "failed",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("failed")
	return nil
}

// handleDestroyCanceled marks a destroy as canceled.
func handleDestroyCanceled(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "destroy-canceled",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-canceled",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"status":            "canceled",
					},
				},
				"towerJobs": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         "canceled",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("canceled")
	return nil
}
