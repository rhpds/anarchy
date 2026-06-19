package handler

import (
	"log/slog"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// handleStop routes a stop action based on the current state.
func handleStop(rc *runner.RunContext) error {
	if rc.CurrentState() != "stopping" {
		return runStop(rc)
	}
	if !rc.DeployerDisabled("stop") {
		return checkDeployerJob(rc, "stop")
	}
	return nil
}

// runStop initiates the stop workflow.
func runStop(rc *runner.RunContext) error {
	// Set startTimestamp.
	ts := types.NowUTC()
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"startTimestamp": ts,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	// If deployer not disabled: get sandbox vars and launch Tower job.
	if !rc.DeployerDisabled("stop") {
		var dynamicJobVars map[string]interface{}
		if rc.SandboxAPIInUse() {
			result, err := sandboxGet(rc, "stop")
			if err != nil {
				slog.Error("runStop: sandbox get error", "subject", rc.SubjectName(), "error", err)
			} else if result != nil {
				dynamicJobVars = result.DynamicVars
			}
		}
		if err := launchTowerJob(rc, "stop", "stopping", nil, dynamicJobVars); err != nil {
			slog.Error("runStop: tower launch failed", "subject", rc.SubjectName(), "error", err)
			return err
		}
		rc.ContinueAction("5m")
		return nil
	}

	// Deployer disabled and sandbox API in use: perform sandbox API stop.
	if rc.SandboxAPIInUse() && sandboxActionEnabled(rc, "stop") {
		if err := sandboxStop(rc); err != nil {
			slog.Error("runStop: sandbox stop error", "subject", rc.SubjectName(), "error", err)
		}
		ts := types.NowUTC()
		if err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
				Metadata: &types.PatchMetadata{
					Labels: map[string]string{
						"state": "stopped",
					},
				},
				Spec: &types.PatchSpec{
					Vars: map[string]interface{}{
						"current_state": "stopped",
					},
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"stop": map[string]interface{}{
							"completeTimestamp": ts,
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
		rc.FinishAction("successful")
	}

	return nil
}

// handleStopComplete finalizes a successful stop.
func handleStopComplete(rc *runner.RunContext) error {
	ts := types.NowUTC()

	// Update tower jobs status.
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": ts,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":        "successful",
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	// Sandbox API stop if enabled.
	if rc.SandboxAPIInUse() && sandboxActionEnabled(rc, "stop") {
		if err := sandboxStop(rc); err != nil {
			slog.Error("handleStopComplete: sandbox stop error", "subject", rc.SubjectName(), "error", err)
		}
	}

	// Update state to stopped.
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{
					"state": "stopped",
				},
			},
			Spec: &types.PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "stopped",
				},
			},
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("successful")
	return nil
}
