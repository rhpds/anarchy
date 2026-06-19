package handler

import (
	"log/slog"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// handleStatus routes a status action based on check_status_state.
func handleStatus(rc *runner.RunContext) error {
	slog.Info("handling status", "subject", rc.SubjectName(), "state", rc.CheckStatusState())
	if rc.CheckStatusState() == "" && rc.Payload.Subject.Spec.Vars.JobVars == nil {
		slog.Warn("handleStatus: no subject vars", "subject", rc.SubjectName())
		return nil
	}
	checkStatusState := rc.CheckStatusState()

	if checkStatusState == "pending" {
		return runStatus(rc)
	}

	if checkStatusState == "running" {
		if !rc.DeployerDisabled("status") {
			return checkDeployerJob(rc, "status")
		}
		// Deployer disabled but state stuck at "running" — finish it.
		if err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
				Spec: &types.PatchSpec{
					Vars: map[string]interface{}{
						"check_status_state": "successful",
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
		rc.FinishAction("successful")
		return nil
	}

	return nil
}

// runStatus initiates the status check workflow.
func runStatus(rc *runner.RunContext) error {
	// Set startTimestamp (always, matching Ansible).
	ts := types.NowUTC()
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"status": map[string]interface{}{
						"startTimestamp": ts,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	if !rc.DeployerDisabled("status") {
		// Get sandbox vars for Tower job.
		var dynamicJobVars map[string]interface{}
		if rc.SandboxAPIInUse() {
			result, err := sandboxGet(rc, "status")
			if err != nil {
				slog.Error("runStatus: sandbox get error", "subject", rc.SubjectName(), "error", err)
			} else if result != nil {
				dynamicJobVars = result.DynamicVars
			}
		}

		// Launch Tower job for status check. Status action does not
		// transition current_state but sets check_status_state.
		extraSpecVars := map[string]interface{}{
			"check_status_state": "running",
		}
		if err := launchTowerJob(rc, "status", "", extraSpecVars, dynamicJobVars); err != nil {
			slog.Error("runStatus: tower launch failed", "subject", rc.SubjectName(), "error", err)
			return err
		}
		rc.ContinueAction("5m")
		return nil
	}

	// Deployer disabled: mark status check as successful immediately.
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Spec: &types.PatchSpec{
				Vars: map[string]interface{}{
					"check_status_state": "successful",
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}
	rc.FinishAction("successful")
	return nil
}
