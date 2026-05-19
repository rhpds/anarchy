package main

import (
	"log/slog"
)

// handleStart routes a start action based on the current state.
func handleStart(rc *RunContext) error {
	if rc.CurrentState() != "starting" {
		return runStart(rc)
	}
	if !rc.DeployerDisabled("start") {
		return checkDeployerJob(rc, "start")
	}
	return nil
}

// runStart initiates the start workflow.
func runStart(rc *RunContext) error {
	// Set startTimestamp.
	ts := nowUTC()
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"start": map[string]interface{}{
						"startTimestamp": ts,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	// Sandbox API start if enabled.
	if rc.SandboxAPIInUse() && sandboxActionEnabled(rc, "start") {
		if err := sandboxStart(rc); err != nil {
			slog.Error("runStart: sandbox start error", "subject", rc.SubjectName, "error", err)
		}
		// If deployer disabled for start: mark started immediately.
		if rc.DeployerDisabled("start") {
			ts := nowUTC()
			if err := rc.SubjectUpdate(SubjectPatch{
				Patch: PatchBody{
					Metadata: &PatchMetadata{
						Labels: map[string]string{
							"state": "started",
						},
					},
					Spec: &PatchSpec{
						Vars: map[string]interface{}{
							"current_state": "started",
						},
					},
					Status: map[string]interface{}{
						"actions": map[string]interface{}{
							"start": map[string]interface{}{
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
			return nil
		}
	}

	// If deployer not disabled: get sandbox vars and launch Tower job.
	if !rc.DeployerDisabled("start") {
		var dynamicJobVars map[string]interface{}
		if rc.SandboxAPIInUse() {
			result, err := sandboxGet(rc, "start")
			if err != nil {
				slog.Error("runStart: sandbox get error", "subject", rc.SubjectName, "error", err)
			} else if result != nil {
				dynamicJobVars = result.DynamicVars
			}
		}
		if err := launchTowerJob(rc, "start", "starting", nil, dynamicJobVars); err != nil {
			slog.Error("runStart: tower launch failed", "subject", rc.SubjectName, "error", err)
			return err
		}
		return rc.ContinueAction("5m")
	}

	return nil
}

// handleStartComplete finalizes a successful start.
func handleStartComplete(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "started",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "started",
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"start": map[string]interface{}{
						"completeTimestamp": ts,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"start": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":        "successful",
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("successful")
	return nil
}

// sandboxActionEnabled checks if a sandbox action is enabled.
// Defaults to true if not configured.
func sandboxActionEnabled(rc *RunContext, action string) bool {
	meta := rc.Meta()
	if meta == nil {
		return true // default enabled
	}
	sandboxAPI, _ := meta["sandbox_api"].(map[string]interface{})
	if sandboxAPI == nil {
		return true
	}
	actions, _ := sandboxAPI["actions"].(map[string]interface{})
	if actions == nil {
		return true
	}
	actionCfg, _ := actions[action].(map[string]interface{})
	if actionCfg == nil {
		return true
	}
	enable, ok := actionCfg["enable"].(bool)
	if !ok {
		return true
	}
	return enable
}
