package main

import (
	"log/slog"
)

// handleStatus routes a status action based on check_status_state.
func handleStatus(rc *RunContext) error {
	subjectVars := getNestedMap(rc.Payload.Subject, "spec", "vars")
	if subjectVars == nil {
		slog.Warn("handleStatus: no subject vars", "subject", rc.SubjectName)
		return nil
	}

	checkStatusState, _ := subjectVars["check_status_state"].(string)

	if checkStatusState == "pending" {
		return runStatus(rc)
	}

	if checkStatusState == "running" && !rc.DeployerDisabled("status") {
		return checkDeployerJob(rc, "status")
	}

	return nil
}

// runStatus initiates the status check workflow.
func runStatus(rc *RunContext) error {
	// Set startTimestamp (always, matching Ansible).
	ts := nowUTC()
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
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
				slog.Error("runStatus: sandbox get error", "subject", rc.SubjectName, "error", err)
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
			slog.Error("runStatus: tower launch failed", "subject", rc.SubjectName, "error", err)
			return err
		}
		rc.ContinueAction("5m")
		return nil
	}

	return nil
}
