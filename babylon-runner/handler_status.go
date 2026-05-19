package main

import (
	"log"
)

// handleStatus routes a status action based on check_status_state.
func handleStatus(rc *RunContext) error {
	subjectVars := getNestedMap(rc.Payload.Subject, "spec", "vars")
	if subjectVars == nil {
		log.Printf("handleStatus: no subject vars for subject=%s", rc.SubjectName)
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
	// Set startTimestamp for status action.
	actions := rc.StatusActions()
	status := getNestedMap(actions, "status")
	if status == nil || status["startTimestamp"] == nil {
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
	}

	if !rc.DeployerDisabled("status") {
		// Get sandbox vars for Tower job.
		if rc.SandboxAPIInUse() {
			if _, err := sandboxGet(rc, "status"); err != nil {
				log.Printf("runStatus: sandbox get error for subject=%s: %v", rc.SubjectName, err)
			}
		}

		// Launch Tower job for status check. Status action does not
		// transition current_state but sets check_status_state.
		extraSpecVars := map[string]interface{}{
			"check_status_state": "running",
		}
		if err := launchTowerJob(rc, "status", "", extraSpecVars); err != nil {
			log.Printf("runStatus: tower launch failed for subject=%s: %v", rc.SubjectName, err)
			return err
		}
		return rc.ContinueAction("5m")
	}

	return nil
}
