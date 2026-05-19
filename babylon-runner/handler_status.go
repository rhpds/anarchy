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
		// Sandbox API integration (TODO).
		if rc.SandboxAPIInUse() {
			log.Printf("runStatus: sandbox get needed for subject=%s (TODO)", rc.SubjectName)
		}

		// Tower job launch needed (TODO).
		log.Printf("runStatus: tower job launch needed for subject=%s (TODO)", rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	return nil
}
