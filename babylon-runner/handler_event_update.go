package main

import (
	"reflect"
)

// handleEventUpdate handles the "update" subject event. It determines
// whether an action (update/start/stop) is needed based on state and
// job_vars changes, and checks for status requests.
func handleEventUpdate(rc *RunContext) error {
	currentState := rc.CurrentState()
	desiredState := rc.DesiredState()

	currentJobVars := rc.JobVars()
	if currentJobVars == nil {
		currentJobVars = make(map[string]interface{})
	}

	// Previous job_vars come from subject.status.previous_state.job_vars.
	// Fall back to current job_vars if not present.
	previousJobVars := getNestedMap(rc.Payload.Subject, "status", "previous_state", "job_vars")
	if previousJobVars == nil {
		previousJobVars = currentJobVars
	}

	// Determine which action to take.
	var action string
	if !reflect.DeepEqual(currentJobVars, previousJobVars) {
		action = "update"
	} else if desiredState == "started" && currentState == "stopped" {
		action = "start"
	} else if desiredState == "stopped" && currentState == "started" {
		action = "stop"
	}

	// If an action is determined and it exists in the governor actions,
	// schedule it.
	if action != "" {
		govActions := rc.GovernorActions()
		if govActions != nil {
			if _, ok := govActions[action]; ok {
				pendingState := action + "-pending"
				err := rc.SubjectUpdate(SubjectPatch{
					Patch: PatchBody{
						Metadata: &PatchMetadata{
							Labels: map[string]string{
								"state": pendingState,
							},
						},
						Spec: &PatchSpec{
							Vars: map[string]interface{}{
								"current_state": pendingState,
							},
						},
						SkipUpdateProcessing: true,
					},
				})
				if err != nil {
					return err
				}

				err = rc.ScheduleAction(ScheduleActionRequest{
					Action: action,
					Cancel: []string{"start", "stop"},
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// Status check scheduling (only if governor has a status action).
	govActions := rc.GovernorActions()
	if govActions == nil {
		return nil
	}
	if _, hasStatus := govActions["status"]; !hasStatus {
		return nil
	}

	subjectVars := getNestedMap(rc.Payload.Subject, "spec", "vars")
	if subjectVars == nil {
		return nil
	}

	previousState := getNestedMap(rc.Payload.Subject, "status", "previous_state")

	checkStatusState, _ := subjectVars["check_status_state"].(string)
	currentTimestamp, _ := subjectVars["check_status_request_timestamp"].(string)
	previousTimestamp := ""
	if previousState != nil {
		previousTimestamp, _ = previousState["check_status_request_timestamp"].(string)
	}

	// Two paths to schedule status (matching Ansible):
	// 1. Legacy: check_status_state set to "pending" directly (no timestamp).
	// 2. Timestamp: new request_timestamp differs from last, and state is empty or "successful".
	scheduleStatus := false
	if checkStatusState == "pending" && currentTimestamp == "" {
		// Legacy path.
		scheduleStatus = true
	} else if currentTimestamp != "" && currentTimestamp != previousTimestamp &&
		(checkStatusState == "" || checkStatusState == "successful") {
		scheduleStatus = true
	}

	if scheduleStatus {
		err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Spec: &PatchSpec{
					Vars: map[string]interface{}{
						"check_status_state": "pending",
					},
				},
				SkipUpdateProcessing: true,
			},
		})
		if err != nil {
			return err
		}

		err = rc.ScheduleAction(ScheduleActionRequest{
			Action: "status",
		})
		if err != nil {
			return err
		}
	}

	return nil
}
