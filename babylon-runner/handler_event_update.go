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

	// Status check: look for check_status_request_timestamp change and
	// check_status_state being empty or "successful".
	subjectVars := getNestedMap(rc.Payload.Subject, "spec", "vars")
	if subjectVars == nil {
		return nil
	}

	previousState := getNestedMap(rc.Payload.Subject, "status", "previous_state")

	currentTimestamp := ""
	if ts, ok := subjectVars["check_status_request_timestamp"]; ok {
		if s, ok := ts.(string); ok {
			currentTimestamp = s
		}
	}

	previousTimestamp := ""
	if previousState != nil {
		if ts, ok := previousState["check_status_request_timestamp"]; ok {
			if s, ok := ts.(string); ok {
				previousTimestamp = s
			}
		}
	}

	if currentTimestamp != "" && currentTimestamp != previousTimestamp {
		checkStatusState := ""
		if css, ok := subjectVars["check_status_state"]; ok {
			if s, ok := css.(string); ok {
				checkStatusState = s
			}
		}

		if checkStatusState == "" || checkStatusState == "successful" {
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
	}

	return nil
}
