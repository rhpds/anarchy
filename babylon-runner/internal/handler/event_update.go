package handler

import (
	"reflect"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// handleEventUpdate handles the "update" subject event. It determines
// whether an action (update/start/stop) is needed based on state and
// job_vars changes, and checks for status requests.
func handleEventUpdate(rc *runner.RunContext) error {
	currentState := rc.CurrentState()
	desiredState := rc.DesiredState()

	currentJobVars := rc.JobVars()
	if currentJobVars == nil {
		currentJobVars = make(map[string]interface{})
	}

	// Previous job_vars come from subject.status.previous_state.job_vars.
	// Fall back to current job_vars if not present.
	previousJobVars := types.GetNestedMap(rc.Payload.Subject.Status.PreviousState, "job_vars")
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
				err := rc.SubjectUpdate(types.SubjectPatch{
					Patch: types.PatchBody{
						Metadata: &types.PatchMetadata{
							Labels: map[string]string{
								"state": pendingState,
							},
						},
						Spec: &types.PatchSpec{
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

				err = rc.ScheduleAction(types.ScheduleActionRequest{
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

	previousState := rc.Payload.Subject.Status.PreviousState

	checkStatusState := rc.CheckStatusState()
	currentTimestamp := rc.Payload.Subject.Spec.Vars.GetString("check_status_request_timestamp")
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
		err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
				Spec: &types.PatchSpec{
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

		err = rc.ScheduleAction(types.ScheduleActionRequest{
			Action: "status",
		})
		if err != nil {
			return err
		}
	}

	return nil
}
