package main

import (
	"fmt"
	"log"
)

// checkDeployerJob polls Tower job status and routes to completion/error handlers.
func checkDeployerJob(rc *RunContext, action string) error {
	// Get tower job info from status.towerJobs.{action}
	towerJobs := rc.StatusTowerJobs()
	actionJob := getNestedMap(towerJobs, action)
	if actionJob == nil {
		log.Printf("checkDeployerJob: no tower job found for action=%s subject=%s", action, rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	// Extract deployerJob (float64 → int).
	deployerJobFloat, ok := actionJob["deployerJob"].(float64)
	if !ok {
		log.Printf("checkDeployerJob: deployerJob not found or not a number for action=%s subject=%s", action, rc.SubjectName)
		return rc.ContinueAction("5m")
	}
	deployerJob := int(deployerJobFloat)

	// Create TowerClient using shared helper.
	tc, _, err := getTowerClientForAction(rc)
	if err != nil {
		log.Printf("checkDeployerJob: failed to get tower client for action=%s subject=%s: %v", action, rc.SubjectName, err)
		return rc.ContinueAction("5m")
	}

	// Create OAuth token
	token, tokenID, err := tc.CreateOAuthToken()
	if err != nil {
		log.Printf("checkDeployerJob: failed to create oauth token for action=%s subject=%s: %v", action, rc.SubjectName, err)
		return rc.ContinueAction("5m")
	}
	defer func() {
		if delErr := tc.DeleteOAuthToken(tokenID); delErr != nil {
			log.Printf("checkDeployerJob: failed to delete oauth token %d: %v", tokenID, delErr)
		}
	}()

	// Get job status
	jobStatus, err := tc.GetJobStatus(token, deployerJob)
	if err != nil {
		log.Printf("checkDeployerJob: failed to get job status for action=%s job=%d subject=%s: %v", action, deployerJob, rc.SubjectName, err)
		return rc.ContinueAction("5m")
	}

	// Extract status field
	status, ok := jobStatus["status"].(string)
	if !ok {
		log.Printf("checkDeployerJob: status field not found or not a string for action=%s job=%d subject=%s", action, deployerJob, rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	// Route based on status
	switch status {
	case "canceled", "error", "failed":
		return handleDeployerJobFailure(rc, action, status)
	case "successful":
		return handleDeployerJobSuccess(rc, action, jobStatus)
	default:
		// Job still running (pending, waiting, running)
		log.Printf("checkDeployerJob: job %d still running (status=%s) for action=%s subject=%s", deployerJob, status, action, rc.SubjectName)
		return rc.ContinueAction("5m")
	}
}

// actionRetryIntervals is the list of retry intervals for failed actions.
var actionRetryIntervals = []string{
	"1m", "5m", "10m", "30m", "1h", "2h", "4h", "8h", "16h", "1d",
}

// actionRetryInterval returns the retry interval for the given retry count.
func actionRetryInterval(retryCount int) string {
	if retryCount < len(actionRetryIntervals) {
		return actionRetryIntervals[retryCount]
	}
	return actionRetryIntervals[len(actionRetryIntervals)-1]
}

// continueWithRetry continues the action with an incremented retry count.
func continueWithRetry(rc *RunContext) error {
	count := rc.ActionRetryCount()
	interval := actionRetryInterval(count)
	return rc.ContinueActionWithVars(interval, map[string]interface{}{
		"action_retry_count": count + 1,
	})
}

// handleDeployerJobFailure routes to action-specific failure handlers.
func handleDeployerJobFailure(rc *RunContext, action, status string) error {
	switch action {
	case "provision":
		if status == "failed" {
			return handleProvisionFailed(rc)
		}
		return handleProvisionError(rc)
	case "destroy":
		return handleDestroyFailure(rc, status)
	case "start":
		return handleStartFailure(rc, status)
	case "stop":
		return handleStopFailure(rc, status)
	case "status":
		return handleStatusFailure(rc, status)
	case "update":
		return handleUpdateFailure(rc, status)
	default:
		log.Printf("handleDeployerJobFailure: unknown action %s for subject=%s", action, rc.SubjectName)
		rc.FinishAction(status)
		return nil
	}
}

// handleDeployerJobSuccess routes to action-specific completion handlers.
func handleDeployerJobSuccess(rc *RunContext, action string, jobStatus map[string]interface{}) error {
	switch action {
	case "provision":
		data, messageBody, messages := extractProvisionData(jobStatus)
		return handleProvisionComplete(rc, data, messageBody, messages)
	case "destroy":
		return handleDestroyComplete(rc)
	case "start":
		return handleStartComplete(rc)
	case "stop":
		return handleStopComplete(rc)
	case "status":
		return handleStatusComplete(rc)
	case "update":
		return handleUpdateComplete(rc)
	default:
		log.Printf("handleDeployerJobSuccess: unknown action %s for subject=%s", action, rc.SubjectName)
		return nil
	}
}

// handleDestroyFailure handles destroy job error/failure.
// Always retries with backoff — catch_all in handleDestroy handles final cleanup.
func handleDestroyFailure(rc *RunContext, status string) error {
	ts := nowUTC()
	state := fmt.Sprintf("destroy-%s", status)

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": state},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": state,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             status,
					},
				},
				"towerJobs": map[string]interface{}{
					"destroy": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         status,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	return continueWithRetry(rc)
}

// handleStartFailure handles start job error/failure.
// Retries if desired_state is "started", schedules stop if "stopped".
func handleStartFailure(rc *RunContext, status string) error {
	ts := nowUTC()
	state := fmt.Sprintf("start-%s", status)

	specVars := map[string]interface{}{
		"current_state": state,
		"healthy":       false,
	}
	// On failed, set agnosticd_collect_forensics.
	if status == "failed" {
		jv := rc.JobVars()
		if jv == nil {
			jv = make(map[string]interface{})
		}
		jv["agnosticd_collect_forensics"] = true
		specVars["job_vars"] = jv
	}

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": state},
			},
			Spec: &PatchSpec{
				Vars: specVars,
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"start": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             status,
					},
				},
				"towerJobs": map[string]interface{}{
					"start": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         status,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	desiredState := rc.DesiredState()
	if desiredState == "started" && !rc.IsBeingDeleted() {
		return continueWithRetry(rc)
	}
	if desiredState == "stopped" && !rc.IsBeingDeleted() {
		return rc.ScheduleAction(ScheduleActionRequest{
			Action: "stop",
			Cancel: []string{"start", "stop"},
		})
	}
	rc.FinishAction(status)
	return nil
}

// handleStopFailure handles stop job error/failure.
// Calls sandbox stop first, then retries if desired_state is "stopped",
// schedules start if "started".
func handleStopFailure(rc *RunContext, status string) error {
	// Call sandbox API stop to save costs even if deployer failed.
	if rc.SandboxAPIInUse() {
		meta := rc.Meta()
		stopEnabled := true
		if meta != nil {
			sbStop := getNestedMap(meta, "sandbox_api", "actions", "stop")
			if sbStop != nil {
				if v, ok := sbStop["enable"].(bool); ok {
					stopEnabled = v
				}
			}
		}
		if stopEnabled {
			if err := sandboxStop(rc); err != nil {
				log.Printf("handleStopFailure: sandbox stop error for subject=%s: %v", rc.SubjectName, err)
			}
		}
	}

	ts := nowUTC()
	state := fmt.Sprintf("stop-%s", status)

	specVars := map[string]interface{}{
		"current_state": state,
		"healthy":       false,
	}
	// On failed, set agnosticd_collect_forensics.
	if status == "failed" {
		jv := rc.JobVars()
		if jv == nil {
			jv = make(map[string]interface{})
		}
		jv["agnosticd_collect_forensics"] = true
		specVars["job_vars"] = jv
	}

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": state},
			},
			Spec: &PatchSpec{
				Vars: specVars,
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             status,
					},
				},
				"towerJobs": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         status,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	desiredState := rc.DesiredState()
	if desiredState == "stopped" && !rc.IsBeingDeleted() {
		return continueWithRetry(rc)
	}
	if desiredState == "started" && !rc.IsBeingDeleted() {
		return rc.ScheduleAction(ScheduleActionRequest{
			Action: "start",
			Cancel: []string{"start", "stop"},
		})
	}
	rc.FinishAction(status)
	return nil
}

// handleStatusFailure handles status check error/failure.
// Sets check_status_state (not current_state), finishes immediately.
func handleStatusFailure(rc *RunContext, status string) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"check_status_state": status,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"status": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             status,
					},
				},
				"towerJobs": map[string]interface{}{
					"status": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         status,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	rc.FinishAction(status)
	return nil
}

// handleUpdateFailure handles update job error/failure.
// Always retries unless the subject is being deleted.
func handleUpdateFailure(rc *RunContext, status string) error {
	ts := nowUTC()
	state := fmt.Sprintf("update-%s", status)

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": state},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": state,
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"update": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             status,
					},
				},
				"towerJobs": map[string]interface{}{
					"update": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":         status,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	if !rc.IsBeingDeleted() {
		return continueWithRetry(rc)
	}
	rc.FinishAction(status)
	return nil
}

// handleStatusComplete finalizes a successful status check.
func handleStatusComplete(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"check_status_state": "successful",
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"status": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             "successful",
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

// handleUpdateComplete finalizes a successful update.
func handleUpdateComplete(rc *RunContext) error {
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
					"update": map[string]interface{}{
						"completeTimestamp": ts,
						"state":             "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"update": map[string]interface{}{
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

	rc.FinishAction("successful")
	return nil
}
