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

// handleDeployerJobFailure routes to action-specific failure handlers.
func handleDeployerJobFailure(rc *RunContext, action, status string) error {
	switch action {
	case "provision":
		if status == "failed" {
			return handleProvisionFailed(rc)
		}
		return handleProvisionError(rc)
	case "destroy":
		// Create a destroy-specific error update
		ts := nowUTC()
		state := fmt.Sprintf("destroy-%s", status)
		if err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Metadata: &PatchMetadata{
					Labels: map[string]string{
						"state": state,
					},
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
							"status":            status,
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
		rc.FinishAction(status)
		return nil
	default:
		// start, stop, status, update
		return handleGenericActionFailure(rc, action, status)
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

// handleGenericActionFailure is a generic failure handler for start/stop/status/update.
func handleGenericActionFailure(rc *RunContext, action, status string) error {
	ts := nowUTC()
	state := fmt.Sprintf("%s-%s", action, status)

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": state,
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": state,
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					action: map[string]interface{}{
						"completeTimestamp": ts,
						"state":             status,
					},
				},
				"towerJobs": map[string]interface{}{
					action: map[string]interface{}{
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
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("successful")
	return nil
}
