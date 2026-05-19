package main

import (
	"log"
)

// handleStop routes a stop action based on the current state.
func handleStop(rc *RunContext) error {
	if rc.CurrentState() != "stopping" {
		return runStop(rc)
	}
	if !rc.DeployerDisabled("stop") {
		return checkDeployerJob(rc, "stop")
	}
	return nil
}

// runStop initiates the stop workflow.
func runStop(rc *RunContext) error {
	// Set startTimestamp.
	ts := nowUTC()
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"startTimestamp": ts,
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	// If deployer not disabled: launch Tower job.
	if !rc.DeployerDisabled("stop") {
		if rc.SandboxAPIInUse() {
			log.Printf("runStop: sandbox_get needed for subject=%s (TODO)", rc.SubjectName)
		}
		log.Printf("runStop: tower job launch needed for subject=%s (TODO)", rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	// Deployer disabled and sandbox API in use: perform sandbox API stop.
	if rc.SandboxAPIInUse() && sandboxActionEnabled(rc, "stop") {
		log.Printf("runStop: sandbox_api_stop needed for subject=%s (TODO)", rc.SubjectName)
		ts := nowUTC()
		if err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Metadata: &PatchMetadata{
					Labels: map[string]string{
						"state": "stopped",
					},
				},
				Spec: &PatchSpec{
					Vars: map[string]interface{}{
						"current_state": "stopped",
					},
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"stop": map[string]interface{}{
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
	}

	return nil
}

// handleStopComplete finalizes a successful stop.
func handleStopComplete(rc *RunContext) error {
	ts := nowUTC()

	// Update tower jobs status.
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": ts,
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"stop": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":        "successful",
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	// Sandbox API stop if enabled.
	if rc.SandboxAPIInUse() && sandboxActionEnabled(rc, "stop") {
		log.Printf("handleStopComplete: sandbox_api_stop needed for subject=%s (TODO)", rc.SubjectName)
	}

	// Update state to stopped.
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "stopped",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "stopped",
				},
			},
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("successful")
	return nil
}
