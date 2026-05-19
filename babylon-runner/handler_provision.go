package main

import (
	"log"
)

// handleProvision routes a provision action based on the current state.
func handleProvision(rc *RunContext) error {
	switch rc.CurrentState() {
	case "provision-pending":
		return runProvision(rc)
	case "provision-queued":
		return checkProvisionQueue(rc)
	case "provisioning":
		if !rc.DeployerDisabled("provision") {
			return checkDeployerJob(rc, "provision")
		}
		return nil
	default:
		log.Printf("handleProvision: unhandled state %q for subject=%s", rc.CurrentState(), rc.SubjectName)
		return nil
	}
}

// runProvision initiates the provisioning workflow.
func runProvision(rc *RunContext) error {
	// Set startTimestamp if not already set.
	actions := rc.StatusActions()
	provision := getNestedMap(actions, "provision")
	if provision == nil || provision["startTimestamp"] == nil {
		ts := nowUTC()
		if err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{
							"startTimestamp": ts,
						},
					},
				},
			},
		}); err != nil {
			return err
		}
	}

	// Sandbox API integration (TODO).
	if rc.SandboxAPIInUse() {
		log.Printf("runProvision: sandbox get needed for subject=%s (TODO)", rc.SubjectName)
	}

	if !rc.DeployerDisabled("provision") {
		// Tower job launch needed (TODO).
		log.Printf("runProvision: tower job launch needed for subject=%s (TODO)", rc.SubjectName)
		return rc.ContinueAction("5m")
	}

	// Deployer disabled and sandbox API in use: mark as started immediately.
	if rc.SandboxAPIInUse() {
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
						"healthy":       true,
					},
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{
							"completeTimestamp": ts,
							"status":           "successful",
						},
					},
				},
			},
		}); err != nil {
			return err
		}
		rc.FinishAction("successful")
	}

	return nil
}

// handleProvisionComplete finalizes a successful provision.
func handleProvisionComplete(rc *RunContext, provisionData, messageBody, messages interface{}) error {
	ts := nowUTC()

	vars := map[string]interface{}{
		"current_state": "started",
		"healthy":       true,
	}
	if provisionData != nil {
		vars["provision_data"] = provisionData
	}
	if messageBody != nil {
		vars["message_body"] = messageBody
	}
	if messages != nil {
		vars["messages"] = messages
	}

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "started",
				},
			},
			Spec: &PatchSpec{
				Vars: vars,
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"status":           "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
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

// handleProvisionError marks a provision as errored (transient failure).
func handleProvisionError(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "provision-error",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-error",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"status":           "error",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("error")
	return nil
}

// handleProvisionFailed marks a provision as permanently failed.
func handleProvisionFailed(rc *RunContext) error {
	ts := nowUTC()

	jobVars := rc.JobVars()
	vars := map[string]interface{}{
		"current_state": "provision-failed",
		"healthy":       false,
	}
	// Merge existing job_vars and set forensics flag.
	jv := make(map[string]interface{})
	if jobVars != nil {
		mergeMap(jv, jobVars)
	}
	jv["agnosticd_collect_forensics"] = true
	vars["job_vars"] = jv

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "provision-failed",
				},
			},
			Spec: &PatchSpec{
				Vars: vars,
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"status":           "failed",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
					},
				},
			},
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("failed")
	return nil
}

// checkProvisionQueue checks whether a queued provision can proceed.
// TODO: implement with sandbox API queue checking.
func checkProvisionQueue(rc *RunContext) error {
	log.Printf("checkProvisionQueue: waiting for provision queue for subject=%s (TODO)", rc.SubjectName)
	return rc.ContinueAction("30s")
}
