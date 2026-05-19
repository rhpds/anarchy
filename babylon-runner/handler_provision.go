package main

import (
	"log"
	"net/http"
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

	// Sandbox API integration: get or book placement.
	var dynamicJobVars map[string]interface{}
	if rc.SandboxAPIInUse() {
		result, err := sandboxGet(rc, "provision")
		if err != nil {
			log.Printf("runProvision: sandbox get error for subject=%s: %v", rc.SubjectName, err)
			return handleProvisionError(rc)
		}
		switch result.Status {
		case "error":
			return handleProvisionError(rc)
		case "queued":
			// Update state to provision-queued and poll.
			if err := rc.SubjectUpdate(SubjectPatch{
				Patch: PatchBody{
					Metadata: &PatchMetadata{
						Labels: map[string]string{"state": "provision-queued"},
					},
					Spec: &PatchSpec{
						Vars: map[string]interface{}{
							"current_state": "provision-queued",
						},
					},
					Status: map[string]interface{}{
						"sandboxAPIJobs": map[string]interface{}{
							"provision": map[string]interface{}{
								"placementStatus":    "queued",
								"lastCheckTimestamp": nowUTC(),
							},
						},
					},
					SkipUpdateProcessing: true,
				},
			}); err != nil {
				return err
			}
			return rc.ContinueAction("30s")
		}
		// Success: capture dynamic vars (with creds) for Tower.
		dynamicJobVars = result.DynamicVars
	}

	if !rc.DeployerDisabled("provision") {
		// Launch Tower job for provisioning.
		if err := launchTowerJob(rc, "provision", "provisioning", nil, dynamicJobVars); err != nil {
			log.Printf("runProvision: tower launch failed for subject=%s: %v", rc.SubjectName, err)
			return handleProvisionError(rc)
		}
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
		vars["provision_message_body"] = messageBody
	}
	if messages != nil {
		vars["provision_messages"] = messages
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
						"state":            "successful",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":        "successful",
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

// handleProvisionCanceled marks a provision as canceled.
func handleProvisionCanceled(rc *RunContext) error {
	ts := nowUTC()

	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "provision-canceled",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-canceled",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"status":           "canceled",
					},
				},
				"towerJobs": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"jobStatus":        "canceled",
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	rc.FinishAction("canceled")
	return nil
}

// checkProvisionQueue checks whether a queued provision can proceed
// by polling the sandbox API placement status.
func checkProvisionQueue(rc *RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		return handleProvisionError(rc)
	}

	accessToken, err := sandboxLogin(rc)
	if err != nil {
		log.Printf("checkProvisionQueue: login failed for subject=%s: %v", rc.SubjectName, err)
		return handleProvisionError(rc)
	}

	client := getSandboxClient(rc)
	placement, statusCode, err := client.GetPlacement(accessToken, uuid)
	if err != nil {
		log.Printf("checkProvisionQueue: get placement failed for subject=%s: %v", rc.SubjectName, err)
		return handleProvisionError(rc)
	}

	// 404 or error status -> provision error.
	if statusCode == http.StatusNotFound {
		log.Printf("checkProvisionQueue: placement not found for subject=%s", rc.SubjectName)
		return handleProvisionError(rc)
	}

	placementStatus, _ := placement["status"].(string)

	switch placementStatus {
	case "error":
		log.Printf("checkProvisionQueue: placement error for subject=%s", rc.SubjectName)
		return handleProvisionError(rc)

	case "queued":
		// Still queued, update status and continue polling.
		if err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Status: map[string]interface{}{
					"sandboxAPIJobs": map[string]interface{}{
						"provision": map[string]interface{}{
							"placementStatus":    "queued",
							"lastCheckTimestamp": nowUTC(),
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
		return rc.ContinueAction("30s")

	default:
		// Success: extract vars (no creds for subject), update subject, and restart provision.
		dynamicVars := extractSandboxVars(placement, false)
		labels := extractSandboxLabels(placement)
		patch := PatchBody{SkipUpdateProcessing: true}

		// Merge labels.
		allLabels := make(map[string]string)
		for k, v := range labels {
			allLabels[k] = v
		}
		allLabels["state"] = "provision-pending"
		patch.Metadata = &PatchMetadata{Labels: allLabels}

		// Merge dynamic vars into job_vars.
		specVars := map[string]interface{}{
			"current_state": "provision-pending",
		}
		if len(dynamicVars) > 0 {
			jv := rc.JobVars()
			if jv == nil {
				jv = make(map[string]interface{})
			}
			mergeMap(jv, dynamicVars)
			specVars["job_vars"] = jv
		}
		patch.Spec = &PatchSpec{Vars: specVars}

		patch.Status = map[string]interface{}{
			"sandboxAPIJobs": map[string]interface{}{
				"provision": map[string]interface{}{
					"placementStatus":   placementStatus,
					"dequeuedTimestamp": nowUTC(),
				},
			},
		}

		if err := rc.SubjectUpdate(SubjectPatch{Patch: patch}); err != nil {
			return err
		}

		// Restart the provision flow now that sandbox is ready.
		return runProvision(rc)
	}
}
