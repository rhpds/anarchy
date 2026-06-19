package handler

import (
	"log/slog"
	"net/http"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// handleProvision routes a provision action based on the current state.
func handleProvision(rc *runner.RunContext) error {
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
		slog.Warn("handleProvision: unhandled state", "state", rc.CurrentState(), "subject", rc.SubjectName())
		return nil
	}
}

// runProvision initiates the provisioning workflow.
func runProvision(rc *runner.RunContext) error {
	// Set startTimestamp if not already set.
	actions := rc.StatusActions()
	provision := types.GetNestedMap(actions, "provision")
	if provision == nil || provision["startTimestamp"] == nil {
		ts := types.NowUTC()
		if err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
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
	var sandboxResult *SandboxResult
	if rc.SandboxAPIInUse() {
		var err error
		sandboxResult, err = sandboxGet(rc, "provision")
		if err != nil {
			slog.Error("runProvision: sandbox get error", "subject", rc.SubjectName(), "error", err)
			return handleProvisionError(rc)
		}
		switch sandboxResult.Status {
		case "error":
			return handleProvisionError(rc)
		case "queued":
			// Update state to provision-queued and poll.
			if err := rc.SubjectUpdate(types.SubjectPatch{
				Patch: types.PatchBody{
					Metadata: &types.PatchMetadata{
						Labels: map[string]string{"state": "provision-queued"},
					},
					Spec: &types.PatchSpec{
						Vars: map[string]interface{}{
							"current_state": "provision-queued",
						},
					},
					Status: map[string]interface{}{
						"sandboxAPIJobs": map[string]interface{}{
							"provision": map[string]interface{}{
								"placementStatus":    "queued",
								"lastCheckTimestamp": types.NowUTC(),
							},
						},
					},
					SkipUpdateProcessing: true,
				},
			}); err != nil {
				return err
			}
			rc.ContinueAction("30s")
			return nil
		}
	}

	if !rc.DeployerDisabled("provision") {
		// Launch Tower job for provisioning.
		var dynamicJobVars map[string]interface{}
		if sandboxResult != nil {
			dynamicJobVars = sandboxResult.DynamicVars
		}
		if err := launchTowerJob(rc, "provision", "provisioning", nil, dynamicJobVars); err != nil {
			slog.Error("runProvision: tower launch failed", "subject", rc.SubjectName(), "error", err)
			return handleProvisionError(rc)
		}
		rc.ContinueAction("5m")
		return nil
	}

	// Deployer disabled and sandbox API in use: mark as started immediately.
	if rc.SandboxAPIInUse() {
		ts := types.NowUTC()
		specVars := map[string]interface{}{
			"current_state": "started",
			"healthy":       true,
		}
		// Set provision_data from sandbox vars (matching Ansible).
		if sandboxResult != nil && len(sandboxResult.SubjectVars) > 0 {
			specVars["provision_data"] = sandboxResult.SubjectVars
		}
		if err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
				Metadata: &types.PatchMetadata{
					Labels: map[string]string{
						"state": "started",
					},
				},
				Spec: &types.PatchSpec{
					Vars: specVars,
				},
				Status: map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{
							"completeTimestamp": ts,
							"state":            "successful",
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
func handleProvisionComplete(rc *runner.RunContext, provisionData, messageBody, messages interface{}) error {
	ts := types.NowUTC()

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

	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{
					"state": "started",
				},
			},
			Spec: &types.PatchSpec{
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
func handleProvisionError(rc *runner.RunContext) error {
	ts := types.NowUTC()

	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{
					"state": "provision-error",
				},
			},
			Spec: &types.PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-error",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"state":            "error",
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
func handleProvisionFailed(rc *runner.RunContext) error {
	ts := types.NowUTC()

	jobVars := rc.JobVars()
	vars := map[string]interface{}{
		"current_state": "provision-failed",
		"healthy":       false,
	}
	// Merge existing job_vars and set forensics flag.
	jv := make(map[string]interface{})
	if jobVars != nil {
		types.MergeMap(jv, jobVars)
	}
	jv["agnosticd_collect_forensics"] = true
	vars["job_vars"] = jv

	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{
					"state": "provision-failed",
				},
			},
			Spec: &types.PatchSpec{
				Vars: vars,
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"state":            "failed",
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
func handleProvisionCanceled(rc *runner.RunContext) error {
	ts := types.NowUTC()

	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{
					"state": "provision-canceled",
				},
			},
			Spec: &types.PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-canceled",
					"healthy":       false,
				},
			},
			Status: map[string]interface{}{
				"actions": map[string]interface{}{
					"provision": map[string]interface{}{
						"completeTimestamp": ts,
						"state":            "canceled",
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
func checkProvisionQueue(rc *runner.RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		return handleProvisionError(rc)
	}

	accessToken, err := sandboxLogin(rc)
	if err != nil {
		slog.Error("checkProvisionQueue: login failed", "subject", rc.SubjectName(), "error", err)
		return handleProvisionError(rc)
	}

	client := getSandboxClient(rc)
	placement, statusCode, err := client.GetPlacement(accessToken, uuid)
	if err != nil {
		slog.Error("checkProvisionQueue: get placement failed", "subject", rc.SubjectName(), "error", err)
		return handleProvisionError(rc)
	}

	// 404 or error status -> provision error.
	if statusCode == http.StatusNotFound {
		slog.Warn("checkProvisionQueue: placement not found", "subject", rc.SubjectName())
		return handleProvisionError(rc)
	}

	placementStatus, _ := placement["status"].(string)

	switch placementStatus {
	case "error":
		slog.Error("checkProvisionQueue: placement error", "subject", rc.SubjectName())
		return handleProvisionError(rc)

	case "queued":
		// Still queued, update status and continue polling.
		if err := rc.SubjectUpdate(types.SubjectPatch{
			Patch: types.PatchBody{
				Status: map[string]interface{}{
					"sandboxAPIJobs": map[string]interface{}{
						"provision": map[string]interface{}{
							"placementStatus":    "queued",
							"lastCheckTimestamp": types.NowUTC(),
						},
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
		rc.ContinueAction("30s")
		return nil

	default:
		// Success: extract vars (no creds for subject), update subject, and restart provision.
		dynamicVars := extractSandboxVars(placement, false)
		labels := extractSandboxLabels(placement)
		patch := types.PatchBody{SkipUpdateProcessing: true}

		// Merge labels.
		allLabels := make(map[string]string)
		for k, v := range labels {
			allLabels[k] = v
		}
		allLabels["state"] = "provision-pending"
		patch.Metadata = &types.PatchMetadata{Labels: allLabels}

		// Merge dynamic vars into job_vars.
		specVars := map[string]interface{}{
			"current_state": "provision-pending",
		}
		if len(dynamicVars) > 0 {
			jv := rc.JobVars()
			if jv == nil {
				jv = make(map[string]interface{})
			}
			types.MergeMap(jv, dynamicVars)
			specVars["job_vars"] = jv
		}
		patch.Spec = &types.PatchSpec{Vars: specVars}

		patch.Status = map[string]interface{}{
			"sandboxAPIJobs": map[string]interface{}{
				"provision": map[string]interface{}{
					"placementStatus":   placementStatus,
					"dequeuedTimestamp": types.NowUTC(),
				},
			},
		}

		if err := rc.SubjectUpdate(types.SubjectPatch{Patch: patch}); err != nil {
			return err
		}

		// Restart the provision flow now that sandbox is ready.
		return runProvision(rc)
	}
}
