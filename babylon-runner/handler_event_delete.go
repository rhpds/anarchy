package main

import (
	"log"
)

// handleEventDelete handles the "delete" subject event. It decides whether
// to schedule a destroy action or finish immediately depending on whether
// a provision tower job exists and a destroy action is available.
func handleEventDelete(rc *RunContext) error {
	// Cancel all incomplete tower jobs before proceeding.
	cancelAllIncompleteTowerJobs(rc)

	// Check if a provision tower job exists.
	hasProvisionJob := false
	towerJobs := rc.StatusTowerJobs()
	if towerJobs != nil {
		if prov := getNestedMap(towerJobs, "provision"); prov != nil {
			if _, ok := prov["deployerJob"]; ok {
				hasProvisionJob = true
			}
		}
	}

	// Check if destroy action exists in governor.
	hasDestroyAction := false
	govActions := rc.GovernorActions()
	if govActions != nil {
		if _, ok := govActions["destroy"]; ok {
			hasDestroyAction = true
		}
	}

	deployerEnabled := !rc.DeployerDisabled("destroy")

	if hasProvisionJob && hasDestroyAction && deployerEnabled {
		return handleEventDeleteWithDestroy(rc)
	}
	return handleEventDeleteWithoutDestroy(rc)
}

// handleEventDeleteWithDestroy schedules a destroy action and updates
// the subject state accordingly.
func handleEventDeleteWithDestroy(rc *RunContext) error {
	err := rc.ScheduleAction(ScheduleActionRequest{
		Action: "destroy",
		Cancel: []string{"start", "stop", "update"},
	})
	if err != nil {
		return err
	}

	return rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-pending",
					"desired_state": "destroyed",
				},
			},
		},
	})
}

// sandboxDestroyCatchAll returns whether catch_all is enabled for destroy.
// Source: __meta__.sandbox_api.actions.destroy.catch_all (default true)
func sandboxDestroyCatchAll(rc *RunContext) bool {
	meta := rc.Meta()
	if meta == nil {
		return true
	}
	sbAPI := getNestedMap(meta, "sandbox_api", "actions", "destroy")
	if sbAPI == nil {
		return true
	}
	if v, ok := sbAPI["catch_all"].(bool); ok {
		return v
	}
	return true
}

// handleEventDeleteWithoutDestroy marks the subject as destroyed and
// finishes the action immediately.
func handleEventDeleteWithoutDestroy(rc *RunContext) error {
	// Sandbox cleanup: release placement if catch_all is enabled.
	if rc.SandboxAPIInUse() && sandboxDestroyCatchAll(rc) && rc.UUID() != "" {
		if err := sandboxCleanup(rc); err != nil {
			log.Printf("handleEventDeleteWithoutDestroy: sandbox cleanup error: %v", err)
		}
	}

	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "destroy-complete",
					"desired_state": "destroyed",
				},
			},
		},
	})
	if err != nil {
		return err
	}

	rc.DeleteSubject(true)
	rc.FinishAction("successful")
	return nil
}
