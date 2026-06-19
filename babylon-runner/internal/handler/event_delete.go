package handler

import (
	"log/slog"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// handleEventDelete handles the "delete" subject event. It decides whether
// to schedule a destroy action or finish immediately depending on whether
// a provision tower job exists and a destroy action is available.
func handleEventDelete(rc *runner.RunContext) error {
	slog.Info("handling delete event", "subject", rc.SubjectName())

	// Cancel all incomplete tower jobs before proceeding.
	cancelAllIncompleteTowerJobs(rc)

	// Check if a provision tower job exists.
	hasProvisionJob := false
	towerJobs := rc.StatusTowerJobs()
	if towerJobs != nil {
		if prov := types.GetNestedMap(towerJobs, "provision"); prov != nil {
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
func handleEventDeleteWithDestroy(rc *runner.RunContext) error {
	err := rc.ScheduleAction(types.ScheduleActionRequest{
		Action: "destroy",
		Cancel: []string{"start", "stop", "update"},
	})
	if err != nil {
		return err
	}

	return rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Spec: &types.PatchSpec{
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
func sandboxDestroyCatchAll(rc *runner.RunContext) bool {
	meta := rc.Meta()
	if meta == nil {
		return true
	}
	if meta.SandboxAPI == nil {
		return true
	}
	actions, _ := meta.SandboxAPI["actions"].(map[string]interface{})
	if actions == nil {
		return true
	}
	destroy, _ := actions["destroy"].(map[string]interface{})
	if destroy == nil {
		return true
	}
	if v, ok := destroy["catch_all"].(bool); ok {
		return v
	}
	return true
}

// handleEventDeleteWithoutDestroy marks the subject as destroyed and
// finishes the action immediately.
func handleEventDeleteWithoutDestroy(rc *runner.RunContext) error {
	// Sandbox cleanup: release placement if catch_all is enabled.
	if rc.SandboxAPIInUse() && sandboxDestroyCatchAll(rc) && rc.UUID() != "" {
		if err := sandboxCleanup(rc); err != nil {
			slog.Error("handleEventDeleteWithoutDestroy: sandbox cleanup error", "error", err)
		}
	}

	err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Spec: &types.PatchSpec{
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
