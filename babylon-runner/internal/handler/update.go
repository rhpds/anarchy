package handler

import (
	"log/slog"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
)

// handleUpdate routes an update action based on the current state.
func handleUpdate(rc *runner.RunContext) error {
	currentState := rc.CurrentState()

	if currentState != "updating" {
		return runUpdate(rc)
	}

	return checkDeployerJob(rc, "update")
}

// runUpdate initiates the update workflow.
func runUpdate(rc *runner.RunContext) error {
	// Get sandbox vars for Tower job.
	var dynamicJobVars map[string]interface{}
	if rc.SandboxAPIInUse() {
		result, err := sandboxGet(rc, "update")
		if err != nil {
			slog.Error("runUpdate: sandbox get error", "subject", rc.SubjectName(), "error", err)
		} else if result != nil {
			dynamicJobVars = result.DynamicVars
		}
	}

	// Launch Tower job for update.
	if err := launchTowerJob(rc, "update", "updating", nil, dynamicJobVars); err != nil {
		slog.Error("runUpdate: tower launch failed", "subject", rc.SubjectName(), "error", err)
		return err
	}
	rc.ContinueAction("5m")
	return nil
}
