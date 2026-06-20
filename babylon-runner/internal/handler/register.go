package handler

import (
	"log/slog"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
)

// Register returns the handler map for all supported run types.
func Register() map[string]runner.HandlerFunc {
	// Callback handlers: immediately reschedule the action so that
	// the next action run re-enters the main handler (e.g. handleStart)
	// which calls checkDeployerJob.
	callbackContinue := func(rc *runner.RunContext) error {
		slog.Info("callback received, scheduling immediate action re-check",
			"action", rc.ActionName(), "subject", rc.SubjectName())
		rc.ContinueAction("0s")
		return nil
	}

	return map[string]runner.HandlerFunc{
		// Event handlers
		"event:create": handleEventCreate,
		"event:update": handleEventUpdate,
		"event:delete": handleEventDelete,

		// Action handlers
		"action:provision": handleProvision,
		"action:destroy":   handleDestroy,
		"action:start":     handleStart,
		"action:stop":      handleStop,
		"action:status":    handleStatus,
		"action:update":    handleUpdate,

		// Callback handlers
		"action:provision:complete": callbackContinue,
		"action:destroy:complete":   callbackContinue,
		"action:start:complete":     callbackContinue,
		"action:stop:complete":      callbackContinue,
		"action:status:complete":    callbackContinue,
		"action:update:complete":    callbackContinue,
	}
}
