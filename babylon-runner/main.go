package main

import (
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	cfg, err := configFromEnv()
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}

	slog.Info("babylon-runner starting",
		"name", cfg.RunnerName, "pod", cfg.PodName, "url", cfg.AnarchyURL,
		"poll", cfg.PollingInterval, "timeout", cfg.RequestTimeout)

	runner := NewRunner(cfg)
	registerHandlers(runner)
	runner.Run()
}

func registerHandlers(r *Runner) {
	// Event handlers
	r.handlers["event:create"] = handleEventCreate
	r.handlers["event:update"] = handleEventUpdate
	r.handlers["event:delete"] = handleEventDelete

	// Action handlers
	r.handlers["action:provision"] = handleProvision
	r.handlers["action:destroy"] = handleDestroy
	r.handlers["action:start"] = handleStart
	r.handlers["action:stop"] = handleStop
	r.handlers["action:status"] = handleStatus
	r.handlers["action:update"] = handleUpdate

	// Callback handlers.
	//
	// Callbacks run as actionCallback runs. The operator assigns these
	// runs to the runner pod via a K8s label update, but the
	// runner_assignments dict is populated asynchronously through the
	// K8s watch. Because the Go runner processes runs in milliseconds
	// (vs seconds for the Ansible runner), SubjectUpdate PATCH calls
	// race with the watch and fail with "not assigned to runner".
	//
	// Fix: callback handlers set ContinueAction("0s") to immediately
	// trigger a new action run. The action run re-enters the handler
	// (e.g. handleStart), which calls checkDeployerJob. That finds the
	// Tower job succeeded and routes to the completion handler from an
	// action context where PATCH calls work.
	callbackContinue := func(rc *RunContext) error {
		slog.Info("callback received, scheduling immediate action re-check",
			"action", rc.ActionName, "subject", rc.SubjectName)
		rc.ContinueAction("0s")
		return nil
	}
	r.handlers["action:provision:complete"] = callbackContinue
	r.handlers["action:destroy:complete"] = callbackContinue
	r.handlers["action:start:complete"] = callbackContinue
	r.handlers["action:stop:complete"] = callbackContinue
	r.handlers["action:status:complete"] = callbackContinue
	r.handlers["action:update:complete"] = callbackContinue
}
