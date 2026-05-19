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

	// Callback handlers
	r.handlers["action:provision:complete"] = func(rc *RunContext) error {
		return handleProvisionComplete(rc, nil, nil, nil)
	}
}
