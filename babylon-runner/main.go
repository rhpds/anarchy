package main

import (
	"log"
	"os"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC)

	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	log.Printf("babylon-runner starting name=%s pod=%s url=%s poll=%ds timeout=%ds",
		cfg.RunnerName, cfg.PodName, cfg.AnarchyURL, cfg.PollingInterval, cfg.RequestTimeout)

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
