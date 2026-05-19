package main

import (
	"log"
)

// handleUpdate routes an update action based on the current state.
func handleUpdate(rc *RunContext) error {
	currentState := rc.CurrentState()

	if currentState != "updating" {
		return runUpdate(rc)
	}

	return checkDeployerJob(rc, "update")
}

// runUpdate initiates the update workflow.
func runUpdate(rc *RunContext) error {
	// Sandbox API integration (TODO).
	if rc.SandboxAPIInUse() {
		log.Printf("runUpdate: sandbox get needed for subject=%s (TODO)", rc.SubjectName)
	}

	// Tower job launch needed (TODO).
	log.Printf("runUpdate: tower job launch needed for subject=%s (TODO)", rc.SubjectName)
	return rc.ContinueAction("5m")
}
