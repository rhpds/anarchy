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
	// Get sandbox vars for Tower job.
	if rc.SandboxAPIInUse() {
		if _, err := sandboxGet(rc, "update"); err != nil {
			log.Printf("runUpdate: sandbox get error for subject=%s: %v", rc.SubjectName, err)
		}
	}

	// Launch Tower job for update.
	if err := launchTowerJob(rc, "update", "updating", nil); err != nil {
		log.Printf("runUpdate: tower launch failed for subject=%s: %v", rc.SubjectName, err)
		return err
	}
	return rc.ContinueAction("5m")
}
