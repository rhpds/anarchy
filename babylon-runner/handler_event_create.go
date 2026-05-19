package main

import (
	"github.com/google/uuid"
)

// handleEventCreate handles the "create" subject event. It initializes a
// newly created subject by merging governor job_vars, generating a UUID,
// setting default cloud_provider/platform, and scheduling the provision action.
func handleEventCreate(rc *RunContext) error {
	// Already initialized — nothing to do.
	if rc.CurrentState() != "" {
		return nil
	}

	// Start with governor job_vars as defaults, then merge subject job_vars
	// on top so that subject-level values take precedence.
	jobVars := make(map[string]interface{})
	if gjv := rc.GovernorJobVars(); gjv != nil {
		mergeMap(jobVars, gjv)
	}
	if sjv := rc.JobVars(); sjv != nil {
		mergeMap(jobVars, sjv)
	}

	// Generate UUID if not already set.
	if _, ok := jobVars["uuid"]; !ok {
		jobVars["uuid"] = uuid.New().String()
	}

	// Set cloud_provider from governor job_vars (default "none").
	if _, ok := jobVars["cloud_provider"]; !ok {
		govJV := rc.GovernorJobVars()
		if govJV != nil {
			if cp, ok := govJV["cloud_provider"]; ok {
				jobVars["cloud_provider"] = cp
			} else {
				jobVars["cloud_provider"] = "none"
			}
		} else {
			jobVars["cloud_provider"] = "none"
		}
	}

	// Set platform default.
	if _, ok := jobVars["platform"]; !ok {
		jobVars["platform"] = "RHPDS"
	}

	// Update subject with initial state and merged job_vars.
	err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{
					"state": "provision-pending",
				},
			},
			Spec: &PatchSpec{
				Vars: map[string]interface{}{
					"current_state": "provision-pending",
					"job_vars":      jobVars,
				},
			},
			SkipUpdateProcessing: true,
		},
	})
	if err != nil {
		return err
	}

	// Schedule the provision action.
	return rc.ScheduleAction(ScheduleActionRequest{
		Action: "provision",
	})
}
