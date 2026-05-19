package main

import (
	"github.com/google/uuid"
)

// handleEventCreate handles the "create" subject event. It initializes a
// newly created subject by setting cloud_provider, platform, uuid in
// job_vars (matching Ansible's handle-event-create.yaml), then scheduling
// the provision action.
func handleEventCreate(rc *RunContext) error {
	// Initialize subject if not already done.
	if rc.CurrentState() == "" {
		govJV := rc.GovernorJobVars()

		// cloud_provider from governor job_vars, default "none".
		cloudProvider := "none"
		if govJV != nil {
			if cp, ok := govJV["cloud_provider"].(string); ok && cp != "" {
				cloudProvider = cp
			}
		}

		// platform from governor job_vars, default "RHPDS".
		platform := "RHPDS"
		if govJV != nil {
			if p, ok := govJV["platform"].(string); ok && p != "" {
				platform = p
			}
		}

		// uuid: use existing subject uuid if set, otherwise generate.
		subjectUUID := uuid.New().String()
		if sjv := rc.JobVars(); sjv != nil {
			if u, ok := sjv["uuid"].(string); ok && u != "" {
				subjectUUID = u
			}
		}

		// Only set the 3 specific fields in job_vars (matching Ansible).
		// anarchy_subject_update deep-merges into existing job_vars.
		jobVarsPatch := map[string]interface{}{
			"cloud_provider": cloudProvider,
			"platform":       platform,
			"uuid":           subjectUUID,
		}

		if err := rc.SubjectUpdate(SubjectPatch{
			Patch: PatchBody{
				Metadata: &PatchMetadata{
					Labels: map[string]string{
						"state": "provision-pending",
					},
				},
				Spec: &PatchSpec{
					Vars: map[string]interface{}{
						"current_state": "provision-pending",
						"job_vars":      jobVarsPatch,
					},
				},
				SkipUpdateProcessing: true,
			},
		}); err != nil {
			return err
		}
	}

	// Always schedule the provision action.
	return rc.ScheduleAction(ScheduleActionRequest{
		Action: "provision",
	})
}
