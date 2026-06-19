package types

// SubjectPatch is the body for PATCH /run/subject/{name}.
type SubjectPatch struct {
	Patch PatchBody `json:"patch"`
}

// PatchBody contains the fields to patch on a subject.
type PatchBody struct {
	Metadata             *PatchMetadata         `json:"metadata,omitempty"`
	Spec                 *PatchSpec             `json:"spec,omitempty"`
	Status               map[string]interface{} `json:"status,omitempty"`
	SkipUpdateProcessing bool                   `json:"skip_update_processing,omitempty"`
}

// PatchMetadata patches metadata labels and annotations.
type PatchMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PatchSpec patches spec-level vars.
type PatchSpec struct {
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// ScheduleActionRequest is the body for POST /run/subject/{name}/actions.
type ScheduleActionRequest struct {
	Action string                 `json:"action"`
	After  string                 `json:"after,omitempty"`
	Cancel []string               `json:"cancel,omitempty"`
	Vars   map[string]interface{} `json:"vars,omitempty"`
}
