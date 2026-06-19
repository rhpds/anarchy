package types

// RunResult is the body for POST /run/{name}.
type RunResult struct {
	RC             int                      `json:"rc"`
	Status         string                   `json:"status"`
	StatusMessage  string                   `json:"statusMessage,omitempty"`
	FinishAction   *FinishActionDirective   `json:"finishAction,omitempty"`
	ContinueAction *ContinueActionDirective `json:"continueAction,omitempty"`
	DeleteSubject  *DeleteSubjectDirective  `json:"deleteSubject,omitempty"`
}

// FinishActionDirective signals the operator to mark the action as finished.
type FinishActionDirective struct {
	State string `json:"state"`
}

// ContinueActionDirective signals the operator to reschedule the action.
type ContinueActionDirective struct {
	After string                 `json:"after"`
	Vars  map[string]interface{} `json:"vars,omitempty"`
}

// DeleteSubjectDirective signals the operator to delete the subject.
type DeleteSubjectDirective struct {
	RemoveFinalizers bool `json:"removeFinalizers"`
}
