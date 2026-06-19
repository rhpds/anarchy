package runner

import (
	"context"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
	"k8s.io/client-go/kubernetes"
)

// RunContext holds the per-run state and provides convenience methods
// for accessing payload data and calling the Anarchy API.
type RunContext struct {
	Payload              types.RunPayload
	Result               types.RunResult
	AnarchyClient        *clients.AnarchyClient
	Clientset            kubernetes.Interface
	TowerBaseURL         string // overridden in tests to inject mock Tower server
	SandboxBaseURL       string // overridden in tests
	DefaultSandboxAPIURL string // from Config; used as fallback when governor vars are absent
	SandboxClientOpts    []clients.SandboxAPIOption // optional; used in tests to disable retries
}

// --- Convenience accessors (typed payloads make these trivial) ---

// SubjectName returns the subject's metadata name.
func (rc *RunContext) SubjectName() string { return rc.Payload.Subject.Metadata.Name }

// RunName returns the run's metadata name.
func (rc *RunContext) RunName() string { return rc.Payload.Run.Metadata.Name }

// CurrentState returns subject.spec.vars.current_state.
func (rc *RunContext) CurrentState() string { return rc.Payload.Subject.Spec.Vars.CurrentState }

// DesiredState returns subject.spec.vars.desired_state.
func (rc *RunContext) DesiredState() string { return rc.Payload.Subject.Spec.Vars.DesiredState }

// CheckStatusState returns subject.spec.vars.check_status_state.
func (rc *RunContext) CheckStatusState() string {
	return rc.Payload.Subject.Spec.Vars.CheckStatusState
}

// ActionName returns the action name from the action spec, or "" for events.
func (rc *RunContext) ActionName() string {
	if rc.Payload.Action != nil {
		return rc.Payload.Action.Spec.Action
	}
	return ""
}

// ActionVars returns action.spec.vars, or nil if no action is present.
func (rc *RunContext) ActionVars() map[string]interface{} {
	if rc.Payload.Action != nil {
		return rc.Payload.Action.Spec.Vars
	}
	return nil
}

// JobVars returns subject.spec.vars.job_vars.
func (rc *RunContext) JobVars() map[string]interface{} {
	return rc.Payload.Subject.Spec.Vars.JobVars
}

// GovernorJobVars returns governor.spec.vars.job_vars.
func (rc *RunContext) GovernorJobVars() map[string]interface{} {
	return rc.Payload.Governor.Spec.Vars.JobVars
}

// SubjectAllVars returns the complete flat map of all subject vars
// (typed fields + Extra), suitable for J2VarContext.
func (rc *RunContext) SubjectAllVars() map[string]interface{} {
	return rc.Payload.Subject.Spec.Vars.AllVars()
}

// GovernorAllVars returns the complete flat map of all governor vars
// (typed fields + Extra), suitable for J2VarContext.
func (rc *RunContext) GovernorAllVars() map[string]interface{} {
	return rc.Payload.Governor.Spec.Vars.AllVars()
}

// Meta returns __meta__ from governor.spec.vars.job_vars. Returns an empty Meta
// if none is set, so callers never need to nil-check.
func (rc *RunContext) Meta() *types.Meta {
	if rc.Payload.Governor.Spec.Vars.Meta != nil {
		return rc.Payload.Governor.Spec.Vars.Meta
	}
	return &types.Meta{}
}

// SandboxAPIInUse returns true if meta.aws_sandboxed is true or
// meta.sandboxes has at least one element.
func (rc *RunContext) SandboxAPIInUse() bool {
	meta := rc.Meta()
	if meta.AWSSandboxed {
		return true
	}
	return len(meta.Sandboxes) > 0
}

// DeployerDisabled returns true if __meta__.deployer.actions.{action}.disabled is true.
func (rc *RunContext) DeployerDisabled(action string) bool {
	meta := rc.Meta()
	if meta.Deployer == nil {
		return false
	}
	cfg, ok := meta.Deployer.Actions[action]
	if !ok {
		return false
	}
	return cfg.Disabled
}

// UUID returns job_vars.uuid from the subject.
func (rc *RunContext) UUID() string {
	return types.StringFromMap(rc.JobVars(), "uuid")
}

// GUID returns job_vars.guid from the subject.
func (rc *RunContext) GUID() string {
	return types.StringFromMap(rc.JobVars(), "guid")
}

// StatusActions returns subject.status.actions.
func (rc *RunContext) StatusActions() map[string]interface{} {
	return rc.Payload.Subject.Status.Actions
}

// StatusTowerJobs returns subject.status.towerJobs.
func (rc *RunContext) StatusTowerJobs() map[string]interface{} {
	return rc.Payload.Subject.Status.TowerJobs
}

// GovernorActions returns governor.spec.actions as a flat map.
func (rc *RunContext) GovernorActions() map[string]interface{} {
	src := rc.Payload.Governor.Spec.Actions
	if src == nil {
		return nil
	}
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// ActionRetryCount returns action_retry_count from the action's spec.vars (default 0).
func (rc *RunContext) ActionRetryCount() int {
	if rc.Payload.Action == nil || rc.Payload.Action.Spec.Vars == nil {
		return 0
	}
	count, _ := rc.Payload.Action.Spec.Vars["action_retry_count"].(float64)
	return int(count)
}

// IsBeingDeleted returns true if the subject has a deletionTimestamp.
func (rc *RunContext) IsBeingDeleted() bool {
	return rc.Payload.Subject.Metadata.DeletionTimestamp != nil
}

// --- Directives ---

// FinishAction marks the action as finished with the given state.
func (rc *RunContext) FinishAction(state string) {
	rc.Result.FinishAction = &types.FinishActionDirective{State: state}
}

// DeleteSubject marks the subject for deletion by the operator.
func (rc *RunContext) DeleteSubject(removeFinalizers bool) {
	rc.Result.DeleteSubject = &types.DeleteSubjectDirective{RemoveFinalizers: removeFinalizers}
}

// ContinueAction sets a directive to reschedule the current action after
// the specified duration string (e.g. "30s", "5m").
func (rc *RunContext) ContinueAction(after string) {
	rc.Result.ContinueAction = &types.ContinueActionDirective{
		After: types.AfterTimestamp(after),
	}
}

// ContinueActionWithVars sets a directive to reschedule the current action
// with additional vars (e.g. action_retry_count).
func (rc *RunContext) ContinueActionWithVars(after string, vars map[string]interface{}) {
	rc.Result.ContinueAction = &types.ContinueActionDirective{
		After: types.AfterTimestamp(after),
		Vars:  vars,
	}
}

// --- Delegates ---

// SubjectUpdate delegates to AnarchyClient.SubjectUpdate for this subject.
func (rc *RunContext) SubjectUpdate(patch types.SubjectPatch) error {
	return rc.AnarchyClient.SubjectUpdate(context.TODO(), rc.SubjectName(), patch)
}

// ScheduleAction delegates to AnarchyClient.ScheduleAction for this subject.
func (rc *RunContext) ScheduleAction(req types.ScheduleActionRequest) error {
	return rc.AnarchyClient.ScheduleAction(context.TODO(), rc.SubjectName(), req)
}
