package types

import "encoding/json"

// ObjectMeta holds Kubernetes-style metadata fields.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	DeletionTimestamp *string           `json:"deletionTimestamp,omitempty"`
}

// Handler identifies which handler to invoke for a run.
type Handler struct {
	Type string                 `json:"type"`
	Name string                 `json:"name"`
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// RunPayload is the response body from GET /run.
type RunPayload struct {
	Handler  Handler  `json:"handler"`
	Governor Governor `json:"governor"`
	Subject  Subject  `json:"subject"`
	Action   *Action  `json:"action,omitempty"`
	Run      Run      `json:"run"`
}

// Governor represents the AnarchyGovernor attached to a run.
type Governor struct {
	Metadata ObjectMeta   `json:"metadata"`
	Spec     GovernorSpec `json:"spec"`
}

// GovernorSpec holds governor specification fields.
type GovernorSpec struct {
	Vars    GovernorVars                      `json:"vars"`
	Actions map[string]map[string]interface{} `json:"actions,omitempty"`
	Runner  string                            `json:"runner,omitempty"`
}

// GovernorVars holds governor variables with typed access for known fields
// and dynamic access for unknown fields via Get/GetString.
type GovernorVars struct {
	JobVars    map[string]interface{} `json:"job_vars,omitempty"`
	Meta       *Meta                  `json:"-"`
	SandboxAPI map[string]interface{} `json:"sandbox_api,omitempty"`
	Extra      map[string]interface{} `json:"-"`
}

func (v *GovernorVars) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	known := map[string]bool{"job_vars": true, "sandbox_api": true}

	if jv, ok := raw["job_vars"]; ok {
		if err := json.Unmarshal(jv, &v.JobVars); err != nil {
			return err
		}
	}

	// Extract __meta__ from inside job_vars (where it lives in real governors).
	if v.JobVars != nil {
		if metaRaw, ok := v.JobVars["__meta__"]; ok {
			metaBytes, err := json.Marshal(metaRaw)
			if err != nil {
				return err
			}
			v.Meta = &Meta{}
			if err := json.Unmarshal(metaBytes, v.Meta); err != nil {
				return err
			}
		}
	}

	if sa, ok := raw["sandbox_api"]; ok {
		if err := json.Unmarshal(sa, &v.SandboxAPI); err != nil {
			return err
		}
	}

	v.Extra = make(map[string]interface{})
	for k, rm := range raw {
		if !known[k] {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err != nil {
				return err
			}
			v.Extra[k] = val
		}
	}
	return nil
}

func (v GovernorVars) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	jv := make(map[string]interface{})
	if v.JobVars != nil {
		for k, val := range v.JobVars {
			jv[k] = val
		}
	}
	if v.Meta != nil {
		if _, has := jv["__meta__"]; !has {
			jv["__meta__"] = v.Meta
		}
	}
	if len(jv) > 0 {
		m["job_vars"] = jv
	}
	if v.SandboxAPI != nil {
		m["sandbox_api"] = v.SandboxAPI
	}
	return json.Marshal(m)
}

// Get returns a dynamic field value from the governor vars that is not
// one of the typed fields (job_vars, sandbox_api).
func (v *GovernorVars) Get(key string) interface{} {
	if v.Extra != nil {
		return v.Extra[key]
	}
	return nil
}

// GetString returns a dynamic field as a string, or "" if missing or not a string.
func (v *GovernorVars) GetString(key string) string {
	if s, ok := v.Get(key).(string); ok {
		return s
	}
	return ""
}

// AllVars reconstructs the complete flat map from typed fields + Extra.
// Used by J2VarContext to maintain the same merge behavior as the
// current code (which merges ALL spec.vars, not just job_vars).
func (v *GovernorVars) AllVars() map[string]interface{} {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	jv := make(map[string]interface{})
	if v.JobVars != nil {
		for k, val := range v.JobVars {
			jv[k] = val
		}
	}
	if v.Meta != nil {
		if _, has := jv["__meta__"]; !has {
			b, _ := json.Marshal(v.Meta)
			var metaMap map[string]interface{}
			_ = json.Unmarshal(b, &metaMap)
			jv["__meta__"] = metaMap
		}
	}
	if len(jv) > 0 {
		m["job_vars"] = jv
	}
	if v.SandboxAPI != nil {
		m["sandbox_api"] = v.SandboxAPI
	}
	return m
}

// Meta holds the __meta__ governor configuration.
type Meta struct {
	Deployer                    *DeployerMeta            `json:"deployer,omitempty"`
	Sandboxes                   []interface{}            `json:"sandboxes,omitempty"`
	AWSSandboxed                bool                     `json:"aws_sandboxed,omitempty"`
	SandboxAPI                  map[string]interface{}   `json:"sandbox_api,omitempty"`
	AnsibleControllers          []map[string]interface{} `json:"ansible_controllers,omitempty"`
	AnsibleControllerSelectMode string                   `json:"ansible_controller_select_mode,omitempty"`
	ControllerScheduler         *ControllerSchedulerMeta `json:"controller_scheduler,omitempty"`
	Tower                       *TowerMeta               `json:"tower,omitempty"`
}

// DeployerMeta configures the deployer (e.g. agnosticd) for Tower job launches.
type DeployerMeta struct {
	Type              string                          `json:"type,omitempty"`
	EntryPoint        string                          `json:"entry_point,omitempty"`
	Actions           map[string]DeployerActionConfig `json:"actions,omitempty"`
	SCMUrl            string                          `json:"scm_url,omitempty"`
	SCMRef            string                          `json:"scm_ref,omitempty"`
	SCMType           string                          `json:"scm_type,omitempty"`
	SCMCredential     string                          `json:"scm_credential,omitempty"`
	SCMUpdateOnLaunch *bool                           `json:"scm_update_on_launch,omitempty"`
	SCMCacheTimeout   int                             `json:"scm_update_cache_timeout,omitempty"`
	SCMClean          *bool                           `json:"scm_clean,omitempty"`
}

// DeployerActionConfig holds per-action deployer overrides.
type DeployerActionConfig struct {
	EntryPoint string `json:"entry_point,omitempty"`
	SCMRef     string `json:"scm_ref,omitempty"`
	Disabled   bool   `json:"disabled,omitempty"`
}

// TowerMeta holds Tower/AAP configuration from __meta__.tower.
type TowerMeta struct {
	Organization string `json:"organization,omitempty"`
	Timeout      int    `json:"timeout,omitempty"`
	Inventory    string `json:"inventory,omitempty"`
}

// ControllerSchedulerMeta configures external controller selection.
type ControllerSchedulerMeta struct {
	URL           string            `json:"url,omitempty"`
	RequireLabels map[string]string `json:"require_labels,omitempty"`
	PreferLabels  map[string]string `json:"prefer_labels,omitempty"`
	InstanceGroup string            `json:"instance_group,omitempty"`
}

// Subject represents the AnarchySubject attached to a run.
type Subject struct {
	Metadata ObjectMeta    `json:"metadata"`
	Spec     SubjectSpec   `json:"spec"`
	Status   SubjectStatus `json:"status"`
}

// SubjectSpec holds subject specification fields.
type SubjectSpec struct {
	Vars SubjectVars `json:"vars"`
}

// SubjectVars holds subject variables with typed access for known fields
// and dynamic access for unknown fields via Get/GetString.
// Uses the same Extra pattern as GovernorVars to avoid losing fields
// like provision_data, action_schedule, provision_messages during unmarshal.
type SubjectVars struct {
	CurrentState     string                 `json:"current_state,omitempty"`
	DesiredState     string                 `json:"desired_state,omitempty"`
	Healthy          *bool                  `json:"healthy,omitempty"`
	JobVars          map[string]interface{} `json:"job_vars,omitempty"`
	CheckStatusState string                 `json:"check_status_state,omitempty"`
	Extra            map[string]interface{} `json:"-"`
}

func (v *SubjectVars) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	known := map[string]bool{
		"current_state": true, "desired_state": true,
		"healthy": true, "job_vars": true, "check_status_state": true,
	}

	if cs, ok := raw["current_state"]; ok {
		if err := json.Unmarshal(cs, &v.CurrentState); err != nil {
			return err
		}
	}
	if ds, ok := raw["desired_state"]; ok {
		if err := json.Unmarshal(ds, &v.DesiredState); err != nil {
			return err
		}
	}
	if h, ok := raw["healthy"]; ok {
		var healthy bool
		if err := json.Unmarshal(h, &healthy); err != nil {
			return err
		}
		v.Healthy = &healthy
	}
	if jv, ok := raw["job_vars"]; ok {
		if err := json.Unmarshal(jv, &v.JobVars); err != nil {
			return err
		}
	}
	if css, ok := raw["check_status_state"]; ok {
		if err := json.Unmarshal(css, &v.CheckStatusState); err != nil {
			return err
		}
	}

	v.Extra = make(map[string]interface{})
	for k, rm := range raw {
		if !known[k] {
			var val interface{}
			if err := json.Unmarshal(rm, &val); err != nil {
				return err
			}
			v.Extra[k] = val
		}
	}
	return nil
}

func (v SubjectVars) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	if v.CurrentState != "" {
		m["current_state"] = v.CurrentState
	}
	if v.DesiredState != "" {
		m["desired_state"] = v.DesiredState
	}
	if v.Healthy != nil {
		m["healthy"] = *v.Healthy
	}
	if v.JobVars != nil {
		m["job_vars"] = v.JobVars
	}
	if v.CheckStatusState != "" {
		m["check_status_state"] = v.CheckStatusState
	}
	return json.Marshal(m)
}

// Get returns a dynamic field value from subject vars.
func (v *SubjectVars) Get(key string) interface{} {
	if v.Extra != nil {
		return v.Extra[key]
	}
	return nil
}

// GetString returns a dynamic field as a string, or "" if missing.
func (v *SubjectVars) GetString(key string) string {
	if s, ok := v.Get(key).(string); ok {
		return s
	}
	return ""
}

// AllVars reconstructs the complete flat map from typed fields + Extra.
// Used by J2VarContext to maintain the same merge behavior as the
// current code (which merges ALL spec.vars, not just job_vars).
func (v *SubjectVars) AllVars() map[string]interface{} {
	m := make(map[string]interface{})
	for k, val := range v.Extra {
		m[k] = val
	}
	if v.CurrentState != "" {
		m["current_state"] = v.CurrentState
	}
	if v.DesiredState != "" {
		m["desired_state"] = v.DesiredState
	}
	if v.Healthy != nil {
		m["healthy"] = *v.Healthy
	}
	if v.JobVars != nil {
		m["job_vars"] = v.JobVars
	}
	if v.CheckStatusState != "" {
		m["check_status_state"] = v.CheckStatusState
	}
	return m
}

// SubjectStatus holds subject status fields. Inner structures remain
// as maps because they are deeply nested and accessed dynamically.
type SubjectStatus struct {
	Actions       map[string]interface{} `json:"actions,omitempty"`
	TowerJobs     map[string]interface{} `json:"towerJobs,omitempty"`
	PreviousState map[string]interface{} `json:"previous_state,omitempty"`
}

// Action represents the AnarchyAction attached to a run (nil for events).
type Action struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     ActionSpec `json:"spec"`
}

// ActionSpec holds action specification fields.
type ActionSpec struct {
	Action        string                 `json:"action"`
	Vars          map[string]interface{} `json:"vars,omitempty"`
	CallbackUrl   string                 `json:"callbackUrl,omitempty"`
	CallbackToken string                 `json:"callbackToken,omitempty"`
}

// Run represents the AnarchyRun being executed.
type Run struct {
	Metadata ObjectMeta `json:"metadata"`
}
