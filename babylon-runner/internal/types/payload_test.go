package types

import (
	"encoding/json"
	"testing"
)

// testPayloadJSON is a realistic run payload based on production data
// (enterprise.event-driven-ansible.prod). All sensitive values have been
// replaced with dummy data. This mirrors the structure GET /run returns
// to the runner — __meta__ is inside job_vars at spec.vars.job_vars.__meta__.
const testPayloadJSON = `{
	"handler": {"type": "action", "name": "provision"},
	"governor": {
		"metadata": {
			"name": "enterprise.event-driven-ansible.prod",
			"namespace": "babylon-anarchy-0"
		},
		"spec": {
			"vars": {
				"job_vars": {
					"cloud_provider": "ec2",
					"env_type": "ocp4-cluster",
					"platform": "RHPDS",
					"purpose": "PROD",
					"aws_region": "us-east-2",
					"student_name": "lab-user",
					"__meta__": {
						"deployer": {
							"type": "agnosticd",
							"scm_url": "https://github.com/redhat-cop/agnosticd.git",
							"scm_ref": "demo-event-driven-ansible-1.0.2",
							"actions": {
								"provision": {"entry_point": "ansible/main.yml"},
								"destroy": {"entry_point": "ansible/destroy.yml"},
								"stop": {"entry_point": "ansible/lifecycle.yml"},
								"start": {"entry_point": "ansible/lifecycle.yml"},
								"status": {"entry_point": "ansible/lifecycle.yml"}
							}
						},
						"aws_sandboxed": true,
						"tower": {
							"organization": "RHPDS",
							"timeout": 7200
						},
						"sandbox_api": {
							"actions": {
								"start": {"enable": true},
								"stop": {"enable": true}
							}
						}
					}
				},
				"sandbox_api": {
					"url": "https://sandbox-api.example.com"
				}
			},
			"actions": {
				"provision": {"roles": [{"name": "check-deployer"}]},
				"destroy": {"roles": [{"name": "check-deployer"}]},
				"start": {"roles": [{"name": "check-deployer"}]},
				"stop": {"roles": [{"name": "check-deployer"}]},
				"status": {"roles": [{"name": "check-deployer"}]}
			},
			"runner": "babylon-go"
		}
	},
	"subject": {
		"metadata": {
			"name": "enterprise.event-driven-ansible.prod-ab12c",
			"namespace": "babylon-anarchy-0",
			"labels": {
				"anarchy.gpte.redhat.com/governor": "enterprise.event-driven-ansible.prod",
				"state": "started"
			},
			"annotations": {
				"poolboy.gpte.redhat.com/resource-claim-name": "enterprise.event-driven-ansible.prod-claim1",
				"poolboy.gpte.redhat.com/resource-requester-email": "user@example.com"
			}
		},
		"spec": {
			"vars": {
				"current_state": "started",
				"desired_state": "started",
				"healthy": true,
				"job_vars": {
					"cloud_provider": "ec2",
					"guid": "ab12c",
					"uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
					"sandbox_name": "sandbox1234",
					"sandbox_zone": "sandbox1234.example.com",
					"sandbox_account": "123456789012",
					"platform": "RHPDS"
				},
				"provision_data": {
					"ocp_console_url": "https://console.cluster.example.com",
					"ssh_password": "REDACTED"
				},
				"action_schedule": {
					"stop": "8h",
					"destroy": "6d"
				},
				"check_status_request_timestamp": "2026-06-18T22:00:00Z"
			}
		},
		"status": {
			"towerJobs": {
				"provision": {
					"completeTimestamp": "2026-06-18T23:06:52Z",
					"deployerJob": 285742,
					"jobStatus": "successful",
					"towerHost": "controller.example.com"
				}
			},
			"actions": {
				"provision": {
					"state": "successful",
					"completeTimestamp": "2026-06-18T23:06:52Z"
				}
			}
		}
	},
	"action": {
		"metadata": {"name": "enterprise.event-driven-ansible.prod-ab12c-provision"},
		"spec": {
			"action": "provision",
			"vars": {"action_retry_count": 0}
		}
	},
	"run": {
		"metadata": {"name": "enterprise.event-driven-ansible.prod-ab12c-provision-run1"}
	}
}`

// NewTestPayload returns a parsed RunPayload from testPayloadJSON.
// Panics on parse error — safe for use in tests only.
func NewTestPayload() RunPayload {
	var p RunPayload
	if err := json.Unmarshal([]byte(testPayloadJSON), &p); err != nil {
		panic("NewTestPayload: " + err.Error())
	}
	return p
}

func TestRunPayloadUnmarshal(t *testing.T) {
	payload := NewTestPayload()

	// Handler
	if payload.Handler.Type != "action" {
		t.Errorf("handler type = %q, want %q", payload.Handler.Type, "action")
	}
	if payload.Handler.Name != "provision" {
		t.Errorf("handler name = %q, want %q", payload.Handler.Name, "provision")
	}

	// Governor metadata
	if payload.Governor.Metadata.Name != "enterprise.event-driven-ansible.prod" {
		t.Errorf("governor name = %q", payload.Governor.Metadata.Name)
	}
	if payload.Governor.Metadata.Namespace != "babylon-anarchy-0" {
		t.Errorf("governor namespace = %q", payload.Governor.Metadata.Namespace)
	}
	if payload.Governor.Spec.Runner != "babylon-go" {
		t.Errorf("runner = %q, want %q", payload.Governor.Spec.Runner, "babylon-go")
	}

	// Governor job_vars
	if payload.Governor.Spec.Vars.JobVars["cloud_provider"] != "ec2" {
		t.Errorf("governor cloud_provider = %v", payload.Governor.Spec.Vars.JobVars["cloud_provider"])
	}
	if payload.Governor.Spec.Vars.JobVars["env_type"] != "ocp4-cluster" {
		t.Errorf("governor env_type = %v", payload.Governor.Spec.Vars.JobVars["env_type"])
	}

	// Meta (__meta__ inside job_vars at spec.vars.job_vars.__meta__)
	meta := payload.Governor.Spec.Vars.Meta
	if meta == nil {
		t.Fatal("meta is nil")
	}
	if !meta.AWSSandboxed {
		t.Error("aws_sandboxed = false, want true")
	}

	// Deployer
	if meta.Deployer == nil {
		t.Fatal("deployer is nil")
	}
	if meta.Deployer.Type != "agnosticd" {
		t.Errorf("deployer type = %q, want %q", meta.Deployer.Type, "agnosticd")
	}
	if meta.Deployer.SCMRef != "demo-event-driven-ansible-1.0.2" {
		t.Errorf("scm_ref = %q", meta.Deployer.SCMRef)
	}
	if cfg, ok := meta.Deployer.Actions["provision"]; !ok {
		t.Error("deployer actions missing 'provision'")
	} else if cfg.EntryPoint != "ansible/main.yml" {
		t.Errorf("entry_point = %q, want %q", cfg.EntryPoint, "ansible/main.yml")
	}
	if _, ok := meta.Deployer.Actions["destroy"]; !ok {
		t.Error("deployer actions missing 'destroy'")
	}

	// Tower meta
	if meta.Tower == nil {
		t.Fatal("tower meta is nil")
	}
	if meta.Tower.Organization != "RHPDS" {
		t.Errorf("tower org = %q, want %q", meta.Tower.Organization, "RHPDS")
	}
	if meta.Tower.Timeout != 7200 {
		t.Errorf("tower timeout = %d, want 7200", meta.Tower.Timeout)
	}

	// Governor sandbox_api (top-level, separate from __meta__.sandbox_api)
	if payload.Governor.Spec.Vars.SandboxAPI == nil {
		t.Fatal("governor sandbox_api is nil")
	}
	if payload.Governor.Spec.Vars.SandboxAPI["url"] != "https://sandbox-api.example.com" {
		t.Errorf("sandbox_api url = %v", payload.Governor.Spec.Vars.SandboxAPI["url"])
	}

	// Subject metadata
	if payload.Subject.Metadata.Name != "enterprise.event-driven-ansible.prod-ab12c" {
		t.Errorf("subject name = %q", payload.Subject.Metadata.Name)
	}
	if payload.Subject.Metadata.Labels["state"] != "started" {
		t.Errorf("subject label state = %q", payload.Subject.Metadata.Labels["state"])
	}

	// Subject vars
	if payload.Subject.Spec.Vars.CurrentState != "started" {
		t.Errorf("current_state = %q", payload.Subject.Spec.Vars.CurrentState)
	}
	if payload.Subject.Spec.Vars.DesiredState != "started" {
		t.Errorf("desired_state = %q", payload.Subject.Spec.Vars.DesiredState)
	}
	if payload.Subject.Spec.Vars.Healthy == nil || !*payload.Subject.Spec.Vars.Healthy {
		t.Error("healthy should be true")
	}
	if payload.Subject.Spec.Vars.JobVars["guid"] != "ab12c" {
		t.Errorf("subject guid = %v", payload.Subject.Spec.Vars.JobVars["guid"])
	}
	if payload.Subject.Spec.Vars.JobVars["uuid"] != "a1b2c3d4-e5f6-7890-abcd-ef1234567890" {
		t.Errorf("subject uuid = %v", payload.Subject.Spec.Vars.JobVars["uuid"])
	}

	// Subject vars Extra fields (not in typed struct — captured in Extra)
	provisionData := payload.Subject.Spec.Vars.Get("provision_data")
	if provisionData == nil {
		t.Fatal("provision_data should be captured in Extra")
	}
	pd, ok := provisionData.(map[string]interface{})
	if !ok {
		t.Fatalf("provision_data type = %T, want map", provisionData)
	}
	if pd["ocp_console_url"] != "https://console.cluster.example.com" {
		t.Errorf("ocp_console_url = %v", pd["ocp_console_url"])
	}
	if payload.Subject.Spec.Vars.GetString("check_status_request_timestamp") != "2026-06-18T22:00:00Z" {
		t.Errorf("check_status_request_timestamp = %q", payload.Subject.Spec.Vars.GetString("check_status_request_timestamp"))
	}

	// Subject status
	provisionJob := payload.Subject.Status.TowerJobs["provision"]
	if provisionJob == nil {
		t.Fatal("towerJobs.provision is nil")
	}

	// Action
	if payload.Action == nil {
		t.Fatal("action is nil")
	}
	if payload.Action.Spec.Action != "provision" {
		t.Errorf("action spec = %q, want %q", payload.Action.Spec.Action, "provision")
	}

	// Run
	if payload.Run.Metadata.Name != "enterprise.event-driven-ansible.prod-ab12c-provision-run1" {
		t.Errorf("run name = %q", payload.Run.Metadata.Name)
	}
}

func TestGovernorVarsExtraFields(t *testing.T) {
	raw := `{
		"job_vars": {"key": "val", "__meta__": {"aws_sandboxed": true}},
		"sandbox_api": {"url": "http://sandbox"},
		"scm_ref_var": "agnosticd_scm_ref",
		"unknown_field": 42
	}`

	var vars GovernorVars
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if vars.JobVars["key"] != "val" {
		t.Error("job_vars not parsed")
	}
	if vars.Meta == nil || !vars.Meta.AWSSandboxed {
		t.Error("__meta__ not parsed")
	}
	if vars.SandboxAPI["url"] != "http://sandbox" {
		t.Error("sandbox_api not parsed")
	}
	if vars.GetString("scm_ref_var") != "agnosticd_scm_ref" {
		t.Errorf("extra field scm_ref_var = %q, want %q", vars.GetString("scm_ref_var"), "agnosticd_scm_ref")
	}
	if vars.Get("unknown_field") == nil {
		t.Error("unknown extra field not captured")
	}
}

func TestGovernorVarsMarshalRoundTrip(t *testing.T) {
	original := GovernorVars{
		JobVars:    map[string]interface{}{"region": "us-east-1"},
		Meta:       &Meta{AWSSandboxed: true},
		SandboxAPI: map[string]interface{}{"url": "http://sandbox"},
		Extra:      map[string]interface{}{"scm_ref_var": "agnosticd_scm_ref"},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var roundTripped GovernorVars
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if roundTripped.GetString("scm_ref_var") != "agnosticd_scm_ref" {
		t.Error("extra field lost during round-trip")
	}
}

func TestGovernorVarsAllVars(t *testing.T) {
	vars := GovernorVars{
		JobVars:    map[string]interface{}{"region": "us-east-1"},
		Meta:       &Meta{AWSSandboxed: true},
		SandboxAPI: map[string]interface{}{"url": "http://sandbox"},
		Extra:      map[string]interface{}{"scm_ref_var": "agnosticd_scm_ref"},
	}

	all := vars.AllVars()
	if all["job_vars"] == nil {
		t.Error("job_vars missing from AllVars")
	}
	jv, ok := all["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("job_vars is not a map")
	}
	if jv["__meta__"] == nil {
		t.Error("__meta__ missing from job_vars in AllVars")
	}
	if all["sandbox_api"] == nil {
		t.Error("sandbox_api missing from AllVars")
	}
	if all["scm_ref_var"] != "agnosticd_scm_ref" {
		t.Error("extra field missing from AllVars")
	}
}

func TestSubjectVarsExtraFields(t *testing.T) {
	raw := `{
		"current_state": "started",
		"desired_state": "started",
		"healthy": true,
		"job_vars": {"guid": "abc"},
		"provision_data": {"console": "https://console.example.com"},
		"action_schedule": {"stop": "8h"},
		"check_status_request_timestamp": "2026-06-18T22:00:00Z",
		"provision_messages": ["provisioned successfully"]
	}`

	var vars SubjectVars
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Typed fields
	if vars.CurrentState != "started" {
		t.Errorf("current_state = %q", vars.CurrentState)
	}
	if vars.JobVars["guid"] != "abc" {
		t.Error("job_vars not parsed")
	}

	// Extra fields
	if vars.Get("provision_data") == nil {
		t.Error("provision_data not captured in Extra")
	}
	if vars.Get("action_schedule") == nil {
		t.Error("action_schedule not captured in Extra")
	}
	if vars.GetString("check_status_request_timestamp") != "2026-06-18T22:00:00Z" {
		t.Error("check_status_request_timestamp not captured")
	}
	if vars.Get("provision_messages") == nil {
		t.Error("provision_messages not captured in Extra")
	}
}

func TestSubjectVarsMarshalRoundTrip(t *testing.T) {
	healthy := true
	original := SubjectVars{
		CurrentState: "started",
		DesiredState: "started",
		Healthy:      &healthy,
		JobVars:      map[string]interface{}{"guid": "abc"},
		Extra: map[string]interface{}{
			"provision_data":  map[string]interface{}{"console": "https://console.example.com"},
			"action_schedule": map[string]interface{}{"stop": "8h"},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var roundTripped SubjectVars
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if roundTripped.CurrentState != "started" {
		t.Error("typed field lost during round-trip")
	}
	if roundTripped.Get("provision_data") == nil {
		t.Error("extra field lost during round-trip")
	}
}

func TestSubjectVarsAllVars(t *testing.T) {
	healthy := true
	vars := SubjectVars{
		CurrentState: "started",
		DesiredState: "started",
		Healthy:      &healthy,
		JobVars:      map[string]interface{}{"guid": "abc"},
		Extra: map[string]interface{}{
			"provision_data":  map[string]interface{}{"console": "url"},
			"action_schedule": map[string]interface{}{"stop": "8h"},
		},
	}

	all := vars.AllVars()

	// Typed fields present
	if all["current_state"] != "started" {
		t.Error("current_state missing from AllVars")
	}
	if all["desired_state"] != "started" {
		t.Error("desired_state missing from AllVars")
	}
	if all["healthy"] != true {
		t.Error("healthy missing from AllVars")
	}
	if all["job_vars"] == nil {
		t.Error("job_vars missing from AllVars")
	}

	// Extra fields present
	if all["provision_data"] == nil {
		t.Error("provision_data missing from AllVars")
	}
	if all["action_schedule"] == nil {
		t.Error("action_schedule missing from AllVars")
	}
}

func TestRunPayloadMarshal(t *testing.T) {
	payload := RunPayload{
		Handler: Handler{Type: "action", Name: "provision"},
		Subject: Subject{
			Metadata: ObjectMeta{Name: "subj-1"},
			Spec: SubjectSpec{
				Vars: SubjectVars{
					CurrentState: "started",
					DesiredState: "started",
				},
			},
		},
		Run: Run{Metadata: ObjectMeta{Name: "run-1"}},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded RunPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.Subject.Spec.Vars.CurrentState != "started" {
		t.Errorf("current_state = %q, want %q", decoded.Subject.Spec.Vars.CurrentState, "started")
	}
	if decoded.Action != nil {
		t.Error("action should be nil when omitted")
	}
}
