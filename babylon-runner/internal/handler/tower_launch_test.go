package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// --- TestGetDeployerEntryPoint ---

func TestGetDeployerEntryPoint(t *testing.T) {
	tests := []struct {
		name   string
		meta   *types.Meta
		action string
		want   string
	}{
		{
			name:   "nil meta - provision",
			meta:   nil,
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name:   "nil meta - destroy",
			meta:   nil,
			action: "destroy",
			want:   "ansible/destroy.yml",
		},
		{
			name:   "nil meta - start",
			meta:   nil,
			action: "start",
			want:   "ansible/lifecycle_entry_point.yml",
		},
		{
			name:   "nil meta - stop",
			meta:   nil,
			action: "stop",
			want:   "ansible/lifecycle_entry_point.yml",
		},
		{
			name:   "nil meta - status",
			meta:   nil,
			action: "status",
			want:   "ansible/lifecycle_entry_point.yml",
		},
		{
			name:   "nil meta - update",
			meta:   nil,
			action: "update",
			want:   "ansible/lifecycle_entry_point.yml",
		},
		{
			name:   "nil meta - unknown action",
			meta:   nil,
			action: "unknown",
			want:   "ansible/main.yml",
		},
		{
			name:   "empty meta with nil deployer - provision",
			meta:   &types.Meta{},
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name: "per-action entry point for provision",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{
						"provision": {EntryPoint: "custom/provision.yml"},
					},
				},
			},
			action: "provision",
			want:   "custom/provision.yml",
		},
		{
			name: "per-action entry point for destroy",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{
						"destroy": {EntryPoint: "custom/destroy.yml"},
					},
				},
			},
			action: "destroy",
			want:   "custom/destroy.yml",
		},
		{
			// Old test "generic entry_point fallback" is not applicable:
			// DeployerMeta has no generic entry_point field. The new code
			// only checks per-action entry_point, then falls back to defaults.
			name: "no per-action entry_point - returns default",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{
						"provision": {}, // empty config
					},
				},
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name: "action not present in deployer actions - returns default",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{
						"destroy": {EntryPoint: "custom/destroy.yml"},
					},
				},
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getDeployerEntryPoint(tt.meta, tt.action)
			if got != tt.want {
				t.Errorf("getDeployerEntryPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- TestGetTowerClientForAction ---

func TestGetTowerClientForAction(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*runner.RunContext)
		wantHost    string
		wantErr     bool
		errContains string
	}{
		{
			// rc.Meta() returns &types.Meta{} when no Meta is set.
			// Empty Meta has no AnsibleControllers, so this errors.
			name: "no meta set - returns error",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = nil
			},
			wantErr:     true,
			errContains: "no ansible_controllers",
		},
		{
			name: "empty controllers list - returns error",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					AnsibleControllers: []map[string]interface{}{},
				}
			},
			wantErr:     true,
			errContains: "no ansible_controllers",
		},
		{
			name: "controller with no hostname - returns error",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					AnsibleControllers: []map[string]interface{}{
						{},
					},
				}
			},
			wantErr:     true,
			errContains: "controller has no hostname",
		},
		{
			name: "valid controller - returns client",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					AnsibleControllers: []map[string]interface{}{
						{
							"hostname": "tower1.example.com",
						},
					},
				}
				rc.SecretCache = newTestSecretCache("tower1.example.com", "admin", "secret")
			},
			wantHost: "tower1.example.com",
			wantErr:  false,
		},
		{
			name: "multiple controllers with balance mode - selects by job count",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					AnsibleControllerSelectMode: "balance",
					AnsibleControllers: []map[string]interface{}{
						{
							"hostname":         "tower1.example.com",
							"active_job_count": float64(10),
						},
						{
							"hostname":         "tower2.example.com",
							"active_job_count": float64(5),
						},
						{
							"hostname":         "tower3.example.com",
							"active_job_count": float64(15),
						},
					},
				}
				rc.SecretCache = newTestSecretCacheMulti(map[string][2]string{
					"tower1.example.com": {"admin", "secret"},
					"tower2.example.com": {"admin", "secret"},
					"tower3.example.com": {"admin", "secret"},
				})
			},
			wantHost: "tower2.example.com",
			wantErr:  false,
		},
		{
			name: "multiple controllers with random mode - returns one",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					AnsibleControllerSelectMode: "random",
					AnsibleControllers: []map[string]interface{}{
						{
							"hostname": "tower1.example.com",
						},
						{
							"hostname": "tower2.example.com",
						},
					},
				}
				rc.SecretCache = newTestSecretCacheMulti(map[string][2]string{
					"tower1.example.com": {"admin", "secret"},
					"tower2.example.com": {"admin", "secret"},
				})
			},
			// Can't predict which one with random, just check no error.
			wantErr: false,
		},
		// Old tests "controllers list with non-map values" and "all controllers
		// are non-map" are not applicable: AnsibleControllers is typed as
		// []map[string]interface{}, so non-map values cannot be stored in the slice.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := newTestAnarchyServer(t)
			defer server.Close()

			rc := newTestRunContext(t, server)
			tt.setup(rc)

			client, hostname, err := getTowerClientForAction(rc)

			if tt.wantErr {
				if err == nil {
					t.Errorf("getTowerClientForAction() expected error containing %q, got nil", tt.errContains)
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("getTowerClientForAction() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("getTowerClientForAction() unexpected error: %v", err)
			}

			if client == nil {
				t.Fatal("getTowerClientForAction() returned nil client")
			}

			if tt.wantHost != "" && hostname != tt.wantHost {
				t.Errorf("getTowerClientForAction() hostname = %v, want %v", hostname, tt.wantHost)
			}
		})
	}
}

// --- TestBuildJobExtraVars ---

func TestBuildJobExtraVars(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		dynamicJobVars map[string]interface{}
		setup          func(*runner.RunContext)
		want           map[string]interface{}
	}{
		{
			name:   "governor job_vars + subject job_vars merged with ACTION and output_dir",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
					"region":         "us-east-1",
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"guid": "abc123",
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				"region":         "us-east-1",
				"guid":           "abc123",
				"uuid":           "xyz789",
				"ACTION":         "provision",
				"output_dir":     "/tmp/output-xyz789",
			},
		},
		{
			name:   "governor vars override subject vars",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
					"region":         "us-east-1",
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"region": "us-west-2", // Subject sets region
					"guid":   "abc123",
					"uuid":   "xyz789",
				}
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				"region":         "us-east-1", // Governor wins
				"guid":           "abc123",
				"uuid":           "xyz789",
				"ACTION":         "provision",
				"output_dir":     "/tmp/output-xyz789",
			},
		},
		{
			name:   "dynamic vars override governor and subject",
			action: "provision",
			dynamicJobVars: map[string]interface{}{
				"sandbox_name":          "sandbox-123",
				"aws_access_key_id":     "AKIA...",
				"aws_secret_access_key": "secret...",
			},
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"guid": "abc123",
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"cloud_provider":        "ec2",
				"guid":                  "abc123",
				"uuid":                  "xyz789",
				"sandbox_name":          "sandbox-123",
				"aws_access_key_id":     "AKIA...",
				"aws_secret_access_key": "secret...",
				"ACTION":                "provision",
				"output_dir":            "/tmp/output-xyz789",
			},
		},
		{
			name:   "__meta__ removed from merged vars",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				// __meta__ present in governor job_vars must be stripped.
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
					"__meta__": map[string]interface{}{
						"deployer": map[string]interface{}{
							"type": "agnosticd",
						},
					},
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"guid": "abc123",
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				// __meta__ should NOT be present
				"guid":       "abc123",
				"uuid":       "xyz789",
				"ACTION":     "provision",
				"output_dir": "/tmp/output-xyz789",
			},
		},
		{
			name:   "callback URL and token from action spec",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
				rc.Payload.Action = &types.Action{
					Spec: types.ActionSpec{
						CallbackUrl:   "https://example.com/callback",
						CallbackToken: "secret-token",
					},
				}
			},
			want: map[string]interface{}{
				"uuid":                     "xyz789",
				"agnosticd_callback_url":   "https://example.com/callback",
				"agnosticd_callback_token": "secret-token",
				"ACTION":                   "provision",
				"output_dir":               "/tmp/output-xyz789",
			},
		},
		{
			name:   "custom callback var names from deployer config",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = nil
				rc.Payload.Governor.Spec.Vars.Extra = nil
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"__meta__": map[string]interface{}{
						"deployer": map[string]interface{}{
							"callback_url_var":   "custom_callback_url",
							"callback_token_var": "custom_callback_token",
						},
					},
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
				rc.Payload.Action = &types.Action{
					Spec: types.ActionSpec{
						CallbackUrl:   "https://example.com/callback",
						CallbackToken: "secret-token",
					},
				}
			},
			want: map[string]interface{}{
				"uuid":                  "xyz789",
				"custom_callback_url":   "https://example.com/callback",
				"custom_callback_token": "secret-token",
				"ACTION":                "provision",
				"output_dir":            "/tmp/output-xyz789",
			},
		},
		{
			name:   "action extra_vars from deployer config override ACTION default",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = nil
				rc.Payload.Governor.Spec.Vars.Extra = nil
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"__meta__": map[string]interface{}{
						"deployer": map[string]interface{}{
							"actions": map[string]interface{}{
								"provision": map[string]interface{}{
									"extra_vars": map[string]interface{}{
										"ACTION":     "custom_provision",
										"extra_flag": true,
									},
								},
							},
						},
					},
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"uuid":       "xyz789",
				"ACTION":     "custom_provision",
				"extra_flag": true,
				"output_dir": "/tmp/output-xyz789",
			},
		},
		{
			name:   "no action - no callback vars",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Action = nil
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				"uuid":           "xyz789",
				"ACTION":         "provision",
				"output_dir":     "/tmp/output-xyz789",
			},
		},
		{
			name:   "output_dir not set when deployer type is not agnosticd",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					Deployer: &types.DeployerMeta{
						Type: "helm",
					},
				}
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"uuid":   "xyz789",
				"ACTION": "provision",
				// no output_dir
			},
		},
		{
			// scm_ref_var injection: rawDeployer is now accessible via
			// AllVars() injecting Meta into job_vars, but scm_ref_var is
			// not a field in DeployerMeta so it won't appear in rawDeployer.
			// No injection occurs.
			name:   "scm_ref_var injection - typed Meta path",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
					Deployer: &types.DeployerMeta{
						SCMRef: "v2.0.0",
					},
				}
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"uuid":       "xyz789",
				"ACTION":     "provision",
				"output_dir": "/tmp/output-xyz789",
			},
		},
		{
			// Raw path: __meta__ is in job_vars (raw map), so rawDeployer
			// is accessible. But Meta is not auto-populated from programmatic
			// job_vars, so meta.Deployer is nil and SCMRef is empty.
			// scm_ref_var injection does not occur.
			name:   "scm_ref_var injection - raw job_vars path",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.Meta = nil
				rc.Payload.Governor.Spec.Vars.Extra = nil
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"__meta__": map[string]interface{}{
						"deployer": map[string]interface{}{
							"scm_ref":     "v2.0.0",
							"scm_ref_var": "agnosticd_version",
						},
					},
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"uuid":       "xyz789",
				"ACTION":     "provision",
				"output_dir": "/tmp/output-xyz789",
			},
		},
		{
			name:   "destroy action gets ACTION=destroy",
			action: "destroy",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"uuid":       "xyz789",
				"ACTION":     "destroy",
				"output_dir": "/tmp/output-xyz789",
			},
		},
		{
			name:   "nil job_vars on governor only",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"guid": "abc123",
					"uuid": "xyz789",
				}
			},
			want: map[string]interface{}{
				"guid":       "abc123",
				"uuid":       "xyz789",
				"ACTION":     "provision",
				"output_dir": "/tmp/output-xyz789",
			},
		},
		{
			name:   "nil job_vars on subject only",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
				}
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				"ACTION":         "provision",
				// No output_dir because no uuid
			},
		},
		{
			name:   "all job_vars and callback vars combined",
			action: "provision",
			setup: func(rc *runner.RunContext) {
				rc.Payload.Governor.Spec.Vars.JobVars = map[string]interface{}{
					"cloud_provider": "ec2",
					"region":         "us-east-1",
				}
				rc.Payload.Subject.Spec.Vars.JobVars = map[string]interface{}{
					"guid": "abc123",
					"uuid": "xyz789",
				}
				rc.Payload.Action = &types.Action{
					Spec: types.ActionSpec{
						CallbackUrl:   "https://example.com/callback",
						CallbackToken: "secret-token",
					},
				}
			},
			want: map[string]interface{}{
				"cloud_provider":           "ec2",
				"region":                   "us-east-1",
				"guid":                     "abc123",
				"uuid":                     "xyz789",
				"agnosticd_callback_url":   "https://example.com/callback",
				"agnosticd_callback_token": "secret-token",
				"ACTION":                   "provision",
				"output_dir":               "/tmp/output-xyz789",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := newTestAnarchyServer(t)
			defer server.Close()

			rc := newTestRunContext(t, server)
			tt.setup(rc)

			action := tt.action
			if action == "" {
				action = "provision"
			}
			got := buildJobExtraVars(rc, action, tt.dynamicJobVars)

			if len(got) != len(tt.want) {
				t.Errorf("buildJobExtraVars() returned %d vars, want %d\ngot:  %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}

			for k, v := range tt.want {
				if !interfaceEqual(got[k], v) {
					t.Errorf("buildJobExtraVars()[%q] = %v, want %v", k, got[k], v)
				}
			}

			// Verify __meta__ is never present.
			if _, hasMetaKey := got["__meta__"]; hasMetaKey {
				t.Error("buildJobExtraVars() should not contain __meta__ key")
			}
		})
	}
}

// --- TestCancelTowerJob ---

func TestCancelTowerJob(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*runner.RunContext, *httptest.Server)
		action       string
		wantCanceled bool
	}{
		{
			name: "no tower job for action - no-op",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				withTowerServer(rc, ts)
				// No towerJobs set
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "job already complete - skips",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":       float64(42),
						"startTimestamp":     "2024-01-01T00:00:00Z",
						"completeTimestamp":  "2024-01-01T01:00:00Z",
						"towerHost":         host,
					},
				}
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "no deployerJob field - skips",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"startTimestamp": "2024-01-01T00:00:00Z",
						"towerHost":     host,
					},
				}
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "deployerJob is zero - skips",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(0),
						"startTimestamp": "2024-01-01T00:00:00Z",
						"towerHost":     host,
					},
				}
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "success - cancels the job",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(42),
						"startTimestamp": "2024-01-01T00:00:00Z",
						"towerHost":     host,
					},
				}
			},
			action:       "provision",
			wantCanceled: true,
		},
		{
			name: "multiple actions - only cancels specified action",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(42),
						"startTimestamp": "2024-01-01T00:00:00Z",
						"towerHost":     host,
					},
					"destroy": map[string]interface{}{
						"deployerJob":    float64(43),
						"startTimestamp": "2024-01-01T00:00:00Z",
						"towerHost":     host,
					},
				}
			},
			action:       "provision",
			wantCanceled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cancelCount := 0
			towerServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
					json.NewEncoder(w).Encode(map[string]interface{}{
						"id":    float64(1),
						"token": "test-token",
					})
				case r.Method == "DELETE" && strings.Contains(r.URL.Path, "/api/v2/tokens/"):
					w.WriteHeader(http.StatusNoContent)
				case r.Method == "POST" && strings.Contains(r.URL.Path, "/cancel/"):
					cancelCount++
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer towerServer.Close()

			anarchyServer, _ := newTestAnarchyServer(t)
			defer anarchyServer.Close()

			rc := newTestRunContext(t, anarchyServer)
			tt.setup(rc, towerServer)

			cancelTowerJob(rc, tt.action)

			if tt.wantCanceled && cancelCount != 1 {
				t.Errorf("cancelTowerJob() canceled %d jobs, want 1", cancelCount)
			}
			if !tt.wantCanceled && cancelCount != 0 {
				t.Errorf("cancelTowerJob() canceled %d jobs, want 0", cancelCount)
			}
		})
	}
}

func TestCancelTowerJobFailure(t *testing.T) {
	// Test that cancel failure is logged but doesn't return error.
	towerServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    float64(1),
				"token": "test-token",
			})
		case r.Method == "DELETE" && strings.Contains(r.URL.Path, "/api/v2/tokens/"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == "POST" && strings.Contains(r.URL.Path, "/cancel/"):
			// Return failure.
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer towerServer.Close()

	anarchyServer, _ := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	host := towerServerHost(t, towerServer)
	withTowerServer(rc, towerServer)
	rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob":    float64(42),
			"startTimestamp": "2024-01-01T00:00:00Z",
			"towerHost":     host,
		},
	}

	// Should not panic or return error.
	cancelTowerJob(rc, "provision")
}

// --- TestCancelAllIncompleteTowerJobs ---

func TestCancelAllIncompleteTowerJobs(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*runner.RunContext, *httptest.Server)
		wantCount int
	}{
		{
			name: "nil towerJobs - no-op",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				withTowerServer(rc, ts)
				// No towerJobs set
			},
			wantCount: 0,
		},
		{
			name: "multiple actions, some complete, some not - only cancels incomplete",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":       float64(42),
						"startTimestamp":     "2024-01-01T00:00:00Z",
						"completeTimestamp":  "2024-01-01T01:00:00Z",
						"towerHost":         host,
					},
					"destroy": map[string]interface{}{
						"deployerJob":    float64(43),
						"startTimestamp": "2024-01-01T02:00:00Z",
						"towerHost":     host,
						// No completeTimestamp - should be canceled
					},
					"start": map[string]interface{}{
						"deployerJob":    float64(44),
						"startTimestamp": "2024-01-01T03:00:00Z",
						"towerHost":     host,
						// No completeTimestamp - should be canceled
					},
				}
			},
			wantCount: 2,
		},
		{
			name: "non-map values in towerJobs - skipped",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(42),
						"startTimestamp": "2024-01-01T00:00:00Z",
						"towerHost":     host,
					},
					"invalid": "not-a-map",
				}
			},
			wantCount: 1,
		},
		{
			name: "all jobs complete - no cancels",
			setup: func(rc *runner.RunContext, ts *httptest.Server) {
				host := towerServerHost(t, ts)
				withTowerServer(rc, ts)
				rc.Payload.Subject.Status.TowerJobs = map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":       float64(42),
						"startTimestamp":     "2024-01-01T00:00:00Z",
						"completeTimestamp":  "2024-01-01T01:00:00Z",
						"towerHost":         host,
					},
					"destroy": map[string]interface{}{
						"deployerJob":       float64(43),
						"startTimestamp":     "2024-01-01T02:00:00Z",
						"completeTimestamp":  "2024-01-01T03:00:00Z",
						"towerHost":         host,
					},
				}
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cancelCount := 0
			towerServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
					json.NewEncoder(w).Encode(map[string]interface{}{
						"id":    float64(1),
						"token": "test-token",
					})
				case r.Method == "DELETE" && strings.Contains(r.URL.Path, "/api/v2/tokens/"):
					w.WriteHeader(http.StatusNoContent)
				case r.Method == "POST" && strings.Contains(r.URL.Path, "/cancel/"):
					cancelCount++
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer towerServer.Close()

			anarchyServer, _ := newTestAnarchyServer(t)
			defer anarchyServer.Close()

			rc := newTestRunContext(t, anarchyServer)
			tt.setup(rc, towerServer)

			cancelAllIncompleteTowerJobs(rc)

			if cancelCount != tt.wantCount {
				t.Errorf("cancelAllIncompleteTowerJobs() canceled %d jobs, want %d", cancelCount, tt.wantCount)
			}
		})
	}
}

// --- TestExtractProvisionData ---

func TestExtractProvisionData(t *testing.T) {
	tests := []struct {
		name            string
		jobStatus       map[string]interface{}
		wantData        interface{}
		wantMessageBody interface{}
		wantMessages    interface{}
	}{
		{
			name: "job status with all three fields",
			jobStatus: map[string]interface{}{
				"artifacts": map[string]interface{}{
					"provision_data": map[string]interface{}{
						"key1": "value1",
						"key2": "value2",
					},
					"provision_message_body": "Provision completed successfully",
					"provision_messages": []interface{}{
						"Message 1",
						"Message 2",
					},
				},
			},
			wantData: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
			wantMessageBody: "Provision completed successfully",
			wantMessages: []interface{}{
				"Message 1",
				"Message 2",
			},
		},
		{
			name: "no artifacts - nil for all",
			jobStatus: map[string]interface{}{
				"status": "successful",
			},
			wantData:        nil,
			wantMessageBody: nil,
			wantMessages:    nil,
		},
		{
			name: "partial artifacts - returns what's present",
			jobStatus: map[string]interface{}{
				"artifacts": map[string]interface{}{
					"provision_data": map[string]interface{}{
						"key1": "value1",
					},
				},
			},
			wantData: map[string]interface{}{
				"key1": "value1",
			},
			wantMessageBody: nil,
			wantMessages:    nil,
		},
		{
			name: "artifacts present but provision fields nil",
			jobStatus: map[string]interface{}{
				"artifacts": map[string]interface{}{
					"other_field": "value",
				},
			},
			wantData:        nil,
			wantMessageBody: nil,
			wantMessages:    nil,
		},
		{
			name:            "nil job status - nil for all",
			jobStatus:       nil,
			wantData:        nil,
			wantMessageBody: nil,
			wantMessages:    nil,
		},
		{
			name:            "empty job status - nil for all",
			jobStatus:       map[string]interface{}{},
			wantData:        nil,
			wantMessageBody: nil,
			wantMessages:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, messageBody, messages := extractProvisionData(tt.jobStatus)

			if !interfaceEqual(data, tt.wantData) {
				t.Errorf("extractProvisionData() data = %v, want %v", data, tt.wantData)
			}
			if !interfaceEqual(messageBody, tt.wantMessageBody) {
				t.Errorf("extractProvisionData() messageBody = %v, want %v", messageBody, tt.wantMessageBody)
			}
			if !interfaceEqual(messages, tt.wantMessages) {
				t.Errorf("extractProvisionData() messages = %v, want %v", messages, tt.wantMessages)
			}
		})
	}
}

// --- Helper functions ---

// interfaceEqual does a deep equality check for interface{} types.
func interfaceEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		if len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !interfaceEqual(v, bv[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok {
			return false
		}
		if len(av) != len(bv) {
			return false
		}
		for i, v := range av {
			if !interfaceEqual(v, bv[i]) {
				return false
			}
		}
		return true
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case int:
		bv, ok := b.(int)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	default:
		return a == b
	}
}
