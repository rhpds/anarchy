package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- TestGetDeployerEntryPoint ---

func TestGetDeployerEntryPoint(t *testing.T) {
	tests := []struct {
		name     string
		deployer map[string]interface{}
		action   string
		want     string
	}{
		{
			name:     "nil deployer - provision",
			deployer: nil,
			action:   "provision",
			want:     "ansible/main.yml",
		},
		{
			name:     "nil deployer - destroy",
			deployer: nil,
			action:   "destroy",
			want:     "ansible/destroy.yml",
		},
		{
			name:     "nil deployer - start",
			deployer: nil,
			action:   "start",
			want:     "ansible/lifecycle_entry_point.yml",
		},
		{
			name:     "nil deployer - stop",
			deployer: nil,
			action:   "stop",
			want:     "ansible/lifecycle_entry_point.yml",
		},
		{
			name:     "nil deployer - status",
			deployer: nil,
			action:   "status",
			want:     "ansible/lifecycle_entry_point.yml",
		},
		{
			name:     "nil deployer - update",
			deployer: nil,
			action:   "update",
			want:     "ansible/lifecycle_entry_point.yml",
		},
		{
			name:     "nil deployer - unknown action",
			deployer: nil,
			action:   "unknown",
			want:     "ansible/main.yml",
		},
		{
			name: "custom entry point for provision",
			deployer: map[string]interface{}{
				"entry_points": map[string]interface{}{
					"provision": "custom/provision.yml",
				},
			},
			action: "provision",
			want:   "custom/provision.yml",
		},
		{
			name: "custom entry point for destroy",
			deployer: map[string]interface{}{
				"entry_points": map[string]interface{}{
					"destroy": "custom/destroy.yml",
				},
			},
			action: "destroy",
			want:   "custom/destroy.yml",
		},
		{
			name: "entry point set to disabled - returns default",
			deployer: map[string]interface{}{
				"entry_points": map[string]interface{}{
					"provision": "disabled",
				},
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name: "entry point set to none - returns default",
			deployer: map[string]interface{}{
				"entry_points": map[string]interface{}{
					"provision": "none",
				},
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name: "entry point empty string - returns default",
			deployer: map[string]interface{}{
				"entry_points": map[string]interface{}{
					"provision": "",
				},
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name: "entry_points is nil - returns default",
			deployer: map[string]interface{}{
				"entry_points": nil,
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
		{
			name: "entry_points exists but action not present",
			deployer: map[string]interface{}{
				"entry_points": map[string]interface{}{
					"destroy": "custom/destroy.yml",
				},
			},
			action: "provision",
			want:   "ansible/main.yml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getDeployerEntryPoint(tt.deployer, tt.action)
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
		setup       func(*RunContext)
		wantHost    string
		wantErr     bool
		errContains string
	}{
		{
			name: "TowerBaseURL set - returns test client",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = "http://test-tower"
			},
			wantHost: "test-tower",
			wantErr:  false,
		},
		{
			name: "no __meta__ - returns error",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				// Don't set __meta__
			},
			wantErr:     true,
			errContains: "no __meta__ in governor",
		},
		{
			name: "no ansible_controllers - returns error",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, map[string]interface{}{}, "spec", "vars", "__meta__")
			},
			wantErr:     true,
			errContains: "no ansible_controllers",
		},
		{
			name: "empty controllers list - returns error",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{}, "spec", "vars", "__meta__", "ansible_controllers")
			},
			wantErr:     true,
			errContains: "no ansible_controllers",
		},
		{
			name: "controller with no hostname - returns error",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{
					map[string]interface{}{
						"user":     "admin",
						"password": "secret",
					},
				}, "spec", "vars", "__meta__", "ansible_controllers")
			},
			wantErr:     true,
			errContains: "controller has no hostname",
		},
		{
			name: "valid controller - returns client",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{
					map[string]interface{}{
						"hostname": "tower1.example.com",
						"user":     "admin",
						"password": "secret",
					},
				}, "spec", "vars", "__meta__", "ansible_controllers")
			},
			wantHost: "tower1.example.com",
			wantErr:  false,
		},
		{
			name: "multiple controllers with balance mode - selects by job count",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{
					map[string]interface{}{
						"hostname":         "tower1.example.com",
						"user":             "admin",
						"password":         "secret",
						"active_job_count": float64(10),
					},
					map[string]interface{}{
						"hostname":         "tower2.example.com",
						"user":             "admin",
						"password":         "secret",
						"active_job_count": float64(5),
					},
					map[string]interface{}{
						"hostname":         "tower3.example.com",
						"user":             "admin",
						"password":         "secret",
						"active_job_count": float64(15),
					},
				}, "spec", "vars", "__meta__", "ansible_controllers")
				setNested(rc.Payload.Governor, "balance", "spec", "vars", "__meta__", "ansible_controller_select_mode")
			},
			wantHost: "tower2.example.com",
			wantErr:  false,
		},
		{
			name: "multiple controllers with random mode - returns one",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{
					map[string]interface{}{
						"hostname": "tower1.example.com",
						"user":     "admin",
						"password": "secret",
					},
					map[string]interface{}{
						"hostname": "tower2.example.com",
						"user":     "admin",
						"password": "secret",
					},
				}, "spec", "vars", "__meta__", "ansible_controllers")
				setNested(rc.Payload.Governor, "random", "spec", "vars", "__meta__", "ansible_controller_select_mode")
			},
			// Can't predict which one with random, just check no error
			wantErr: false,
		},
		{
			name: "controllers list with non-map values - skipped",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{
					"not-a-map",
					map[string]interface{}{
						"hostname": "tower1.example.com",
						"user":     "admin",
						"password": "secret",
					},
				}, "spec", "vars", "__meta__", "ansible_controllers")
			},
			wantHost: "tower1.example.com",
			wantErr:  false,
		},
		{
			name: "all controllers are non-map - error",
			setup: func(rc *RunContext) {
				rc.TowerBaseURL = ""
				setNested(rc.Payload.Governor, []interface{}{
					"not-a-map",
					"also-not-a-map",
				}, "spec", "vars", "__meta__", "ansible_controllers")
			},
			wantErr:     true,
			errContains: "no valid controllers",
		},
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
		name  string
		setup func(*RunContext)
		want  map[string]interface{}
	}{
		{
			name: "governor job_vars + subject job_vars merged",
			setup: func(rc *RunContext) {
				setNested(rc.Payload.Governor, map[string]interface{}{
					"cloud_provider": "ec2",
					"region":         "us-east-1",
				}, "spec", "vars", "job_vars")
				setNested(rc.Payload.Subject, map[string]interface{}{
					"guid": "abc123",
					"uuid": "xyz789",
				}, "spec", "vars", "job_vars")
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				"region":         "us-east-1",
				"guid":           "abc123",
				"uuid":           "xyz789",
			},
		},
		{
			name: "subject vars override governor vars",
			setup: func(rc *RunContext) {
				setNested(rc.Payload.Governor, map[string]interface{}{
					"cloud_provider": "ec2",
					"region":         "us-east-1",
				}, "spec", "vars", "job_vars")
				setNested(rc.Payload.Subject, map[string]interface{}{
					"region": "us-west-2", // Override
					"guid":   "abc123",
				}, "spec", "vars", "job_vars")
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
				"region":         "us-west-2",
				"guid":           "abc123",
			},
		},
		{
			name: "callback URL and token from action spec",
			setup: func(rc *RunContext) {
				// Clear default governor job_vars
				setNested(rc.Payload.Governor, map[string]interface{}{}, "spec", "vars", "job_vars")
				rc.Payload.Action = map[string]interface{}{
					"spec": map[string]interface{}{
						"callbackUrl":   "https://example.com/callback",
						"callbackToken": "secret-token",
					},
				}
			},
			want: map[string]interface{}{
				"agnosticd_callback_url":   "https://example.com/callback",
				"agnosticd_callback_token": "secret-token",
			},
		},
		{
			name: "no action - no callback vars",
			setup: func(rc *RunContext) {
				rc.Payload.Action = nil
				setNested(rc.Payload.Governor, map[string]interface{}{
					"cloud_provider": "ec2",
				}, "spec", "vars", "job_vars")
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
			},
		},
		{
			name: "empty job_vars on both - empty result with callback",
			setup: func(rc *RunContext) {
				setNested(rc.Payload.Governor, map[string]interface{}{}, "spec", "vars", "job_vars")
				setNested(rc.Payload.Subject, map[string]interface{}{}, "spec", "vars", "job_vars")
				rc.Payload.Action = map[string]interface{}{
					"spec": map[string]interface{}{
						"callbackUrl": "https://example.com/callback",
					},
				}
			},
			want: map[string]interface{}{
				"agnosticd_callback_url": "https://example.com/callback",
			},
		},
		{
			name: "nil job_vars on governor only",
			setup: func(rc *RunContext) {
				// Clear default governor job_vars
				setNested(rc.Payload.Governor, map[string]interface{}{}, "spec", "vars", "job_vars")
				setNested(rc.Payload.Subject, map[string]interface{}{
					"guid": "abc123",
				}, "spec", "vars", "job_vars")
			},
			want: map[string]interface{}{
				"guid": "abc123",
			},
		},
		{
			name: "nil job_vars on subject only",
			setup: func(rc *RunContext) {
				setNested(rc.Payload.Governor, map[string]interface{}{
					"cloud_provider": "ec2",
				}, "spec", "vars", "job_vars")
			},
			want: map[string]interface{}{
				"cloud_provider": "ec2",
			},
		},
		{
			name: "all job_vars and callback vars combined",
			setup: func(rc *RunContext) {
				setNested(rc.Payload.Governor, map[string]interface{}{
					"cloud_provider": "ec2",
					"region":         "us-east-1",
				}, "spec", "vars", "job_vars")
				setNested(rc.Payload.Subject, map[string]interface{}{
					"guid": "abc123",
				}, "spec", "vars", "job_vars")
				rc.Payload.Action = map[string]interface{}{
					"spec": map[string]interface{}{
						"callbackUrl":   "https://example.com/callback",
						"callbackToken": "secret-token",
					},
				}
			},
			want: map[string]interface{}{
				"cloud_provider":           "ec2",
				"region":                   "us-east-1",
				"guid":                     "abc123",
				"agnosticd_callback_url":   "https://example.com/callback",
				"agnosticd_callback_token": "secret-token",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := newTestAnarchyServer(t)
			defer server.Close()

			rc := newTestRunContext(t, server)
			tt.setup(rc)

			got := buildJobExtraVars(rc, "provision")

			if len(got) != len(tt.want) {
				t.Errorf("buildJobExtraVars() returned %d vars, want %d", len(got), len(tt.want))
			}

			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("buildJobExtraVars()[%q] = %v, want %v", k, got[k], v)
				}
			}
		})
	}
}

// --- TestCancelTowerJob ---

func TestCancelTowerJob(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*RunContext, *httptest.Server)
		action       string
		wantCanceled bool
	}{
		{
			name: "no tower job for action - no-op",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				// No towerJobs set
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "job already complete - skips",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":       float64(42),
						"startTimestamp":    "2024-01-01T00:00:00Z",
						"completeTimestamp": "2024-01-01T01:00:00Z",
					},
				}, "status", "towerJobs")
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "no deployerJob field - skips",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"startTimestamp": "2024-01-01T00:00:00Z",
					},
				}, "status", "towerJobs")
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "deployerJob is zero - skips",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(0),
						"startTimestamp": "2024-01-01T00:00:00Z",
					},
				}, "status", "towerJobs")
			},
			action:       "provision",
			wantCanceled: false,
		},
		{
			name: "success - cancels the job",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(42),
						"startTimestamp": "2024-01-01T00:00:00Z",
					},
				}, "status", "towerJobs")
			},
			action:       "provision",
			wantCanceled: true,
		},
		{
			name: "multiple actions - only cancels specified action",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(42),
						"startTimestamp": "2024-01-01T00:00:00Z",
					},
					"destroy": map[string]interface{}{
						"deployerJob":    float64(43),
						"startTimestamp": "2024-01-01T00:00:00Z",
					},
				}, "status", "towerJobs")
			},
			action:       "provision",
			wantCanceled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cancelCount := 0
			towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	// Test that cancel failure is logged but doesn't return error
	towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    float64(1),
				"token": "test-token",
			})
		case r.Method == "DELETE" && strings.Contains(r.URL.Path, "/api/v2/tokens/"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == "POST" && strings.Contains(r.URL.Path, "/cancel/"):
			// Return failure
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer towerServer.Close()

	anarchyServer, _ := newTestAnarchyServer(t)
	defer anarchyServer.Close()

	rc := newTestRunContext(t, anarchyServer)
	rc.TowerBaseURL = towerServer.URL
	setNested(rc.Payload.Subject, map[string]interface{}{
		"provision": map[string]interface{}{
			"deployerJob":    float64(42),
			"startTimestamp": "2024-01-01T00:00:00Z",
		},
	}, "status", "towerJobs")

	// Should not panic or return error
	cancelTowerJob(rc, "provision")
}

// --- TestCancelAllIncompleteTowerJobs ---

func TestCancelAllIncompleteTowerJobs(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*RunContext, *httptest.Server)
		wantCount int
	}{
		{
			name: "nil towerJobs - no-op",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				// No towerJobs set
			},
			wantCount: 0,
		},
		{
			name: "multiple actions, some complete, some not - only cancels incomplete",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":       float64(42),
						"startTimestamp":    "2024-01-01T00:00:00Z",
						"completeTimestamp": "2024-01-01T01:00:00Z",
					},
					"destroy": map[string]interface{}{
						"deployerJob":    float64(43),
						"startTimestamp": "2024-01-01T02:00:00Z",
						// No completeTimestamp - should be canceled
					},
					"start": map[string]interface{}{
						"deployerJob":    float64(44),
						"startTimestamp": "2024-01-01T03:00:00Z",
						// No completeTimestamp - should be canceled
					},
				}, "status", "towerJobs")
			},
			wantCount: 2,
		},
		{
			name: "non-map values in towerJobs - skipped",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				towerJobs := map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":    float64(42),
						"startTimestamp": "2024-01-01T00:00:00Z",
					},
					"invalid": "not-a-map",
				}
				setNested(rc.Payload.Subject, towerJobs, "status", "towerJobs")
			},
			wantCount: 1,
		},
		{
			name: "all jobs complete - no cancels",
			setup: func(rc *RunContext, ts *httptest.Server) {
				rc.TowerBaseURL = ts.URL
				setNested(rc.Payload.Subject, map[string]interface{}{
					"provision": map[string]interface{}{
						"deployerJob":       float64(42),
						"startTimestamp":    "2024-01-01T00:00:00Z",
						"completeTimestamp": "2024-01-01T01:00:00Z",
					},
					"destroy": map[string]interface{}{
						"deployerJob":       float64(43),
						"startTimestamp":    "2024-01-01T02:00:00Z",
						"completeTimestamp": "2024-01-01T03:00:00Z",
					},
				}, "status", "towerJobs")
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cancelCount := 0
			towerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
// This is a simplified version that works for our test cases.
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
