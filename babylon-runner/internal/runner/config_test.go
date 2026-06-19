package runner

import (
	"os"
	"testing"
	"time"
)

func TestConfigFromEnv(t *testing.T) {
	// Set all env vars including optional ones.
	os.Setenv("ANARCHY_URL", "https://anarchy.example.com")
	os.Setenv("RUNNER_NAME", "test-runner")
	os.Setenv("RUNNER_TOKEN", "s3cret")
	os.Setenv("HOSTNAME", "pod-abc-123")
	os.Setenv("POLLING_INTERVAL", "10")
	os.Setenv("REQUEST_TIMEOUT", "60")
	os.Setenv("SANDBOX_API_URL", "http://custom-sandbox:9090")
	defer func() {
		os.Unsetenv("ANARCHY_URL")
		os.Unsetenv("RUNNER_NAME")
		os.Unsetenv("RUNNER_TOKEN")
		os.Unsetenv("HOSTNAME")
		os.Unsetenv("POLLING_INTERVAL")
		os.Unsetenv("REQUEST_TIMEOUT")
		os.Unsetenv("SANDBOX_API_URL")
	}()

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.AnarchyURL != "https://anarchy.example.com" {
		t.Errorf("AnarchyURL = %q, want %q", cfg.AnarchyURL, "https://anarchy.example.com")
	}
	if cfg.RunnerName != "test-runner" {
		t.Errorf("RunnerName = %q, want %q", cfg.RunnerName, "test-runner")
	}
	if cfg.RunnerToken != "s3cret" {
		t.Errorf("RunnerToken = %q, want %q", cfg.RunnerToken, "s3cret")
	}
	if cfg.PodName != "pod-abc-123" {
		t.Errorf("PodName = %q, want %q", cfg.PodName, "pod-abc-123")
	}
	if cfg.PollingInterval != 10*time.Second {
		t.Errorf("PollingInterval = %v, want %v", cfg.PollingInterval, 10*time.Second)
	}
	if cfg.RequestTimeout != 60*time.Second {
		t.Errorf("RequestTimeout = %v, want %v", cfg.RequestTimeout, 60*time.Second)
	}
	if cfg.SandboxAPIURL != "http://custom-sandbox:9090" {
		t.Errorf("SandboxAPIURL = %q, want %q", cfg.SandboxAPIURL, "http://custom-sandbox:9090")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	os.Setenv("ANARCHY_URL", "https://anarchy.example.com")
	os.Setenv("RUNNER_NAME", "test-runner")
	os.Setenv("RUNNER_TOKEN", "s3cret")
	os.Setenv("HOSTNAME", "pod-abc-123")
	os.Unsetenv("POLLING_INTERVAL")
	os.Unsetenv("REQUEST_TIMEOUT")
	os.Unsetenv("SANDBOX_API_URL")
	defer func() {
		os.Unsetenv("ANARCHY_URL")
		os.Unsetenv("RUNNER_NAME")
		os.Unsetenv("RUNNER_TOKEN")
		os.Unsetenv("HOSTNAME")
	}()

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PollingInterval != 5*time.Second {
		t.Errorf("PollingInterval default = %v, want %v", cfg.PollingInterval, 5*time.Second)
	}
	if cfg.RequestTimeout != 35*time.Second {
		t.Errorf("RequestTimeout default = %v, want %v", cfg.RequestTimeout, 35*time.Second)
	}
	if cfg.SandboxAPIURL != DefaultSandboxAPIURL {
		t.Errorf("SandboxAPIURL default = %q, want %q", cfg.SandboxAPIURL, DefaultSandboxAPIURL)
	}
}

func TestConfigFromEnvMissingRequired(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
	}{
		{
			name: "missing ANARCHY_URL",
			envVars: map[string]string{
				"RUNNER_NAME": "test-runner", "RUNNER_TOKEN": "s3cret", "HOSTNAME": "pod",
			},
		},
		{
			name: "missing RUNNER_NAME",
			envVars: map[string]string{
				"ANARCHY_URL": "http://example.com", "RUNNER_TOKEN": "s3cret", "HOSTNAME": "pod",
			},
		},
		{
			name: "missing RUNNER_TOKEN",
			envVars: map[string]string{
				"ANARCHY_URL": "http://example.com", "RUNNER_NAME": "test-runner", "HOSTNAME": "pod",
			},
		},
		{
			name: "missing HOSTNAME",
			envVars: map[string]string{
				"ANARCHY_URL": "http://example.com", "RUNNER_NAME": "test-runner", "RUNNER_TOKEN": "s3cret",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars.
			for _, k := range []string{"ANARCHY_URL", "RUNNER_NAME", "RUNNER_TOKEN", "HOSTNAME"} {
				os.Unsetenv(k)
			}
			// Set only what the test case provides.
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}
			defer func() {
				for k := range tt.envVars {
					os.Unsetenv(k)
				}
			}()

			_, err := ConfigFromEnv()
			if err == nil {
				t.Fatal("expected error for missing required variable, got nil")
			}
		})
	}
}

func TestAuthHeader(t *testing.T) {
	cfg := Config{
		RunnerName:  "runner",
		PodName:     "pod",
		RunnerToken: "token",
	}
	want := "Bearer runner:pod:token"
	got := cfg.AuthHeader()
	if got != want {
		t.Errorf("AuthHeader() = %q, want %q", got, want)
	}
}

func TestEnvIntInvalidValue(t *testing.T) {
	os.Setenv("TEST_INVALID_INT", "not-a-number")
	defer os.Unsetenv("TEST_INVALID_INT")

	result := envInt("TEST_INVALID_INT", 42)
	if result != 42 {
		t.Errorf("envInt with invalid value = %d, want %d (default)", result, 42)
	}
}

func TestEnvIntNegative(t *testing.T) {
	os.Setenv("TEST_NEGATIVE_INT", "-10")
	defer os.Unsetenv("TEST_NEGATIVE_INT")

	result := envInt("TEST_NEGATIVE_INT", 42)
	if result != -10 {
		t.Errorf("envInt with negative value = %d, want %d", result, -10)
	}
}

func TestConfigPollingInterval(t *testing.T) {
	os.Setenv("ANARCHY_URL", "https://anarchy.example.com")
	os.Setenv("RUNNER_NAME", "test-runner")
	os.Setenv("RUNNER_TOKEN", "s3cret")
	os.Setenv("HOSTNAME", "pod-abc-123")
	os.Setenv("POLLING_INTERVAL", "15")
	defer func() {
		os.Unsetenv("ANARCHY_URL")
		os.Unsetenv("RUNNER_NAME")
		os.Unsetenv("RUNNER_TOKEN")
		os.Unsetenv("HOSTNAME")
		os.Unsetenv("POLLING_INTERVAL")
	}()

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PollingInterval != 15*time.Second {
		t.Errorf("PollingInterval = %v, want %v", cfg.PollingInterval, 15*time.Second)
	}
}

func setRequiredEnvs(t *testing.T) {
	t.Helper()
	t.Setenv("ANARCHY_URL", "https://anarchy.example.com")
	t.Setenv("RUNNER_NAME", "test-runner")
	t.Setenv("RUNNER_TOKEN", "s3cret")
	t.Setenv("HOSTNAME", "pod-abc-123")
}

func TestConfigTowerTLS(t *testing.T) {
	setRequiredEnvs(t)

	t.Run("defaults to verify=true", func(t *testing.T) {
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if !cfg.TowerTLSVerify {
			t.Error("TowerTLSVerify should default to true")
		}
	})

	t.Run("TOWER_TLS_VERIFY=false", func(t *testing.T) {
		t.Setenv("TOWER_TLS_VERIFY", "false")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.TowerTLSVerify {
			t.Error("TowerTLSVerify should be false")
		}
	})

	t.Run("TOWER_CA_CERT set", func(t *testing.T) {
		t.Setenv("TOWER_CA_CERT", "/etc/pki/ca.crt")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.TowerCACert != "/etc/pki/ca.crt" {
			t.Errorf("TowerCACert = %q, want /etc/pki/ca.crt", cfg.TowerCACert)
		}
	})
}

func TestConfigActionRetryIntervals(t *testing.T) {
	setRequiredEnvs(t)

	t.Run("default intervals", func(t *testing.T) {
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		expected := []string{"1m", "5m", "10m", "30m", "1h", "2h", "4h", "8h", "16h", "1d"}
		if len(cfg.ActionRetryIntervals) != len(expected) {
			t.Fatalf("len = %d, want %d", len(cfg.ActionRetryIntervals), len(expected))
		}
		for i, v := range expected {
			if cfg.ActionRetryIntervals[i] != v {
				t.Errorf("[%d] = %q, want %q", i, cfg.ActionRetryIntervals[i], v)
			}
		}
	})

	t.Run("custom intervals", func(t *testing.T) {
		t.Setenv("ACTION_RETRY_INTERVALS", "30s,2m,10m")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if len(cfg.ActionRetryIntervals) != 3 {
			t.Fatalf("len = %d, want 3", len(cfg.ActionRetryIntervals))
		}
		if cfg.ActionRetryIntervals[0] != "30s" {
			t.Errorf("[0] = %q, want 30s", cfg.ActionRetryIntervals[0])
		}
	})
}
