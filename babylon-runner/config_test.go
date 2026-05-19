package main

import (
	"os"
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	// Set all env vars including optional ones.
	os.Setenv("ANARCHY_URL", "https://anarchy.example.com")
	os.Setenv("RUNNER_NAME", "test-runner")
	os.Setenv("RUNNER_TOKEN", "s3cret")
	os.Setenv("HOSTNAME", "pod-abc-123")
	os.Setenv("POLLING_INTERVAL", "10")
	os.Setenv("REQUEST_TIMEOUT", "60")
	defer func() {
		os.Unsetenv("ANARCHY_URL")
		os.Unsetenv("RUNNER_NAME")
		os.Unsetenv("RUNNER_TOKEN")
		os.Unsetenv("HOSTNAME")
		os.Unsetenv("POLLING_INTERVAL")
		os.Unsetenv("REQUEST_TIMEOUT")
	}()

	cfg, err := configFromEnv()
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
	if cfg.PollingInterval != 10 {
		t.Errorf("PollingInterval = %d, want %d", cfg.PollingInterval, 10)
	}
	if cfg.RequestTimeout != 60 {
		t.Errorf("RequestTimeout = %d, want %d", cfg.RequestTimeout, 60)
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	os.Setenv("ANARCHY_URL", "https://anarchy.example.com")
	os.Setenv("RUNNER_NAME", "test-runner")
	os.Setenv("RUNNER_TOKEN", "s3cret")
	os.Setenv("HOSTNAME", "pod-abc-123")
	os.Unsetenv("POLLING_INTERVAL")
	os.Unsetenv("REQUEST_TIMEOUT")
	defer func() {
		os.Unsetenv("ANARCHY_URL")
		os.Unsetenv("RUNNER_NAME")
		os.Unsetenv("RUNNER_TOKEN")
		os.Unsetenv("HOSTNAME")
	}()

	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PollingInterval != 5 {
		t.Errorf("PollingInterval default = %d, want %d", cfg.PollingInterval, 5)
	}
	if cfg.RequestTimeout != 35 {
		t.Errorf("RequestTimeout default = %d, want %d", cfg.RequestTimeout, 35)
	}
}

func TestConfigFromEnvMissingRequired(t *testing.T) {
	// Clear all required vars.
	os.Unsetenv("ANARCHY_URL")
	os.Setenv("RUNNER_NAME", "test-runner")
	os.Setenv("RUNNER_TOKEN", "s3cret")
	os.Setenv("HOSTNAME", "pod-abc-123")
	defer func() {
		os.Unsetenv("RUNNER_NAME")
		os.Unsetenv("RUNNER_TOKEN")
		os.Unsetenv("HOSTNAME")
	}()

	_, err := configFromEnv()
	if err == nil {
		t.Fatal("expected error for missing ANARCHY_URL, got nil")
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

	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PollingInterval != 15 {
		t.Errorf("PollingInterval = %d, want %d", cfg.PollingInterval, 15)
	}
}
