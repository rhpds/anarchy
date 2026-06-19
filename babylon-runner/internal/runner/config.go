package runner

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// DefaultSandboxAPIURL is the default base URL for the Sandbox API
// when running inside the cluster.
const DefaultSandboxAPIURL = "http://sandbox-api.babylon-sandbox-api.svc.cluster.local:8080"

// Config holds the runner configuration parsed from environment variables.
type Config struct {
	AnarchyURL      string
	RunnerName      string
	RunnerToken     string
	PodName         string
	PollingInterval time.Duration
	RequestTimeout  time.Duration
	SandboxAPIURL   string
}

// AuthHeader returns the Bearer token for Anarchy API requests.
func (c Config) AuthHeader() string {
	return fmt.Sprintf("Bearer %s:%s:%s", c.RunnerName, c.PodName, c.RunnerToken)
}

// ConfigFromEnv reads configuration from environment variables.
// Returns an error if any required variable is missing or empty.
func ConfigFromEnv() (Config, error) {
	cfg := Config{}
	cfg.AnarchyURL = os.Getenv("ANARCHY_URL")
	if cfg.AnarchyURL == "" {
		return cfg, fmt.Errorf("ANARCHY_URL is required")
	}
	cfg.RunnerName = os.Getenv("RUNNER_NAME")
	if cfg.RunnerName == "" {
		return cfg, fmt.Errorf("RUNNER_NAME is required")
	}
	cfg.RunnerToken = os.Getenv("RUNNER_TOKEN")
	if cfg.RunnerToken == "" {
		return cfg, fmt.Errorf("RUNNER_TOKEN is required")
	}
	cfg.PodName = os.Getenv("HOSTNAME")
	if cfg.PodName == "" {
		return cfg, fmt.Errorf("HOSTNAME is required")
	}
	cfg.PollingInterval = time.Duration(envInt("POLLING_INTERVAL", 5)) * time.Second
	cfg.RequestTimeout = time.Duration(envInt("REQUEST_TIMEOUT", 35)) * time.Second
	cfg.SandboxAPIURL = os.Getenv("SANDBOX_API_URL")
	if cfg.SandboxAPIURL == "" {
		cfg.SandboxAPIURL = DefaultSandboxAPIURL
	}
	return cfg, nil
}

// envInt reads an integer from an environment variable, returning defaultVal
// if the variable is unset or cannot be parsed.
func envInt(key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}
