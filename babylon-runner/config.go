package main

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds the runner configuration parsed from environment variables.
type Config struct {
	AnarchyURL      string // env: ANARCHY_URL (required)
	RunnerName      string // env: RUNNER_NAME (required)
	RunnerToken     string // env: RUNNER_TOKEN (required)
	PodName         string // env: HOSTNAME (required)
	PollingInterval int    // env: POLLING_INTERVAL (default: 5, seconds)
	RequestTimeout  int    // env: REQUEST_TIMEOUT (default: 35, seconds)
}

// AuthHeader returns the Bearer token for Anarchy API requests.
func (c Config) AuthHeader() string {
	return fmt.Sprintf("Bearer %s:%s:%s", c.RunnerName, c.PodName, c.RunnerToken)
}

// configFromEnv reads configuration from environment variables.
// Returns an error if any required variable is missing or empty.
func configFromEnv() (Config, error) {
	cfg := Config{
		AnarchyURL:  os.Getenv("ANARCHY_URL"),
		RunnerName:  os.Getenv("RUNNER_NAME"),
		RunnerToken: os.Getenv("RUNNER_TOKEN"),
		PodName:     os.Getenv("HOSTNAME"),
	}

	// Validate required fields.
	var missing []string
	if cfg.AnarchyURL == "" {
		missing = append(missing, "ANARCHY_URL")
	}
	if cfg.RunnerName == "" {
		missing = append(missing, "RUNNER_NAME")
	}
	if cfg.RunnerToken == "" {
		missing = append(missing, "RUNNER_TOKEN")
	}
	if cfg.PodName == "" {
		missing = append(missing, "HOSTNAME")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %v", missing)
	}

	// Parse optional integer fields with defaults.
	cfg.PollingInterval = envInt("POLLING_INTERVAL", 5)
	cfg.RequestTimeout = envInt("REQUEST_TIMEOUT", 35)

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
