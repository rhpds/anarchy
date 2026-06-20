package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegistered(t *testing.T) {
	ch := make(chan *prometheus.Desc, 20)
	RunDuration.Describe(ch)
	desc := <-ch
	if desc == nil {
		t.Error("RunDuration not registered")
	}

	RunTotal.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("RunTotal not registered")
	}

	PollDuration.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("PollDuration not registered")
	}

	ActiveRun.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("ActiveRun not registered")
	}

	TowerJobDuration.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("TowerJobDuration not registered")
	}

	SandboxAPIDuration.Describe(ch)
	desc = <-ch
	if desc == nil {
		t.Error("SandboxAPIDuration not registered")
	}
}
