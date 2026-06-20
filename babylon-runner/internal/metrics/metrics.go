package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RunDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "babylon_runner_run_duration_seconds",
		Help:    "Duration of run execution by handler type and action",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300},
	}, []string{"handler_type", "action"})

	RunTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "babylon_runner_runs_total",
		Help: "Total runs processed by status",
	}, []string{"handler_type", "action", "status"})

	PollDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "babylon_runner_poll_duration_seconds",
		Help:    "Duration of GET /run poll requests",
		Buckets: []float64{0.01, 0.1, 1, 5, 10, 30, 35},
	})

	TowerJobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "babylon_runner_tower_job_duration_seconds",
		Help:    "Duration of Tower API operations",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30},
	}, []string{"operation"})

	SandboxAPIDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "babylon_runner_sandbox_api_duration_seconds",
		Help:    "Duration of Sandbox API operations",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60},
	}, []string{"operation"})

	ActiveRun = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "babylon_runner_active_run",
		Help: "1 if currently processing a run, 0 if idle",
	})
)
