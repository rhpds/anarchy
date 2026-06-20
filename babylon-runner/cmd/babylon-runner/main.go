package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rhpds/anarchy/babylon-runner/internal/handler"
	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
	"github.com/rhpds/anarchy/babylon-runner/internal/metrics"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := runner.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	towerTLSConfig, err := httputil.NewTLSConfig(cfg.TowerTLSVerify, cfg.TowerCACert)
	if err != nil {
		log.Fatalf("tower TLS config: %v", err)
	}

	clientset, err := buildClientset()
	if err != nil {
		slog.Warn("kubernetes client not available", "error", err)
	}

	r := runner.New(cfg, clientset, towerTLSConfig)
	r.SetHandlers(handler.Register())

	metricsServer := metrics.NewServer(cfg.MetricsPort, r.IsReady)
	go func() {
		if err := metricsServer.Start(context.Background()); err != nil {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	r.Run(ctx)
}

func buildClientset() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = clientcmd.RecommendedHomeFile
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(config)
}
