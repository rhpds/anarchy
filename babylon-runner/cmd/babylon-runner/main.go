package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/rhpds/anarchy/babylon-runner/internal/handler"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := runner.ConfigFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	clientset, err := buildClientset()
	if err != nil {
		slog.Warn("kubernetes client not available", "error", err)
	}

	r := runner.New(cfg, clientset)
	r.SetHandlers(handler.Register())
	r.Run()
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
