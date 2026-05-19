package main

import (
	"log"
	"os"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC)

	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	log.Printf("babylon-runner starting name=%s pod=%s url=%s poll=%ds timeout=%ds",
		cfg.RunnerName, cfg.PodName, cfg.AnarchyURL, cfg.PollingInterval, cfg.RequestTimeout)

	// TODO(task-3): NewRunner(cfg).Run()
	log.Println("runner loop not yet implemented, exiting")
}
