package httputil

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestInstrumentedTransport(t *testing.T) {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "test_http_duration_seconds",
		Help:    "test",
		Buckets: []float64{0.01, 0.1, 1},
	}, []string{"method"})

	transport := InstrumentedTransport(http.DefaultTransport, hist, "GET")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &http.Client{Transport: transport}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	ch := make(chan prometheus.Metric, 1)
	hist.Collect(ch)
	m := <-ch
	if m == nil {
		t.Error("expected histogram metric to be collected")
	}
}

func TestInstrumentedTransportError(t *testing.T) {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "test_http_error_duration",
		Help:    "test",
		Buckets: []float64{0.01, 0.1, 1},
	}, []string{"method"})

	transport := InstrumentedTransport(http.DefaultTransport, hist, "GET")
	client := &http.Client{Transport: transport}

	_, err := client.Get("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}
