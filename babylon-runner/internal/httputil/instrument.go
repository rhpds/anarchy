package httputil

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type instrumentedTransport struct {
	next      http.RoundTripper
	histogram *prometheus.HistogramVec
	labels    []string
}

// InstrumentedTransport wraps a RoundTripper and records request
// durations in the provided histogram with the given label values.
func InstrumentedTransport(next http.RoundTripper, histogram *prometheus.HistogramVec, labels ...string) http.RoundTripper {
	return &instrumentedTransport{
		next:      next,
		histogram: histogram,
		labels:    labels,
	}
}

func (t *instrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	t.histogram.WithLabelValues(t.labels...).Observe(time.Since(start).Seconds())
	return resp, err
}
