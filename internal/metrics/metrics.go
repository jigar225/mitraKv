package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Recorder tracks request volume and latency so operators can inspect behavior.
type Recorder struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

// NewRecorder creates a dedicated registry and registers MitraKV metrics.
func NewRecorder() (*Recorder, error) {
	registry := prometheus.NewRegistry()

	requests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mitrakv_requests_total",
			Help: "Total number of requests by command.",
		},
		[]string{"command"},
	)

	latency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mitrakv_request_duration_seconds",
			Help:    "Request duration by command in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"command"},
	)

	if err := registry.Register(requests); err != nil {
		return nil, fmt.Errorf("register requests metric: %w", err)
	}
	if err := registry.Register(latency); err != nil {
		return nil, fmt.Errorf("register latency metric: %w", err)
	}

	return &Recorder{
		registry: registry,
		requests: requests,
		latency:  latency,
	}, nil
}

// ObserveRequest records one request and its execution duration.
func (r *Recorder) ObserveRequest(command string, duration time.Duration) {
	r.requests.WithLabelValues(command).Inc()
	r.latency.WithLabelValues(command).Observe(duration.Seconds())
}

// Handler returns an HTTP handler that exposes Prometheus text metrics.
func (r *Recorder) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}
