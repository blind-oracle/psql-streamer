package prom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics
var (
	tags = []string{"sourceName", "sourceType", "table"}

	Timings = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "source_event_processing_time_sec",
		Help: "Source event processing time (sec)",
	}, tags)

	Events = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "source_events",
		Help: "Source events count",
	}, tags)

	Errors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "source_errors",
		Help: "Source errors count",
	}, tags)
)

func init() {
	prometheus.MustRegister(Timings)
	prometheus.MustRegister(Events)
	prometheus.MustRegister(Errors)
}

// Observe reports event to Prometheus
func Observe(tags []string, dur time.Duration) {
	Timings.WithLabelValues(tags...).Observe(dur.Seconds())
	Events.WithLabelValues(tags...).Inc()
}
