package prom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics
var (
	tags = []string{"sinkName", "sinkType", "table"}

	Timings = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "sink_event_processing_time_sec",
		Help: "Sink event processing time (sec)",
	}, tags)

	Events = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sink_events",
		Help: "Sink events count",
	}, tags)

	Errors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sink_errors",
		Help: "Sink errors count",
	}, tags)
)

func init() {
	prometheus.MustRegister(Timings)
	prometheus.MustRegister(Events)
	prometheus.MustRegister(Errors)
}

// Observe reports event to Prometheus
func Observe(tags []string, dur time.Duration, err bool) {
	Events.WithLabelValues(tags...).Add(1)
	Timings.WithLabelValues(tags...).Observe(dur.Seconds())

	if err {
		Errors.WithLabelValues(tags...).Add(1)
	}
}
