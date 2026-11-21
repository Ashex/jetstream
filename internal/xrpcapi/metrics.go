package xrpcapi

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "getblock"

	resultOK         = "ok"
	resultNotFound   = "not_found"
	resultBadRequest = "bad_request"
	resultError      = "error"
	// resultUnavailable is a 503: the object store did not answer.
	resultUnavailable = "unavailable"
)

// Metrics owns the prometheus state for getBlock. A nil *Metrics is valid:
// every method is a no-op, so tests and the zero-config server can skip
// registration.
type Metrics struct {
	requests   *prometheus.CounterVec
	servedByte prometheus.Counter
	duration   prometheus.Histogram

	belowRetention prometheus.Counter
}

// NewMetrics registers and returns the getBlock metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "requests_total", Help: "getBlock requests served, by outcome.",
		}, []string{"result"}),
		servedByte: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "served_bytes_total", Help: "Block frame bytes written to clients.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "duration_seconds", Help: "getBlock handler wall-clock latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
	}
	m.belowRetention = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: "plansnapshot",
		Name: "below_retention_total", Help: "planSnapshot requests refused because afterSeq is below the retention floor.",
	})
	reg.MustRegister(m.requests, m.servedByte, m.duration, m.belowRetention)
	return m
}

// observeServe records one request outcome. Nil-safe.
func (m *Metrics) observeServe(result string, bytes int, seconds float64) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(result).Inc()
	if bytes > 0 {
		m.servedByte.Add(float64(bytes))
	}
	m.duration.Observe(seconds)
}

func (m *Metrics) incBelowRetention() {
	if m != nil {
		m.belowRetention.Inc()
	}
}
