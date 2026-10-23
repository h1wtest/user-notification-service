package platform

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
)

var metricsRegistry = prometheus.NewRegistry()
var httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "Completed HTTP requests."}, []string{"method", "route", "status"})
var httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"method", "route", "status"})
var metricMu sync.Mutex

type lagSample struct {
	Value      int64
	ObservedAt time.Time
}

var consumerLag = map[string]lagSample{}

func init() {
	metricsRegistry.MustRegister(httpRequests, httpDuration, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}
func RecordHTTP(method, route string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	// ServeMux registered patterns, not raw paths/query strings, bound label
	// cardinality and exclude reset tokens or user-supplied path identifiers.
	httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	httpDuration.WithLabelValues(method, route, strconv.Itoa(status)).Observe(duration.Seconds())
}
func RecordLag(topic string, partition int32, lag int64) {
	if lag < 0 {
		lag = 0
	}
	key := fmt.Sprintf("topic=%s,partition=%s", strconv.Quote(topic), strconv.Quote(strconv.Itoa(int(partition))))
	metricMu.Lock()
	consumerLag[key] = lagSample{Value: lag, ObservedAt: time.Now()}
	metricMu.Unlock()
}
func ClearLag(partitions map[string][]int32) {
	metricMu.Lock()
	defer metricMu.Unlock()
	for topic, ids := range partitions {
		for _, partition := range ids {
			key := fmt.Sprintf("topic=%s,partition=%s", strconv.Quote(topic), strconv.Quote(strconv.Itoa(int(partition))))
			delete(consumerLag, key)
		}
	}
}
func WriteMetrics(w io.Writer) {
	families, err := metricsRegistry.Gather()
	if err == nil {
		for _, family := range families {
			_, _ = expfmt.MetricFamilyToText(w, family)
		}
	}
	metricMu.Lock()
	defer metricMu.Unlock()
	for key, value := range consumerLag {
		fmt.Fprintf(w, "kafka_sampled_committed_lag{%s} %d\nkafka_sampled_committed_lag_timestamp_seconds{%s} %f\n", key, value.Value, key, float64(value.ObservedAt.UnixNano())/1e9)
	}
}
