package platformkit

import (
	"fmt"
	"io"
	"maps"
	"strconv"
	"sync"
	"time"
)

type httpMetricKey struct {
	method, route string
	status        int
}
type httpMetric struct {
	count   uint64
	sum     float64
	buckets [11]uint64
}
type lagMetricKey struct {
	topic     string
	partition int32
}
type lagMetric struct {
	value    int64
	observed time.Time
}
type retryMetricKey struct{ operation, reason string }
type runtimeMetrics struct {
	mu      sync.Mutex
	http    map[httpMetricKey]*httpMetric
	lag     map[lagMetricKey]lagMetric
	retries map[retryMetricKey]uint64
}

var durationBuckets = [...]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

func metricMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		return method
	default:
		return "OTHER"
	}
}
func (m *runtimeMetrics) recordHTTP(method, route string, status int, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.http == nil {
		m.http = make(map[httpMetricKey]*httpMetric)
	}
	key := httpMetricKey{metricMethod(method), route, status}
	value := m.http[key]
	if value == nil {
		value = &httpMetric{}
		m.http[key] = value
	}
	value.count++
	seconds := elapsed.Seconds()
	value.sum += seconds
	for i, upper := range durationBuckets {
		if seconds <= upper {
			value.buckets[i]++
		}
	}
}
func (m *runtimeMetrics) recordLag(topic string, partition int32, lag int64) {
	if lag < 0 {
		lag = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lag == nil {
		m.lag = make(map[lagMetricKey]lagMetric)
	}
	m.lag[lagMetricKey{topic, partition}] = lagMetric{lag, time.Now()}
}
func (m *runtimeMetrics) clearLag(partitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for topic, ids := range partitions {
		for _, partition := range ids {
			delete(m.lag, lagMetricKey{topic, partition})
		}
	}
}
func (m *runtimeMetrics) retry(operation, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retries == nil {
		m.retries = make(map[retryMetricKey]uint64)
	}
	m.retries[retryMetricKey{operation, reason}]++
}
func (m *runtimeMetrics) write(w io.Writer) {
	m.mu.Lock()
	httpSnapshot := make(map[httpMetricKey]httpMetric, len(m.http))
	for key, value := range m.http {
		httpSnapshot[key] = *value
	}
	lagSnapshot, retrySnapshot := maps.Clone(m.lag), maps.Clone(m.retries)
	m.mu.Unlock()
	fmt.Fprint(w, "# TYPE http_requests_total counter\n# TYPE http_request_duration_seconds histogram\n# TYPE kafka_sampled_committed_lag gauge\n# TYPE kafka_sampled_committed_lag_timestamp_seconds gauge\n# TYPE event_retries_total counter\n")
	for key, value := range httpSnapshot {
		labels := fmt.Sprintf("method=%s,route=%s,status=%s", strconv.Quote(key.method), strconv.Quote(key.route), strconv.Quote(strconv.Itoa(key.status)))
		fmt.Fprintf(w, "http_requests_total{%s} %d\n", labels, value.count)
		for i, upper := range durationBuckets {
			fmt.Fprintf(w, "http_request_duration_seconds_bucket{%s,le=%s} %d\n", labels, strconv.Quote(strconv.FormatFloat(upper, 'g', -1, 64)), value.buckets[i])
		}
		fmt.Fprintf(w, "http_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\nhttp_request_duration_seconds_sum{%s} %g\nhttp_request_duration_seconds_count{%s} %d\n", labels, value.count, labels, value.sum, labels, value.count)
	}
	for key, value := range lagSnapshot {
		labels := fmt.Sprintf("topic=%s,partition=%s", strconv.Quote(key.topic), strconv.Quote(strconv.Itoa(int(key.partition))))
		fmt.Fprintf(w, "kafka_sampled_committed_lag{%s} %d\nkafka_sampled_committed_lag_timestamp_seconds{%s} %g\n", labels, value.value, labels, float64(value.observed.UnixNano())/1e9)
	}
	for key, value := range retrySnapshot {
		fmt.Fprintf(w, "event_retries_total{operation=%s,reason=%s} %d\n", strconv.Quote(key.operation), strconv.Quote(key.reason), value)
	}
}
