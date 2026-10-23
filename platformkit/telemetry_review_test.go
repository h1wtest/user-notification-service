package platformkit

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockedMetricWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *blockedMetricWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestSlowMetricScrapeDoesNotBlockBusinessUpdates(t *testing.T) {
	var metrics runtimeMetrics
	metrics.recordHTTP("GET", "GET /orders/{id}", 200, time.Millisecond)
	writer := &blockedMetricWriter{entered: make(chan struct{}), release: make(chan struct{})}
	scraped := make(chan struct{})
	defer func() { close(writer.release); <-scraped }()
	go func() { metrics.write(writer); close(scraped) }()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("scrape did not reach writer")
	}
	updated := make(chan struct{})
	go func() {
		metrics.recordHTTP("POST", "POST /orders", 202, time.Millisecond)
		metrics.recordLag("billing.events.v2", 1, 8)
		metrics.retry("consume", "dependency_retry")
		close(updated)
	}()
	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("blocked scrape held business metrics lock")
	}
}

func TestNestedDeadlineMuxReportsLeafPatternAcrossRequestCopies(t *testing.T) {
	telemetry := recordingTelemetry("referral")
	defer telemetry.close(context.Background())
	leaf := http.NewServeMux()
	leaf.HandleFunc("GET /api/v1/referral/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	wrapper := http.NewServeMux()
	wrapper.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		leaf.ServeHTTP(w, RequestWithContext(r, ctx))
	}))
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		telemetryHTTP(wrapper).ServeHTTP(w, r.WithContext(withTelemetry(r.Context(), telemetry)))
	}))
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/api/v1/referral/secret-uuid?token=secret-query")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	span := <-telemetry.queue
	if span.Name != "GET /api/v1/referral/{id}" {
		t.Fatalf("leaf route lost: %q", span.Name)
	}
	var output strings.Builder
	telemetry.metrics.write(&output)
	if !strings.Contains(output.String(), `route="GET /api/v1/referral/{id}"`) || strings.Contains(output.String(), "secret") {
		t.Fatal("leaf route metric invalid", output.String())
	}
}

func TestDisabledAndUnsampledTracingDoNotConstructRecordingData(t *testing.T) {
	for _, endpoint := range []string{"", "test-recording-sink"} {
		telemetry := newTelemetry("catalog", "")
		if endpoint != "" {
			telemetry = recordingTelemetry("catalog")
		}
		parent := "00-11111111111111111111111111111111-2222222222222222-00"
		if endpoint == "" {
			parent = "00-11111111111111111111111111111111-2222222222222222-01"
		}
		ctx, span := startSpan(withTelemetry(context.Background(), telemetry), parent, 2, parent)
		if traceparentFrom(ctx) == "" {
			t.Fatal("disabled recording lost propagation")
		}
		span.finish("GET /plans/{id}", false)
		if span.sampled || span.item.Start != "" || span.item.Attributes != nil || span.item.Links != nil || len(telemetry.queue) != 0 || telemetry.dropped.Load() != 0 {
			t.Fatal("inactive recorder constructed or enqueued OTLP data")
		}
		if endpoint == "" && telemetry.queue != nil {
			t.Fatal("disabled exporter allocated unserviced queue")
		}
		telemetry.close(context.Background())
	}
}

func TestHTTPDuplicateTraceparentsAreRejected(t *testing.T) {
	telemetry := recordingTelemetry("catalog")
	defer telemetry.close(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /plans", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	req := httptest.NewRequest("GET", "/plans", nil)
	req.Header.Add("Traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	req.Header.Add("Traceparent", "00-33333333333333333333333333333333-4444444444444444-01")
	telemetryHTTP(mux).ServeHTTP(httptest.NewRecorder(), req.WithContext(withTelemetry(req.Context(), telemetry)))
	span := <-telemetry.queue
	if span.ParentSpanID != "" || span.TraceID == "11111111111111111111111111111111" || span.TraceID == "33333333333333333333333333333333" {
		t.Fatal("ambiguous HTTP parent accepted")
	}
}
