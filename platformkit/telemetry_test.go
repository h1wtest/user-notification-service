package platformkit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTelemetryTLSRequestPropagationAndOTLPCollector(t *testing.T) {
	var mu sync.Mutex
	var received []otlpSpan
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("invalid OTLP HTTP request")
		}
		var payload struct {
			ResourceSpans []struct {
				Resource struct {
					Attributes []spanAttribute `json:"attributes"`
				} `json:"resource"`
				ScopeSpans []struct {
					Spans []otlpSpan `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for _, resource := range payload.ResourceSpans {
			if len(resource.Resource.Attributes) != 1 || resource.Resource.Attributes[0].Value.String != "billing-service" {
				t.Error("canonical owner resource missing")
			}
			for _, scope := range resource.ScopeSpans {
				received = append(received, scope.Spans...)
			}
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer collector.Close()
	telemetry := newTelemetry("billing-service", collector.URL)
	defer telemetry.close(context.Background())
	base := withTelemetry(context.Background(), telemetry)
	remoteParent := "00-11111111111111111111111111111111-2222222222222222-01"
	propagatedHeader := make(chan string, 1)
	dependencyMux := http.NewServeMux()
	dependencyMux.HandleFunc("POST /private/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Token") != "secret-token" {
			t.Error("workload authentication changed")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	dependency := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		propagatedHeader <- r.Header.Get("Traceparent")
		telemetryHTTP(dependencyMux).ServeHTTP(w, r.WithContext(withTelemetry(r.Context(), telemetry)))
	}))
	defer dependency.Close()
	client := &Client{Config: Config{Token: "secret-token"}, HTTP: dependency.Client()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		status, _, err := client.Call(r.Context(), "POST", dependency.URL+"/private/secret-id?token=secret-query", map[string]string{"secret": "secret-body"}, r)
		if err != nil || status != 204 {
			t.Errorf("TLS dependency failed: %d %v", status, err)
		}
		w.WriteHeader(http.StatusAccepted)
	})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		telemetryHTTP(mux).ServeHTTP(w, r.WithContext(withTelemetry(r.Context(), telemetry)))
	}))
	defer server.Close()
	req, _ := http.NewRequestWithContext(base, "POST", server.URL+"/orders/secret-user-id?token=secret-query", strings.NewReader("secret-body"))
	req.Header.Set("Traceparent", remoteParent)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	propagated := <-propagatedHeader
	if response.StatusCode != 202 {
		t.Fatal(response.StatusCode)
	}
	if propagated == "" || propagated == remoteParent || !strings.HasPrefix(propagated, "00-11111111111111111111111111111111-") {
		t.Fatal("outbound request did not propagate active client span")
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	telemetry.close(shutdown)
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("expected upstream, client, and dependency server spans, got %d", len(received))
	}
	var serverSpan, clientSpan, dependencySpan otlpSpan
	for _, item := range received {
		if item.Kind == 2 && item.Name == "POST /orders/{id}" {
			serverSpan = item
		}
		if item.Kind == 2 && item.Name == "POST /private/{id}" {
			dependencySpan = item
		}
		if item.Kind == 3 {
			clientSpan = item
		}
	}
	if serverSpan.ParentSpanID != "2222222222222222" || clientSpan.ParentSpanID != serverSpan.SpanID || dependencySpan.ParentSpanID != clientSpan.SpanID || propagated != "00-"+clientSpan.TraceID+"-"+clientSpan.SpanID+"-01" {
		t.Fatal("broken W3C parent chain")
	}
	if serverSpan.Name != "POST /orders/{id}" || serverSpan.End <= serverSpan.Start {
		t.Fatal("server route/timing missing")
	}
	raw, _ := json.Marshal(received)
	for _, secret := range []string{"secret-token", "secret-query", "secret-body", "secret-id", "secret-user-id"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("secret leaked in OTLP")
		}
	}
	var metrics strings.Builder
	telemetry.metrics.write(&metrics)
	if !strings.Contains(metrics.String(), `http_requests_total{method="POST",route="POST /orders/{id}",status="202"} 1`) || !strings.Contains(metrics.String(), "http_request_duration_seconds_bucket") {
		t.Fatal(metrics.String())
	}
	if strings.Contains(metrics.String(), "secret-") {
		t.Fatal("secret leaked in metrics")
	}
}

func TestTraceparentValidationAndUnsampledPropagation(t *testing.T) {
	for _, parent := range []string{"00-00000000000000000000000000000000-2222222222222222-01", "00-11111111111111111111111111111111-0000000000000000-01", "ff-11111111111111111111111111111111-2222222222222222-01", "00-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA-2222222222222222-01", "00-11111111111111111111111111111111-2222222222222222-01-extra", "00-11111111111111111111111111111111-2222222222222222-zz"} {
		if _, ok := parseTraceparent(parent); ok {
			t.Errorf("accepted invalid parent %q", parent)
		}
	}
	telemetry := recordingTelemetry("catalog")
	ctx := withTelemetry(context.Background(), telemetry)
	_, active := startSpan(ctx, "00-11111111111111111111111111111111-2222222222222222-00", 2, "")
	if !strings.HasSuffix(active.header(), "-00") {
		t.Fatal("remote sampling decision ignored")
	}
	active.finish("GET /plans/{id}", false)
	if len(telemetry.queue) != 0 {
		t.Fatal("unsampled span exported")
	}
	telemetry.close(context.Background())
}

func TestOTLPQueueDoesNotBlockAndShutdownIsBounded(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}))
	defer collector.Close()
	defer close(release)
	telemetry := newTelemetry("referral", collector.URL)
	ctx := withTelemetry(context.Background(), telemetry)
	_, active := startSpan(ctx, "", 1, "")
	active.finish("background", false)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("collector was never called")
	}
	finished := make(chan struct{})
	go func() {
		for range 1000 {
			_, span := startSpan(ctx, "", 1, "")
			span.finish("background", false)
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("full telemetry queue blocked application")
	}
	if telemetry.dropped.Load() == 0 || len(telemetry.queue) > 256 {
		t.Fatal("queue was not bounded")
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	telemetry.close(shutdown)
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown returned after %s", elapsed)
	}
	if !errors.Is(shutdown.Err(), context.DeadlineExceeded) {
		t.Fatal("test did not exercise deadline")
	}
	select {
	case <-telemetry.done:
	default:
		t.Fatal("shutdown detached active exporter worker")
	}
}

func TestKafkaProducerAndConsumerRetainOutboxLinks(t *testing.T) {
	telemetry := recordingTelemetry("subscription")
	ctx := withTelemetry(context.Background(), telemetry)
	parent := "00-11111111111111111111111111111111-2222222222222222-01"
	record, producer := publicationRecord(ctx, publication{Topic: "platform.subscription.events.v2", Raw: []byte(`{"traceparent":"` + parent + `"}`)})
	producer.finish("kafka.publish", false)
	_, consumer := consumerSpan(ctx, record)
	consumer.finish("kafka.process", false)
	first, second := <-telemetry.queue, <-telemetry.queue
	if first.ParentSpanID != "2222222222222222" || second.ParentSpanID != first.SpanID || len(second.Links) != 1 || second.Links[0].SpanID != "2222222222222222" {
		t.Fatal("durable outbox/producer/consumer ancestry or link lost")
	}
	telemetry.close(context.Background())
}
