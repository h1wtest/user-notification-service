package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTelemetryLifetimeFlushesCompletedProducerSpan(t *testing.T) {
	received := make(chan struct{}, 8)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received <- struct{}{}; w.WriteHeader(200) }))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	lifetime, stop := context.WithCancel(context.Background())
	InitTelemetry(lifetime, "regression")
	_, _, finish := StartSpan(context.Background(), "", "delivery.email")
	finish(false)
	// Producers have completed before the independent exporter is canceled.
	stop()
	wait, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	WaitTelemetry(wait)
	select {
	case <-received:
	default:
		t.Fatal("completed span lost during exporter drain")
	}
}
