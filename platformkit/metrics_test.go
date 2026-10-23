package platformkit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRuntimeMetricsLatencyLagAndRetries(t *testing.T) {
	var metrics runtimeMetrics
	metrics.recordHTTP("GET", "GET /orders/{id}", 503, 250*time.Millisecond)
	metrics.recordLag("subscription.events.v2", 3, 7)
	metrics.recordLag("billing.events.v2", 1, -2)
	metrics.retry("consume", "dependency_retry")
	metrics.retry("consume", "dependency_retry")
	metrics.retry("publish", "broker_unavailable")
	var output strings.Builder
	metrics.write(&output)
	for _, expected := range []string{
		`http_requests_total{method="GET",route="GET /orders/{id}",status="503"} 1`,
		`http_request_duration_seconds_bucket{method="GET",route="GET /orders/{id}",status="503",le="0.25"} 1`,
		`http_request_duration_seconds_sum{method="GET",route="GET /orders/{id}",status="503"} 0.25`,
		`kafka_sampled_committed_lag{topic="subscription.events.v2",partition="3"} 7`,
		`kafka_sampled_committed_lag{topic="billing.events.v2",partition="1"} 0`,
		`event_retries_total{operation="consume",reason="dependency_retry"} 2`,
		`event_retries_total{operation="publish",reason="broker_unavailable"} 1`,
	} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("missing %q in %s", expected, output.String())
		}
	}
	metrics.clearLag(map[string][]int32{"subscription.events.v2": {3}})
	output.Reset()
	metrics.write(&output)
	if strings.Contains(output.String(), "subscription.events.v2") {
		t.Fatal("revoked partition retained stale lag")
	}
}

func TestTelemetryUnmatchedRoutesAndMethodsDoNotBecomeLabels(t *testing.T) {
	telemetry := newTelemetry("catalog", "")
	defer telemetry.close(context.Background())
	handler := telemetryHTTP(http.NewServeMux())
	for _, method := range []string{"CUSTOM-one", "CUSTOM-two"} {
		r := httptest.NewRequest(method, "/secret-user-id?secret=token", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r.WithContext(withTelemetry(r.Context(), telemetry)))
	}
	var output strings.Builder
	telemetry.metrics.write(&output)
	if !strings.Contains(output.String(), `http_requests_total{method="OTHER",route="unmatched",status="404"} 2`) {
		t.Fatal(output.String())
	}
	if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "CUSTOM") {
		t.Fatal("unbounded request label")
	}
}

func TestMetricsReportsAgeOfOldestPendingOutbox(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event := fixtureEvent(t, "00000000-0000-4000-8000-000000000001", 1)
	if err = Enqueue(ctx, tx, event); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "UPDATE outbox SET created_at=now()-interval '2 minutes' WHERE event_id=$1", event.EventID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	telemetry := newTelemetry("catalog", "")
	defer telemetry.close(ctx)
	request := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	telemetryHTTP(healthHandler(Config{Name: "catalog"}, db, http.NewServeMux(), nil)).ServeHTTP(w, request.WithContext(withTelemetry(ctx, telemetry)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "outbox_pending 1\n") || !strings.Contains(w.Body.String(), "outbox_oldest_pending_age_seconds 12") {
		t.Fatal("oldest pending outbox metric absent", w.Body.String())
	}
}
