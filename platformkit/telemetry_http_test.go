package platformkit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTelemetryCapturesActualHTTPFailuresWithoutDependencySecrets(t *testing.T) {
	telemetry := recordingTelemetry("billing-service")
	defer telemetry.close(context.Background())
	dependency := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer dependency.Close()
	client := &Client{Config: Config{Token: "private-workload-token"}, HTTP: dependency.Client()}
	ctx := withTelemetry(context.Background(), telemetry)
	status, _, err := client.Call(ctx, "GET", dependency.URL+"/private-identifier?access_token=private-token", nil, nil)
	if err != nil || status != 503 {
		t.Fatal("real TLS status changed", status, err)
	}
	span := <-telemetry.queue
	if span.Kind != 3 || span.Status.Code != 2 || span.Name != "GET http.client" {
		t.Fatal("dependency failure not represented")
	}
	for _, attribute := range span.Attributes {
		if strings.Contains(attribute.Value.String, "private") {
			t.Fatal("dependency secrets exported")
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	req := httptest.NewRequest("GET", "/orders/private-identifier?access_token=private-token", nil)
	telemetryHTTP(mux).ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
	span = <-telemetry.queue
	if span.Kind != 2 || span.Status.Code != 2 || span.Name != "GET /orders/{id}" {
		t.Fatal("server failure not represented")
	}
}

func TestOTLPExporterNeverFollowsRedirects(t *testing.T) {
	var redirected atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer destination.Close()
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer collector.Close()
	telemetry := newTelemetry("referral", collector.URL)
	_, span := startSpan(withTelemetry(context.Background(), telemetry), "", 1, "")
	span.finish("background", false)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	telemetry.close(ctx)
	if ctx.Err() != nil {
		t.Fatal("redirect response blocked exporter shutdown")
	}
	if redirected.Load() != 0 {
		t.Fatal("OTLP payload followed redirect")
	}
}
