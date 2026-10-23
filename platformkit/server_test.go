package platformkit

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestHealthDoesNotMigrateEmptyDatabaseAndRejectsForeignOwner(t *testing.T) {
	db := isolatedDB(t)
	called := false
	handler := healthHandler(Config{Name: "catalog"}, db, http.NewServeMux(), func(context.Context) error { called = true; return nil })
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/health/live", nil))
	if w.Code != 200 {
		t.Fatal("empty schema is not live")
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil))
	if w.Code != 503 || called {
		t.Fatal("empty schema reported ready or called domain readiness")
	}
	var exists bool
	if err := db.QueryRow(context.Background(), "SELECT to_regclass('platformkit_schema') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatal("runtime performed DDL")
	}
	if err := Migrate(context.Background(), db, "catalog", []Migration{{Version: 1, SQL: SchemaSQL}}); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil))
	if w.Code != 200 || !called {
		t.Fatal("migrated owner not ready")
	}
	foreign := healthHandler(Config{Name: "billing"}, db, http.NewServeMux(), nil)
	w = httptest.NewRecorder()
	foreign.ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil))
	if w.Code != 503 {
		t.Fatal("foreign owner marked ready")
	}
}

func TestServeGatesWorkersAndBusinessRoutesThenCancelsGracefully(t *testing.T) {
	db := migratedDB(t)
	config, client, base := serveFixture(t)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var domainReady atomic.Bool
	checks := make(chan struct{}, 16)
	started := make(chan struct{})
	stopped := make(chan struct{})
	ready := func(context.Context) error {
		select {
		case checks <- struct{}{}:
		default:
		}
		if !domainReady.Load() {
			return errors.New("required owner migration not applied")
		}
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/catalog", func(w http.ResponseWriter, r *http.Request) { JSON(w, 200, map[string]bool{"served": true}) })
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, config, db, mux, ready, func(ctx context.Context) error { close(started); <-ctx.Done(); close(stopped); return ctx.Err() })
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := client.Get(base + "/health/live")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("not live while schema incomplete")
			}
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("TLS listener did not start")
		}
	}
	response, err := client.Get(base + "/api/v1/catalog")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("business route admitted incomplete owner migration")
	}
	select {
	case <-checks:
	case <-time.After(2 * time.Second):
		t.Fatal("startup did not evaluate domain readiness")
	}
	select {
	case <-started:
		t.Fatal("background worker admitted incomplete owner migration")
	default:
	}
	domainReady.Store(true)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start after exact schema readiness")
	}
	response, err = client.Get(base + "/api/v1/catalog")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("ready business route rejected")
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTPS/background shutdown exceeded bound")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("worker context not canceled before server returned")
	}
}

func serveFixture(t *testing.T) (Config, *http.Client, string) {
	t.Helper()
	seed := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := seed.TLS.Certificates[0]
	client := seed.Client()
	seed.Close()
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey}), 0600); err != nil {
		t.Fatal(err)
	}
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(reservation.Addr().(*net.TCPAddr).Port)
	_ = reservation.Close()
	return Config{Name: "catalog", Port: port, Cert: certFile, Key: keyFile}, client, "https://127.0.0.1:" + port
}

func TestServeRetainsJoinedCancellationCleanupFailure(t *testing.T) {
	db := migratedDB(t)
	config, client, _ := serveFixture(t)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cleanupFailure := errors.New("durable cleanup failed")
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, config, db, http.NewServeMux(), nil, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return errors.Join(ctx.Err(), cleanupFailure)
		})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker not started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, cleanupFailure) {
			t.Fatal("cleanup failure hidden by cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("joined failure shutdown blocked")
	}
}

func TestServePropagatesWorkerFailureAndUnexpectedCleanExit(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(strconv.FormatBool(clean), func(t *testing.T) {
			db := migratedDB(t)
			config, client, _ := serveFixture(t)
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			failure := errors.New("worker failed")
			done := make(chan error, 1)
			go func() {
				done <- Serve(ctx, config, db, http.NewServeMux(), nil, func(context.Context) error {
					if clean {
						return nil
					}
					return failure
				})
			}()
			select {
			case err := <-done:
				if err == nil || !clean && !errors.Is(err, failure) {
					t.Fatal("worker failure not propagated", err)
				}
			case <-ctx.Done():
				t.Fatal("worker failure did not stop HTTPS server")
			}
		})
	}
}

func TestServeBusinessPreflightDeadlineFails503WithoutInvokingHandler(t *testing.T) {
	db := migratedDB(t)
	config, client, base := serveFixture(t)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var blocked atomic.Bool
	var handlerCalled atomic.Bool
	remaining := make(chan time.Duration, 1)
	ready := func(check context.Context) error {
		if !blocked.Load() {
			return nil
		}
		deadline, ok := check.Deadline()
		if !ok {
			return errors.New("readiness deadline missing")
		}
		select {
		case remaining <- time.Until(deadline):
		default:
		}
		<-check.Done()
		return check.Err()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /callback", func(w http.ResponseWriter, r *http.Request) {
		handlerCalled.Store(true)
		w.WriteHeader(http.StatusAccepted)
	})
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, config, db, mux, ready, nil) }()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	startup := time.NewTimer(5 * time.Second)
	defer startup.Stop()
	for {
		response, err := client.Get(base + "/health/live")
		if err == nil {
			_ = response.Body.Close()
			break
		}
		select {
		case <-ticker.C:
		case <-startup.C:
			t.Fatal("TLS listener did not start")
		}
	}
	blocked.Store(true)
	response, err := client.Post(base+"/callback", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || handlerCalled.Load() {
		t.Fatal("readiness timeout admitted work or masqueraded as auth denial")
	}
	select {
	case budget := <-remaining:
		if budget <= 0 || budget > 250*time.Millisecond {
			t.Fatalf("business preflight exceeds reserved callback budget: %s", budget)
		}
	default:
		t.Fatal("business readiness callback was not bounded")
	}
	response, err = client.Get(base + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("readiness timeout changed independent liveness")
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded preflight listener did not stop")
	}
}
