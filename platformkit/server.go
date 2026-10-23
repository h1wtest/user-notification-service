package platformkit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func schemaReady(ctx context.Context, c Config, db *pgxpool.Pool) error {
	if db == nil {
		return errors.New("database unavailable")
	}
	var owner string
	var version, applied int
	if err := db.QueryRow(ctx, `SELECT owner,version,(SELECT count(*) FROM platformkit_migrations WHERE owner=s.owner) FROM platformkit_schema s WHERE singleton`).Scan(&owner, &version, &applied); err != nil {
		return err
	}
	if owner != c.Name || version < 1 || version != applied {
		return errors.New("owner schema not ready")
	}
	return nil
}

func healthHandler(c Config, db *pgxpool.Pool, mux *http.ServeMux, ready func(context.Context) error) http.Handler {
	check := func(ctx context.Context) error {
		if err := schemaReady(ctx, c, db); err != nil {
			return err
		}
		if ready != nil {
			return ready(ctx)
		}
		return nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/live" || r.URL.Path == "/health/ready" || r.URL.Path == "/metrics" {
			r.Pattern = "GET " + r.URL.Path
		}
		if r.Method == http.MethodGet {
			switch r.URL.Path {
			case "/health/live":
				JSON(w, 200, map[string]bool{"live": true})
				return
			case "/health/ready":
				ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				defer cancel()
				if check(ctx) != nil {
					Error(w, 503, "not_ready")
					return
				}
				JSON(w, 200, map[string]bool{"ready": true})
				return
			case "/metrics":
				ctx, cancel := context.WithTimeout(r.Context(), time.Second)
				defer cancel()
				state := 0
				if check(ctx) == nil {
					state = 1
				}
				w.Header().Set("Content-Type", "text/plain; version=0.0.4")
				fmt.Fprintf(w, "service_ready %d\n", state)
				if db != nil {
					stats := db.Stat()
					fmt.Fprintf(w, "db_pool_acquired_connections %d\ndb_pool_total_connections %d\n", stats.AcquiredConns(), stats.TotalConns())
					var outbox, quarantine int64
					var age float64
					if db.QueryRow(ctx, "SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL),(SELECT count(*) FROM quarantine WHERE replayed_at IS NULL),COALESCE((SELECT GREATEST(0,EXTRACT(EPOCH FROM now()-min(created_at)))::double precision FROM outbox WHERE published_at IS NULL),0)").Scan(&outbox, &quarantine, &age) == nil {
						fmt.Fprintf(w, "outbox_pending %d\nquarantine_pending %d\noutbox_oldest_pending_age_seconds %g\n", outbox, quarantine, age)
					}
				}
				if telemetry := telemetryFrom(r.Context()); telemetry != nil {
					telemetry.metrics.write(w)
					fmt.Fprintf(w, "telemetry_dropped_spans_total %d\n", telemetry.dropped.Load())
				}
				return
			}
		}
		// Admission checks only owner-local schema/runtime state. Keep this
		// independent preflight short so a caller's own bounded handler budget
		// is not preceded by a multi-second readiness delay (e.g. callbacks).
		ctx, cancel := context.WithTimeout(r.Context(), 250*time.Millisecond)
		defer cancel()
		if check(ctx) != nil {
			Error(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func Serve(ctx context.Context, c Config, db *pgxpool.Pool, mux *http.ServeMux, ready func(context.Context) error, background func(context.Context) error) error {
	if c.Name == "" || c.Cert == "" || c.Key == "" || c.Port == "" || db == nil || mux == nil {
		return errors.New("TLS server configuration required")
	}
	certificate, err := tls.LoadX509KeyPair(c.Cert, c.Key)
	if err != nil {
		return errors.New("invalid server TLS certificate/key")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort("", c.Port))
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	telemetry := runtimeTelemetry(c.Name)
	defer func() {
		flush, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		telemetry.close(flush)
	}()
	runCtx = withTelemetry(runCtx, telemetry)
	server := &http.Server{Handler: telemetryHTTP(healthHandler(c, db, mux, ready)), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}, BaseContext: func(net.Listener) context.Context { return runCtx }}
	serverDone := make(chan error, 1)
	go func() {
		err := server.Serve(tls.NewListener(listener, server.TLSConfig))
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverDone <- err
	}()
	var workerDone chan error
	if background != nil {
		workerDone = make(chan error, 1)
		go func() {
			for runCtx.Err() == nil {
				check, stop := context.WithTimeout(runCtx, time.Second)
				err := schemaReady(check, c, db)
				if err == nil && ready != nil {
					err = ready(check)
				}
				stop()
				if err == nil {
					break
				}
				if !pause(runCtx, time.Second) {
					workerDone <- nil
					return
				}
			}
			if runCtx.Err() != nil {
				workerDone <- nil
				return
			}
			err := background(runCtx)
			if err == nil && runCtx.Err() == nil {
				err = errors.New("background worker stopped unexpectedly")
			}
			workerDone <- err
		}()
	}
	var result error
	serverStopped, workerStopped := false, background == nil
	select {
	case <-ctx.Done():
	case result = <-serverDone:
		serverStopped = true
	case result = <-workerDone:
		workerStopped = true
		if result == context.Canceled && ctx.Err() != nil {
			result = nil
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := server.Shutdown(shutdown); err != nil {
		result = errors.Join(result, err)
		_ = server.Close()
	}
	if !serverStopped {
		select {
		case err := <-serverDone:
			result = errors.Join(result, err)
		case <-shutdown.Done():
			result = errors.Join(result, errors.New("HTTPS shutdown deadline exceeded"))
		}
	}
	if !workerStopped {
		select {
		case err := <-workerDone:
			if err != context.Canceled {
				result = errors.Join(result, err)
			}
		case <-shutdown.Done():
			result = errors.Join(result, errors.New("background shutdown deadline exceeded"))
		}
	}
	return result
}
