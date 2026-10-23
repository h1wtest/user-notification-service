package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"platform-contracts/gen/go/eventsv2"
)

func TestV2ConsumerExportsTraceAroundTransactionalReceiptAndEffect(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("disposable TEST_DATABASE_URL required")
	}
	// The legacy exporter is process-singleton. A fresh test process isolates its
	// real HTTP collector without resetting production globals or test ordering.
	if os.Getenv("TEST_V2_CONSUMER_TRACE_CHILD") != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^TestV2ConsumerExportsTraceAroundTransactionalReceiptAndEffect$", "-test.count=1")
		command.Env = append(os.Environ(), "TEST_V2_CONSUMER_TRACE_CHILD=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("isolated v2 trace regression: %v\n%s", err, output)
		}
		return
	}
	db := nestedRekeyDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.Exec(ctx, "CREATE TABLE telemetry_v2_effects(event_id uuid PRIMARY KEY,enabled boolean NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var exported []span
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/traces" {
			t.Error("unexpected OTLP endpoint")
		}
		var payload struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []span `json:"spans"`
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
			for _, scope := range resource.ScopeSpans {
				exported = append(exported, scope.Spans...)
			}
		}
		_, _ = w.Write([]byte("{}"))
	}))
	defer collector.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	InitTelemetry(lifetime, "notification-service")
	dependencyErr := errors.New("temporary effect dependency unavailable")
	type markerKey struct{}
	markedCtx := context.WithValue(ctx, markerKey{}, "processing-budget")
	for _, failing := range []bool{false, true} {
		user := uuid.NewString()
		event := eventsv2.Envelope{SchemaVersion: 2, EventID: uuid.Must(uuid.NewV7()).String(), EventType: "identity.mfa_changed", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Traceparent: "00-11111111111111111111111111111111-2222222222222222-01", Payload: json.RawMessage(`{"user_id":"` + user + `","enabled":true}`)}
		if failing {
			event.Traceparent = "00-33333333333333333333333333333333-4444444444444444-01"
		}
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		consumer := Consumer{DB: db, ApplyV2: func(effectCtx context.Context, incoming eventsv2.Envelope) error {
			if effectCtx.Value(markerKey{}) != "processing-budget" || effectCtx.Err() != nil {
				t.Error("processing context lost")
			}
			if deadline, ok := effectCtx.Deadline(); !ok || deadline.After(time.Now().Add(10*time.Second)) {
				t.Error("processing deadline lost")
			}
			tx, err := db.Begin(effectCtx)
			if err != nil {
				return err
			}
			defer tx.Rollback(effectCtx)
			fresh, err := AcceptV2Receipt(effectCtx, tx, incoming)
			if err != nil || !fresh {
				return errors.New("fresh v2 receipt not accepted")
			}
			if _, err = tx.Exec(effectCtx, "INSERT INTO telemetry_v2_effects(event_id,enabled) VALUES($1,true)", incoming.EventID); err != nil {
				return err
			}
			if failing {
				return dependencyErr
			}
			return tx.Commit(effectCtx)
		}}
		err = consumer.processRecord(markedCtx, &kgo.Record{Topic: event.Topic(), Key: []byte(user), Value: raw})
		if failing && !errors.Is(err, dependencyErr) || !failing && err != nil {
			t.Fatalf("processing outcome changed: %v", err)
		}
		var receipts, effects int
		if err = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM immutable_v2_receipts WHERE event_id=$1),(SELECT count(*) FROM telemetry_v2_effects WHERE event_id=$1)", event.EventID).Scan(&receipts, &effects); err != nil {
			t.Fatal(err)
		}
		expected := 1
		if failing {
			expected = 0
		}
		if receipts != expected || effects != expected {
			t.Fatalf("receipt/effect atomicity changed: %d/%d", receipts, effects)
		}
	}
	stop()
	flush, finish := context.WithTimeout(context.Background(), 4*time.Second)
	defer finish()
	WaitTelemetry(flush)
	if flush.Err() != nil {
		t.Fatal("exporter did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(exported) != 2 {
		t.Fatalf("expected actual OTLP spans for both v2 processing outcomes, got %d", len(exported))
	}
	for _, item := range exported {
		if item.Name != "kafka.process identity.mfa_changed" || item.Start == "" || item.End <= item.Start || item.SpanID == "" {
			t.Fatalf("invalid processing span: %+v", item)
		}
		expectedParent, expectedStatus := "2222222222222222", float64(1)
		if item.TraceID == "33333333333333333333333333333333" {
			expectedParent, expectedStatus = "4444444444444444", 2
		} else if item.TraceID != "11111111111111111111111111111111" {
			t.Fatal("source trace changed", item.TraceID)
		}
		if item.ParentSpanID != expectedParent || item.Status["code"] != expectedStatus {
			t.Fatalf("trace parent/outcome lost: %+v", item)
		}
	}
}
