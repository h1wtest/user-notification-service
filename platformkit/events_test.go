package platformkit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"platform-contracts/gen/go/eventsv2"
)

func fixtureEvent(t *testing.T, aggregate string, version int64) eventsv2.Envelope {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	return eventsv2.Envelope{SchemaVersion: 2, EventID: id.String(), EventType: "identity.email_verified", Producer: "identity", AggregateType: "user", AggregateID: aggregate, AggregateVersion: version, OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"user_id":"` + aggregate + `"}`)}
}
func fixtureRecord(t *testing.T, e eventsv2.Envelope, offset int64) *kgo.Record {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "identity.events.v2", Partition: 0, Offset: offset, Key: []byte(e.AggregateID), Value: raw}
}

func TestEnqueueRejectsInvalidEnvelopeAndConflictingDuplicate(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	e := fixtureEvent(t, uuid.NewString(), 1)
	if err = Enqueue(ctx, tx, e); err != nil {
		t.Fatal(err)
	}
	if err = Enqueue(ctx, tx, e); err != nil {
		t.Fatal("exact retry rejected", err)
	}
	e.Payload = json.RawMessage(`{"user_id":"00000000-0000-0000-0000-000000000000"}`)
	if err = Enqueue(ctx, tx, e); err == nil {
		t.Fatal("same ID conflicting envelope accepted")
	}
	e = fixtureEvent(t, uuid.NewString(), 1)
	e.Producer = "billing"
	if err = Enqueue(ctx, tx, e); !errors.Is(err, ErrPoison) {
		t.Fatal("forged producer accepted", err)
	}
}

func TestProcessRecordRollsBackEffectsAndInboxThenRetriesExactlyOnce(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE effects(event_id uuid PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	e := fixtureEvent(t, uuid.NewString(), 1)
	record := fixtureRecord(t, e, 1)
	attempts := 0
	consumer := eventConsumer{db: db, key: make([]byte, 32), apply: func(ctx context.Context, tx pgx.Tx, e eventsv2.Envelope) error {
		attempts++
		if _, err := tx.Exec(ctx, "INSERT INTO effects VALUES($1)", e.EventID); err != nil {
			return err
		}
		if attempts == 1 {
			return errors.New("temporary dependency outage")
		}
		return nil
	}}
	if err := consumer.processRecord(ctx, record); err == nil {
		t.Fatal("dependency failure acknowledged")
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM inbox").Scan(&count); err != nil || count != 0 {
		t.Fatal("inbox committed failed effects")
	}
	if err := consumer.processRecord(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := consumer.processRecord(ctx, record); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatal("duplicate effect reapplied", attempts)
	}
	if err := db.QueryRow(ctx, "SELECT count(*) FROM effects").Scan(&count); err != nil || count != 1 {
		t.Fatal("effects not atomic", err)
	}
}

func TestProcessRecordGapRetriesAndPoisonQuarantinesEncrypted(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	aggregate := uuid.NewString()
	called := false
	consumer := eventConsumer{db: db, key: make([]byte, 32), apply: func(context.Context, pgx.Tx, eventsv2.Envelope) error { called = true; return nil }}
	gap := fixtureRecord(t, fixtureEvent(t, aggregate, 2), 1)
	if err := consumer.processRecord(ctx, gap); !errors.Is(err, ErrSequenceGap) || called {
		t.Fatal("sequence gap admitted effects", err)
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM quarantine").Scan(&count); err != nil || count != 0 {
		t.Fatal("dependency/gap misclassified poison")
	}
	bad := fixtureRecord(t, fixtureEvent(t, aggregate, 1), 2)
	bad.Key = []byte(uuid.NewString())
	if err := consumer.processRecord(ctx, bad); err != nil {
		t.Fatal(err)
	}
	var encrypted string
	if err := db.QueryRow(ctx, "SELECT encrypted_record FROM quarantine").Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	raw, err := decryptRecord(make([]byte, 32), "identity.events.v2:0:2", encrypted)
	if err != nil || string(raw) != string(bad.Value) {
		t.Fatal("quarantine bytes not recoverable", err)
	}
	if encrypted == string(bad.Value) {
		t.Fatal("quarantine stored plaintext")
	}
	if err := db.QueryRow(ctx, "SELECT count(*) FROM dlq_outbox").Scan(&count); err != nil || count != 1 {
		t.Fatal("quarantine has no atomic DLQ metadata")
	}
	if err := consumer.processRecord(ctx, bad); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, "SELECT count(*) FROM quarantine").Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate poison duplicated quarantine")
	}
}
