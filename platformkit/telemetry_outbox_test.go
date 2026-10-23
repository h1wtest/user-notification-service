package platformkit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
)

func TestEnqueuePersistsFirstTraceAcrossChangedRequestRetry(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	event := fixtureEvent(t, uuid.NewString(), 1)
	event.EventType = "identity.mfa_changed"
	event.Payload = json.RawMessage(`{"user_id":"` + event.AggregateID + `","enabled":false}`)
	if err = event.Validate(); err != nil {
		t.Fatal("initial collision fixture invalid", err)
	}
	first, _ := parseTraceparent("00-11111111111111111111111111111111-2222222222222222-01")
	second, _ := parseTraceparent("00-33333333333333333333333333333333-4444444444444444-01")
	if err = Enqueue(context.WithValue(ctx, traceKey{}, first), tx, event); err != nil {
		t.Fatal(err)
	}
	var original, replay []byte
	if err = tx.QueryRow(ctx, "SELECT envelope FROM outbox WHERE event_id=$1", event.EventID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if err = Enqueue(context.WithValue(ctx, traceKey{}, second), tx, event); err != nil {
		t.Fatal("changed request trace poisoned stable event retry", err)
	}
	if err = tx.QueryRow(ctx, "SELECT envelope FROM outbox WHERE event_id=$1", event.EventID).Scan(&replay); err != nil {
		t.Fatal(err)
	}
	if string(original) != string(replay) {
		t.Fatal("retry modified durable event bytes")
	}
	var stored eventsv2.Envelope
	if err = json.Unmarshal(replay, &stored); err != nil || stored.Traceparent != first.header() {
		t.Fatal("first durable trace not retained", err)
	}
	event.Payload = json.RawMessage(`{"user_id":"` + event.AggregateID + `","enabled":true}`)
	if err = event.Validate(); err != nil {
		t.Fatal("competing business envelope invalid", err)
	}
	if err = Enqueue(context.WithValue(ctx, traceKey{}, second), tx, event); !errors.Is(err, ErrPoison) {
		t.Fatal("business payload collision silently accepted", err)
	}
}

func TestEnqueueDoesNotReplaceExplicitEnvelopeTrace(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	event := fixtureEvent(t, uuid.NewString(), 1)
	event.Traceparent = "00-11111111111111111111111111111111-2222222222222222-01"
	other, _ := parseTraceparent("00-33333333333333333333333333333333-4444444444444444-01")
	if err = Enqueue(context.WithValue(ctx, traceKey{}, other), tx, event); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = tx.QueryRow(ctx, "SELECT envelope FROM outbox WHERE event_id=$1", event.EventID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var stored eventsv2.Envelope
	if err = json.Unmarshal(raw, &stored); err != nil || stored.Traceparent != event.Traceparent {
		t.Fatal("explicit producer trace overwritten", err)
	}
	event.Traceparent = other.header()
	if err = Enqueue(ctx, tx, event); !errors.Is(err, ErrPoison) {
		t.Fatal("explicit metadata collision ignored", err)
	}
}
