package platformkit

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"platform-contracts/gen/go/eventsv2"
)

func TestDurableOutboxContextReachesTransactionalConsumerEffects(t *testing.T) {
	db := migratedDB(t)
	telemetry := recordingTelemetry("subscription")
	defer telemetry.close(context.Background())
	ctx := withTelemetry(context.Background(), telemetry)
	requestCtx, requestSpan := startSpan(ctx, "00-11111111111111111111111111111111-2222222222222222-01", 2, "")
	event := fixtureEvent(t, uuid.NewString(), 1)
	tx, err := db.Begin(requestCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err = Enqueue(requestCtx, tx, event); err != nil {
		_ = tx.Rollback(requestCtx)
		t.Fatal(err)
	}
	if err = tx.Commit(requestCtx); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = db.QueryRow(ctx, "SELECT envelope FROM outbox WHERE event_id=$1", event.EventID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var stored eventsv2.Envelope
	if err = json.Unmarshal(raw, &stored); err != nil || stored.Traceparent != requestSpan.header() {
		t.Fatal("request context not durable", err)
	}
	record, producer := publicationRecord(ctx, publication{Topic: event.Topic(), Key: event.AggregateID, Raw: raw})
	consumerCtx, consumer := consumerSpan(ctx, record)
	applied := false
	processor := eventConsumer{db: db, key: make([]byte, 32), apply: func(effectCtx context.Context, transaction pgx.Tx, incoming eventsv2.Envelope) error {
		applied = true
		if traceparentFrom(effectCtx) != consumer.header() {
			t.Error("transaction callback lost processing span context")
		}
		return nil
	}}
	if err = processor.processRecord(consumerCtx, record); err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("valid durable event did not reach real transactional effects")
	}
	var inboxCount int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM inbox WHERE event_id=$1", event.EventID).Scan(&inboxCount); err != nil || inboxCount != 1 {
		t.Fatal("transactional inbox not committed", err)
	}
	producer.finish("kafka.publish", false)
	consumer.finish("kafka.process", false)
	published, processed := <-telemetry.queue, <-telemetry.queue
	if published.ParentSpanID != requestSpan.trace.spanID || processed.ParentSpanID != published.SpanID || len(processed.Links) != 1 || processed.Links[0].SpanID != requestSpan.trace.spanID {
		t.Fatal("durable processing ancestry/link missing")
	}
}
