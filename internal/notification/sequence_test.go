package notification

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestV2IrrelevantEventsAdvanceContiguousStreamAndGapDefers(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	user := uuid.NewString()
	ctx := context.Background()
	payload, err := json.Marshal(eventsv2.EmailVerified{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: uuid.Must(uuid.NewV7()).String(), EventType: "identity.email_verified", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 2, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = s.ApplyV2(ctx, e); !errors.Is(err, errSequenceGap) {
		t.Fatalf("gap acknowledged: %v", err)
	}
	first := e
	first.EventID = uuid.Must(uuid.NewV7()).String()
	first.AggregateVersion = 1
	if err = s.ApplyV2(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyV2(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyV2(ctx, e); err != nil {
		t.Fatal(err)
	}
	var version int64
	if err = db.QueryRow(ctx, "SELECT version FROM notification_aggregate_versions WHERE producer='identity' AND aggregate_type='user' AND aggregate_id=$1", user).Scan(&version); err != nil || version != 2 {
		t.Fatalf("irrelevant receipt lost: %d %v", version, err)
	}
}

func TestV2RetainedLegacyReceiptStillAdvancesCursor(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	ctx := context.Background()
	user, event := uuid.NewString(), uuid.Must(uuid.NewV7()).String()
	if _, err := db.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,'identity.email_verified')", event); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(eventsv2.EmailVerified{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: event, EventType: "identity.email_verified", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = s.ApplyV2(ctx, e); err != nil {
		t.Fatal(err)
	}
	var version int64
	if err = db.QueryRow(ctx, "SELECT version FROM notification_aggregate_versions WHERE producer='identity' AND aggregate_type='user' AND aggregate_id=$1", user).Scan(&version); err != nil || version != 1 {
		t.Fatalf("legacy cutover did not advance: %d %v", version, err)
	}
}

func TestV2ReusedEventIDRejectsChangedImmutableEnvelope(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	ctx := context.Background()
	user := uuid.NewString()
	payload, err := json.Marshal(eventsv2.EmailVerified{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	original := eventsv2.Envelope{SchemaVersion: 2, EventID: uuid.Must(uuid.NewV7()).String(), EventType: "identity.email_verified", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = s.ApplyV2(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyV2(ctx, original); err != nil {
		t.Fatalf("exact duplicate rejected: %v", err)
	}
	changedVersion := original
	changedVersion.AggregateVersion = 2
	changedType := original
	changedType.EventType = "identity.password_changed"
	changedTime := original
	changedTime.OccurredAt = original.OccurredAt.Add(time.Second)
	otherUser := uuid.NewString()
	changedAggregate := original
	changedAggregate.AggregateID = otherUser
	changedAggregate.Payload, err = json.Marshal(eventsv2.EmailVerified{UserID: otherUser})
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []eventsv2.Envelope{changedVersion, changedType, changedTime, changedAggregate} {
		if err = s.acceptV2(ctx, changed, nil, nil); !errors.Is(err, platform.ErrPoison) {
			t.Fatalf("event identity collision acknowledged: %v", err)
		}
	}
	var version int64
	if err = db.QueryRow(ctx, "SELECT version FROM notification_aggregate_versions WHERE producer='identity' AND aggregate_type='user' AND aggregate_id=$1", user).Scan(&version); err != nil || version != 1 {
		t.Fatalf("collision changed cursor: %d %v", version, err)
	}
}

func TestV2RetainedLegacyReceiptRejectsEventTypeCollision(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	ctx := context.Background()
	user, event := uuid.NewString(), uuid.Must(uuid.NewV7()).String()
	if _, err := db.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,'identity.email_verified')", event); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(eventsv2.PasswordChanged{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: event, EventType: "identity.password_changed", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = s.acceptV2(ctx, e, nil, nil); !errors.Is(err, platform.ErrPoison) {
		t.Fatalf("legacy type collision acknowledged: %v", err)
	}
	var count int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM notification_aggregate_versions WHERE producer='identity' AND aggregate_type='user' AND aggregate_id=$1", user).Scan(&count); err != nil || count != 0 {
		t.Fatalf("legacy collision retained cursor: %d %v", count, err)
	}
}
