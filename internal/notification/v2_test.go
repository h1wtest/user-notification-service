package notification

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestV2EncryptedCommandUsesRecipientNotCommandAggregate(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{DeliveryKey: make([]byte, 32)}, nil)
	eventID := uuid.Must(uuid.NewV7()).String()
	user := uuid.NewString()
	cleanupDelivery(t, db, eventID)
	payload, err := json.Marshal(eventsv2.EmailVerification{UserID: user, Email: "user@example.test", Code: "123456", ExpiresAt: time.Now().UTC().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := platform.Encrypt(s.Config.DeliveryKey, eventID, payload)
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: eventID, EventType: "notification.email_verification", Producer: "identity", AggregateType: "delivery_command", AggregateID: eventID, AggregateVersion: 1, OccurredAt: time.Now().UTC(), EncryptedPayload: encrypted}
	for range 2 {
		if err = s.ApplyV2(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	var recipient string
	if err = db.QueryRow(context.Background(), "SELECT count(*),min(user_id::text) FROM delivery_jobs WHERE event_id=$1", eventID).Scan(&count, &recipient); err != nil || count != 1 || recipient != user {
		t.Fatalf("wrong receipt/recipient %d %s %v", count, recipient, err)
	}
}
func TestV2PublicSecretCommandIsRejected(t *testing.T) {
	s := New(nil, platform.Config{}, nil)
	id := uuid.Must(uuid.NewV7()).String()
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: id, EventType: "notification.password_reset", Producer: "identity", AggregateType: "delivery_command", AggregateID: id, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"token":"secret"}`)}
	if s.ApplyV2(context.Background(), e) == nil {
		t.Fatal("plaintext command accepted")
	}
}
