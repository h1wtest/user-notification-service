package profile

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
)

func TestProfileV2CutoverAdvancesSameLegacyReceiptAndRejectsGap(t *testing.T) {
	db := profilePatchDB(t)
	ctx := context.Background()
	s := &Service{DB: db}
	user := uuid.NewString()
	id, _ := uuid.NewV7()
	if _, err := db.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,'identity.user_registered')", id.String()); err != nil {
		t.Fatal(err)
	}
	registration, _ := uuid.NewV7()
	payload, err := json.Marshal(eventsv2.UserRegistered{UserID: user, Email: "profile@example.invalid", Locale: "ru", RegistrationID: registration.String()})
	if err != nil {
		t.Fatal(err)
	}
	event := eventsv2.Envelope{SchemaVersion: 2, EventID: id.String(), EventType: "identity.user_registered", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = s.ApplyV2(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyV2(ctx, event); err != nil {
		t.Fatal(err)
	}
	nextID, _ := uuid.NewV7()
	nextPayload, err := json.Marshal(eventsv2.EmailVerified{UserID: user})
	if err != nil {
		t.Fatal(err)
	}
	event.EventID = nextID.String()
	event.EventType = "identity.email_verified"
	event.Payload = nextPayload
	event.AggregateVersion = 3
	if err = s.ApplyV2(ctx, event); err == nil {
		t.Fatal("sequence gap accepted")
	}
	event.AggregateVersion = 2
	if err = s.ApplyV2(ctx, event); err != nil {
		t.Fatal(err)
	}
	var version int64
	var language string
	if err = db.QueryRow(ctx, "SELECT version FROM profile_aggregate_versions WHERE aggregate_id=$1", user).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(ctx, "SELECT website_locale FROM profiles WHERE user_id=$1", user).Scan(&language); err != nil {
		t.Fatal(err)
	}
	if version != 2 || language != "ru" {
		t.Fatalf("version=%d language=%s", version, language)
	}
}
