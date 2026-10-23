// SPDX-License-Identifier: MIT
package platform

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
)

func TestV2ReceiptRejectsReusedIdentityWithoutAdvancingEffects(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	user := uuid.NewString()
	id := uuid.Must(uuid.NewV7()).String()
	raw, _ := json.Marshal(eventsv2.EmailVerified{UserID: user})
	event := eventsv2.Envelope{SchemaVersion: 2, EventID: id, EventType: "identity.email_verified", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: raw}
	accept := func(e eventsv2.Envelope) (bool, error) {
		tx, err := db.Begin(ctx)
		if err != nil {
			return false, err
		}
		defer tx.Rollback(ctx)
		fresh, err := AcceptV2Receipt(ctx, tx, e)
		if err != nil {
			return false, err
		}
		return fresh, tx.Commit(ctx)
	}
	if fresh, err := accept(event); err != nil || !fresh {
		t.Fatalf("new receipt=%v %v", fresh, err)
	}
	if fresh, err := accept(event); err != nil || fresh {
		t.Fatalf("exact replay=%v %v", fresh, err)
	}
	changed := event
	changed.AggregateVersion = 2
	if _, err := accept(changed); !errors.Is(err, ErrPoison) {
		t.Fatalf("changed version silently acknowledged: %v", err)
	}
	changed = event
	changed.OccurredAt = changed.OccurredAt.Add(time.Second)
	if _, err := accept(changed); !errors.Is(err, ErrPoison) {
		t.Fatalf("changed immutable content acknowledged: %v", err)
	}
	var receipts int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM immutable_v2_receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("receipts=%d", receipts)
	}
}
