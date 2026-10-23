// SPDX-License-Identifier: MIT
package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
)

func TestKafkaAggregateKeyMismatchQuarantinesBeforeEitherOwnerCallback(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	key := make([]byte, 32)
	user := uuid.NewString()
	rawPayload, _ := json.Marshal(map[string]string{"user_id": user})
	legacy := Envelope{SchemaVersion: 1, EventID: uuid.NewString(), EventType: "identity.email_verified", AggregateID: user, OccurredAt: time.Now().UTC(), Payload: rawPayload}
	legacyRaw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	modern := eventsv2.Envelope{SchemaVersion: 2, EventID: uuid.Must(uuid.NewV7()).String(), EventType: "identity.email_verified", Producer: "identity", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: rawPayload}
	modernRaw, err := json.Marshal(modern)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	c := Consumer{DB: db, Config: Config{QuarantineKey: key}, Apply: func(context.Context, Envelope) error { called = true; return nil }, ApplyV2: func(context.Context, eventsv2.Envelope) error { called = true; return nil }}
	for i, tc := range []struct {
		topic string
		raw   []byte
	}{{"identity.events.v1", legacyRaw}, {"identity.events.v2", modernRaw}} {
		record := &kgo.Record{Topic: tc.topic, Partition: 2, Offset: int64(i), Key: []byte(uuid.NewString()), Value: tc.raw}
		if err = c.processRecord(ctx, record); err != nil {
			t.Fatal(err)
		}
		if called {
			t.Fatal("wrong-key event reached owner callback")
		}
		var encrypted string
		if err = db.QueryRow(ctx, "SELECT encrypted_record FROM quarantine WHERE topic=$1 AND partition_id=$2 AND offset_id=$3", record.Topic, record.Partition, record.Offset).Scan(&encrypted); err != nil {
			t.Fatal(err)
		}
		decoded, err := Decrypt(key, fmt.Sprintf("%s:%d:%d", record.Topic, record.Partition, record.Offset), encrypted)
		if err != nil || string(decoded) != string(tc.raw) {
			t.Fatalf("quarantine lost original bytes: %v", err)
		}
		record.Key = []byte(user)
		record.Offset += 100
		if err = c.processRecord(ctx, record); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("valid keyed event did not reach owner callback")
		}
		called = false
	}
}
