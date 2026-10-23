package platform

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func nestedRekeyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "rekey_" + uuid.NewString()
	schema = "\"" + schema + "\""
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() })
	if err = Migrate(ctx, db, "notification"); err != nil {
		t.Fatal(err)
	}
	return db
}
func TestRekeyNestedSensitiveOriginalAndCorrectionRemainReplayable(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	legacy, delivery, quarantine := make([]byte, 32), make([]byte, 32), make([]byte, 32)
	delivery[0] = 1
	quarantine[0] = 2
	id, user := uuid.NewString(), uuid.NewString()
	plain, _ := json.Marshal(map[string]any{"user_id": user, "email": "test@example.test", "code": "123456", "expires_at": time.Now().Add(time.Hour)})
	inner, err := Encrypt(legacy, id, plain)
	if err != nil {
		t.Fatal(err)
	}
	event := Envelope{SchemaVersion: 1, EventID: id, AggregateID: user, EventType: "notification.email_verification", OccurredAt: time.Now(), EncryptedPayload: inner}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := Encrypt(legacy, "notification.commands.v1:0:7", raw)
	if err != nil {
		t.Fatal(err)
	}
	var quarantineID string
	if err = db.QueryRow(ctx, "INSERT INTO quarantine(topic,partition_id,offset_id,encrypted_record,reason) VALUES('notification.commands.v1',0,7,$1,'test') RETURNING id", outer).Scan(&quarantineID); err != nil {
		t.Fatal(err)
	}
	correction, _ := Encrypt(legacy, quarantineID, raw)
	if _, err = db.Exec(ctx, "INSERT INTO quarantine_replays(quarantine_id,actor_id,encrypted_event) VALUES($1,$2,$3)", quarantineID, user, correction); err != nil {
		t.Fatal(err)
	}
	if err = Rekey(ctx, db, "notification", legacy, delivery, quarantine); err != nil {
		t.Fatal(err)
	}
	var convertedOriginal, convertedCorrection string
	if err = db.QueryRow(ctx, "SELECT q.encrypted_record,r.encrypted_event FROM quarantine q JOIN quarantine_replays r ON r.quarantine_id=q.id WHERE q.id=$1", quarantineID).Scan(&convertedOriginal, &convertedCorrection); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct{ aad, value string }{{"notification.commands.v1:0:7", convertedOriginal}, {quarantineID, convertedCorrection}} {
		decoded, err := Decrypt(quarantine, fixture.aad, fixture.value)
		if err != nil {
			t.Fatal(err)
		}
		var converted Envelope
		if json.Unmarshal(decoded, &converted) != nil {
			t.Fatal("converted envelope invalid")
		}
		payload, err := Decrypt(delivery, converted.EventID, converted.EncryptedPayload)
		if err != nil || string(payload) != string(plain) {
			t.Fatal("nested sensitive command cannot replay under delivery key")
		}
		if _, err = Decrypt(legacy, converted.EventID, converted.EncryptedPayload); err == nil {
			t.Fatal("nested command retained legacy delivery key")
		}
	}
	if err = Rekey(ctx, db, "notification", legacy, delivery, quarantine); err != nil {
		t.Fatal("repeat migration failed", err)
	}
}
func TestTamperedNestedCiphertextRollsBackOwnerRekey(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	legacy, delivery, quarantine := make([]byte, 32), make([]byte, 32), make([]byte, 32)
	delivery[0] = 1
	quarantine[0] = 2
	id, user := uuid.NewString(), uuid.NewString()
	inner, _ := Encrypt(legacy, id, []byte(`{"invalid":"payload"}`))
	event := Envelope{SchemaVersion: 1, EventID: id, AggregateID: user, EventType: "notification.email_verification", OccurredAt: time.Now(), EncryptedPayload: inner}
	raw, _ := json.Marshal(event)
	outer, _ := Encrypt(legacy, "notification.commands.v1:0:8", raw)
	if _, err := db.Exec(ctx, "INSERT INTO quarantine(topic,partition_id,offset_id,encrypted_record,reason) VALUES('notification.commands.v1',0,8,$1,'test')", outer); err != nil {
		t.Fatal(err)
	}
	if err := Rekey(ctx, db, "notification", legacy, delivery, quarantine); err == nil {
		t.Fatal("invalid authenticated inner payload accepted")
	}
	var stored string
	if err := db.QueryRow(ctx, "SELECT encrypted_record FROM quarantine WHERE offset_id=8").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != outer {
		t.Fatal("failed nested migration partially changed original")
	}
	var exists bool
	if err := db.QueryRow(ctx, "SELECT to_regclass('service_key_migrations') IS NOT NULL").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("failed migration committed marker/table")
	}
}

func TestNestedCiphertextTagTamperIsRejectedBeforeOuterRekey(t *testing.T) {
	legacy, delivery, quarantine := make([]byte, 32), make([]byte, 32), make([]byte, 32)
	delivery[0] = 1
	quarantine[0] = 2
	id, user := uuid.NewString(), uuid.NewString()
	plain, _ := json.Marshal(map[string]any{"user_id": user, "email": "test@example.test", "code": "123456", "expires_at": time.Now().Add(time.Hour)})
	inner, _ := Encrypt(legacy, id, plain)
	bytes, _ := base64.StdEncoding.DecodeString(inner)
	bytes[len(bytes)-1] ^= 1
	envelope := Envelope{SchemaVersion: 1, EventID: id, AggregateID: user, EventType: "notification.email_verification", OccurredAt: time.Now(), EncryptedPayload: base64.StdEncoding.EncodeToString(bytes)}
	raw, _ := json.Marshal(envelope)
	outer, _ := Encrypt(legacy, "notification.commands.v1:0:9", raw)
	if _, err := rekeyQuarantined("notification", legacy, delivery, quarantine, "notification.commands.v1:0:9", outer); err == nil {
		t.Fatal("tampered nested GCM tag accepted")
	}
}
