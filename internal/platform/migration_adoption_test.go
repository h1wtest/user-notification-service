package platform

import (
	"context"
	"github.com/google/uuid"
	"testing"
)

func TestLegacyMigrationAdoptionPreservesDataAndFencesOtherOwner(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	user := uuid.NewString()
	if _, err := db.Exec(ctx, "UPDATE service_schema SET version=1; DELETE FROM service_migrations; DROP TABLE immutable_v2_receipts,notification_templates,notification_reminders,notification_aggregate_versions,notification_v2_receipts,owner_outbox,owner_event_versions; ALTER TABLE delivery_jobs DROP COLUMN template_version,DROP COLUMN replay_version; ALTER TABLE replay_audit DROP COLUMN reason; ALTER TABLE quarantine_replays DROP COLUMN reason; ALTER TABLE delivery_jobs DROP CONSTRAINT delivery_jobs_event_recipient_channel_key; ALTER TABLE delivery_jobs ADD CONSTRAINT delivery_jobs_event_id_channel_key UNIQUE(event_id,channel)"); err != nil {
		t.Fatal(err)
	}
	job := uuid.NewString()
	event := uuid.NewString()
	if _, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id) VALUES($1,$2,$3,'email','retained-ciphertext','retained-message')", job, event, user); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db, "notification"); err != nil {
		t.Fatal(err)
	}
	var ciphertext string
	var version int
	if err := db.QueryRow(ctx, "SELECT encrypted_body FROM delivery_jobs WHERE id=$1", job).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, "SELECT version FROM service_schema WHERE owner='notification'").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if ciphertext != "retained-ciphertext" || version != ownerSchemaVersion("notification") {
		t.Fatalf("ciphertext changed or migration incomplete version=%d", version)
	}
	if err := Migrate(ctx, db, "profile"); err == nil {
		t.Fatal("foreign migration owner admitted")
	}
}
