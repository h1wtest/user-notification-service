package platform

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPurposeKeyMigrationPreservesCiphertextAndIsRepeatSafe(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = Migrate(ctx, db, "notification"); err != nil {
		t.Fatal(err)
	}
	legacy := make([]byte, 32)
	delivery := make([]byte, 32)
	quarantine := make([]byte, 32)
	delivery[0] = 1
	quarantine[0] = 2
	id, user := uuid.NewString(), uuid.NewString()
	body, err := Encrypt(legacy, id, []byte("secret intent"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,status) VALUES($1,$1,$2,'email',$3,'<rekey@sso.local>','failed')", id, user, body)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(ctx, "DELETE FROM delivery_jobs WHERE id=$1", id)
	// Transform persisted ciphertext inside a rollback-only transaction; do
	// not mark unrelated fixture data as a whole-database migration.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = rekeyDelivery(ctx, tx, id, body, legacy, delivery); err != nil {
		t.Fatal(err)
	}
	var encrypted string
	if err = tx.QueryRow(ctx, "SELECT encrypted_body FROM delivery_jobs WHERE id=$1", id).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	plain, err := Decrypt(delivery, id, encrypted)
	if err != nil || string(plain) != "secret intent" {
		t.Fatal("delivery payload lost during key split")
	}
	if _, err = Decrypt(quarantine, id, encrypted); err == nil {
		t.Fatal("quarantine key can decrypt delivery intent")
	}
	if err = rekeyDelivery(ctx, tx, id, encrypted, legacy, delivery); err != nil {
		t.Fatal("repeat-safe ciphertext conversion failed", err)
	}
}
