package platform

import (
	"context"
	"testing"
)

func TestMigrationChecksumRejectsModifiedAppliedVersion(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, "notification"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "UPDATE service_migrations SET checksum='changed' WHERE owner='notification' AND version=1"); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db, "notification"); err == nil {
		t.Fatal("changed applied migration accepted")
	}
}
