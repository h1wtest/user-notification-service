package profile

import (
	"context"
	"github.com/google/uuid"
	"testing"
)

func TestProfileOutboxUsesDedicatedContiguousEventSequence(t *testing.T) {
	db := profilePatchDB(t)
	ctx := context.Background()
	id := uuid.NewString()
	if _, err := db.Exec(ctx, "INSERT INTO profiles(user_id,email,version) VALUES($1,'profile@example.invalid',40)", id); err != nil {
		t.Fatal(err)
	}
	repo := &PostgresProfiles{DB: db}
	for range 2 {
		if err := repo.Update(ctx, id, ProfileChanges{}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.Query(ctx, "SELECT aggregate_version,(event->'payload'->>'version')::bigint FROM owner_outbox WHERE aggregate_id=$1 ORDER BY aggregate_version", id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var emitted, domain int64
		if err := rows.Scan(&emitted, &domain); err != nil {
			t.Fatal(err)
		}
		if emitted != int64(count) || domain != int64(40+count) {
			t.Fatalf("emitted=%d domain=%d", emitted, domain)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if count != 2 {
		t.Fatalf("events=%d", count)
	}
}
