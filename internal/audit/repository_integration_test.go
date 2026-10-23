// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"user-notification-service/internal/platform"
)

func auditDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("isolated migrated AUDIT_TEST_DATABASE_URL or TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"audit_test_" + uuid.NewString()}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	// Register cleanup before opening the owner pool so even failed setup
	// removes only this test's schema and never another fixture's rows.
	var db *pgxpool.Pool
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		if _, cleanupErr := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); cleanupErr != nil {
			t.Errorf("remove audit fixture schema: %v", cleanupErr)
		}
		admin.Close()
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	db, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = platform.Migrate(ctx, db, "admin"); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestIntegrationConcurrentCommandAdmissionAndTerminalOutcome(t *testing.T) {
	db := auditDatabase(t)
	p := &PostgresAudit{DB: db}
	ctx := context.Background()
	actor, target, key := uuid.NewString(), uuid.NewString(), uuid.NewString()
	const workers = 12
	ids := make(chan string, workers)
	created := make(chan bool, workers)
	failures := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			c, fresh, err := p.Admit(ctx, actor, "ban", target, "case reason", key)
			if err != nil {
				failures <- err
				return
			}
			ids <- c.ID
			created <- fresh
		}()
	}
	group.Wait()
	close(ids)
	close(created)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	var id string
	for got := range ids {
		if id != "" && got != id {
			t.Fatalf("duplicate key produced IDs %s and %s", id, got)
		}
		id = got
	}
	count := 0
	for fresh := range created {
		if fresh {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("fresh dispatches=%d", count)
	}
	if _, _, err := p.Admit(ctx, actor, "unban", target, "case reason", key); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different command reused key: %v", err)
	}
	if err := p.Finish(ctx, id, "succeeded", nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Finish(ctx, id, "unknown", nil); err != nil {
		t.Fatal(err)
	}
	// Recreate repository to prove status does not depend on process memory.
	recovered, err := (&PostgresAudit{DB: db}).Lookup(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != "succeeded" || recovered.Reason != "case reason" {
		t.Fatalf("terminal outcome regressed: %+v", recovered)
	}
}

func TestIntegrationProjectionDeduplicatesAndPagesStableTies(t *testing.T) {
	db := auditDatabase(t)
	p := &PostgresAudit{DB: db}
	ctx := context.Background()
	at := time.Date(2099, 10, 24, 12, 0, 0, 0, time.UTC)
	ids := []string{"019a0000-0000-7000-8000-000000000010", "019a0000-0000-7000-8000-000000000011", "019a0000-0000-7000-8000-000000000012"}
	for _, id := range ids {
		e := AuditEvent{ID: id, EventType: "profile.updated", CreatedAt: at}
		if err := p.ProjectMetadata(ctx, e, ""); err != nil {
			t.Fatal(err)
		}
		if err := p.ProjectMetadata(ctx, e, ""); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM audit_events WHERE id=ANY($1::uuid[])", ids).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("duplicate projections=%d", n)
	}
	rows, err := p.List(ctx, Page{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].ID != ids[2] || rows[1].ID != ids[1] {
		t.Fatalf("unstable first page: %+v", rows)
	}
	rows, err = p.List(ctx, Page{Limit: 2, Before: at, BeforeID: ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 || rows[0].ID != ids[0] {
		t.Fatalf("tie cursor skipped/duplicated item: %+v", rows)
	}
}

func TestIntegrationAggregateGapRollsBackReceiptAndRecoversInOrder(t *testing.T) {
	db := auditDatabase(t)
	p := &PostgresAudit{DB: db}
	ctx := context.Background()
	aggregate, kind, producer := uuid.NewString(), "user", "profile"
	one, two := int64(1), int64(2)
	first := AuditEvent{ID: uuid.NewString(), EventType: "profile.updated", AggregateID: &aggregate, AggregateType: &kind, AggregateVersion: &one, Producer: &producer, CreatedAt: time.Now().UTC()}
	second := first
	second.ID = uuid.NewString()
	second.AggregateVersion = &two
	if err := p.ProjectMetadata(ctx, second, ""); !errors.Is(err, ErrAggregateGap) {
		t.Fatalf("gap advanced projection: %v", err)
	}
	var n int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM audit_v2_receipts WHERE event_id=$1", second.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("gap committed inbox receipt")
	}
	if err := p.ProjectMetadata(ctx, first, ""); err != nil {
		t.Fatal(err)
	}
	if err := p.ProjectMetadata(ctx, second, ""); err != nil {
		t.Fatal(err)
	}
	if err := p.ProjectMetadata(ctx, first, ""); err != nil {
		t.Fatal(err)
	}
	var version int64
	if err := db.QueryRow(ctx, "SELECT version FROM audit_aggregate_versions WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3", producer, kind, aggregate).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("duplicate regressed aggregate: %d", version)
	}
}

func TestIntegrationV1DrainReceiptDoesNotBlockV2Stream(t *testing.T) {
	db := auditDatabase(t)
	p := &PostgresAudit{DB: db}
	ctx := context.Background()
	aggregate, kind, producer := uuid.NewString(), "user", "identity"
	id := uuid.NewString()
	old := platform.Envelope{EventID: id, EventType: "identity.email_verified", AggregateID: aggregate, OccurredAt: time.Now().UTC()}
	if err := p.Project(ctx, old); err != nil {
		t.Fatal(err)
	}
	one, two := int64(1), int64(2)
	row := AuditEvent{ID: id, EventType: old.EventType, UserID: &aggregate, AggregateID: &aggregate, AggregateType: &kind, AggregateVersion: &one, Producer: &producer, CreatedAt: old.OccurredAt}
	if err := p.ProjectMetadata(ctx, row, ""); err != nil {
		t.Fatal(err)
	}
	row.ID = uuid.NewString()
	row.AggregateVersion = &two
	if err := p.ProjectMetadata(ctx, row, ""); err != nil {
		t.Fatalf("v1 duplicate prevented v2 next version: %v", err)
	}
	var n int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM audit_events WHERE id=$1", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cutover duplicated projection: %d", n)
	}
}

func TestIntegrationScannedAuditTimestampsUseCanonicalUTC(t *testing.T) {
	db := auditDatabase(t)
	p := &PostgresAudit{DB: db}
	ctx := context.Background()
	c, _, err := p.Admit(ctx, uuid.NewString(), "ban", uuid.NewString(), "timezone case", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if c.CreatedAt.Location() != time.UTC || c.UpdatedAt.Location() != time.UTC {
		t.Fatalf("command timestamps were not normalized: %v %v", c.CreatedAt.Location(), c.UpdatedAt.Location())
	}
	items, err := p.List(ctx, Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.CreatedAt.Location() != time.UTC {
			t.Fatalf("audit timestamp was not normalized: %v", item.CreatedAt.Location())
		}
	}
}
