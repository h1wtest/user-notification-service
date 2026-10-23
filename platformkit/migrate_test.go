package platformkit

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func isolatedDB(t *testing.T) *pgxpool.Pool {
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
	schema := "kit_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return db
}

func migratedDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := isolatedDB(t)
	if err := Migrate(context.Background(), db, "catalog", []Migration{{Version: 1, SQL: SchemaSQL}}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestMigrateRejectsWrongOwnerAndChecksumChanges(t *testing.T) {
	db := isolatedDB(t)
	ctx := context.Background()
	steps := []Migration{{Version: 1, SQL: SchemaSQL}, {Version: 2, SQL: "CREATE TABLE owned_value(id int PRIMARY KEY);"}}
	if err := Migrate(ctx, db, "catalog", steps); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db, "billing", steps); err == nil {
		t.Fatal("foreign owner admitted")
	}
	changed := append([]Migration(nil), steps...)
	changed[1].SQL = "CREATE TABLE changed_value(id int PRIMARY KEY);"
	if err := Migrate(ctx, db, "catalog", changed); err == nil {
		t.Fatal("changed applied migration admitted")
	}
	if err := Migrate(ctx, db, "catalog", steps[:1]); err == nil {
		t.Fatal("schema downgrade admitted")
	}
	if err := Migrate(ctx, db, "catalog", steps); err != nil {
		t.Fatal("identical migration replay failed", err)
	}
}

func TestMigrateRollsBackFailedStepsAndRejectsLegacyOwner(t *testing.T) {
	db := isolatedDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, "catalog", []Migration{{Version: 1, SQL: SchemaSQL}, {Version: 2, SQL: "CREATE TABLE partial_change(id int); SELECT missing_function();"}}); err == nil {
		t.Fatal("failed migration accepted")
	}
	var exists bool
	if err := db.QueryRow(ctx, "SELECT to_regclass('partial_change') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatal("migration transaction left partial DDL")
	}
	if _, err := db.Exec(ctx, "CREATE TABLE service_schema(owner text PRIMARY KEY,version int);INSERT INTO service_schema VALUES('notification',1)"); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db, "catalog", []Migration{{Version: 1, SQL: SchemaSQL}}); err == nil {
		t.Fatal("legacy foreign database adopted")
	}
}

func TestInboxDuplicateAndTypeCollisionAreNotReapplied(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	id := uuid.NewString()
	for i := range 2 {
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := Inbox(ctx, tx, id, "catalog.published")
		if err != nil || fresh != (i == 0) {
			_ = tx.Rollback(ctx)
			t.Fatalf("fresh=%v error=%v", fresh, err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = Inbox(ctx, tx, id, "billing.payment_accepted"); !errors.Is(err, ErrPoison) {
		t.Fatal("event ID type collision not rejected", err)
	}
}

func TestRelayLeaseFencesExpiredPublisherAndBlocksNextAggregateVersion(t *testing.T) {
	db := migratedDB(t)
	ctx := context.Background()
	aggregate := uuid.NewString()
	id1, id2 := uuid.NewString(), uuid.NewString()
	for version, id := range []string{id1, id2} {
		if _, err := db.Exec(ctx, "INSERT INTO outbox(event_id,event_type,producer,aggregate_type,aggregate_id,aggregate_version,topic,envelope) VALUES($1,'catalog.published','catalog','revision',$2,$3,'catalog.events.v1',$4)", id, aggregate, version+1, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := leaseOutbox(ctx, db, "catalog")
	if err != nil || first.ID != id1 {
		t.Fatalf("first lease: %+v %v", first, err)
	}
	if _, err = leaseOutbox(ctx, db, "catalog"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("later version overtook leased predecessor", err)
	}
	if _, err = db.Exec(ctx, "UPDATE outbox SET lease_until=now()-interval '1 second' WHERE event_id=$1", id1); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := leaseOutbox(ctx, db, "catalog")
	if err != nil || reclaimed.Token == first.Token {
		t.Fatal("expired lease not fenced", err)
	}
	if err = finishOutbox(ctx, db, first, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("stale publisher acknowledged reclaimed lease", err)
	}
	if err = finishOutbox(ctx, db, reclaimed, errors.New("broker timeout")); err != nil {
		t.Fatal(err)
	}
	var published *time.Time
	var attempts int
	if err = db.QueryRow(ctx, "SELECT published_at,attempts FROM outbox WHERE event_id=$1", id1).Scan(&published, &attempts); err != nil || published != nil || attempts != 2 {
		t.Fatalf("unacked publication lost: published=%v attempts=%d error=%v", published, attempts, err)
	}
}

func TestConcurrentMigrationOwnersCannotBothClaimDatabase(t *testing.T) {
	db := isolatedDB(t)
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, owner := range []string{"catalog", "billing"} {
		go func() { <-start; results <- Migrate(ctx, db, owner, []Migration{{Version: 1, SQL: SchemaSQL}}) }()
	}
	close(start)
	success := 0
	for range 2 {
		if err := <-results; err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("database claimed by %d owners", success)
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM platformkit_schema").Scan(&count); err != nil || count != 1 {
		t.Fatal("ownership marker is not singleton", err)
	}
}
