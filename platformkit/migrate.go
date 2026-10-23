package platformkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Migration struct {
	Version int
	SQL     string
}

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, errors.New("database URL required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("database pool unavailable")
	}
	ping, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.Ping(ping); err != nil {
		db.Close()
		return nil, errors.New("database unavailable")
	}
	return db, nil
}

func Migrate(ctx context.Context, db *pgxpool.Pool, owner string, steps []Migration) error {
	if db == nil || owner == "" || strings.TrimSpace(owner) != owner || len(steps) == 0 {
		return errors.New("owner and migrations required")
	}
	steps = append([]Migration(nil), steps...)
	sort.Slice(steps, func(i, j int) bool { return steps[i].Version < steps[j].Version })
	for i, step := range steps {
		if step.Version != i+1 || strings.TrimSpace(step.SQL) == "" {
			return errors.New("migration versions must be consecutive from one with nonempty SQL")
		}
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// The lock must not include owner: competing wrong-owner initializers must
	// serialize before either installs the singleton database ownership marker.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('platformkit-migrations',0))"); err != nil {
		return err
	}
	var legacy bool
	if err = tx.QueryRow(ctx, "SELECT to_regclass('service_schema') IS NOT NULL").Scan(&legacy); err != nil {
		return err
	}
	if legacy {
		return errors.New("legacy owner schema requires its existing internal/platform migration runner")
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS platformkit_schema(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),owner text NOT NULL,version integer NOT NULL CHECK(version>=0));
CREATE TABLE IF NOT EXISTS platformkit_migrations(version integer PRIMARY KEY CHECK(version>0),owner text NOT NULL,checksum text NOT NULL CHECK(length(checksum)=64),applied_at timestamptz NOT NULL DEFAULT now());`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO platformkit_schema(singleton,owner,version) VALUES(true,$1,0) ON CONFLICT DO NOTHING", owner); err != nil {
		return err
	}
	var actual string
	var version int
	if err = tx.QueryRow(ctx, "SELECT owner,version FROM platformkit_schema WHERE singleton FOR UPDATE").Scan(&actual, &version); err != nil {
		return err
	}
	if actual != owner {
		return errors.New("database already owned by another service")
	}
	if version > len(steps) {
		return errors.New("database schema is newer than supplied migrations")
	}
	rows, err := tx.Query(ctx, "SELECT version,owner,checksum FROM platformkit_migrations ORDER BY version")
	if err != nil {
		return err
	}
	applied := 0
	for rows.Next() {
		var v int
		var recordedOwner, checksum string
		if err = rows.Scan(&v, &recordedOwner, &checksum); err != nil {
			rows.Close()
			return err
		}
		applied++
		if v != applied || v > len(steps) || recordedOwner != owner || checksum != migrationChecksum(steps[v-1].SQL) {
			rows.Close()
			return errors.New("migration owner, history or checksum mismatch")
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if applied != version {
		return errors.New("migration marker does not match applied history")
	}
	for _, step := range steps[applied:] {
		if _, err = tx.Exec(ctx, step.SQL); err != nil {
			return fmt.Errorf("migration %d failed: %w", step.Version, err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO platformkit_migrations(version,owner,checksum) VALUES($1,$2,$3)", step.Version, owner, migrationChecksum(step.SQL)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "UPDATE platformkit_schema SET version=$1 WHERE singleton", step.Version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func migrationChecksum(sql string) string {
	sum := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(sum[:])
}
