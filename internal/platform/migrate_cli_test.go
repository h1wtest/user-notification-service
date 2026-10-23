package platform

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNotificationMigrationCLIRequiresOnlyDatabaseURL(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable TEST_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "run", "./cmd/notification", "migrate")
	command.Dir = filepath.Join("..", "..")
	for _, entry := range os.Environ() {
		key := strings.SplitN(entry, "=", 2)[0]
		if key == "DATABASE_URL" || strings.HasSuffix(key, "ENCRYPTION_KEY") || key == "PUBLIC_ORIGIN" || strings.Contains(key, "INTERNAL_TOKEN") || strings.HasPrefix(key, "TLS_") || key == "INTERNAL_CA_FILE" {
			continue
		}
		command.Env = append(command.Env, entry)
	}
	command.Env = append(command.Env, "DATABASE_URL="+dsn)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("migration with DSN only failed: %v %s", err, output)
	}
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var owner string
	if err = db.QueryRow(ctx, "SELECT owner FROM service_schema WHERE owner='notification' AND version=$1", ownerSchemaVersion("notification")).Scan(&owner); err != nil {
		t.Fatal(err)
	}
}
