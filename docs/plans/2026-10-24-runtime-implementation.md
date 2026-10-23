# C02 Technical Runtime Implementation Plan

> **For agentic workers:** execute inline against the approved generic-platform design; parent owns all builds, tests, lint, formatting, commits and runtime verification.

**Goal:** Export the exact `platformkit` contract for independently owned Go services without sharing domain models or accessing another owner's database.

**Architecture:** A pure technical Go package provides configuration, bounded HTTPS JSON and authoritative identity admission, checksum/owner migrations, health lifecycle, transactional event storage and Kafka delivery. Business owners supply migrations and transaction callbacks. Runtime startup never executes DDL.

**Tech Stack:** Go 1.26.10; existing pinned pgx/v5 5.11.0, franz-go 1.22.2, google/uuid 1.6.0; generated platform-contracts/gen/go/eventsv2.

## Global Constraints
- Edit only platformkit and this explicitly required plan; root go.mod only if a dependency is missing. Existing internal/platform remains untouched.
- Exact public signatures in local://generic-platform-integration.md; Kafka TLS/SCRAM required, HTTPS no redirects, no actor caching or synthetic identity.
- Preserve existing v1 wire; `/internal/v2/introspect` explicitly accepts v1-compatible credential/permission fields and `active,is_verified` response.
- Every owner's first migration includes `platformkit.SchemaSQL`; kit uses owner singleton plus immutable SHA-256 migration ledger with a database-wide advisory lock.
- Consume requires QUARANTINE_ENCRYPTION_KEY (base64 32 bytes), same convention as existing runtime, without expanding Config.
- Parent alone runs verification after all owners land. Author regression tests before their implementation; no claim of observed red/green here.

## File Map and Tasks

### 1. Configuration, JSON and actor admission
Files: platformkit/config.go, http.go, http_test.go, config_test.go.
- Author behavioral regressions for unknown/trailing/duplicate JSON, oversized/empty/null bodies, invalid bearer, inactive/malformed identity, missing permission/MFA, response-size overflow, redirects and actor-header forgery.
- Implement Config/Load, Client/NewClient/Call/ForwardActor/Authenticate and JSON/Error/Decode exact signatures. Bound request/response bodies to 1 MiB and calls to five seconds. Forward browser credentials and CSRF/origin only; forwarded actor credential headers are accepted only from internal routes whose owner has first authenticated its workload caller.
- Authenticate calls live identity v2 with original method/origin/permission/admin MFA requirement. Explicitly parse active and is_verified compatibility fields. Fail closed to 401/403/503; never trust actor IDs/roles supplied in headers.

### 2. Owner migrations and TLS lifecycle
Files: platformkit/migrate.go, server.go, migrate_test.go, server_test.go.
- Author real disposable-PostgreSQL regressions for wrong owner (including concurrent ownership), changed checksum, missing/history downgrade, failed transaction rollback, empty-schema readiness and migration readiness.
- Connect supplies bounded pgx connection configuration. Migrate sorts/validates consecutive versioned steps, serializes globally, creates only migration-ledger DDL and runs supplied owner SQL atomically. Existing legacy migrations are not imported.
- Serve binds HTTPS health/live, health/ready, metrics; checks owner migration marker before callback; starts cancellable background work; propagates background/listen errors and gracefully shuts down with a bounded deadline without modifying schema.

### 3. Durable v2 event transport
Files: platformkit/schema.go, events.go, relay.go, consume.go, events_test.go, kafka_integration_test.go.
- Author real database behavioral regressions before implementation for inbox duplicates/type collisions, Enqueue validation/duplicate IDs/version collision, rollback-on-apply failure, gap/order rejection, lease expiry/fencing, unacknowledged records, quarantine encryption and DLQ transaction atomicity. Author real Kafka regression for reseek-before-rebalance and no commit over failed record.
- SchemaSQL defines standard outbox, inbox, aggregate cursor, encrypted quarantine and metadata DLQ outbox tables; no runtime DDL.
- Enqueue validates generated envelope, serializes exact bytes and inserts same caller transaction; Inbox returns true only once in caller transaction and rejects mismatched reused event type.
- Relay leases eligible earliest unpublished aggregate records, fences each attempt with UUIDv7 token, waits broker acknowledgment, persists retry/deadline and only marks success for current lease. Publish Kafka key aggregate UUID; explicit AllISRAcks and idempotent producer. Relay includes encrypted quarantine's metadata-only DLQ outbox with the same durable lease/ack rules.
- Consume validates typed producer/event/topic/key, owns transaction, serializes per aggregate, records inbox plus effects plus version cursor atomically and commits offsets afterward. Duplicate committed events acknowledge without effects. Sequence gaps and dependency errors retry, not poison; typed-invalid events/explicit ErrPoison atomically quarantine encrypted source and metadata DLQ. Bound attempts then reseek failed partition before AllowRebalance; never commit past it.

## Consumer Interfaces
Exact contract: Load(name,defaultPort)(Config,error); NewClient(Config)(*Client,error); Connect(ctx,dsn)(*pgxpool.Pool,error); Migrate(ctx,db,owner,[]Migration)error; Serve(ctx,c,db,mux,ready,background)error; Enqueue(ctx,pgx.Tx,eventsv2.Envelope)error; Inbox(ctx,pgx.Tx,eventID,eventType)(bool,error); Relay(ctx,db,c)error; Consume(ctx,db,c,group,topics,apply(ctx,pgx.Tx,eventsv2.Envelope)error)error. ErrPoison marks permanent invalid application input, not dependency failures. SchemaSQL is included by owner migrations.

## Parent Verification Commands
- `gofmt -w platformkit/*.go` then `go test ./platformkit` and `go vet ./platformkit` in pinned Linux Go 1.26.10 environment.
- Set disposable TEST_DATABASE_URL for database regressions; set TEST_KAFKA_BROKERS for disposable integration broker tests. Database tests create and drop isolated schemas only.
- `go test -race ./platformkit`; then all service builds/tests and runtime proofs: fresh schema live200/ready503, migration ready200, TLS/auth dependency failures, duplicate events, Kafka acknowledgment interruption and restart recovery.
- Parent independent review must inspect migration and transactional ack/rebalance boundaries before merge readiness.
