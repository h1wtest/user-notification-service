# Notification Delivery Implementation Plan

**Goal:** Complete N01/N02 notification binding enforcement and N04 delivery of identity, commerce, subscription, referral and external notifications without foreign database access.

**Architecture:** Notification owns immutable bilingual template revisions, encrypted channel jobs and durable subscription reminder schedules. Every leased attempt queries authoritative profile and identity APIs; old Telegram bindings are immutable. Existing v1 encrypted commands and provider lease/ambiguity semantics remain supported during cutover. Generated contracts own v2 payloads and HTTP DTOs.

**Constraints:** Only internal/notification code and this requested plan; no platform migrations edits, commits, secrets, cloud, deployment, tests/build/lint/format execution. Parent integrates additive SQL from local artifact. Existing approved platform design is authoritative. Regressions are authored before corresponding behavior; parent executes checks after integration.

## Files and execution

1. Add `templates_test.go` behavioral regressions for EN/RU completeness, unknown/malformed placeholders, escaping, CRLF, unsafe URLs, fallback locale and optimistic publish; implement `templates.go` with event-specific typed variable registry, immutable defaults/revisions and authenticated internal template API. Publish uses expected_version and transaction advisory lock, preserving older revisions.
2. Add `policy_test.go` regressions for disabled optional channels, nondisablable fresh transactional flow, revoked staff, unavailable authority and old Telegram binding. Implement `policy.go` with fresh profile/identity owner calls immediately before every provider attempt. Unknown dependencies retry rather than succeed; confirmed ineligible jobs fail safely with no body disclosure.
3. Add `v2_test.go` consumer behavioral regressions for generated event payload routing and encrypted command protection; implement `v2.go` using eventsv2 and generated owner DTOs. Reuse existing inbox/job atomic boundary, recording actual recipient user rather than commerce aggregate. Snapshot template revision/body/binding and category/flow metadata inside encrypted job. Build subscribes exact v1/v2 domain topics and registers ApplyV2 callback.
4. Add `reminders_test.go` behavioral regressions for version/end stale schedules and idempotent 1/3-day reminders; implement `reminders.go` persistent schedules and authoritative subscription checks. Worker pumps due reminder schedules through ordinary encrypted job admission with durable receipt identifiers.
5. Extend existing worker regressions for Telegram retry_after and retained encrypted legacy jobs. Modify `worker.go`, `repository.go`, `notification.go` to check policy after lease, persist provider-specific retry deadlines, preserve encryption-key and lease-token fencing, permissions, safe paginated history and reason-audited failed-only replay.
6. Send exact additive SQL (jobs metadata as needed, template revisions, reminder schedules, replay reason) and owner API/topic integration details to parent. Write local://generic-notification-delivery-report.md describing authored tests, interfaces and parent commands.

## Interfaces

- `Service.ApplyV2(context.Context,eventsv2.Envelope) error`; parent-owned Consumer.ApplyV2 callback.
- `GET /internal/v2/users/{id}/recipient-policy` owned by profile, authenticated notification workload token; current identity eligibility/flow/staff decision supplied by identity owner.
- `GET /internal/v1/deliveries`, `POST /internal/v1/deliveries/{id}/replay {reason}`, `GET /internal/v2/templates`, `PUT /internal/v2/templates/{event_type} {expected_version,locales}`; AdminToken and fresh identity authorization.
- Parent adds Config.SubscriptionURL/BillingURL/ReferralURL and SQL to append-only migrations_v2.go.

## Parent verification commands (not executed by worker)

`go test ./internal/notification ./internal/platform ./internal/profile ./internal/audit`

With disposable migrated notification PostgreSQL: `TEST_DATABASE_URL=... go test ./internal/notification`.

Integration exercises real ingress → outbox → Kafka → v2 consumer → fresh policy → provider attempt; identity/profile outage must leave retry, old binding and stale flow must never send, 429 retry_after must persist future retry, concurrent template publishes must produce exactly one new immutable revision.

## Authorized review corrections I3 and I5

- Author tied-row PostgreSQL history regression before changing pagination. Add internal/notification/pagination.go using the existing audit convention base64url(RFC3339Nano UTC timestamp + `|` + canonical UUID), replace timestamp-only predicate with `(created_at,id)<(cursor timestamp,cursor UUID)`, fetch limit+1, preserve array response and expose X-Next-Cursor only when more rows exist. Coordinate header forwarding with audit and opaque cursor consumption with frontend.
- Author exact duplicate versus changed-version/type/time/aggregate receipt regressions plus retained-v1 type collision before changing admission. Invoke parent-owned platform.AcceptV2Receipt(context.Context,pgx.Tx,eventsv2.Envelope)(bool,error) inside acceptV2 transaction before old v2 receipt/cursor logic; collisions propagate ErrPoison and rollback. Existing inbox duplicate additionally compares event_type before the same-ID v1→v2 no-effects commit. Parent owns helper migration and verification.
- No worker test/build/lint/format commands; parent executes real PostgreSQL suites and review acceptance.
