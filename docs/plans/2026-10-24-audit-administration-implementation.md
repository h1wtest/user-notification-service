# Audit administration implementation plan

**Goal:** Deliver A01/A03 safe administrative user orchestration, durable recoverable command metadata, safe paginated audit projections, and notification-owner administration without cross-owner SQL.

**Architecture:** The existing admin process remains the only admin runtime. Identity authorizes both ingress and owner commands; notification owns delivery/template mutations. Audit owns only inbox/projections/admin command journal. Generated eventsv2 and internal HTTP contracts define wire payloads; legacy identity v1 drain remains.

**Constraints:** Edit internal/audit only, plus this explicitly requested plan and local integration/report artifacts. No migrations outside owner: send exact additive SQL to parent. No tests/build/lint/format/commits/cloud/secrets. Regressions are authored before behavior; parent executes verification after integration.

## Files and interfaces
- internal/audit/audit.go: service routes, consumer callbacks, typed safe owner forwarding, notification delegation.
- internal/audit/repository.go: transactional projection/inbox, durable idempotent command admission, terminal-state monotonic completion, command lookup and keyset audit pagination.
- internal/audit/commands.go: validated user moderation admission, dispatch and same-ID identity recovery; required reason and actor-scoped idempotency.
- internal/audit/pagination.go: bounded limit and opaque timestamp/UUID cursor validation.
- internal/audit/audit_test.go and new command/projection regression files: HTTP behavior and opt-in real PostgreSQL persistence behavior.
- Parent consumes local://generic-audit-admin-integration.md for migration SQL, exact topics/routes/callback requirements.

## Implementation sequence
1. Author behavioral tests: whitespace reason rejected for all moderation operations; duplicate key reuses same command ID; changed payload conflicts; timeout/malformed success persists unknown and returns recoverable ID; GET recovery updates only when identity confirms same ID; user safe DTO removes private fields; pagination rejects malformed cursor/limit; projections retain metadata only and deduplicate.
2. Implement durable command model and PostgreSQL repository. Admission inserts actor+idempotency scoped unique journal record before dispatch; same input resolves same row, different action/target/reason conflicts. Only requested/unknown can transition to confirmed succeeded/denied. Request context loss does not erase command identity; completion uses bounded independent persistence context.
3. Implement identity orchestration against exact coordinated /internal/v2 admin APIs. Fresh users:read for safe list/detail; users:write ban/unban, users:sessions:revoke for revoke-sessions; audit:read recovery. Require trimmed nonempty <=500-byte reason and <=200-byte idempotency key; canonical UUID command IDs and user IDs. A network error, 5xx or invalid response is unknown, never success; GET calls identity same command ID, never allocates another command.
4. Add deterministic timestamp+UUID keyset audit pagination with limit1..100, default50. Only event/actor/user/aggregate IDs, timestamps, status/action/reason metadata are returned; never event payload, owner response, raw provider data or credential material. Projection consumes generated validated v2 envelopes and typed identity/commerce/profile/notification event metadata; inbox+projection transaction stays atomic.
5. Preserve safe quarantine metadata/replay orchestration. Replace deliveries permissions with notifications:read/replay. Delegate failed-only replay with required reason and sanitized success DTO to notification; route templates to notification with notifications:templates:write. Role/permission and commerce operations remain direct owner frontend calls, no added proxy.
6. Publish exact SQL/topics/routes and parent commands in local report. Do not execute verification.

## Parent verification commands
- go test ./internal/audit ./internal/platform ./internal/notification
- With an isolated migrated audit PostgreSQL DSN in AUDIT_TEST_DATABASE_URL: go test ./internal/audit -run Integration
- go test ./...
- Parent-managed HTTP/Kafka smoke: fresh staff MFA permission matrix; ban/unban/revoke idempotent retry plus identity timeout recovery; paginated safe audit; v1/v2 duplicate events; replay queued/failed/denied; templates expected_version conflict; quarantine retained.

## Approved review I3/I4 corrections
1. Add HTTP regressions for admitted-but-undispatched requested/unknown records recovered by same-key original POST or explicit POST /api/v1/admin/commands/{id}/resume {}. GET status remains strictly read-only and only reconciles confirmed results. Require current users:write/users:sessions:revoke, fresh MFA, original actor and POST Origin/CSRF before dispatch; only typed authoritative command_not_found404 permits resubmission. Network failures, malformed404 and foreign actor never dispatch. Resubmit stored reason/action/target with exactly original journal UUID; persist confirmed result as before. Identity-style CSRF fixture proves GET absence cannot accidentally submit a POST that records denied.
2. Carry validated original eventsv2.Envelope in AuditEvent.Envelope json:"-" and call parent platform.AcceptV2Receipt(ctx,tx,e)(bool,error) before owner receipt/cursor handling. Exact replay returns false; changed event identity/content returns ErrPoison. Add real PostgreSQL changed-ID content/aggregate/version collision regressions and preserve same-ID v1 drain tests. Legacy inbox duplicate event_type mismatch also poisons.
3. Notification delivery list now returns X-Next-Cursor; forward the exact safe header through parent-owned header-preserving client interface. Do not execute checks; parent owns migrations and verification.
