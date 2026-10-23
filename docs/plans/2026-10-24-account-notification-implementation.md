# Account and notification implementation

Approved scope P01/P02/N01–N04/A01/A03 and existing owner migration C02. Existing identity owns email, sessions and live access; profile owns preferences/bindings and ingress; notification owns templates/jobs; audit owns sanitized projections/command outcomes. No foreign SQL. Existing v1 SQL bytes and encrypted jobs remain intact. Tests authored before behavior; parent runs all verification after integration.

1. Append checksum migration ledger with owner lock and preserved legacy v1 marker; additive per-owner locale/category/handshake/template/job/outbox/command state migrations. Reject checksum/owner drift.
2. Extend profile nullable PATCH validation for independent EN/RU locales, current recipient policy endpoint, bounded parallel authenticated overview owner calls, and category preference rules.
3. Replace webhook implicit link with session-bound hashed10min candidate/site-confirm protocol; website fresh-auth unlink plus one-use bot callback, update dedup, binding fencing and scoped staff reset audit.
4. Add exact scoped HMAC external ingress and persisted nonce/idempotent event/outbox; validate authoritative identity and subscription over HTTPS. Consume existing v1 during drain and exact v2 identity/commercial/external event kinds.
5. Add immutable bilingual event templates with typed variable allowlists, escaped substitutions, header sanitation and unsafe-link refusal; all chosen job revisions frozen. Durable event/reminder schedule and fresh profile/identity policy at every leased provider attempt; retain SMTP ambiguity, lease fencing and redaction. Honor Telegram retry_after.
6. Extend safe admin user/detail/session commands, persisted requested/succeeded/denied/unknown status and recovery read, pagination, narrow reset, templates/replay permissions and audited reasons. Frontend calls foreign domain owners directly.

Files: internal/platform/{migrate.go,runtime.go,events.go,new migration/event helpers}; internal/profile/{profile.go,repository.go,new overview/handshake/ingress/v2 files}; internal/notification/{notification.go,repository.go,worker.go,new templates/v2/policy files}; internal/audit/{audit.go,repository.go,new command/v2 files}; focused regression files; existing README/CHANGELOG/.env.example.

Interfaces published local://generic-account-notification-api.md. Generated DTO packages coordinated with contracts. Workload permissions/identity eligibility and subscription authority must fail closed. No secrets read/copied.

Parent commands: go test ./internal/... ./cmd/...; go test -race ./internal/...; go vet ./...; actual legacy populated DB migration/checksum/owner rejection, Kafka dual-version receipt, Mailpit localized verification/reset/optional/security retries, external HMAC→Kafka→delivery, Telegram credential-gated candidate/site-confirm/unlink callback/replay, per-section overview timeout, current-role/preference/binding revocation and admin unknown outcome recovery. No verification is run in this slice.
