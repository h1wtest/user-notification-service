# Changelog

## 2026-10-24 — generic account and notification owners

- Adopt retained inline v1 migrations into an append-only checksum ledger under a database-wide owner lock; additive owner migrations preserve encrypted jobs and purpose-separated keys.
- Add independent EN/RU profile and notification locales, category preferences, masked Telegram state, live recipient-policy lookup and bounded HTTPS overview with explicit per-owner unavailable sections.
- Require private Bot candidate plus same-session website confirmation for linking; fresh website reauthentication plus one-use Bot confirmation for unlinking, update deduplication and binding-version fencing. Narrow staff reset requires current permission, fresh MFA, reason and durable local audit.
- Accept allowlisted external readiness/trading events only through scoped integration HMAC, timestamp/nonce checks and current identity/subscription authority; acceptance commits receipts plus owner outbox before HTTP202.
- Consume strict v2 owner events alongside retained v1 drain, maintain immutable localized templates, durable reminders and fresh policy before provider attempts; preserve SMTP ambiguity and lease fencing.
- Extend safe admin orchestration with recoverable command outcomes, current permissions, audited reasons, pagination and immutable template administration. No business SQL crosses owner databases.
- Add admin-owner migration3 for typed identity administrative audit metadata: actor, operation, textual role/user/OAuth-client target and sanitized reason arrive through an independent one-event Kafka stream; no synchronous audit dependency or raw details payload is introduced.
- Serialize notification preference changes on the same profile row as unlink/reset so replacement Telegram bindings cannot inherit stale concurrent opt-ins. Reject mismatched Kafka aggregate keys before owner callbacks and retain the original record encrypted in quarantine. Typed billing payment-review audit preserves actor/operation/reason with exact decision aggregate binding, never provider evidence bodies.
- Reject reused v2 event IDs with changed immutable envelope content through transactional canonical receipt fingerprints, preserving explicit legacy drain semantics and quarantining unverifiable pre-fingerprint v2 replays. Resume undispatched moderation only after current authorization and authoritative owner absence using the same journal UUID. Delivery history now uses complete timestamp+UUID pagination via `X-Next-Cursor` without changing its array response.
- Include a trusted-origin account recovery link in default EN/RU verification emails for fresh-browser verification; keep code/expiry separate and preserve immutable custom templates/history. Both retained v1 and v2 encrypted command paths supply the event-specific `verification_url`; PostgreSQL-to-real-SMTP and safe rendering regressions cover both paths.
- Resolve profile-owned notification locale for reset commands without an explicit v2 locale, including retained v1 drain; profile failures defer before acceptance instead of freezing an EN fallback, while registration planning remains profile-independent and optional switches cannot suppress transactional mail.
- Encode localized SMTP Subjects with folded RFC2047 and UTF-8 bodies with quoted-printable for seven-bit relays; preserve stable Message-ID, cancellation, CRLF denial and ambiguous-outcome handling. Recovery regressions decode MIME rather than inspect encoded wire text.
- Expose sorted event-specific template variables and typed, MFA/write-authorized public/internal draft previews through the real validator/renderer with deterministic nonsecret samples; preview never publishes, versions or audits, and accepts no arbitrary example inputs.
- Permit original-actor SUPPORT session-command recovery under the persisted action's current mutation permission and fresh MFA, without global audit permission; GET stays nondispatching, explicit resume stays original-actor/current-permission/Origin/CSRF gated and requires authoritative absence.
- New regressions are authored for parent integration verification; no new passing test/runtime/provider claim is made by this change entry.

## 2026-10-23 — platform remediation

### Changed

- Replaced the obsolete combined application with independent HTTPS profile, notification, worker and admin commands, each with explicit owner migrations and database credentials.
- Protected every browser operation through live identity introspection and CSRF/Origin enforcement; admin additionally requires live MFA and exact permissions. Internal APIs consume generated canonical HTTP DTOs and use distinct tokens and TLS.
- Added transactional inbox/projections/jobs, manual success-only Kafka commits, bounded retry/rebalance recovery, encrypted quarantine, validated corrected replay and durable actor audit.
- Added leased SMTP/Telegram delivery, bounded network calls, stable Message-ID, persisted retries/attempt outcomes, exhausted-lease fencing and shutdown draining. Telegram linking requires genuine Bot API identity and secret-authenticated verified private-chat updates.
- Split MFA, delivery and per-owner quarantine key purposes; added atomic offline rekey preserving nested sensitive command envelopes and rollback on invalid authenticated payloads.
- Added real latency histograms, durable status counters with zero baselines, sampled committed-lag freshness/assignment cleanup and cross-service OTLP traces.
- Pinned Go 1.26.10 and x/text 0.41.0 security updates, immutable builder/scratch runtime, PostgreSQL/Gitleaks/Redpanda CI fixtures and explicit contracts revision/tree provenance.
- Require the canonical contracts repository and verified checkout SHA, scan full service history plus pinned contracts before dependencies with redacted output, and avoid persisted Git credentials; retain real PostgreSQL/Kafka race gates and exact contract-tree checks.
- Profile PATCH now distinguishes omission, JSON null and empty strings for each nullable profile field: null persists SQL NULL without changing omitted fields; strict decoding and existing non-null validation remain in force. Added real PostgreSQL/public-handler regressions for independent clearing and invalid field types.
- Updated Go identity fixtures to complete canonical introspection responses with valid user/session UUIDs and non-null role/permission arrays; added fail-closed client regressions for malformed successful identity responses without relaxing generated validation.
- Removed vulnerability scanner commands and final-image scan/archive/report-cleanup steps by policy; retained secret scans, contracts provenance, all four immutable command image builds and their labels, and real build/test gates. Historical scan observations below are superseded as active requirements and must not be rerun; this removal makes no claim that vulnerabilities are absent.
- Upgraded golang.org/x/sys from v0.35.0 to v0.44.0 for CVE-2026-39824, discovered by a historical scan of the actual profile command image; the patched dependency remains pinned.

### Observed verification

The integration controller reported passing Go 1.26.10 vet, real PostgreSQL/Kafka race tests, all four command builds and the disposable Kafka CI lifecycle. Trivy scanned the actual final scratch-image Go executable with zero HIGH/CRITICAL findings. Real HTTPS/Mailpit/Kafka acceptance covered profiles, sessions, CSRF, MFA-protected admin ban/unban/audit, SMTP outage retry/recovery, duplicates, poison quarantine and corrected replay. Actual worker terminal invalid-recipient failure/replay and Prometheus alert were exercised as business-validation failures, not misrepresented as SMTP-provider rejection. A known trace was observed across all five application services.

### Explicit limits

SMTP is at-least-once, not exactly-once: external acceptance before local commit can remain ambiguous and lead to a duplicate. Genuine Telegram acceptance remains external because bot credentials and a user-controlled test chat were absent and public HTTPS webhook provisioning is required. Remote Actions were not executed; immutable 40-hex contracts revision configuration remains required. No production/cloud or remote publication acceptance is claimed.
