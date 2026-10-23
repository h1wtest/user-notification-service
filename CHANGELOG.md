# Changelog

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
- Scan each actual final command image archive with matching image-ID and nonzero-package checks, rejecting vulnerabilities at every severity and retaining raw reports only in disposable runner storage.

### Observed verification

The integration controller reported passing Go 1.26.10 vet, real PostgreSQL/Kafka race tests, all four command builds and the disposable Kafka CI lifecycle. Trivy scanned the actual final scratch-image Go executable with zero HIGH/CRITICAL findings. Real HTTPS/Mailpit/Kafka acceptance covered profiles, sessions, CSRF, MFA-protected admin ban/unban/audit, SMTP outage retry/recovery, duplicates, poison quarantine and corrected replay. Actual worker terminal invalid-recipient failure/replay and Prometheus alert were exercised as business-validation failures, not misrepresented as SMTP-provider rejection. A known trace was observed across all five application services.

### Explicit limits

SMTP is at-least-once, not exactly-once: external acceptance before local commit can remain ambiguous and lead to a duplicate. Genuine Telegram acceptance remains external because bot credentials and a user-controlled test chat were absent and public HTTPS webhook provisioning is required. Remote Actions were not executed; immutable 40-hex contracts revision configuration remains required. No production/cloud or remote publication acceptance is claimed.
