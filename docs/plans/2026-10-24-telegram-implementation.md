# Session-confirmed Telegram binding implementation plan

**Goal:** Implement the approved existing-webhook Telegram candidate/site-confirm protocol, fresh-reauth website/bot unlink, and narrowly authorized audited reset without exposing chat IDs publicly.

**Architecture:** `internal/profile/telegram.go` owns handlers and PostgreSQL transactions. Existing `Service.telegram` remains the HTTPS Bot API boundary. Every binding mutation locks the profile, fences the prior binding version, increments profile domain and binding versions, and enqueues the generated v2 event in the same transaction. Parent `platform.EnqueueV2` assigns the contiguous emitted-event version shared by the profile user stream. The parent owns route registration, legacy handler removal, additive migration SQL, and all verification.

**Constraints:** Edit only the new Telegram implementation, focused regression files and this requested plan. No secret access, tests/build/lint/format/commits/deploy. No foreign business SQL; identity authenticates own reauth via `users:self:reauth`, and staff reset via `notifications:bindings:reset` with fresh MFA. Hash all random tokens; ten-minute expiry; no polling; do not expose raw chat IDs publicly.

## Files and interfaces

- Create `internal/profile/telegram.go`: `link`, `telegramStatus`, `confirmTelegram`, `unlinkTelegram`, `resetTelegram`, `webhook`; internal transactional methods for begin/candidate/confirm/unlink/reset and dedup.
- Create `internal/profile/telegram_protocol_test.go`: real PostgreSQL behavior in an isolated schema behind `TEST_DATABASE_URL`; TLS identity/Bot API fixtures only at external boundaries.
- Keep existing `internal/profile/telegram_test.go` and other profile regressions unchanged.
- Parent removes old `profile.go` link/webhook methods and unused imports, preserving `telegram(context.Context,string,any)(json.RawMessage,error)`; parent registers the four new routes.
- Consume generated `eventsv2.Envelope`, binding-change payload, and public Telegram DTOs from contracts owner; use parent-owned `platform.EnqueueV2(context.Context,pgx.Tx,eventsv2.Envelope)` with `aggregate_type=user` and the existing owner-local outbox/relay.
- Send exact additive `telegram_links`/`telegram_unlinks`/reset-audit SQL to parent via local artifact; parent owns `internal/platform/migrations_v2.go`.

## Implementation sequence

1. Author behavioral regressions before production code: `/start` cannot bind; wrong session/expired/unused/wrong-user token cannot confirm; candidate and update replay cannot replace chat; concurrent unique-chat confirmation cannot steal; unlink requires identity reauth and only the bound private chat can confirm once; reset requires fresh-MFA narrow permission and reason, clears pending tokens, increments fences and durably audits/enqueues; GET never exposes chat/token; configured capability failure is not fake success.
2. Implement begin and status: generated public DTOs, cryptographic 32-byte base64url token/hash, initiating session and current version persisted under profile lock, expiry ten minutes, prior pending link replacement, already-linked conflict, getMe capability check.
3. Implement existing exact-secret webhook: bounded Telegram-compatible decoder, private nonbot sender identity check, atomic update dedup and candidate-only token consumption; bot callbacks consume one-use hashed unlink tokens only when user/chat/version/expiry still match.
4. Implement explicit website confirmation: authenticate same user/session, lock profile and handshake, require consumed candidate/hash/expiry/version, rely on unique chat constraint, atomically change binding, increment versions, invalidate stale handshakes and enqueue `profile.telegram_binding_changed`.
5. Implement unlink and reset: current identity fresh-reauth, issue one-use ten-minute callback bound to current chat/version, send inline confirmation button with no public token/chat, remove token on provider send failure; support reset rejects arbitrary fields, requires reason and narrow fresh-MFA auth, clears binding/preferences and pending requests, persists audit and outbox atomically.
6. Write `local://generic-telegram-report.md` with exact paths/interfaces/SQL/parent commands and unexecuted verification status. Parent integrates routes/migrations/legacy removal and runs tests once after sibling changes land.

## Parent verification commands and scenarios (not run here)

- `go test ./internal/profile -run 'Telegram|MissingBot'`.
- `TEST_DATABASE_URL=<disposable PostgreSQL URL> go test ./internal/profile -run TelegramProtocol -count=1`.
- `TEST_DATABASE_URL=<disposable PostgreSQL URL> go test -race ./internal/profile -run TelegramProtocol -count=1`.
- Real credential-gated Telegram HTTPS webhook: begin, private `/start`, website confirm from initiating session, website unlink after password reauth, private bot callback, then denied replay and old-binding delivery. External provider acceptance remains separate from TLS fixtures.
