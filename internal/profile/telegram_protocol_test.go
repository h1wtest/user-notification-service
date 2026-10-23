package profile

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"user-notification-service/internal/platform"
)

// Removing the candidate-only transition, any session/version check, or the
// outbox side effect must break these real PostgreSQL state-machine tests.
func telegramProtocolService(t *testing.T) (*Service, string) {
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
	schema := "telegram_protocol_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	if err = platform.Migrate(ctx, db, "profile"); err != nil {
		t.Fatal(err)
	}
	user := uuid.NewString()
	if _, err = db.Exec(ctx, "INSERT INTO profiles(user_id,email) VALUES($1,'telegram@example.test');", user); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, "INSERT INTO notification_settings(user_id) VALUES($1)", user); err != nil {
		t.Fatal(err)
	}
	return &Service{DB: db}, user
}

func telegramProtocolToken() string {
	value := uuid.New()
	return base64.RawURLEncoding.EncodeToString(append(value[:], value[:]...))
}

func TestTelegramProtocolStartOnlyCreatesCandidateAndConfirmRequiresSameSession(t *testing.T) {
	s, user := telegramProtocolService(t)
	ctx := context.Background()
	token := telegramProtocolToken()
	if err := s.beginTelegramLink(ctx, user, "initiating-session", token, time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.setTelegramCandidate(ctx, 11, token, "12345"); err != nil {
		t.Fatal(err)
	}
	var chat *string
	var version int64
	if err := s.DB.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1", user).Scan(&chat, &version); err != nil {
		t.Fatal(err)
	}
	if chat != nil || version != 0 {
		t.Fatalf("/start bound account: chat=%v version=%d", chat, version)
	}
	state, err := s.readTelegramStatus(ctx, user, "initiating-session")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(state)
	if !strings.Contains(string(raw), `"candidate_ready":true`) || strings.Contains(string(raw), "12345") || strings.Contains(string(raw), token) {
		t.Fatalf("unsafe or missing candidate status: %s", raw)
	}
	if err = s.confirmTelegramLink(ctx, user, "different-session", token); !errors.Is(err, errTelegramLinkInvalid) {
		t.Fatalf("other session confirmed: %v", err)
	}
	if err = s.confirmTelegramLink(ctx, user, "initiating-session", token); err != nil {
		t.Fatal(err)
	}
	if err = s.DB.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1", user).Scan(&chat, &version); err != nil {
		t.Fatal(err)
	}
	if chat == nil || *chat != "12345" || version != 1 {
		t.Fatalf("confirmation missing binding: %v %d", chat, version)
	}
	var events int
	if err = s.DB.QueryRow(ctx, "SELECT count(*) FROM owner_outbox WHERE event->>'event_type'='profile.telegram_binding_changed' AND event->'payload'->>'linked'='true'").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("binding and durable outbox diverged: events=%d", events)
	}
	if err = s.confirmTelegramLink(ctx, user, "initiating-session", token); !errors.Is(err, errTelegramLinkInvalid) {
		t.Fatalf("confirmation replay accepted: %v", err)
	}
}

func TestTelegramProtocolTokenAndUpdateReplayCannotReplaceCandidate(t *testing.T) {
	s, user := telegramProtocolService(t)
	ctx := context.Background()
	token := telegramProtocolToken()
	if err := s.beginTelegramLink(ctx, user, "session", token, time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.setTelegramCandidate(ctx, 21, token, "111"); err != nil {
		t.Fatal(err)
	}
	if err := s.setTelegramCandidate(ctx, 21, token, "222"); err != nil {
		t.Fatal(err)
	}
	if err := s.setTelegramCandidate(ctx, 22, token, "333"); err != nil {
		t.Fatal(err)
	}
	var candidate string
	hash := sha256.Sum256([]byte(token))
	if err := s.DB.QueryRow(ctx, "SELECT candidate_chat_id FROM telegram_links WHERE token_hash=$1", hash[:]).Scan(&candidate); err != nil {
		t.Fatal(err)
	}
	if candidate != "111" {
		t.Fatalf("replay replaced candidate with %q", candidate)
	}
}

func TestTelegramProtocolConfirmRejectsUnstartedExpiredWrongUserAndStaleBinding(t *testing.T) {
	for _, scenario := range []string{"unstarted", "expired", "wrong-user", "stale-binding"} {
		t.Run(scenario, func(t *testing.T) {
			s, user := telegramProtocolService(t)
			ctx := context.Background()
			token := telegramProtocolToken()
			if err := s.beginTelegramLink(ctx, user, "session", token, time.Now().Add(10*time.Minute)); err != nil {
				t.Fatal(err)
			}
			if scenario != "unstarted" {
				if err := s.setTelegramCandidate(ctx, 31, token, "123"); err != nil {
					t.Fatal(err)
				}
			}
			confirmUser := user
			switch scenario {
			case "expired":
				if _, err := s.DB.Exec(ctx, "UPDATE telegram_links SET expires_at=now()-interval '1 second'"); err != nil {
					t.Fatal(err)
				}
			case "wrong-user":
				confirmUser = uuid.NewString()
				if _, err := s.DB.Exec(ctx, "INSERT INTO profiles(user_id,email) VALUES($1,'other@example.test')", confirmUser); err != nil {
					t.Fatal(err)
				}
			case "stale-binding":
				if _, err := s.DB.Exec(ctx, "UPDATE profiles SET binding_version=binding_version+1 WHERE user_id=$1", user); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.confirmTelegramLink(ctx, confirmUser, "session", token); !errors.Is(err, errTelegramLinkInvalid) {
				t.Fatalf("%s confirmation accepted: %v", scenario, err)
			}
			var linked bool
			if err := s.DB.QueryRow(ctx, "SELECT telegram_chat_id IS NOT NULL FROM profiles WHERE user_id=$1", user).Scan(&linked); err != nil {
				t.Fatal(err)
			}
			if linked {
				t.Fatal("rejected confirmation changed binding")
			}
		})
	}
}

func TestTelegramProtocolConcurrentChatConfirmationHasOneOwner(t *testing.T) {
	s, first := telegramProtocolService(t)
	ctx := context.Background()
	second := uuid.NewString()
	if _, err := s.DB.Exec(ctx, "INSERT INTO profiles(user_id,email) VALUES($1,'second@example.test')", second); err != nil {
		t.Fatal(err)
	}
	tokens := []string{telegramProtocolToken(), telegramProtocolToken()}
	users := []string{first, second}
	for i, user := range users {
		if err := s.beginTelegramLink(ctx, user, "session", tokens[i], time.Now().Add(10*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := s.setTelegramCandidate(ctx, int64(41+i), tokens[i], "shared-chat"); err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range users {
		wg.Add(1)
		go func(i int) { defer wg.Done(); results <- s.confirmTelegramLink(ctx, users[i], "session", tokens[i]) }(i)
	}
	wg.Wait()
	close(results)
	var succeeded, conflicts int
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, errTelegramLinkConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if succeeded != 1 || conflicts != 1 {
		t.Fatalf("unique chat stolen: success=%d conflict=%d", succeeded, conflicts)
	}
	var owners, events int
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM profiles WHERE telegram_chat_id='shared-chat'").Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM owner_outbox").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if owners != 1 || events != 1 {
		t.Fatalf("binding/outbox not atomic: owners=%d events=%d", owners, events)
	}
}

func TestTelegramProtocolUnlinkRequiresBoundChatAndConsumesOnce(t *testing.T) {
	s, user := telegramProtocolService(t)
	ctx := context.Background()
	if _, err := s.DB.Exec(ctx, "UPDATE profiles SET telegram_chat_id='123',binding_version=7 WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, "UPDATE notification_settings SET telegram_login_alerts=true,telegram_security_alerts=true,telegram_subscription=true,telegram_trading=true WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	token := telegramProtocolToken()
	chat, err := s.beginTelegramUnlink(ctx, user, token, time.Now().Add(10*time.Minute))
	if err != nil || chat != "123" {
		t.Fatalf("unlink begin: chat=%q err=%v", chat, err)
	}
	if err = s.applyTelegramUnlink(ctx, 51, token, "wrong-chat"); err != nil {
		t.Fatal(err)
	}
	var version int64
	if err = s.DB.QueryRow(ctx, "SELECT binding_version FROM profiles WHERE user_id=$1", user).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 7 {
		t.Fatal("other chat unlinked binding")
	}
	if err = s.applyTelegramUnlink(ctx, 52, token, "123"); err != nil {
		t.Fatal(err)
	}
	if err = s.applyTelegramUnlink(ctx, 53, token, "123"); err != nil {
		t.Fatal(err)
	}
	var linked, enabled bool
	if err = s.DB.QueryRow(ctx, "SELECT p.telegram_chat_id IS NOT NULL,p.binding_version,n.telegram_login_alerts OR n.telegram_security_alerts OR n.telegram_subscription OR n.telegram_trading FROM profiles p JOIN notification_settings n USING(user_id) WHERE p.user_id=$1", user).Scan(&linked, &version, &enabled); err != nil {
		t.Fatal(err)
	}
	if linked || enabled || version != 8 {
		t.Fatalf("unlink not fenced/cleared: linked=%v enabled=%v version=%d", linked, enabled, version)
	}
	var events int
	if err = s.DB.QueryRow(ctx, "SELECT count(*) FROM owner_outbox").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("callback replay emitted %d events", events)
	}
}

func TestTelegramProtocolResetInvalidatesPendingTokensAndAuditsAtomically(t *testing.T) {
	s, user := telegramProtocolService(t)
	ctx := context.Background()
	if _, err := s.DB.Exec(ctx, "UPDATE profiles SET telegram_chat_id='123',binding_version=4 WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	token := telegramProtocolToken()
	if _, err := s.beginTelegramUnlink(ctx, user, token, time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	actor := uuid.NewString()
	if err := s.forceResetTelegram(ctx, user, actor, "Account recovery verified"); err != nil {
		t.Fatal(err)
	}
	if err := s.applyTelegramUnlink(ctx, 61, token, "123"); err != nil {
		t.Fatal(err)
	}
	var version int64
	var linked bool
	if err := s.DB.QueryRow(ctx, "SELECT telegram_chat_id IS NOT NULL,binding_version FROM profiles WHERE user_id=$1", user).Scan(&linked, &version); err != nil {
		t.Fatal(err)
	}
	var audits, pending, events int
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM profile_telegram_reset_audit WHERE actor_id=$1 AND user_id=$2 AND reason='Account recovery verified' AND binding_version=5", actor, user).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM telegram_unlinks WHERE user_id=$1", user).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM owner_outbox").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if linked || version != 5 || audits != 1 || pending != 0 || events != 1 {
		t.Fatalf("reset not atomic: linked=%v version=%d audit=%d pending=%d events=%d", linked, version, audits, pending, events)
	}
}

func TestTelegramProtocolUnlinkDeniedWhenIdentityReauthenticationIsStale(t *testing.T) {
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Permission string `json:"permission"`
			RequireMFA bool   `json:"require_mfa"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/internal/v2/introspect" || body.Permission != "users:self:reauth" || body.RequireMFA {
			t.Errorf("wrong identity reauth request: %s %+v", r.URL.Path, body)
		}
		platform.Error(w, 403, "reauth_required")
	}))
	defer identity.Close()
	s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}}
	r := httptest.NewRequest("POST", "https://app.test/api/v1/users/me/telegram/unlink", strings.NewReader(`{}`))
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "stale-session"})
	w := httptest.NewRecorder()
	s.unlinkTelegram(w, r)
	if w.Code != 403 {
		t.Fatalf("stale reauth reached database: %d %s", w.Code, w.Body.String())
	}
}

func TestTelegramProtocolWebhookAcceptsTelegramFieldsButRejectsPublicChatSender(t *testing.T) {
	s := &Service{Config: platform.Config{TelegramToken: "configured", TelegramWebhookSecret: "exact-secret"}}
	r := httptest.NewRequest("POST", "https://app.test/internal/v1/telegram/webhook", strings.NewReader(`{"update_id":1,"message":{"message_id":4,"date":123,"from":{"id":123,"is_bot":false,"first_name":"User"},"chat":{"id":123,"type":"group","title":"Public"},"text":"/start token"}}`))
	r.Header.Set("X-Telegram-Bot-Api-Secret-Token", "exact-secret")
	w := httptest.NewRecorder()
	s.webhook(w, r)
	if w.Code != 200 {
		t.Fatalf("public-chat update reached database or Telegram metadata rejected: %d %s", w.Code, w.Body.String())
	}
}

func TestTelegramProtocolUnlinkExpiryAndVersionFencePreventOldCallback(t *testing.T) {
	for _, scenario := range []string{"expired", "version", "replacement"} {
		t.Run(scenario, func(t *testing.T) {
			s, user := telegramProtocolService(t)
			ctx := context.Background()
			if _, err := s.DB.Exec(ctx, "UPDATE profiles SET telegram_chat_id='123',binding_version=2 WHERE user_id=$1", user); err != nil {
				t.Fatal(err)
			}
			token := telegramProtocolToken()
			if _, err := s.beginTelegramUnlink(ctx, user, token, time.Now().Add(10*time.Minute)); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "expired":
				if _, err := s.DB.Exec(ctx, "UPDATE telegram_unlinks SET expires_at=now()-interval '1 second'"); err != nil {
					t.Fatal(err)
				}
			case "version":
				if _, err := s.DB.Exec(ctx, "UPDATE profiles SET binding_version=3 WHERE user_id=$1", user); err != nil {
					t.Fatal(err)
				}
			case "replacement":
				if _, err := s.beginTelegramUnlink(ctx, user, telegramProtocolToken(), time.Now().Add(10*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.applyTelegramUnlink(ctx, 71, token, "123"); err != nil {
				t.Fatal(err)
			}
			var linked bool
			if err := s.DB.QueryRow(ctx, "SELECT telegram_chat_id='123' FROM profiles WHERE user_id=$1", user).Scan(&linked); err != nil {
				t.Fatal(err)
			}
			if !linked {
				t.Fatalf("%s callback removed binding", scenario)
			}
		})
	}
}

func TestTelegramProtocolOutboxFailureRollsBackBindingAndResetAudit(t *testing.T) {
	s, user := telegramProtocolService(t)
	ctx := context.Background()
	if _, err := s.DB.Exec(ctx, "UPDATE profiles SET telegram_chat_id='123',binding_version=2 WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ctx, "ALTER TABLE owner_outbox ADD CONSTRAINT deny_test_event CHECK (event_id IS NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := s.forceResetTelegram(ctx, user, uuid.NewString(), "Verified recovery"); err == nil {
		t.Fatal("reset succeeded without durable event")
	}
	var chat string
	var version int64
	if err := s.DB.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1", user).Scan(&chat, &version); err != nil {
		t.Fatal(err)
	}
	var audits int
	if err := s.DB.QueryRow(ctx, "SELECT count(*) FROM profile_telegram_reset_audit").Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if chat != "123" || version != 2 || audits != 0 {
		t.Fatalf("partial reset committed: chat=%s version=%d audits=%d", chat, version, audits)
	}
}

func TestTelegramProtocolResetChecksNarrowFreshMFAAndRejectsChatOrMissingReason(t *testing.T) {
	for _, scenario := range []string{"permission-denied", "stale-mfa", "arbitrary-chat", "blank-reason"} {
		t.Run(scenario, func(t *testing.T) {
			identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Permission string `json:"permission"`
					RequireMFA bool   `json:"require_mfa"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body.Permission != "notifications:bindings:reset" || !body.RequireMFA {
					t.Errorf("reset bypassed narrow fresh MFA check: %+v", body)
				}
				if scenario == "permission-denied" || scenario == "stale-mfa" {
					platform.Error(w, 403, "access_denied")
					return
				}
				platform.JSON(w, 200, platform.Actor{Active: true, UserID: uuid.NewString(), SessionID: uuid.NewString(), MFAVerified: true, Permissions: []string{"notifications:bindings:reset"}, Roles: []string{"SUPPORT"}})
			}))
			defer identity.Close()
			s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}}
			body := `{"reason":"Verified recovery"}`
			if scenario == "arbitrary-chat" {
				body = `{"reason":"Verified recovery","chat_id":"123"}`
			}
			if scenario == "blank-reason" {
				body = `{"reason":"  "}`
			}
			r := httptest.NewRequest("POST", "https://app.test/api/v1/admin/users/"+uuid.NewString()+"/telegram/reset", strings.NewReader(body))
			r.SetPathValue("id", uuid.NewString())
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: "support-session"})
			w := httptest.NewRecorder()
			s.resetTelegram(w, r)
			want := 400
			if scenario == "permission-denied" || scenario == "stale-mfa" {
				want = 403
			}
			if w.Code != want {
				t.Fatalf("reset %s returned %d want %d: %s", scenario, w.Code, want, w.Body.String())
			}
		})
	}
}

func TestTelegramProtocolHTTPWebsiteBotCandidateConfirmAndUnlink(t *testing.T) {
	s, user := telegramProtocolService(t)
	var mu sync.Mutex
	var callback string
	dependency := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/v2/introspect":
			var request struct {
				SessionCookie *string `json:"session_cookie"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			session := "019a0000-0000-7000-8000-000000000004"
			if request.SessionCookie != nil && *request.SessionCookie == "other-session" {
				session = "019a0000-0000-7000-8000-000000000005"
			}
			platform.JSON(w, 200, platform.Actor{Active: true, UserID: user, SessionID: session, Permissions: []string{"users:self:reauth"}, Roles: []string{}})
		case "/botfixture-token/getMe":
			platform.JSON(w, 200, map[string]any{"ok": true, "result": map[string]string{"username": "fixture_bot"}})
		case "/botfixture-token/sendMessage":
			var request struct {
				ChatID      string `json:"chat_id"`
				ReplyMarkup struct {
					InlineKeyboard [][]struct {
						CallbackData string `json:"callback_data"`
					} `json:"inline_keyboard"`
				} `json:"reply_markup"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request.ChatID != "123" || len(request.ReplyMarkup.InlineKeyboard) != 1 || len(request.ReplyMarkup.InlineKeyboard[0]) != 1 {
				t.Errorf("unlink confirmation not sent to bound chat: %+v", request)
				platform.Error(w, 500, "fixture_request_invalid")
				return
			}
			mu.Lock()
			callback = request.ReplyMarkup.InlineKeyboard[0][0].CallbackData
			mu.Unlock()
			platform.JSON(w, 200, map[string]any{"ok": true, "result": map[string]int{"message_id": 1}})
		default:
			t.Errorf("unexpected external dependency request: %s", r.URL.Path)
			platform.Error(w, 404, "fixture_route_missing")
		}
	}))
	defer dependency.Close()
	s.Config = platform.Config{IdentityURL: dependency.URL, TelegramBase: dependency.URL, TelegramToken: "fixture-token", TelegramWebhookSecret: "fixture-secret"}
	s.Client = &platform.Client{Config: s.Config, HTTP: dependency.Client()}
	mux := http.NewServeMux()
	s.Routes(mux)
	call := func(method, path, body, session string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://app.test"+path, strings.NewReader(body))
		if strings.Contains(path, "webhook") {
			r.Header.Set("X-Telegram-Bot-Api-Secret-Token", "fixture-secret")
		} else {
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: session})
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	w := call("POST", "/api/v1/users/me/telegram/link", `{}`, "origin-session")
	if w.Code != 200 {
		t.Fatalf("begin: %d %s", w.Code, w.Body.String())
	}
	var link struct {
		LinkToken string    `json:"link_token"`
		ExpiresAt time.Time `json:"expires_at"`
		BotURL    string    `json:"bot_url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &link); err != nil {
		t.Fatal(err)
	}
	if len(link.LinkToken) != 43 || !strings.HasPrefix(link.BotURL, "https://t.me/fixture_bot?start=") || time.Until(link.ExpiresAt) < 9*time.Minute || time.Until(link.ExpiresAt) > 10*time.Minute {
		t.Fatalf("invalid real begin response: %+v", link)
	}
	w = call("POST", "/internal/v1/telegram/webhook", `{"update_id":81,"message":{"message_id":5,"date":123,"from":{"id":123,"is_bot":false,"first_name":"Real"},"chat":{"id":123,"type":"private","first_name":"Real"},"text":"/start `+link.LinkToken+`"}}`, "")
	if w.Code != 200 {
		t.Fatalf("start: %d %s", w.Code, w.Body.String())
	}
	w = call("GET", "/api/v1/users/me/telegram", "", "origin-session")
	var state struct {
		Linked         bool  `json:"linked"`
		CandidateReady bool  `json:"candidate_ready"`
		BindingVersion int64 `json:"binding_version"`
		UnlinkPending  bool  `json:"unlink_pending"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || state.Linked || !state.CandidateReady || state.BindingVersion != 0 || strings.Contains(w.Body.String(), "chat") || strings.Contains(w.Body.String(), link.LinkToken) {
		t.Fatalf("candidate leaked or bound prematurely: %d %s", w.Code, w.Body.String())
	}
	w = call("POST", "/api/v1/users/me/telegram/confirm", `{"link_token":"`+link.LinkToken+`"}`, "other-session")
	if w.Code != 409 {
		t.Fatalf("other browser confirmed candidate: %d %s", w.Code, w.Body.String())
	}
	w = call("POST", "/api/v1/users/me/telegram/confirm", `{"link_token":"`+link.LinkToken+`"}`, "origin-session")
	if w.Code != 200 {
		t.Fatalf("site confirm: %d %s", w.Code, w.Body.String())
	}
	w = call("POST", "/api/v1/users/me/telegram/unlink", `{}`, "origin-session")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"pending"`) || strings.Contains(w.Body.String(), "chat_id") || strings.Contains(w.Body.String(), "unlink:") {
		t.Fatalf("unlink begin leaked callback: %d %s", w.Code, w.Body.String())
	}
	mu.Lock()
	data := callback
	mu.Unlock()
	if !strings.HasPrefix(data, "unlink:") || len(data) > 64 {
		t.Fatalf("invalid Bot API callback size: %q", data)
	}
	w = call("GET", "/api/v1/users/me/telegram", "", "origin-session")
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if !state.Linked || !state.UnlinkPending || state.BindingVersion != 1 {
		t.Fatalf("unlink completed without bot: %s", w.Body.String())
	}
	callbackBody := `{"update_id":82,"callback_query":{"id":"callback-id","from":{"id":123,"is_bot":false},"message":{"message_id":9,"from":{"id":999,"is_bot":true},"chat":{"id":123,"type":"private"}},"data":"` + data + `"}}`
	w = call("POST", "/internal/v1/telegram/webhook", callbackBody, "")
	if w.Code != 200 {
		t.Fatalf("bot unlink: %d %s", w.Code, w.Body.String())
	}
	w = call("POST", "/internal/v1/telegram/webhook", callbackBody, "")
	if w.Code != 200 {
		t.Fatalf("duplicate bot update: %d", w.Code)
	}
	w = call("GET", "/api/v1/users/me/telegram", "", "origin-session")
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Linked || state.UnlinkPending || state.BindingVersion != 2 {
		t.Fatalf("bot callback did not clear binding: %s", w.Body.String())
	}
	var events int
	if err := s.DB.QueryRow(context.Background(), "SELECT count(*) FROM owner_outbox").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 2 {
		t.Fatalf("handshake emitted %d binding events, want 2", events)
	}
}

func TestTelegramProtocolProviderFailureDoesNotLeaveUsableUnlinkToken(t *testing.T) {
	s, user := telegramProtocolService(t)
	if _, err := s.DB.Exec(context.Background(), "UPDATE profiles SET telegram_chat_id='123',binding_version=1 WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	dependency := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/introspect" {
			platform.JSON(w, 200, platform.Actor{Active: true, UserID: user, SessionID: uuid.NewString(), Permissions: []string{"users:self:reauth"}, Roles: []string{}})
			return
		}
		platform.Error(w, 503, "provider_unavailable")
	}))
	defer dependency.Close()
	s.Config = platform.Config{IdentityURL: dependency.URL, TelegramBase: dependency.URL, TelegramToken: "fixture-token", TelegramWebhookSecret: "fixture-secret"}
	s.Client = &platform.Client{Config: s.Config, HTTP: dependency.Client()}
	r := httptest.NewRequest("POST", "https://app.test/api/v1/users/me/telegram/unlink", strings.NewReader(`{}`))
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "fresh-session"})
	w := httptest.NewRecorder()
	s.unlinkTelegram(w, r)
	if w.Code != 503 {
		t.Fatalf("provider failure returned pending success: %d %s", w.Code, w.Body.String())
	}
	var tokens int
	if err := s.DB.QueryRow(context.Background(), "SELECT count(*) FROM telegram_unlinks").Scan(&tokens); err != nil {
		t.Fatal(err)
	}
	if tokens != 0 {
		t.Fatalf("provider failure left %d live confirmation tokens", tokens)
	}
}
