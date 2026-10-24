package notification

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"platform-contracts/gen/go/events"
	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

func decodeSMTPMessage(t *testing.T, raw []byte) (string, string, string) {
	t.Helper()
	message, err := mail.ReadMessage(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	subject, err := new(mime.WordDecoder).DecodeHeader(message.Header.Get("Subject"))
	if err != nil {
		t.Fatal(err)
	}
	var reader io.Reader = message.Body
	switch message.Header.Get("Content-Transfer-Encoding") {
	case "quoted-printable":
		reader = quotedprintable.NewReader(reader)
	case "base64":
		reader = base64.NewDecoder(base64.StdEncoding, reader)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return subject, strings.ReplaceAll(string(body), "\r\n", "\n"), message.Header.Get("Message-ID")
}

// Advertises neither SMTPUTF8 nor 8BITMIME and rejects any non-ASCII DATA.
func asciiSMTPReceiver(t *testing.T) (string, string, <-chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	messages := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		_, _ = fmt.Fprint(conn, "220 strict-test ESMTP\r\n")
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			switch {
			case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "HELO"), strings.HasPrefix(line, "MAIL FROM:"), strings.HasPrefix(line, "RCPT TO:"):
				_, _ = fmt.Fprint(conn, "250 ok\r\n")
			case line == "DATA\r\n":
				_, _ = fmt.Fprint(conn, "354 send message\r\n")
				var message strings.Builder
				valid := true
				for {
					data, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if data == ".\r\n" {
						break
					}
					if !strings.HasSuffix(data, "\r\n") || len(data) > 1000 {
						valid = false
					}
					for _, b := range []byte(data) {
						if b > 127 {
							valid = false
						}
					}
					message.WriteString(strings.TrimPrefix(data, "."))
				}
				messages <- []byte(message.String())
				if valid {
					_, _ = fmt.Fprint(conn, "250 accepted\r\n")
				} else {
					_, _ = fmt.Fprint(conn, "550 non-7bit MIME\r\n")
				}
			case line == "QUIT\r\n":
				_, _ = fmt.Fprint(conn, "221 bye\r\n")
				return
			default:
				_, _ = fmt.Fprint(conn, "500 unsupported\r\n")
			}
		}
	}()
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return host, port, messages
}

func TestSMTPSevenBitMIMEPreservesRussianAndStableID(t *testing.T) {
	for _, subject := range []string{"Сброс пароля", strings.Repeat("Длинная тема ", 18), "English subject"} {
		t.Run(subject, func(t *testing.T) {
			for range 2 {
				host, port, messages := asciiSMTPReceiver(t)
				s := New(nil, platform.Config{SMTPHost: host, SMTPPort: port, SMTPFrom: "no-reply@example.test", SMTPInsecure: true}, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				body := "Сбросить пароль: https://accounts.example.test/?token=not-a-real-token\nДействует до: 2030-01-02T03:04:05Z\n.line"
				if permanent, reason, err := s.sendSMTP(ctx, Job{MessageID: "<stable@example.test>"}, Body{To: "user@example.test", Subject: subject, Text: body}); err != nil {
					t.Fatalf("strict SMTP rejected message: %v %s %v", permanent, reason, err)
				}
				raw := <-messages
				gotSubject, gotBody, id := decodeSMTPMessage(t, raw)
				if gotSubject != subject || gotBody != body+"\n" || id != "<stable@example.test>" {
					t.Fatalf("MIME roundtrip changed content: %q %q %q", gotSubject, gotBody, id)
				}
				for _, b := range raw {
					if b > 127 {
						t.Fatal("non-ASCII SMTP wire")
					}
				}
				if strings.Contains(strings.ReplaceAll(string(raw), "\r\n", ""), "\n") {
					t.Fatal("bare LF on SMTP wire")
				}
			}
		})
	}
}

func TestResetPublisherWithoutLocaleUsesProfileAndFreezesPlan(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	user := uuid.NewString()
	locale, unavailable, calls := "ru", false, 0
	profile := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/users/"+user+"/eligibility" {
			platform.JSON(w, 200, map[string]any{"user_id": user, "email": "reset@example.test", "active": true, "verified": true, "banned": false, "locale": "en", "staff": false, "permissions": []string{}})
			return
		}
		if r.URL.Path == "/internal/v2/users/"+user+"/notification-flow" {
			platform.JSON(w, 200, map[string]bool{"active": true})
			return
		}
		calls++
		if r.URL.Path != "/internal/v2/users/"+user+"/recipient-policy" {
			w.WriteHeader(404)
			return
		}
		if unavailable {
			w.WriteHeader(503)
			return
		}
		platform.JSON(w, 200, map[string]any{"user_id": user, "email": "reset@example.test", "locale": locale, "telegram_chat_id": nil, "binding_version": 0, "email_subscription": false, "email_security": false, "email_login_alerts": false, "telegram_subscription": false, "telegram_security": false, "telegram_trading": false, "telegram_login_alerts": false})
	}))
	defer profile.Close()
	c := platform.Config{DeliveryKey: make([]byte, 32), Origin: "https://accounts.example.test", ProfileURL: profile.URL, IdentityURL: profile.URL, Token: "notification-test"}
	s := New(db, c, &platform.Client{Config: c, HTTP: profile.Client()})
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			eventID := uuid.Must(uuid.NewV7()).String()
			cleanupDelivery(t, db, eventID)
			// Exact identity forgot publisher shape: locale is intentionally absent.
			payload := []byte(fmt.Sprintf(`{"user_id":%q,"email":"reset@example.test","token":"%s","expires_at":"2030-01-02T03:04:05Z"}`, user, strings.Repeat("a", 64)))
			encrypted, err := platform.Encrypt(c.DeliveryKey, eventID, payload)
			if err != nil {
				t.Fatal(err)
			}
			apply := func() error {
				if version == 2 {
					return s.ApplyV2(ctx, eventsv2.Envelope{SchemaVersion: 2, EventID: eventID, EventType: "notification.password_reset", Producer: "identity", AggregateType: "delivery_command", AggregateID: eventID, AggregateVersion: 1, OccurredAt: time.Now().UTC(), EncryptedPayload: encrypted})
				}
				if err := events.ValidatePayload("notification.password_reset", payload); err != nil {
					t.Fatal(err)
				}
				return s.Apply(ctx, platform.Envelope{SchemaVersion: 1, EventID: eventID, AggregateID: user, EventType: "notification.password_reset", OccurredAt: time.Now().UTC(), EncryptedPayload: encrypted})
			}
			unavailable = true
			if err := apply(); err == nil {
				t.Fatal("profile outage silently accepted reset with fallback locale")
			}
			var count int
			if err := db.QueryRow(ctx, "SELECT count(*) FROM delivery_jobs WHERE event_id=$1", eventID).Scan(&count); err != nil || count != 0 {
				t.Fatal("outage persisted fallback job", count, err)
			}
			unavailable = false
			locale = "ru"
			if err := apply(); err != nil {
				t.Fatal(err)
			}
			var j Job
			if err := db.QueryRow(ctx, "SELECT id,encrypted_body,message_id FROM delivery_jobs WHERE event_id=$1", eventID).Scan(&j.ID, &j.EncryptedBody, &j.MessageID); err != nil {
				t.Fatal(err)
			}
			raw, err := platform.Decrypt(c.DeliveryKey, j.ID, j.EncryptedBody)
			if err != nil {
				t.Fatal(err)
			}
			var b Body
			if err = json.Unmarshal(raw, &b); err != nil {
				t.Fatal(err)
			}
			if b.Subject != "Сброс пароля" || !strings.Contains(b.Text, "Сбросить пароль:") {
				t.Fatal("profile RU ignored", b.Subject, b.Text)
			}
			locale = "en"
			j.UserID, j.EventID, j.EventType, j.Channel = user, eventID, "notification.password_reset", "email"
			if permanent, reason, err := s.checkAttempt(ctx, j, b); err != nil {
				t.Fatal("optional false switches suppressed mandatory reset", permanent, reason, err)
			}
			host, port, messages := asciiSMTPReceiver(t)
			s.Config.SMTPHost, s.Config.SMTPPort, s.Config.SMTPFrom, s.Config.SMTPInsecure = host, port, "no-reply@example.test", true
			if _, reason, err := s.sendSMTP(ctx, j, b); err != nil {
				t.Fatal(reason, err)
			}
			subject, text, _ := decodeSMTPMessage(t, <-messages)
			if subject != b.Subject || text != b.Text+"\n" {
				t.Fatal("persisted RU plan changed after preference update")
			}
		})
	}
	if calls < 4 {
		t.Fatal("reset did not resolve authoritative profile")
	}
}

func TestTemplatePreviewValidatesAuthorityWithoutPublishing(t *testing.T) {
	permissions := []string{"notifications:templates:write"}
	mfa := true
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, platform.Actor{Active: true, UserID: uuid.NewString(), Email: "admin@example.test", SessionID: uuid.NewString(), Roles: []string{"admin"}, Permissions: permissions, MFAVerified: mfa})
	}))
	defer identity.Close()
	c := platform.Config{IdentityURL: identity.URL, AdminToken: "admin-test", Token: "notification-test", Origin: "https://accounts.example.test"}
	// A nil DB deliberately proves preview never loads/publishes/audits a revision.
	s := New(nil, c, &platform.Client{Config: c, HTTP: identity.Client()})
	mux := http.NewServeMux()
	s.Routes(mux)
	request := func(event, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "https://notification.test/internal/v2/templates/"+event+"/preview", strings.NewReader(body))
		r.Header.Set("X-Service-Token", token)
		r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	valid := `{"locales":{"en":{"subject":"Preview {{device_name}}","body":"{{device_name}} {{ip_address}} {{occurred_at}}"},"ru":{"subject":"Просмотр {{device_name}}","body":"{{device_name}}"}}}`
	if w := request("identity.user_logged_in", valid, c.AdminToken); w.Code != 200 {
		t.Fatalf("preview absent: %d %s", w.Code, w.Body.String())
	} else {
		var got struct {
			Locales map[string]TemplateText `json:"locales"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Locales) != 2 || !strings.Contains(got.Locales["en"].Body, "&lt;") || strings.Contains(got.Locales["en"].Body, "{{") {
			t.Fatal("preview bypassed real escaped renderer", w.Body.String())
		}
	}
	for _, bad := range []string{strings.ReplaceAll(valid, "device_name", "secret"), strings.ReplaceAll(valid, "{{device_name}}", "javascript:alert(1)"), strings.Replace(valid, "Preview {{device_name}}", `Bad\r\nBcc: victim`, 1), strings.TrimSuffix(valid, "}") + `,"examples":{"device_name":"secret"}}`} {
		if w := request("identity.user_logged_in", bad, c.AdminToken); w.Code != 400 {
			t.Fatalf("unsafe/arbitrary preview accepted: %d %s", w.Code, w.Body.String())
		}
	}
	if w := request("unknown.event", valid, c.AdminToken); w.Code != 400 {
		t.Fatal("unknown event allowed", w.Code)
	}
	if w := request("identity.user_logged_in", valid, "wrong"); w.Code != 403 {
		t.Fatal("workload not required", w.Code)
	}
	permissions = []string{"notifications:read"}
	if w := request("identity.user_logged_in", valid, c.AdminToken); w.Code != 403 {
		t.Fatal("read-only actor previewed", w.Code)
	}
	permissions = []string{"notifications:templates:write"}
	mfa = false
	if w := request("identity.user_logged_in", valid, c.AdminToken); w.Code != 403 {
		t.Fatal("fresh MFA not required", w.Code)
	}
	wire, err := json.Marshal(templateWire(defaultTemplate("notification.email_verification")))
	if err != nil {
		t.Fatal(err)
	}
	var revision struct {
		Variables []string `json:"variables"`
	}
	if err = json.Unmarshal(wire, &revision); err != nil {
		t.Fatal(err)
	}
	if strings.Join(revision.Variables, ",") != "code,expires_at,verification_url" {
		t.Fatal("missing sorted event variable allowlist", string(wire))
	}
}

func TestTransactionalLocalePrecedenceAndRegistrationBeforeProfile(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	user := uuid.NewString()
	locale := "ru"
	calls := 0
	profile := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		platform.JSON(w, 200, map[string]any{"user_id": user, "email": "reset@example.test", "locale": locale, "telegram_chat_id": nil, "binding_version": 0, "email_subscription": false, "email_security": false, "email_login_alerts": false, "telegram_subscription": false, "telegram_security": false, "telegram_trading": false, "telegram_login_alerts": false})
	}))
	defer profile.Close()
	c := platform.Config{DeliveryKey: make([]byte, 32), ProfileURL: profile.URL, Token: "notification-test"}
	s := New(db, c, &platform.Client{Config: c, HTTP: profile.Client()})
	for _, test := range []struct {
		event, explicit, profile, want string
		lookup                         bool
	}{
		{"notification.password_reset", "", "ru", "Сброс пароля", true},
		{"notification.password_reset", "en", "ru", "Reset your password", false},
		{"notification.password_reset", "ru", "en", "Сброс пароля", false},
		{"notification.password_reset", "unknown", "ru", "Reset your password", false},
		{"notification.password_reset", "", "en", "Reset your password", true},
		{"notification.email_verification", "ru", "en", "Подтвердите электронную почту", false},
		{"notification.email_verification", "", "ru", "Verify your email", false},
	} {
		locale = test.profile
		before := calls
		intents, err := s.planIntents(ctx, uuid.NewString(), test.event, deliveryPlan{UserID: user, Locale: test.explicit, Target: "reset@example.test", Category: "transactional", Vars: map[string]string{"reset_url": "https://accounts.example.test/?token=example-not-auth", "verification_url": "https://accounts.example.test/?user_id=" + user, "code": "654321", "expires_at": "2030-01-02T03:04:05Z"}})
		if err != nil || len(intents) != 1 {
			t.Fatal("transactional intent missing", err)
		}
		raw, err := platform.Decrypt(c.DeliveryKey, intents[0].ID, intents[0].EncryptedBody)
		if err != nil {
			t.Fatal(err)
		}
		var body Body
		if err = json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		if body.Subject != test.want || (calls > before) != test.lookup {
			t.Fatalf("locale precedence event=%s explicit=%q profile=%q: subject=%q lookup=%v", test.event, test.explicit, test.profile, body.Subject, calls > before)
		}
	}
	for _, invalid := range []string{"", "unknown"} {
		locale = invalid
		if _, err := s.planIntents(ctx, uuid.NewString(), "notification.password_reset", deliveryPlan{UserID: user, Target: "reset@example.test", Category: "transactional"}); err == nil {
			t.Fatal("malformed authoritative policy silently became EN")
		}
	}
}

func TestSMTPRejectsNonASCIIEnvelopeAndMessageIDWithoutSMTPUTF8(t *testing.T) {
	for _, test := range []struct{ from, to, id, reason string }{
		{"почта@example.test", "user@example.test", "<stable@example.test>", "invalid_sender"},
		{"no-reply@example.test", "почта@example.test", "<stable@example.test>", "invalid_recipient"},
		{"no-reply@example.test", "user@example.test", "<тест@example.test>", "invalid_headers"},
		{"no-reply@example.test", "user@example.test", "<test@example.test>\r\nBcc: victim@example.test", "invalid_headers"},
	} {
		s := New(nil, platform.Config{SMTPFrom: test.from}, nil)
		permanent, reason, err := s.sendSMTP(context.Background(), Job{MessageID: test.id}, Body{To: test.to, Subject: "Сброс пароля", Text: "Текст"})
		if err == nil || !permanent || reason != test.reason {
			t.Fatalf("nonportable headers reached network: %v %s %v", permanent, reason, err)
		}
	}
}

func TestTemplatePreviewUsesOnlyTrustedDeterministicSamplesForEveryEvent(t *testing.T) {
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, platform.Actor{Active: true, UserID: uuid.NewString(), SessionID: uuid.NewString(), Email: "admin@example.test", Roles: []string{"admin"}, Permissions: []string{"notifications:templates:write"}, MFAVerified: true})
	}))
	defer identity.Close()
	c := platform.Config{IdentityURL: identity.URL, AdminToken: "admin-test", Token: "notification-test", Origin: "https://accounts.example.test"}
	s := New(nil, c, &platform.Client{Config: c, HTTP: identity.Client()})
	mux := http.NewServeMux()
	s.Routes(mux)
	preview := func(event string) *httptest.ResponseRecorder {
		raw, err := json.Marshal(map[string]any{"locales": defaultTemplate(event).Locales})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "https://notification.test/internal/v2/templates/"+event+"/preview", strings.NewReader(string(raw)))
		r.Header.Set("X-Service-Token", c.AdminToken)
		r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	for event := range templateVariables {
		first, second := preview(event), preview(event)
		if first.Code != 200 || first.Body.String() != second.Body.String() {
			t.Fatalf("event preview unavailable or nondeterministic: %s %d %s", event, first.Code, first.Body.String())
		}
		if strings.Contains(first.Body.String(), "{{") {
			t.Fatal("unrendered sample variable", event)
		}
		if event == "notification.password_reset" && (!strings.Contains(first.Body.String(), "https://accounts.example.test/?token=example-not-a-valid-authentication-token") || strings.Contains(first.Body.String(), "reset@example.test")) {
			t.Fatal("preview not trusted noncredential URL", first.Body.String())
		}
	}
	s.Config.Origin = "http://untrusted.example.test"
	if w := preview("notification.password_reset"); w.Code != 503 {
		t.Fatal("unsafe configured preview URL accepted", w.Code)
	}
}
