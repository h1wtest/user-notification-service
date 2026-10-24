// SPDX-License-Identifier: MIT
package notification

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"platform-contracts/gen/go/events"
	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

// Recovering in a fresh browser requires the public account UUID from the
// email; the six-digit verification code must remain separate from the URL.
func assertVerificationRecoveryEmail(t *testing.T, body, user string) {
	t.Helper()
	var recovery *url.URL
	for _, field := range strings.Fields(body) {
		if strings.HasPrefix(field, "https://accounts.example.test/") {
			parsed, err := url.Parse(field)
			if err != nil {
				t.Fatal(err)
			}
			recovery = parsed
		}
	}
	if recovery == nil {
		t.Fatal("verification email has no fresh-browser account recovery URL")
	}
	query := recovery.Query()
	if recovery.Scheme != "https" || recovery.Host != "accounts.example.test" || recovery.Path != "/" || recovery.Fragment != "" || len(query) != 1 || query.Get("user_id") != user {
		t.Fatal("recovery URL must select only the recipient's account on the trusted origin")
	}
	if strings.Contains(recovery.String(), "654321") || query.Has("code") || query.Has("token") {
		t.Fatal("verification credential leaked into recovery URL")
	}
	if !strings.Contains(body, "654321") || !strings.Contains(body, "2030-01-02T03:04:05Z") {
		t.Fatal("verification email lost its separate code or expiry")
	}
}

func TestVerificationRecoveryTemplateAllowsOnlyTypedTrustedURL(t *testing.T) {
	user := "00000000-0000-4000-8000-000000000001"
	vars := map[string]string{"code": "654321", "expires_at": "2030-01-02T03:04:05Z", "verification_url": "https://accounts.example.test/?user_id=" + user}
	for _, locale := range []string{"en", "ru"} {
		_, body, err := renderTemplate(defaultTemplate("notification.email_verification"), locale, vars)
		if err != nil {
			t.Fatal(err)
		}
		assertVerificationRecoveryEmail(t, body, user)
	}
	custom := defaultTemplate("notification.email_verification")
	custom.Locales["en"] = TemplateText{Subject: "Verify", Body: "{{verification_url}} {{code}} {{expires_at}}"}
	if _, _, err := renderTemplate(custom, "en", vars); err != nil {
		t.Fatal("typed verification URL cannot be used in an owner-published template")
	}
	vars["verification_url"] = "javascript:alert(1)"
	if _, _, err := renderTemplate(custom, "en", vars); err == nil {
		t.Fatal("unsafe verification URL was rendered")
	}
	login := defaultTemplate("identity.user_logged_in")
	login.Locales["en"] = TemplateText{Subject: "Sign-in", Body: "{{verification_url}}"}
	if validateTemplate(login) == nil {
		t.Fatal("verification URL variable escaped its event-specific allowlist")
	}
}

// This receiver exercises the real SMTP provider's network/MIME output, not a
// replacement DeliveryProvider or an assertion about a mocked Send call.
func verificationSMTPReceiver(t *testing.T) (string, string, <-chan []byte) {
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
		reader := textproto.NewReader(bufio.NewReader(conn))
		_, _ = fmt.Fprint(conn, "220 recovery-test ESMTP\r\n")
		for {
			line, err := reader.ReadLine()
			if err != nil {
				return
			}
			switch {
			case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "HELO"), strings.HasPrefix(line, "MAIL FROM:"), strings.HasPrefix(line, "RCPT TO:"):
				_, _ = fmt.Fprint(conn, "250 ok\r\n")
			case line == "DATA":
				_, _ = fmt.Fprint(conn, "354 send message\r\n")
				message, err := io.ReadAll(reader.DotReader())
				if err != nil {
					return
				}
				messages <- message
				_, _ = fmt.Fprint(conn, "250 accepted\r\n")
			case line == "QUIT":
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

func TestVerificationRecoveryV1AndV2PersistAndSendFreshBrowserURL(t *testing.T) {
	db := testDB(t)
	for _, test := range []struct {
		name   string
		v2     bool
		locale string
	}{
		{name: "retained_v1", locale: "en"},
		{name: "v2_en_actual_smtp", v2: true, locale: "en"},
		{name: "v2_ru_actual_smtp", v2: true, locale: "ru"},
	} {
		t.Run(test.name, func(t *testing.T) {
			host, port, messages := verificationSMTPReceiver(t)
			s := New(db, platform.Config{DeliveryKey: make([]byte, 32), Origin: "https://accounts.example.test/", SMTPHost: host, SMTPPort: port, SMTPFrom: "no-reply@example.test", SMTPInsecure: true}, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			eventID, user := uuid.Must(uuid.NewV7()).String(), uuid.NewString()
			cleanupDelivery(t, db, eventID)
			expires := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
			var payload []byte
			var err error
			if test.v2 {
				payload, err = json.Marshal(eventsv2.EmailVerification{UserID: user, Email: "recovery@example.test", Code: "654321", ExpiresAt: expires, Locale: &test.locale})
			} else {
				payload, err = json.Marshal(events.EmailVerification{UserID: user, Email: "recovery@example.test", Code: "654321", ExpiresAt: expires})
			}
			if err != nil {
				t.Fatal(err)
			}
			encrypted, err := platform.Encrypt(s.Config.DeliveryKey, eventID, payload)
			if err != nil {
				t.Fatal(err)
			}
			if test.v2 {
				err = s.ApplyV2(ctx, eventsv2.Envelope{SchemaVersion: 2, EventID: eventID, EventType: "notification.email_verification", Producer: "identity", AggregateType: "delivery_command", AggregateID: eventID, AggregateVersion: 1, OccurredAt: time.Now().UTC(), EncryptedPayload: encrypted})
			} else {
				err = s.Apply(ctx, platform.Envelope{SchemaVersion: 1, EventID: eventID, AggregateID: user, EventType: "notification.email_verification", OccurredAt: time.Now().UTC(), EncryptedPayload: encrypted})
			}
			if err != nil {
				t.Fatal(err)
			}
			var job Job
			if err = db.QueryRow(ctx, "SELECT id,encrypted_body,message_id FROM delivery_jobs WHERE event_id=$1 AND user_id=$2 AND channel='email'", eventID, user).Scan(&job.ID, &job.EncryptedBody, &job.MessageID); err != nil {
				t.Fatal(err)
			}
			raw, err := platform.Decrypt(s.Config.DeliveryKey, job.ID, job.EncryptedBody)
			if err != nil {
				t.Fatal(err)
			}
			var body Body
			if err = json.Unmarshal(raw, &body); err != nil {
				t.Fatal(err)
			}
			assertVerificationRecoveryEmail(t, body.Text, user)
			if permanent, reason, err := s.sendSMTP(ctx, job, body); err != nil {
				t.Fatalf("actual SMTP failed: permanent=%v reason=%s err=%v", permanent, reason, err)
			}
			select {
			case raw := <-messages:
				subject, text, messageID := decodeSMTPMessage(t, raw)
				assertVerificationRecoveryEmail(t, text, user)
				if subject != body.Subject || messageID != job.MessageID {
					t.Fatal("MIME lost localized subject or stable Message-ID")
				}
				if test.locale == "ru" && !strings.Contains(text, "Подтверд") {
					t.Fatal("RU verification email lost its localized recovery instructions")
				}
			case <-ctx.Done():
				t.Fatal("actual SMTP receiver did not receive verification email")
			}
		})
	}
}
