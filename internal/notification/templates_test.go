package notification

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"strings"
	"testing"
	"user-notification-service/internal/platform"
)

func TestTemplateRejectsUnknownVariablesAndMissingLanguages(t *testing.T) {
	base := defaultTemplate("identity.user_logged_in")
	if err := validateTemplate(base); err != nil {
		t.Fatal(err)
	}
	base.Locales["ru"] = TemplateText{Subject: "{{token}}", Body: "test"}
	if err := validateTemplate(base); !errors.Is(err, errUnknownVariable) {
		t.Fatalf("unknown variable accepted: %v", err)
	}
	delete(base.Locales, "ru")
	if err := validateTemplate(base); !errors.Is(err, errInvalidTemplate) {
		t.Fatalf("missing locale accepted: %v", err)
	}
}
func TestTemplateRejectsUnsafeURLsAndMalformedPlaceholders(t *testing.T) {
	for _, body := range []string{"javascript:alert(1)", "DATA:text/html,test", "{{device_name", "<a href='jav&#97;script:alert(1)'>x</a>"} {
		v := defaultTemplate("identity.user_logged_in")
		v.Locales["en"] = TemplateText{Subject: "test", Body: body}
		if validateTemplate(v) == nil {
			t.Fatalf("unsafe template accepted: %q", body)
		}
	}
}
func TestTemplateRenderingEscapesVariablesStripsHeaderCRLFAndFallsBack(t *testing.T) {
	v := defaultTemplate("identity.user_logged_in")
	v.Locales["en"] = TemplateText{Subject: "Sign-in {{device_name}}", Body: "{{device_name}}"}
	subject, body, err := renderTemplate(v, "unknown", map[string]string{"device_name": "<script>\r\nBcc: x"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(subject, "\r\n") || strings.Contains(body, "<script>") || !strings.Contains(body, "&lt;script&gt;") {
		t.Fatalf("unsafe render: %q %q", subject, body)
	}
}
func TestTemplatePublishIsImmutableAndVersionFenced(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	ctx := context.Background()
	old, err := s.loadTemplate(ctx, "identity.user_logged_in")
	if err != nil {
		t.Fatal(err)
	}
	next := old
	next.Locales = map[string]TemplateText{"en": {Subject: "New", Body: "New"}, "ru": {Subject: "Новый", Body: "Новый"}}
	published, err := s.publishTemplate(ctx, next, old.Version, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if published.Version != old.Version+1 {
		t.Fatal("version not incremented")
	}
	if _, err = s.publishTemplate(ctx, next, old.Version, uuid.NewString()); !errors.Is(err, errVersionConflict) {
		t.Fatalf("stale publish accepted: %v", err)
	}
	var count int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM notification_templates WHERE event_type=$1 AND version=$2", "identity.user_logged_in", published.Version).Scan(&count); err != nil || count != 1 {
		t.Fatalf("revision lost: %d %v", count, err)
	}
}
