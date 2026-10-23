package profile

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"testing"
)

func TestLocalePatchSeparatesWebsiteAndNotificationLanguage(t *testing.T) {
	db := profilePatchDB(t)
	ctx := context.Background()
	id := uuid.NewString()
	if _, err := db.Exec(ctx, "INSERT INTO profiles(user_id,email) VALUES($1,'locale@example.invalid')", id); err != nil {
		t.Fatal(err)
	}
	repo := &PostgresProfiles{DB: db}
	for _, tc := range []struct{ raw, website, notification string }{
		{`{"website_locale":"ru"}`, "ru", "en"},
		{`{"notification_locale":"ru"}`, "ru", "ru"},
		{`{"website_locale":null}`, "en", "ru"},
		{`{}`, "en", "ru"},
	} {
		var patch ProfileChanges
		if err := json.Unmarshal([]byte(tc.raw), &patch); err != nil {
			t.Fatal(err)
		}
		if err := repo.Update(ctx, id, patch); err != nil {
			t.Fatal(err)
		}
		var website, notification string
		if err := db.QueryRow(ctx, "SELECT website_locale,notification_locale FROM profiles WHERE user_id=$1", id).Scan(&website, &notification); err != nil {
			t.Fatal(err)
		}
		if website != tc.website || notification != tc.notification {
			t.Fatalf("patch %s got %s/%s want %s/%s", tc.raw, website, notification, tc.website, tc.notification)
		}
	}
}

func TestLocalePatchRejectsEmptyOrUnsupportedLocale(t *testing.T) {
	for _, raw := range []string{`{"website_locale":""}`, `{"notification_locale":"de"}`} {
		var patch ProfileChanges
		if err := json.Unmarshal([]byte(raw), &patch); err != nil {
			continue
		}
		if patch.Validate() == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestLocaleFallbackIsDeterministic(t *testing.T) {
	for _, tc := range []struct{ in, want string }{{"ru", "ru"}, {"en", "en"}, {"de", "en"}, {"", "en"}} {
		if got := locale(tc.in); got != tc.want {
			t.Fatalf("locale(%q)=%q", tc.in, got)
		}
	}
}
