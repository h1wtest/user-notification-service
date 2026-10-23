package profile

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"user-notification-service/internal/platform"
)

// Keep the profile owner's schema separate from the notification database used
// by the rest of the suite, even when they share TEST_DATABASE_URL.
func profilePatchDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable TEST_DATABASE_URL with CREATE SCHEMA permission required")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{"profile_patch_" + uuid.NewString()}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("remove profile test schema: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err = platform.Migrate(ctx, db, "profile"); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestProfilePatchPersistsNullableFieldsIndependently(t *testing.T) {
	db := profilePatchDB(t)
	user := uuid.NewString()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "INSERT INTO profiles(user_id,email) VALUES($1,'profile@example.test')", user); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO notification_settings(user_id) VALUES($1)", user); err != nil {
		t.Fatal(err)
	}
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, http.StatusOK, platform.Actor{Active: true, UserID: user, Email: "profile@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{}, Permissions: []string{}})
	}))
	defer identity.Close()
	s := &Service{
		DB:       db,
		Profiles: &PostgresProfiles{DB: db},
		Client:   &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()},
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	for _, tc := range []struct {
		name string
		body string
		want [3]any
	}{
		{"omitted", `{}`, [3]any{"Existing name", "https://example.test/old.png", "+1234567890"}},
		{"null display", `{"display_name":null}`, [3]any{nil, "https://example.test/old.png", "+1234567890"}},
		{"null avatar", `{"avatar_url":null}`, [3]any{"Existing name", nil, "+1234567890"}},
		{"null phone", `{"phone_number":null}`, [3]any{"Existing name", "https://example.test/old.png", nil}},
		{"empty display", `{"display_name":""}`, [3]any{"", "https://example.test/old.png", "+1234567890"}},
		{"empty avatar", `{"avatar_url":""}`, [3]any{"Existing name", "", "+1234567890"}},
		{"empty phone", `{"phone_number":""}`, [3]any{"Existing name", "https://example.test/old.png", ""}},
		{"replace display", `{"display_name":"New name"}`, [3]any{"New name", "https://example.test/old.png", "+1234567890"}},
		{"replace avatar", `{"avatar_url":"https://example.test/new.png"}`, [3]any{"Existing name", "https://example.test/new.png", "+1234567890"}},
		{"replace phone", `{"phone_number":"+1987654321"}`, [3]any{"Existing name", "https://example.test/old.png", "+1987654321"}},
		{"mixed", `{"display_name":null,"phone_number":""}`, [3]any{nil, "https://example.test/old.png", ""}},
		{"all null", `{"display_name":null,"avatar_url":null,"phone_number":null}`, [3]any{nil, nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.Exec(ctx, "UPDATE profiles SET display_name='Existing name',avatar_url='https://example.test/old.png',phone_number='+1234567890' WHERE user_id=$1", user); err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPatch, "https://app.test/api/v1/users/me", strings.NewReader(tc.body))
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: "profile-patch-session"})
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("PATCH returned %d: %s", w.Code, w.Body.String())
			}
			var response struct {
				Profile map[string]any `json:"profile"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			got := [3]any{response.Profile["display_name"], response.Profile["avatar_url"], response.Profile["phone_number"]}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("public profile = %#v; want %#v", got, tc.want)
			}
			var persisted [3]any
			if err := db.QueryRow(ctx, "SELECT display_name,avatar_url,phone_number FROM profiles WHERE user_id=$1", user).Scan(&persisted[0], &persisted[1], &persisted[2]); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(persisted, tc.want) {
				t.Fatalf("persisted profile = %#v; want %#v", persisted, tc.want)
			}
		})
	}
	if _, err := db.Exec(ctx, "UPDATE profiles SET display_name=NULL,avatar_url=NULL,phone_number=NULL WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	if err := s.Profiles.Update(ctx, user, ProfileChanges{}); err != nil {
		t.Fatal(err)
	}
	var remainNull bool
	if err := db.QueryRow(ctx, "SELECT display_name IS NULL AND avatar_url IS NULL AND phone_number IS NULL FROM profiles WHERE user_id=$1", user).Scan(&remainNull); err != nil {
		t.Fatal(err)
	}
	if !remainNull {
		t.Fatal("omitted fields replaced pre-existing SQL NULL values")
	}
}
