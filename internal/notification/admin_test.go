package notification

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"user-notification-service/internal/platform"
)

func TestReplayRequiresReasonOnlyFailedAndPersistsEvent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	actor, user, id := uuid.NewString(), uuid.NewString(), uuid.NewString()
	cleanupDelivery(t, db, id)
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, platform.Actor{Active: true, UserID: actor, Email: "admin@example.test", SessionID: uuid.NewString(), Roles: []string{"admin"}, Permissions: []string{"notifications:replay"}, MFAVerified: true})
	}))
	defer identity.Close()
	c := platform.Config{IdentityURL: identity.URL, AdminToken: "admin-test"}
	s := New(db, c, &platform.Client{Config: c, HTTP: identity.Client()})
	if _, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,status) VALUES($1,$1,$2,'email','encrypted','<test@example.test>','failed')", id, user); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Routes(mux)
	request := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "https://notification.test/internal/v1/deliveries/"+id+"/replay", strings.NewReader(body))
		r.Header.Set("X-Service-Token", c.AdminToken)
		r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if w := request(`{"reason":""}`); w.Code != 400 {
		t.Fatalf("reason omitted: %d", w.Code)
	}
	if w := request(`{"reason":"approved investigation"}`); w.Code != 200 {
		t.Fatalf("failed replay denied: %d %s", w.Code, w.Body.String())
	}
	if w := request(`{"reason":"again"}`); w.Code != 409 {
		t.Fatalf("pending replay accepted: %d", w.Code)
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM replay_audit WHERE delivery_id=$1 AND actor_id=$2 AND reason='approved investigation'", id, actor).Scan(&count); err != nil || count != 1 {
		t.Fatalf("reason audit lost: %d %v", count, err)
	}
}
func TestConcurrentTemplatePublishHasOneWinner(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	ctx := context.Background()
	v, err := s.loadTemplate(ctx, "identity.mfa_changed")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.publishTemplate(ctx, v, v.Version, uuid.NewString())
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	winners, conflicts := 0, 0
	for err := range results {
		if err == nil {
			winners++
		} else if errors.Is(err, errVersionConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("lost version fence: %d %d", winners, conflicts)
	}
}
