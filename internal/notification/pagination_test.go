// SPDX-License-Identifier: MIT
package notification

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http/httptest"
	"net/url"
	"platform-contracts/gen/go/internalhttp"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestDeliveryHistoryKeysetVisitsEveryTiedTimestampOnce(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	user := uuid.NewString()
	stamp := time.Date(2026, 10, 24, 12, 0, 0, 123456000, time.UTC)
	for range 5 {
		id := uuid.NewString()
		cleanupDelivery(t, db, id)
		if _, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,status,created_at) VALUES($1,$1,$2,'email','','<history@example.test>','sent',$3)", id, user, stamp); err != nil {
			t.Fatal(err)
		}
	}
	s := New(db, platform.Config{}, nil)
	cursor := ""
	seen := map[string]bool{}
	for page := range 4 {
		request := httptest.NewRequest("GET", "https://notification.test/api/v1/notifications?limit=2&cursor="+url.QueryEscape(cursor), nil)
		if cursor == "" {
			request = httptest.NewRequest("GET", "https://notification.test/api/v1/notifications?limit=2", nil)
		}
		response := httptest.NewRecorder()
		s.list(response, request, user)
		if response.Code != 200 {
			t.Fatalf("page %d: %d %s", page, response.Code, response.Body.String())
		}
		var deliveries internalhttp.DeliveriesResponse
		if err := json.Unmarshal(response.Body.Bytes(), &deliveries); err != nil {
			t.Fatal(err)
		}
		for _, delivery := range deliveries {
			if seen[delivery.ID] {
				t.Fatalf("delivery repeated: %s", delivery.ID)
			}
			seen[delivery.ID] = true
		}
		cursor = response.Header().Get("X-Next-Cursor")
		if cursor == "" {
			break
		}
		if page == 3 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Fatalf("timestamp ties dropped deliveries: got %d want 5", len(seen))
	}
}

func TestDeliveryHistoryRejectsTimestampOnlyOrMalformedCursor(t *testing.T) {
	s := New(nil, platform.Config{}, nil)
	for _, cursor := range []string{time.Now().UTC().Format(time.RFC3339Nano), "not_base64", ""} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest("GET", "https://notification.test/api/v1/notifications?cursor="+url.QueryEscape(cursor), nil)
		s.list(response, request, uuid.NewString())
		if response.Code != 400 {
			t.Fatalf("invalid cursor %q accepted: %d", cursor, response.Code)
		}
	}
}
