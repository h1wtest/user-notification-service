package notification

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"user-notification-service/internal/platform"
)

func TestDeliveryHistorySerializesCanonicalUTCTimestamps(t *testing.T) {
	db := testDB(t)
	id, user := uuid.NewString(), uuid.NewString()
	cleanupDelivery(t, db, id)
	if _, err := db.Exec(context.Background(), "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,status,delivered_at) VALUES($1,$1,$2,'email','','<test@example.test>','sent',now())", id, user); err != nil {
		t.Fatal(err)
	}
	s := New(db, platform.Config{}, nil)
	request := httptest.NewRequest(http.MethodGet, "https://notification.test/api/v1/notifications", nil)
	response := httptest.NewRecorder()
	s.list(response, request, user)
	if response.Code != 200 {
		t.Fatalf("history response %d %s", response.Code, response.Body.String())
	}
	var deliveries []map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &deliveries); err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected own delivery, got %d", len(deliveries))
	}
	for _, field := range []string{"next_attempt_at", "created_at", "delivered_at"} {
		var value string
		if err := json.Unmarshal(deliveries[0][field], &value); err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(value, "Z") {
			t.Fatalf("noncanonical %s: %s", field, value)
		}
	}
}
