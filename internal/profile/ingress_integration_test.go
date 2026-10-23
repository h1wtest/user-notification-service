package profile

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestIngressAuthorityFailureNeverPersistsAcceptedEvent(t *testing.T) {
	db := profilePatchDB(t)
	id, _ := uuid.NewV7()
	user := uuid.NewString()
	subscription := uuid.NewString()
	product := uuid.NewString()
	key := bytes.Repeat([]byte{7}, 32)
	registry, _ := json.Marshal(map[string]externalIntegration{"test": {Secret: base64.StdEncoding.EncodeToString(key), EventTypes: []string{"trading_alert"}, ProductIDs: []string{product}}})
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer authority.Close()
	s := &Service{DB: db, Config: platform.Config{ExternalIntegrations: string(registry), IdentityURL: authority.URL, SubscriptionURL: authority.URL}, Client: &platform.Client{HTTP: authority.Client()}}
	body, _ := json.Marshal(map[string]any{"event_id": id.String(), "user_id": user, "subscription_id": subscription, "product_id": product, "event_kind": "trading_alert", "status": "ready", "message": "test", "occurred_at": time.Now().UTC()})
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "durable-regression-nonce"
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(timestamp + "." + nonce + "."))
	mac.Write(body)
	request := httptest.NewRequest("POST", "https://app.invalid/internal/v1/external-events", bytes.NewReader(body))
	request.Header.Set("X-Integration-ID", "test")
	request.Header.Set("X-Event-Timestamp", timestamp)
	request.Header.Set("X-Event-Nonce", nonce)
	request.Header.Set("X-Event-Signature", hex.EncodeToString(mac.Sum(nil)))
	w := httptest.NewRecorder()
	s.externalEvent(w, request)
	if w.Code != 503 {
		t.Fatalf("authority outage status=%d body=%s", w.Code, w.Body.String())
	}
	var receipts, events int
	if err := db.QueryRow(context.Background(), "SELECT count(*) FROM external_event_receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(context.Background(), "SELECT count(*) FROM owner_outbox").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 || events != 0 {
		t.Fatalf("outage persisted receipts=%d events=%d", receipts, events)
	}
}
