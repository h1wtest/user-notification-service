package profile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"platform-contracts/gen/go/businessv1"
	"platform-contracts/gen/go/eventsv2"
	"platform-contracts/gen/go/internalhttpv2"
	"strconv"
	"strings"
	"time"
	"user-notification-service/internal/platform"
)

type externalIntegration struct {
	Secret     string   `json:"secret"`
	EventTypes []string `json:"event_types"`
	ProductIDs []string `json:"product_ids"`
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
func validExternalSignature(r *http.Request, body, key []byte, now time.Time) bool {
	timestamp := r.Header.Get("X-Event-Timestamp")
	nonce := r.Header.Get("X-Event-Nonce")
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || len(nonce) < 8 || len(nonce) > 200 {
		return false
	}
	age := now.Sub(time.Unix(seconds, 0))
	if age > 120*time.Second || age < -120*time.Second {
		return false
	}
	signature, err := hex.DecodeString(r.Header.Get("X-Event-Signature"))
	if err != nil || len(signature) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(timestamp + "." + nonce + "."))
	mac.Write(body)
	return hmac.Equal(signature, mac.Sum(nil))
}
func (s *Service) externalEvent(w http.ResponseWriter, r *http.Request) {
	var integrations map[string]externalIntegration
	sender := r.Header.Get("X-Integration-ID")
	if json.Unmarshal([]byte(s.Config.ExternalIntegrations), &integrations) != nil {
		platform.Error(w, 403, "sender_denied")
		return
	}
	integration, ok := integrations[sender]
	if !ok {
		platform.Error(w, 403, "sender_denied")
		return
	}
	key, err := base64.StdEncoding.DecodeString(integration.Secret)
	if err != nil || len(key) != 32 {
		platform.Error(w, 503, "integration_unavailable")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16384))
	if err != nil {
		platform.Error(w, 400, "invalid_event")
		return
	}
	if !validExternalSignature(r, raw, key, time.Now()) {
		platform.Error(w, 401, "invalid_signature")
		return
	}
	var event businessv1.ExternalEvent
	if json.Unmarshal(raw, &event) != nil || event.Validate() != nil {
		platform.Error(w, 400, "invalid_event")
		return
	}
	if !contains(integration.EventTypes, event.EventKind) || !contains(integration.ProductIDs, event.ProductID) {
		platform.Error(w, 403, "scope_denied")
		return
	}
	status, b, err := s.Client.Call(r.Context(), "GET", s.Config.IdentityURL+"/internal/v2/users/"+event.UserID+"/eligibility", nil, nil)
	if err != nil || status >= 500 {
		platform.Error(w, 503, "authority_unavailable")
		return
	}
	var user internalhttpv2.UserEligibility
	if status == 200 && json.Unmarshal(b, &user) != nil {
		platform.Error(w, 503, "authority_unavailable")
		return
	}
	if status != 200 || !user.Active || user.Banned || user.UserID != event.UserID {
		platform.Error(w, 403, "recipient_denied")
		return
	}
	status, b, err = s.Client.Call(r.Context(), "GET", s.Config.SubscriptionURL+"/internal/v2/subscriptions/"+event.SubscriptionID, nil, nil)
	if err != nil || status >= 500 {
		platform.Error(w, 503, "authority_unavailable")
		return
	}
	// Subscription authority extends the public snapshot with a live entitlement decision.
	var subscription internalhttpv2.SubscriptionEligibility
	if status == 200 && json.Unmarshal(b, &subscription) != nil {
		platform.Error(w, 503, "authority_unavailable")
		return
	}
	if status != 200 || subscription.ID != event.SubscriptionID || subscription.UserID != event.UserID || subscription.ProductID != event.ProductID {
		platform.Error(w, 403, "scope_denied")
		return
	}
	if event.EventKind == "trading_alert" && (!subscription.Allowed || subscription.State != "ACTIVE" || !time.Now().Before(subscription.CurrentPeriodEnd)) {
		platform.Error(w, 403, "entitlement_required")
		return
	}
	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	digest := sha256.Sum256(raw)
	bodyHash := hex.EncodeToString(digest[:])
	// Serialize duplicate event IDs before allocating a sequence or persisting a nonce.
	if _, err = tx.Exec(r.Context(), "SELECT pg_advisory_xact_lock(hashtext($1))", event.EventID); err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	var priorSender, priorHash string
	err = tx.QueryRow(r.Context(), "SELECT integration_id,body_hash FROM external_event_receipts WHERE event_id=$1", event.EventID).Scan(&priorSender, &priorHash)
	if err == nil {
		if priorSender != sender || priorHash != bodyHash {
			platform.Error(w, 409, "event_conflict")
			return
		}
		platform.JSON(w, 202, struct {
			EventID string `json:"event_id"`
			Status  string `json:"status"`
		}{event.EventID, "accepted"})
		return
	}
	if err != pgx.ErrNoRows {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	tag, err := tx.Exec(r.Context(), "INSERT INTO external_event_nonces(integration_id,nonce) VALUES($1,$2) ON CONFLICT DO NOTHING", sender, r.Header.Get("X-Event-Nonce"))
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	if tag.RowsAffected() != 1 {
		platform.Error(w, 401, "nonce_replayed")
		return
	}
	payload := eventsv2.ExternalReadinessChanged{UserID: event.UserID, SubscriptionID: event.SubscriptionID, ProductID: event.ProductID, IntegrationID: sender, EventKind: event.EventKind, Status: event.Status, Message: event.Message, OccurredAt: event.OccurredAt}
	encoded, err := json.Marshal(payload)
	if event.EventKind == "trading_alert" {
		encoded, err = json.Marshal(eventsv2.ExternalTradingAlert(payload))
	}
	if err == nil {
		err = platform.EnqueueV2(r.Context(), tx, eventsv2.Envelope{SchemaVersion: 2, EventID: event.EventID, EventType: "external." + strings.TrimSpace(event.EventKind), Producer: "profile", AggregateType: "external_event", AggregateID: event.EventID, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: encoded})
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), "INSERT INTO external_event_receipts(event_id,integration_id,body_hash) VALUES($1,$2,$3)", event.EventID, sender, bodyHash)
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), "DELETE FROM external_event_nonces WHERE created_at<now()-interval '10 minutes'")
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		platform.Error(w, 503, "ingress_unavailable")
		return
	}
	platform.JSON(w, 202, struct {
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	}{event.EventID, "accepted"})
}
