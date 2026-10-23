package profile

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExternalSignatureBindsExactRawBodyNonceAndTimestamp(t *testing.T) {
	now := time.Unix(1800000000, 0)
	body := []byte(`{"event_kind":"trading_alert"}`)
	r := httptest.NewRequest("POST", "https://profile.invalid/internal/v1/external-events", nil)
	r.Header.Set("X-Event-Timestamp", "1800000000")
	r.Header.Set("X-Event-Nonce", "one-use-nonce")
	key := []byte("01234567890123456789012345678901")
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("1800000000.one-use-nonce."))
	mac.Write(body)
	r.Header.Set("X-Event-Signature", hex.EncodeToString(mac.Sum(nil)))
	if !validExternalSignature(r, body, key, now) {
		t.Fatal("valid signature rejected")
	}
	if validExternalSignature(r, append(body, ' '), key, now) {
		t.Fatal("altered raw body accepted")
	}
	if validExternalSignature(r, body, key, now.Add(121*time.Second)) {
		t.Fatal("stale signature accepted")
	}
	r.Header.Set("X-Event-Nonce", "different")
	if validExternalSignature(r, body, key, now) {
		t.Fatal("altered nonce accepted")
	}
}
