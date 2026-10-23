package platformkit

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"platform-contracts/gen/go/eventsv2"
)

var (
	ErrPoison      = errors.New("invalid_event")
	ErrSequenceGap = errors.New("event_sequence_gap_owner_replay_required")
	ErrLeaseLost   = errors.New("event_lease_lost")
)

func Enqueue(ctx context.Context, tx pgx.Tx, e eventsv2.Envelope) error {
	derivedTrace := e.Traceparent == ""
	if derivedTrace {
		e.Traceparent = traceparentFrom(ctx)
	}
	if err := e.Validate(); err != nil {
		return fmt.Errorf("%w: invalid envelope", ErrPoison)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(raw) > bodyLimit {
		return fmt.Errorf("%w: oversized envelope", ErrPoison)
	}
	result, err := tx.Exec(ctx, `INSERT INTO outbox(event_id,event_type,producer,aggregate_type,aggregate_id,aggregate_version,topic,envelope)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.EventType, e.Producer, e.AggregateType, e.AggregateID, e.AggregateVersion, e.Topic(), raw)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		return nil
	}
	var existing []byte
	if err = tx.QueryRow(ctx, "SELECT envelope FROM outbox WHERE event_id=$1", e.EventID).Scan(&existing); err != nil {
		return err
	}
	if derivedTrace {
		// Retry correlation is not event identity. Retain the first durable
		// trace while still detecting every business-envelope collision.
		var stored eventsv2.Envelope
		if json.Unmarshal(existing, &stored) == nil {
			e.Traceparent = stored.Traceparent
			raw, err = json.Marshal(e)
			if err != nil {
				return err
			}
		}
	}
	if !bytes.Equal(existing, raw) {
		return fmt.Errorf("%w: conflicting event ID", ErrPoison)
	}
	return nil
}

func Inbox(ctx context.Context, tx pgx.Tx, eventID, eventType string) (bool, error) {
	id, err := uuid.Parse(eventID)
	if err != nil || id.String() != eventID || eventType == "" {
		return false, ErrPoison
	}
	result, err := tx.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", eventID, eventType)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 1 {
		return true, nil
	}
	var existing string
	if err = tx.QueryRow(ctx, "SELECT event_type FROM inbox WHERE event_id=$1", eventID).Scan(&existing); err != nil {
		return false, err
	}
	if existing != eventType {
		return false, fmt.Errorf("%w: event ID type collision", ErrPoison)
	}
	return false, nil
}

func encryptRecord(key []byte, aad string, raw []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, raw, []byte(aad))), nil
}
func decryptRecord(key []byte, aad, encrypted string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(raw) < gcm.NonceSize() {
		return nil, errors.New("invalid quarantine ciphertext")
	}
	return gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], []byte(aad))
}
