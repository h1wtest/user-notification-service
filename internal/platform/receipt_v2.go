// SPDX-License-Identifier: MIT
package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"platform-contracts/gen/go/eventsv2"
)

const immutableReceiptSQL = `CREATE TABLE immutable_v2_receipts(event_id uuid PRIMARY KEY,event_type text NOT NULL,producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,aggregate_version bigint NOT NULL,canonical_sha256 text NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());`

// AcceptV2Receipt shares the caller's effects transaction. Exact canonical
// replays are harmless; changed identities/content are poison, never successful
// duplicates that could strand the next aggregate cursor.
func AcceptV2Receipt(ctx context.Context, tx pgx.Tx, e eventsv2.Envelope) (bool, error) {
	if e.Validate() != nil {
		return false, ErrPoison
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return false, ErrPoison
	}
	// Canonicalize object key order and insignificant whitespace in RawMessage
	// payloads as well as envelope fields before hashing immutable semantics.
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&value); err != nil {
		return false, ErrPoison
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return false, ErrPoison
	}
	digest := sha256.Sum256(canonical)
	hash := hex.EncodeToString(digest[:])
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", e.EventID); err != nil {
		return false, err
	}
	var existingType string
	err = tx.QueryRow(ctx, "SELECT event_type FROM inbox WHERE event_id=$1", e.EventID).Scan(&existingType)
	if err == nil && existingType != e.EventType {
		return false, ErrPoison
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	var fingerprintExists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM immutable_v2_receipts WHERE event_id=$1)", e.EventID).Scan(&fingerprintExists); err != nil {
		return false, err
	}
	if !fingerprintExists {
		// Old v2 ledgers did not retain canonical bytes; do not invent a matching
		// fingerprint for a replay whose immutable content can no longer be proven.
		for _, table := range []string{"profile_v2_receipts", "notification_v2_receipts", "audit_v2_receipts"} {
			var present bool
			if err = tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&present); err != nil {
				return false, err
			}
			if !present {
				continue
			}
			var seen bool
			if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+table+" WHERE event_id=$1)", e.EventID).Scan(&seen); err != nil {
				return false, err
			}
			if seen {
				return false, ErrPoison
			}
		}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO immutable_v2_receipts(event_id,event_type,producer,aggregate_type,aggregate_id,aggregate_version,canonical_sha256) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, e.EventID, e.EventType, e.Producer, e.AggregateType, e.AggregateID, e.AggregateVersion, hash)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var recordedType, producer, aggregateType, aggregateID, recordedHash string
	var version int64
	if err = tx.QueryRow(ctx, "SELECT event_type,producer,aggregate_type,aggregate_id,aggregate_version,canonical_sha256 FROM immutable_v2_receipts WHERE event_id=$1", e.EventID).Scan(&recordedType, &producer, &aggregateType, &aggregateID, &version, &recordedHash); err != nil {
		return false, err
	}
	if recordedType != e.EventType || producer != e.Producer || aggregateType != e.AggregateType || aggregateID != e.AggregateID || version != e.AggregateVersion || recordedHash != hash {
		return false, ErrPoison
	}
	return false, nil
}
