package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"platform-contracts/gen/go/events"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Rekey runs only while all owner runtimes are stopped. All ciphertext and the
// key fingerprint marker commit atomically; failure leaves legacy data intact.
func Rekey(ctx context.Context, db *pgxpool.Pool, owner string, legacy, delivery, quarantine []byte) error {
	if len(legacy) != 32 || len(quarantine) != 32 || (owner == "notification" && len(delivery) != 32) {
		return errors.New("purpose and legacy keys must be 32 bytes")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", "platform-migration-"+owner); err != nil {
		return err
	}
	var actual string
	if err = tx.QueryRow(ctx, "SELECT owner FROM service_schema WHERE owner=$1", owner).Scan(&actual); err != nil {
		return errors.New("owner schema must be migrated before rekey")
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS service_key_migrations(owner text PRIMARY KEY,key_fingerprint text NOT NULL,completed_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	hash := sha256.New()
	hash.Write(quarantine)
	if owner == "notification" {
		hash.Write(delivery)
	}
	fingerprint := hex.EncodeToString(hash.Sum(nil))
	var previous string
	err = tx.QueryRow(ctx, "SELECT key_fingerprint FROM service_key_migrations WHERE owner=$1", owner).Scan(&previous)
	if err == nil {
		if previous != fingerprint {
			return errors.New("owner already rekeyed to different purpose keys")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT id,topic,partition_id,offset_id,encrypted_record FROM quarantine FOR UPDATE")
	if err != nil {
		return err
	}
	type entry struct{ id, aad, encrypted string }
	var originals []entry
	for rows.Next() {
		var id, topic, encrypted string
		var partition int32
		var offset int64
		if err = rows.Scan(&id, &topic, &partition, &offset, &encrypted); err != nil {
			rows.Close()
			return err
		}
		originals = append(originals, entry{id, fmt.Sprintf("%s:%d:%d", topic, partition, offset), encrypted})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, record := range originals {
		value, err := rekeyQuarantined(owner, legacy, delivery, quarantine, record.aad, record.encrypted)
		if err != nil {
			return errors.New("quarantine legacy key mismatch")
		}
		if _, err = tx.Exec(ctx, "UPDATE quarantine SET encrypted_record=$2 WHERE id=$1", record.id, value); err != nil {
			return err
		}
	}
	rows, err = tx.Query(ctx, "SELECT id,quarantine_id,encrypted_event FROM quarantine_replays FOR UPDATE")
	if err != nil {
		return err
	}
	var corrections []entry
	for rows.Next() {
		var item entry
		if err = rows.Scan(&item.id, &item.aad, &item.encrypted); err != nil {
			rows.Close()
			return err
		}
		corrections = append(corrections, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, record := range corrections {
		value, err := rekeyQuarantined(owner, legacy, delivery, quarantine, record.aad, record.encrypted)
		if err != nil {
			return errors.New("correction legacy key mismatch")
		}
		if _, err = tx.Exec(ctx, "UPDATE quarantine_replays SET encrypted_event=$2 WHERE id=$1", record.id, value); err != nil {
			return err
		}
	}
	if owner == "notification" {
		rows, err = tx.Query(ctx, "SELECT id,encrypted_body FROM delivery_jobs WHERE encrypted_body<>'' FOR UPDATE")
		if err != nil {
			return err
		}
		var jobs []entry
		for rows.Next() {
			var item entry
			if err = rows.Scan(&item.id, &item.encrypted); err != nil {
				rows.Close()
				return err
			}
			jobs = append(jobs, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if err = rekeyDelivery(ctx, tx, job.id, job.encrypted, legacy, delivery); err != nil {
				return err
			}
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO service_key_migrations(owner,key_fingerprint) VALUES($1,$2)", owner, fingerprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func reencrypt(legacy, target []byte, aad, encrypted string) (string, error) {
	if _, err := Decrypt(target, aad, encrypted); err == nil {
		return encrypted, nil
	}
	plain, err := Decrypt(legacy, aad, encrypted)
	if err != nil {
		return "", err
	}
	return Encrypt(target, aad, plain)
}
func rekeyDelivery(ctx context.Context, tx pgx.Tx, id, encrypted string, legacy, target []byte) error {
	value, err := reencrypt(legacy, target, id, encrypted)
	if err != nil {
		return errors.New("delivery legacy key mismatch")
	}
	_, err = tx.Exec(ctx, "UPDATE delivery_jobs SET encrypted_body=$2 WHERE id=$1", id, value)
	return err
}

func rekeyQuarantined(owner string, legacy, delivery, quarantine []byte, aad, encrypted string) (string, error) {
	raw, err := Decrypt(quarantine, aad, encrypted)
	if err != nil {
		raw, err = Decrypt(legacy, aad, encrypted)
	}
	if err != nil {
		return "", err
	}
	if owner == "notification" {
		var metadata struct {
			EventType        string          `json:"event_type"`
			EncryptedPayload json.RawMessage `json:"encrypted_payload"`
		}
		if json.Unmarshal(raw, &metadata) == nil && (strings.HasPrefix(metadata.EventType, "notification.") || len(metadata.EncryptedPayload) > 0) {
			var envelope Envelope
			if err = json.Unmarshal(raw, &envelope); err != nil {
				return "", errors.New("invalid nested sensitive envelope")
			}
			payload, decodeErr := Decrypt(delivery, envelope.EventID, envelope.EncryptedPayload)
			if decodeErr != nil {
				payload, decodeErr = Decrypt(legacy, envelope.EventID, envelope.EncryptedPayload)
			}
			if decodeErr != nil || events.ValidatePayload(envelope.EventType, payload) != nil {
				return "", errors.New("nested sensitive payload cannot authenticate")
			}
			var identity struct {
				UserID string `json:"user_id"`
			}
			if json.Unmarshal(payload, &identity) != nil || identity.UserID != envelope.AggregateID {
				return "", errors.New("nested sensitive identity mismatch")
			}
			envelope.EncryptedPayload, err = Encrypt(delivery, envelope.EventID, payload)
			if err != nil {
				return "", err
			}
			raw, err = json.Marshal(envelope)
			if err != nil {
				return "", err
			}
		}
	}
	return Encrypt(quarantine, aad, raw)
}
