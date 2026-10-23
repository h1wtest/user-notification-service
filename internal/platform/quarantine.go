package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"platform-contracts/gen/go/events"
	"platform-contracts/gen/go/eventsv2"
	"platform-contracts/gen/go/internalhttp"
	"platform-contracts/gen/go/internalhttpv2"
	"strings"
	"time"
)

type QuarantineAPI struct {
	DB      *pgxpool.Pool
	Config  Config
	Client  *Client
	Apply   func(context.Context, Envelope) error
	ApplyV2 func(context.Context, eventsv2.Envelope) error
	Owner   string
	Local   bool
}

func (q QuarantineAPI) authorize(w http.ResponseWriter, r *http.Request, permission string) (Actor, bool) {
	if !q.Local && !ServiceAuthorized(r, q.Config.AdminToken) {
		Error(w, 403, "service_denied")
		return Actor{}, false
	}
	return q.Client.Authenticate(w, r, permission, true)
}
func (q QuarantineAPI) List(w http.ResponseWriter, r *http.Request) {
	if _, ok := q.authorize(w, r, "notifications:read"); !ok {
		return
	}
	rows, err := q.DB.Query(r.Context(), "SELECT id,topic,partition_id,offset_id,reason,created_at,replayed_at FROM quarantine ORDER BY created_at DESC LIMIT 200")
	if err != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	defer rows.Close()
	out := internalhttp.QuarantineResponse{}
	for rows.Next() {
		var id, topic, reason string
		var partition int32
		var offset int64
		var created time.Time
		var replayed *time.Time
		if rows.Scan(&id, &topic, &partition, &offset, &reason, &created, &replayed) != nil {
			Error(w, 503, "database_unavailable")
			return
		}
		created = created.UTC()
		if replayed != nil {
			value := replayed.UTC()
			replayed = &value
		}
		out = append(out, internalhttp.QuarantineRecord{ID: id, Topic: topic, Partition: int64(partition), Offset: offset, Reason: reason, CreatedAt: created, ReplayedAt: replayed, Owner: q.Owner})
	}
	if rows.Err() != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	JSON(w, 200, out)
}
func (q QuarantineAPI) Replay(w http.ResponseWriter, r *http.Request) {
	actor, ok := q.authorize(w, r, "notifications:replay")
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		Error(w, 400, "invalid_quarantine")
		return
	}
	var input internalhttpv2.QuarantineReplayRequest
	if !Decode(w, r, &input) {
		return
	}
	if strings.TrimSpace(input.Reason) == "" || len(input.Reason) > 500 {
		Error(w, 400, "invalid_reason")
		return
	}
	var topic, encrypted string
	var partition int32
	var offset int64
	err := q.DB.QueryRow(r.Context(), "SELECT topic,partition_id,offset_id,encrypted_record FROM quarantine WHERE id=$1 AND replayed_at IS NULL", id).Scan(&topic, &partition, &offset, &encrypted)
	if errors.Is(err, pgx.ErrNoRows) {
		Error(w, 404, "quarantine_not_found")
		return
	}
	if err != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	raw, err := Decrypt(q.Config.QuarantineKey, fmt.Sprintf("%s:%d:%d", topic, partition, offset), encrypted)
	if err != nil {
		Error(w, 503, "quarantine_unreadable")
		return
	}
	var original struct {
		EventID     string `json:"event_id"`
		AggregateID string `json:"aggregate_id"`
	}
	_ = json.Unmarshal(raw, &original)
	if len(input.CorrectedEvent) > 0 {
		raw = input.CorrectedEvent
	}
	var header struct {
		SchemaVersion int `json:"schema_version"`
	}
	_ = json.Unmarshal(raw, &header)
	var eventID, aggregateID string
	var apply func() error
	if header.SchemaVersion == 2 {
		var e eventsv2.Envelope
		if json.Unmarshal(raw, &e) != nil || e.Validate() != nil || q.ApplyV2 == nil {
			Error(w, 409, "record_still_invalid")
			return
		}
		if e.Topic() != topic {
			Error(w, 409, "record_topic_mismatch")
			return
		}
		eventID, aggregateID = e.EventID, e.AggregateID
		apply = func() error { return q.ApplyV2(r.Context(), e) }
	} else {
		var e Envelope
		if json.Unmarshal(raw, &e) != nil || Validate(e) != nil {
			Error(w, 409, "record_still_invalid")
			return
		}
		expected, topicErr := events.Topic(e.EventType)
		if topicErr != nil || expected != topic {
			Error(w, 409, "record_topic_mismatch")
			return
		}
		eventID, aggregateID = e.EventID, e.AggregateID
		apply = func() error { return q.Apply(r.Context(), e) }
	}
	if (original.EventID != "" && original.EventID != eventID) || (original.AggregateID != "" && original.AggregateID != aggregateID) {
		Error(w, 409, "record_identity_mismatch")
		return
	}
	lock, err := q.DB.Acquire(r.Context())
	if err != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	defer lock.Release()
	var locked bool
	err = lock.QueryRow(r.Context(), "SELECT pg_try_advisory_lock(hashtextextended($1,0))", id).Scan(&locked)
	if err != nil || !locked {
		Error(w, 409, "replay_in_progress")
		return
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = lock.Exec(unlockCtx, "SELECT pg_advisory_unlock(hashtextextended($1,0))", id)
	}()
	var completed bool
	if err = q.DB.QueryRow(r.Context(), "SELECT replayed_at IS NOT NULL FROM quarantine WHERE id=$1", id).Scan(&completed); err != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	if completed {
		Error(w, 404, "quarantine_not_found")
		return
	}
	// Preserve the original immutable encrypted record. Record the authorization
	// and validated correction encrypted before applying effects, so a crash has
	// durable evidence and replays use inbox uniqueness rather than duplicate jobs.
	correction, err := Encrypt(q.Config.QuarantineKey, id, raw)
	if err != nil {
		Error(w, 503, "encryption_unavailable")
		return
	}
	var replayID string
	err = q.DB.QueryRow(r.Context(), "INSERT INTO quarantine_replays(quarantine_id,actor_id,encrypted_event,reason) VALUES($1,$2,$3,$4) RETURNING id", id, actor.UserID, correction, input.Reason).Scan(&replayID)
	if err != nil {
		Error(w, 503, "audit_unavailable")
		return
	}
	if err = apply(); err != nil {
		Error(w, 409, "record_not_recoverable")
		return
	}
	tx, err := q.DB.Begin(r.Context())
	if err != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	_, err = tx.Exec(r.Context(), "UPDATE quarantine_replays SET completed_at=now() WHERE id=$1", replayID)
	if err == nil {
		_, err = tx.Exec(r.Context(), "UPDATE quarantine SET replayed_at=now() WHERE id=$1", id)
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		Error(w, 503, "database_unavailable")
		return
	}
	JSON(w, 200, internalhttp.ReplayResponse{Status: "replayed"})
}
