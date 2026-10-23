package profile

import (
	"context"
	"encoding/json"
	"errors"
	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

func (s *Service) ApplyV2(ctx context.Context, e eventsv2.Envelope) error {
	if e.Validate() != nil || e.Producer != "identity" {
		return platform.ErrPoison
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	fresh, err := platform.AcceptV2Receipt(ctx, tx, e)
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, "INSERT INTO profile_v2_receipts(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", e.EventID, e.EventType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO profile_aggregate_versions(producer,aggregate_type,aggregate_id,version) VALUES($1,$2,$3,0) ON CONFLICT DO NOTHING", e.Producer, e.AggregateType, e.AggregateID); err != nil {
		return err
	}
	var version int64
	if err = tx.QueryRow(ctx, "SELECT version FROM profile_aggregate_versions WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3 FOR UPDATE", e.Producer, e.AggregateType, e.AggregateID).Scan(&version); err != nil {
		return err
	}
	if e.AggregateVersion != version+1 {
		return errors.New("profile aggregate sequence gap")
	}
	if _, err = tx.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", e.EventID, e.EventType); err != nil {
		return err
	}
	if e.EventType == "identity.user_registered" {
		var user eventsv2.UserRegistered
		if json.Unmarshal(e.Payload, &user) != nil || user.UserID != e.AggregateID {
			return platform.ErrPoison
		}
		if _, err = tx.Exec(ctx, "INSERT INTO profiles(user_id,email,website_locale,notification_locale) VALUES($1,$2,$3,$3) ON CONFLICT DO NOTHING", user.UserID, user.Email, locale(user.Locale)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO notification_settings(user_id) VALUES($1) ON CONFLICT DO NOTHING", user.UserID); err != nil {
			return err
		}
	}
	// All valid non-profile identity facts still have durable receipts. Identity
	// remains the authority for verified email/security and current eligibility.
	if _, err = tx.Exec(ctx, "UPDATE profile_aggregate_versions SET version=$4 WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3", e.Producer, e.AggregateType, e.AggregateID, e.AggregateVersion); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
