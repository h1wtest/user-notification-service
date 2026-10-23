package profile

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
	"platform-contracts/gen/go/events"
	"user-notification-service/internal/platform"
)

// NullableString distinguishes an omitted PATCH field from an explicit null.
// For Go callers, Present selects the field and a nil Value clears it.
type NullableString struct {
	Present bool
	Value   *string
}

func (s *NullableString) UnmarshalJSON(data []byte) error {
	var value *string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	s.Present, s.Value = true, value
	return nil
}

type ProfileChanges struct {
	Display NullableString `json:"display_name"`
	Avatar  NullableString `json:"avatar_url"`
	Phone   NullableString `json:"phone_number"`
}
type Preferences struct {
	EL *bool `json:"email_login_alerts"`
	ES *bool `json:"email_security_alerts"`
	TL *bool `json:"telegram_login_alerts"`
	TS *bool `json:"telegram_security_alerts"`
}
type Repository interface {
	Apply(context.Context, platform.Envelope) error
	Update(context.Context, string, ProfileChanges) error
	Preferences(context.Context, string, Preferences, bool) (bool, error)
}
type PostgresProfiles struct{ DB *pgxpool.Pool }

func (p ProfileChanges) Validate() error {
	if (p.Display.Value != nil && len(*p.Display.Value) > 100) || (p.Phone.Value != nil && len(*p.Phone.Value) > 30) {
		return errors.New("invalid_profile")
	}
	if p.Avatar.Value != nil && *p.Avatar.Value != "" {
		u, err := url.Parse(*p.Avatar.Value)
		if err != nil || u.Scheme != "https" || u.Host == "" || len(*p.Avatar.Value) > 2048 {
			return errors.New("invalid_avatar")
		}
	}
	return nil
}
func (p *PostgresProfiles) Apply(ctx context.Context, e platform.Envelope) error {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", e.EventID, e.EventType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if e.EventType == "identity.user_registered" {
		var user events.UserRegistered
		if json.Unmarshal(e.Payload, &user) != nil || user.UserID != e.AggregateID {
			return platform.ErrPoison
		}
		if _, err = tx.Exec(ctx, "INSERT INTO profiles(user_id,email) VALUES($1,$2) ON CONFLICT DO NOTHING", user.UserID, user.Email); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO notification_settings(user_id) VALUES($1) ON CONFLICT DO NOTHING", user.UserID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (p *PostgresProfiles) Update(ctx context.Context, id string, c ProfileChanges) error {
	if err := c.Validate(); err != nil {
		return err
	}
	tag, err := p.DB.Exec(ctx, "UPDATE profiles SET display_name=CASE WHEN $2 THEN $3::text ELSE display_name END,avatar_url=CASE WHEN $4 THEN $5::text ELSE avatar_url END,phone_number=CASE WHEN $6 THEN $7::text ELSE phone_number END,updated_at=now() WHERE user_id=$1", id, c.Display.Present, c.Display.Value, c.Avatar.Present, c.Avatar.Value, c.Phone.Present, c.Phone.Value)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("profile_unavailable")
	}
	return nil
}
func (p *PostgresProfiles) Preferences(ctx context.Context, id string, c Preferences, telegram bool) (bool, error) {
	tag, err := p.DB.Exec(ctx, `UPDATE notification_settings n SET email_login_alerts=coalesce($2,email_login_alerts),email_security_alerts=coalesce($3,email_security_alerts),telegram_login_alerts=coalesce($4,telegram_login_alerts),telegram_security_alerts=coalesce($5,telegram_security_alerts) FROM profiles p WHERE n.user_id=$1 AND p.user_id=n.user_id AND (NOT(coalesce($4,false) OR coalesce($5,false)) OR (p.telegram_chat_id IS NOT NULL AND $6))`, id, c.EL, c.ES, c.TL, c.TS, telegram)
	return tag.RowsAffected() == 1, err
}
