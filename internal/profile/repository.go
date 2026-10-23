package profile

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/url"
	"platform-contracts/gen/go/businessv1"
	"platform-contracts/gen/go/eventsv2"
	"time"

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
	Display            NullableString `json:"display_name"`
	Avatar             NullableString `json:"avatar_url"`
	Phone              NullableString `json:"phone_number"`
	WebsiteLocale      NullableString `json:"website_locale"`
	NotificationLocale NullableString `json:"notification_locale"`
}
type Preferences struct {
	EL                   *bool `json:"email_login_alerts"`
	ES                   *bool `json:"email_security_alerts"`
	TL                   *bool `json:"telegram_login_alerts"`
	TS                   *bool `json:"telegram_security_alerts"`
	EmailSubscription    *bool `json:"email_subscription"`
	TelegramSubscription *bool `json:"telegram_subscription"`
	TelegramTrading      *bool `json:"telegram_trading"`
}
type Repository interface {
	Apply(context.Context, platform.Envelope) error
	Update(context.Context, string, ProfileChanges) error
	Preferences(context.Context, string, Preferences, bool) (bool, error)
}
type PostgresProfiles struct{ DB *pgxpool.Pool }

func (p ProfileChanges) Validate() error {
	for _, value := range []NullableString{p.WebsiteLocale, p.NotificationLocale} {
		if value.Present && value.Value != nil && *value.Value != "en" && *value.Value != "ru" {
			return errors.New("invalid_locale")
		}
	}
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
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, "UPDATE profiles SET display_name=CASE WHEN $2 THEN $3::text ELSE display_name END,avatar_url=CASE WHEN $4 THEN $5::text ELSE avatar_url END,phone_number=CASE WHEN $6 THEN $7::text ELSE phone_number END,website_locale=CASE WHEN $8 THEN coalesce($9::text,'en') ELSE website_locale END,notification_locale=CASE WHEN $10 THEN coalesce($11::text,'en') ELSE notification_locale END,version=version+1,updated_at=now() WHERE user_id=$1", id, c.Display.Present, c.Display.Value, c.Avatar.Present, c.Avatar.Value, c.Phone.Present, c.Phone.Value, c.WebsiteLocale.Present, c.WebsiteLocale.Value, c.NotificationLocale.Present, c.NotificationLocale.Value)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("profile_unavailable")
	}
	var fact eventsv2.ProfileUpdated
	fact.UserID = id
	if err = tx.QueryRow(ctx, "SELECT website_locale,notification_locale,version FROM profiles WHERE user_id=$1", id).Scan(&fact.WebsiteLocale, &fact.NotificationLocale, &fact.Version); err != nil {
		return err
	}
	raw, err := json.Marshal(fact)
	if err != nil {
		return err
	}
	eventID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err = platform.EnqueueV2(ctx, tx, eventsv2.Envelope{SchemaVersion: 2, EventID: eventID.String(), EventType: "profile.updated", Producer: "profile", AggregateType: "user", AggregateID: id, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: raw}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (p *PostgresProfiles) Preferences(ctx context.Context, id string, c Preferences, telegram bool) (bool, error) {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// Profile is the common serialization point for preferences, unlink and
	// support reset. Keep profile -> settings lock order; never evaluate a
	// joined binding snapshot after waiting on an already-cleared settings row.
	var chat *string
	if err = tx.QueryRow(ctx, "SELECT telegram_chat_id FROM profiles WHERE user_id=$1 FOR UPDATE", id).Scan(&chat); err != nil {
		return false, err
	}
	enablesTelegram := (c.TL != nil && *c.TL) || (c.TS != nil && *c.TS) || (c.TelegramSubscription != nil && *c.TelegramSubscription) || (c.TelegramTrading != nil && *c.TelegramTrading)
	if enablesTelegram && (chat == nil || !telegram) {
		return false, nil
	}
	tag, err := tx.Exec(ctx, `UPDATE notification_settings SET email_login_alerts=coalesce($2,email_login_alerts),email_security_alerts=coalesce($3,email_security_alerts),telegram_login_alerts=coalesce($4,telegram_login_alerts),telegram_security_alerts=coalesce($5,telegram_security_alerts),email_subscription=coalesce($6,email_subscription),telegram_subscription=coalesce($7,telegram_subscription),telegram_trading=coalesce($8,telegram_trading) WHERE user_id=$1`, id, c.EL, c.ES, c.TL, c.TS, c.EmailSubscription, c.TelegramSubscription, c.TelegramTrading)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (p *ProfileChanges) UnmarshalJSON(raw []byte) error {
	if err := businessv1.Validate("ProfileChanges", raw); err != nil {
		return err
	}
	type changes ProfileChanges
	var value changes
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*p = ProfileChanges(value)
	return nil
}
func (p *Preferences) UnmarshalJSON(raw []byte) error {
	if err := businessv1.Validate("PreferencesChanges", raw); err != nil {
		return err
	}
	type changes Preferences
	var value changes
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*p = Preferences(value)
	return nil
}

func locale(value string) string {
	if value == "ru" {
		return "ru"
	}
	return "en"
}
