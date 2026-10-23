package profile

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/url"
	"platform-contracts/gen/go/businessv1"
	"platform-contracts/gen/go/internalhttp"
	"strings"
	"user-notification-service/internal/platform"
)

type Service struct {
	DB       *pgxpool.Pool
	Config   platform.Config
	Client   *platform.Client
	Consumer *platform.Consumer
	Profiles Repository
}

func New(c platform.Config, db *pgxpool.Pool, client *platform.Client) (platform.App, error) {
	s := &Service{DB: db, Config: c, Client: client, Profiles: &PostgresProfiles{DB: db}}
	var err error
	s.Consumer, err = platform.NewConsumer(db, c, "profile-v1", []string{"identity.events.v1", "identity.events.v2"}, s.Apply)
	if err == nil {
		s.Consumer.ApplyV2 = s.ApplyV2
	}
	return s, err
}
func (s *Service) Ready(ctx context.Context) error {
	if _, err := s.DB.Exec(ctx, "SELECT user_id FROM profiles LIMIT 0; SELECT user_id FROM notification_settings LIMIT 0; SELECT event_id FROM inbox LIMIT 0; SELECT id FROM quarantine LIMIT 0"); err != nil {
		return err
	}
	if err := s.Client.DependencyReady(ctx, s.Config.IdentityURL); err != nil {
		return err
	}
	return s.Consumer.Kafka.Ping(ctx)
}
func (s *Service) Run(ctx context.Context) error { return s.Consumer.Run(ctx) }
func (s *Service) Apply(ctx context.Context, e platform.Envelope) error {
	if err := platform.Validate(e); err != nil {
		return err
	}
	return s.Profiles.Apply(ctx, e)
}
func (s *Service) Routes(m *http.ServeMux) {
	m.HandleFunc("GET /api/v1/users/me", s.me)
	m.HandleFunc("PATCH /api/v1/users/me", s.patch)
	m.HandleFunc("PATCH /api/v1/users/me/notifications", s.settings)
	m.HandleFunc("POST /api/v1/users/me/telegram/link", s.link)
	m.HandleFunc("POST /internal/v1/telegram/webhook", s.webhook)
	m.HandleFunc("GET /internal/v1/users/{id}/notification-settings", s.internalSettings)
	m.HandleFunc("GET /api/v1/users/me/telegram", s.telegramStatus)
	m.HandleFunc("POST /api/v1/users/me/telegram/confirm", s.confirmTelegram)
	m.HandleFunc("POST /api/v1/users/me/telegram/unlink", s.unlinkTelegram)
	m.HandleFunc("POST /api/v1/admin/users/{id}/telegram/reset", s.resetTelegram)
	m.HandleFunc("GET /api/v1/users/me/overview", s.overview)
	m.HandleFunc("GET /internal/v2/users/{id}/recipient-policy", s.recipientPolicy)
	m.HandleFunc("POST /internal/v1/external-events", s.externalEvent)
	q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, ApplyV2: s.ApplyV2, Owner: "profile"}
	m.HandleFunc("GET /internal/v1/quarantine", q.List)
	m.HandleFunc("POST /internal/v1/quarantine/{id}/replay", q.Replay)
}
func (s *Service) response(ctx context.Context, id string) (businessv1.ProfileResponse, error) {
	var out businessv1.ProfileResponse
	p := &out.Profile
	n := &out.NotificationSettings
	p.UserID = id
	err := s.DB.QueryRow(ctx, `SELECT p.email,p.display_name,p.avatar_url,p.phone_number,p.website_locale,p.notification_locale,p.telegram_chat_id IS NOT NULL,p.binding_version,n.email_login_alerts,n.email_security_alerts,n.telegram_login_alerts,n.telegram_security_alerts,n.email_subscription,n.telegram_subscription,n.telegram_trading FROM profiles p JOIN notification_settings n USING(user_id) WHERE p.user_id=$1`, id).Scan(&p.Email, &p.DisplayName, &p.AvatarURL, &p.PhoneNumber, &p.WebsiteLocale, &p.NotificationLocale, &p.TelegramLinked, &p.BindingVersion, &n.EmailLoginAlerts, &n.EmailSecurityAlerts, &n.TelegramLoginAlerts, &n.TelegramSecurityAlerts, &n.EmailSubscription, &n.TelegramSubscription, &n.TelegramTrading)
	out.Capabilities.Telegram = s.Config.TelegramToken != "" && s.Config.TelegramWebhookSecret != ""
	return out, err
}
func (s *Service) send(w http.ResponseWriter, r *http.Request, id string) {
	v, err := s.response(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		platform.Error(w, 503, "profile_pending")
		return
	}
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	platform.JSON(w, 200, v)
}
func (s *Service) me(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if ok {
		s.send(w, r, a.UserID)
	}
}
func (s *Service) patch(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if !ok {
		return
	}
	var p ProfileChanges
	if !platform.Decode(w, r, &p) {
		return
	}
	if err := p.Validate(); err != nil {
		platform.Error(w, 400, err.Error())
		return
	}
	if err := s.Profiles.Update(r.Context(), a.UserID, p); err != nil {
		platform.Error(w, 503, "profile_unavailable")
		return
	}
	s.send(w, r, a.UserID)
}
func (s *Service) settings(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if !ok {
		return
	}
	var p Preferences
	if !platform.Decode(w, r, &p) {
		return
	}
	updated, err := s.Profiles.Preferences(r.Context(), a.UserID, p, s.Config.TelegramToken != "" && s.Config.TelegramWebhookSecret != "")
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	if !updated {
		platform.Error(w, 409, "telegram_link_required")
		return
	}
	s.send(w, r, a.UserID)
}
func (s *Service) internalSettings(w http.ResponseWriter, r *http.Request) {
	if !platform.ServiceAuthorized(r, s.Config.NotificationToken) {
		platform.Error(w, 403, "service_denied")
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		platform.Error(w, 400, "invalid_user_id")
		return
	}
	var email string
	var chat *string
	var el, es, tl, ts bool
	err := s.DB.QueryRow(r.Context(), `SELECT p.email,p.telegram_chat_id,n.email_login_alerts,n.email_security_alerts,n.telegram_login_alerts,n.telegram_security_alerts FROM profiles p JOIN notification_settings n USING(user_id) WHERE p.user_id=$1`, id).Scan(&email, &chat, &el, &es, &tl, &ts)
	if err != nil {
		platform.Error(w, 503, "profile_pending")
		return
	}
	platform.JSON(w, 200, internalhttp.NotificationSettings{Email: email, TelegramChatID: chat, EmailLoginAlerts: el, EmailSecurityAlerts: es, TelegramLoginAlerts: tl, TelegramSecurityAlerts: ts})
}
func (s *Service) telegram(ctx context.Context, method string, body any) (json.RawMessage, error) {
	u := s.Config.TelegramBase + "/bot" + s.Config.TelegramToken + "/" + method
	parsed, err := url.Parse(u)
	if err != nil || parsed.Scheme != "https" {
		return nil, errors.New("Telegram HTTPS required")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(string(raw)))
	if err != nil {
		return nil, errors.New("Telegram configuration invalid")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client.HTTP.Do(req)
	if err != nil {
		return nil, errors.New("telegram unavailable")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if err != nil || resp.StatusCode != 200 {
		return nil, errors.New("telegram unavailable")
	}
	var response struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	if json.Unmarshal(b, &response) != nil || !response.OK {
		return nil, errors.New("telegram rejected")
	}
	return response.Result, nil
}
