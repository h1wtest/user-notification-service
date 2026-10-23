package profile

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/url"
	"platform-contracts/gen/go/internalhttp"
	"strconv"
	"strings"
	"time"
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
	s.Consumer, err = platform.NewConsumer(db, c, "profile-v1", []string{"identity.events.v1"}, s.Apply)
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
	q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, Owner: "profile"}
	m.HandleFunc("GET /internal/v1/quarantine", q.List)
	m.HandleFunc("POST /internal/v1/quarantine/{id}/replay", q.Replay)
}
func (s *Service) response(ctx context.Context, id string) (map[string]any, error) {
	var email string
	var display, avatar, phone, chat *string
	var el, es, tl, ts bool
	err := s.DB.QueryRow(ctx, `SELECT p.email,p.display_name,p.avatar_url,p.phone_number,p.telegram_chat_id,n.email_login_alerts,n.email_security_alerts,n.telegram_login_alerts,n.telegram_security_alerts FROM profiles p JOIN notification_settings n USING(user_id) WHERE p.user_id=$1`, id).Scan(&email, &display, &avatar, &phone, &chat, &el, &es, &tl, &ts)
	if err != nil {
		return nil, err
	}
	return map[string]any{"profile": map[string]any{"user_id": id, "email": email, "display_name": display, "avatar_url": avatar, "phone_number": phone, "telegram_id": chat}, "notification_settings": map[string]bool{"email_login_alerts": el, "email_security_alerts": es, "telegram_login_alerts": tl, "telegram_security_alerts": ts}, "capabilities": map[string]bool{"telegram": s.Config.TelegramToken != "" && s.Config.TelegramWebhookSecret != ""}}, nil
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
func (s *Service) link(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if !ok {
		return
	}
	if s.Config.TelegramToken == "" || s.Config.TelegramWebhookSecret == "" {
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	result, err := s.telegram(r.Context(), "getMe", map[string]any{})
	var bot struct {
		Username string `json:"username"`
	}
	if err != nil || json.Unmarshal(result, &bot) != nil || bot.Username == "" {
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	token := make([]byte, 32)
	if _, err = rand.Read(token); err != nil {
		platform.Error(w, 503, "random_unavailable")
		return
	}
	encoded := base64.RawURLEncoding.EncodeToString(token)
	hash := sha256.Sum256([]byte(encoded))
	expires := time.Now().Add(10 * time.Minute)
	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	_, err = tx.Exec(r.Context(), "DELETE FROM telegram_links WHERE user_id=$1", a.UserID)
	if err == nil {
		_, err = tx.Exec(r.Context(), "INSERT INTO telegram_links(token_hash,user_id,expires_at) VALUES($1,$2,$3)", hash[:], a.UserID, expires)
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		platform.Error(w, 503, "profile_pending")
		return
	}
	platform.JSON(w, 200, map[string]any{"link_token": encoded, "expires_at": expires, "bot_url": "https://t.me/" + bot.Username + "?start=" + encoded})
}
func (s *Service) webhook(w http.ResponseWriter, r *http.Request) {
	r.Header.Set("X-Service-Token", r.Header.Get("X-Telegram-Bot-Api-Secret-Token"))
	if !platform.ServiceAuthorized(r, s.Config.TelegramWebhookSecret) || s.Config.TelegramToken == "" {
		platform.Error(w, 403, "service_denied")
		return
	}
	var u struct {
		UpdateID int64 `json:"update_id"`
		Message  *struct {
			Text string `json:"text"`
			From struct {
				ID  int64 `json:"id"`
				Bot bool  `json:"is_bot"`
			} `json:"from"`
			Chat struct {
				ID   int64  `json:"id"`
				Type string `json:"type"`
			} `json:"chat"`
		} `json:"message"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	if json.NewDecoder(r.Body).Decode(&u) != nil {
		platform.Error(w, 400, "invalid_update")
		return
	}
	if u.Message == nil || u.Message.Chat.Type != "private" || u.Message.From.Bot || u.Message.From.ID != u.Message.Chat.ID {
		platform.JSON(w, 200, map[string]bool{"ok": true})
		return
	}
	parts := strings.Fields(u.Message.Text)
	if len(parts) != 2 || parts[0] != "/start" {
		platform.JSON(w, 200, map[string]bool{"ok": true})
		return
	}
	hash := sha256.Sum256([]byte(parts[1]))
	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	tag, err := tx.Exec(r.Context(), "INSERT INTO telegram_updates(update_id) VALUES($1) ON CONFLICT DO NOTHING", u.UpdateID)
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	if tag.RowsAffected() == 0 {
		platform.JSON(w, 200, map[string]bool{"ok": true})
		return
	}
	var user string
	err = tx.QueryRow(r.Context(), "UPDATE telegram_links SET consumed_at=now() WHERE token_hash=$1 AND expires_at>now() AND consumed_at IS NULL RETURNING user_id", hash[:]).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		if err = tx.Commit(r.Context()); err != nil {
			platform.Error(w, 503, "database_unavailable")
			return
		}
		platform.JSON(w, 200, map[string]bool{"ok": true})
		return
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), "UPDATE profiles SET telegram_chat_id=$2,updated_at=now() WHERE user_id=$1", user, strconv.FormatInt(u.Message.Chat.ID, 10))
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		platform.Error(w, 409, "telegram_link_conflict")
		return
	}
	platform.JSON(w, 200, map[string]bool{"ok": true})
}
