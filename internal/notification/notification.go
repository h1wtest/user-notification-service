package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/url"
	"platform-contracts/gen/go/events"
	"platform-contracts/gen/go/internalhttp"
	"strings"
	"time"
	"user-notification-service/internal/platform"
)

type Service struct {
	DB        *pgxpool.Pool
	Config    platform.Config
	Client    *platform.Client
	Consumer  *platform.Consumer
	Worker    bool
	Jobs      JobRepository
	Providers map[string]DeliveryProvider
}

func New(db *pgxpool.Pool, c platform.Config, client *platform.Client) *Service {
	s := &Service{DB: db, Config: c, Client: client, Jobs: &PostgresJobs{DB: db}}
	s.Providers = map[string]DeliveryProvider{"email": smtpProvider{Service: s}, "telegram": telegramProvider{Service: s}}
	return s
}
func Build(c platform.Config, db *pgxpool.Pool, client *platform.Client) (platform.App, error) {
	s := New(db, c, client)
	s.Worker = c.Kind == "worker"
	if !s.Worker {
		var err error
		s.Consumer, err = platform.NewConsumer(db, c, "notification-v1", []string{"identity.events.v1", "notification.commands.v1"}, s.Apply)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}
func (s *Service) Ready(ctx context.Context) error {
	if _, err := s.DB.Exec(ctx, "SELECT id FROM delivery_jobs LIMIT 0; SELECT id FROM delivery_attempts LIMIT 0; SELECT event_id FROM inbox LIMIT 0; SELECT id FROM quarantine LIMIT 0"); err != nil {
		return err
	}
	if s.Worker {
		return nil
	}
	if err := s.Client.DependencyReady(ctx, s.Config.IdentityURL); err != nil {
		return err
	}
	if err := s.Client.DependencyReady(ctx, s.Config.ProfileURL); err != nil {
		return err
	}
	return s.Consumer.Kafka.Ping(ctx)
}
func (s *Service) Run(ctx context.Context) error {
	if s.Worker {
		return s.Work(ctx)
	}
	return s.Consumer.Run(ctx)
}

type Body struct {
	To        string     `json:"to"`
	Subject   string     `json:"subject"`
	Text      string     `json:"body"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func (s *Service) Apply(ctx context.Context, e platform.Envelope) error {
	if err := platform.Validate(e); err != nil {
		return err
	}
	type planned struct {
		Channel string
		Body    Body
	}
	var jobs []planned
	if strings.HasPrefix(e.EventType, "notification.") {
		raw, err := platform.Decrypt(s.Config.DeliveryKey, e.EventID, e.EncryptedPayload)
		if err != nil || events.ValidatePayload(e.EventType, raw) != nil {
			return platform.ErrPoison
		}
		switch e.EventType {
		case "notification.email_verification":
			var p events.EmailVerification
			if json.Unmarshal(raw, &p) != nil || p.UserID != e.AggregateID {
				return platform.ErrPoison
			}
			jobs = append(jobs, planned{"email", Body{p.Email, "Verify your email", "Verification code: " + p.Code, &p.ExpiresAt}})
		case "notification.password_reset":
			var p events.PasswordReset
			if json.Unmarshal(raw, &p) != nil || p.UserID != e.AggregateID {
				return platform.ErrPoison
			}
			jobs = append(jobs, planned{"email", Body{p.Email, "Reset your password", "Reset your password: " + strings.TrimRight(s.Config.Origin, "/") + "/?token=" + url.QueryEscape(p.Token), &p.ExpiresAt}})
		default:
			return platform.ErrPoison
		}
	} else if e.EventType == "identity.user_logged_in" || e.EventType == "identity.password_changed" || e.EventType == "identity.mfa_changed" || e.EventType == "identity.session_revoked" || e.EventType == "identity.user_banned" || e.EventType == "identity.user_unbanned" {
		status, b, err := s.Client.Call(ctx, "GET", s.Config.ProfileURL+"/internal/v1/users/"+e.AggregateID+"/notification-settings", nil, nil)
		if err != nil || status != 200 {
			return errors.New("profile unavailable")
		}
		var p internalhttp.NotificationSettings
		if json.Unmarshal(b, &p) != nil {
			return errors.New("invalid profile response")
		}
		login := e.EventType == "identity.user_logged_in"
		subject := "Account security change"
		text := "Your account security state changed: " + e.EventType
		if login {
			var p events.UserLoggedIn
			if json.Unmarshal(e.Payload, &p) != nil || p.UserID != e.AggregateID {
				return platform.ErrPoison
			}
			subject = "New sign-in"
			text = fmt.Sprintf("Sign-in at %s\nDevice: %s\nIP: %s", p.LoggedInAt.UTC().Format(time.RFC3339), p.DeviceName, p.IPAddress)
		}
		if (login && p.EmailLoginAlerts) || (!login && p.EmailSecurityAlerts) {
			jobs = append(jobs, planned{"email", Body{To: p.Email, Subject: subject, Text: text}})
		}
		if p.TelegramChatID != nil && ((login && p.TelegramLoginAlerts) || (!login && p.TelegramSecurityAlerts)) {
			jobs = append(jobs, planned{"telegram", Body{To: *p.TelegramChatID, Subject: subject, Text: text}})
		}
	}
	intents := make([]Intent, 0, len(jobs))
	for _, job := range jobs {
		id := uuid.NewString()
		raw, err := json.Marshal(job.Body)
		if err != nil {
			return err
		}
		encrypted, err := platform.Encrypt(s.Config.DeliveryKey, id, raw)
		if err != nil {
			return err
		}
		intents = append(intents, Intent{ID: id, Channel: job.Channel, EncryptedBody: encrypted, MessageID: "<" + id + "@sso.local>"})
	}
	return s.Jobs.Accept(ctx, e, intents)
}
func (s *Service) Routes(m *http.ServeMux) {
	if s.Worker {
		return
	}
	m.HandleFunc("GET /api/v1/notifications", s.history)
	m.HandleFunc("GET /internal/v1/deliveries", s.internalList)
	m.HandleFunc("POST /internal/v1/deliveries/{id}/replay", s.replay)
	q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, Owner: "notification"}
	m.HandleFunc("GET /internal/v1/quarantine", q.List)
	m.HandleFunc("POST /internal/v1/quarantine/{id}/replay", q.Replay)
}
func (s *Service) list(w http.ResponseWriter, r *http.Request, user string) {
	rows, err := s.DB.Query(r.Context(), `SELECT id,user_id,channel,status,attempts,next_attempt_at,last_error,message_id,created_at,delivered_at,ambiguous FROM delivery_jobs WHERE ($1='' OR user_id::text=$1) ORDER BY created_at DESC LIMIT 200`, user)
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	defer rows.Close()
	out := internalhttp.DeliveriesResponse{}
	for rows.Next() {
		var id, uid, ch, status, msg string
		var attempts int
		var next, created time.Time
		var delivered *time.Time
		var last *string
		var ambiguous bool
		if err = rows.Scan(&id, &uid, &ch, &status, &attempts, &next, &last, &msg, &created, &delivered, &ambiguous); err != nil {
			platform.Error(w, 503, "database_unavailable")
			return
		}
		out = append(out, internalhttp.Delivery{ID: id, UserID: uid, Channel: ch, Status: status, Attempts: int64(attempts), NextAttemptAt: next, LastError: last, MessageID: msg, CreatedAt: created, DeliveredAt: delivered, Ambiguous: ambiguous})
	}
	if rows.Err() != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	platform.JSON(w, 200, out)
}
func (s *Service) history(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if ok {
		s.list(w, r, a.UserID)
	}
}
func (s *Service) admin(w http.ResponseWriter, r *http.Request, permission string) (platform.Actor, bool) {
	if !platform.ServiceAuthorized(r, s.Config.AdminToken) {
		platform.Error(w, 403, "service_denied")
		return platform.Actor{}, false
	}
	return s.Client.Authenticate(w, r, permission, true)
}
func (s *Service) internalList(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.admin(w, r, "deliveries:read"); ok {
		s.list(w, r, "")
	}
}
func (s *Service) replay(w http.ResponseWriter, r *http.Request) {
	a, ok := s.admin(w, r, "deliveries:replay")
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		platform.Error(w, 400, "invalid_delivery")
		return
	}
	tx, err := s.DB.Begin(r.Context())
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	tag, err := tx.Exec(r.Context(), "UPDATE delivery_jobs SET status='retry',attempts=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error=NULL WHERE id=$1 AND status='failed'", id)
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	if tag.RowsAffected() != 1 {
		platform.Error(w, 409, "delivery_not_failed")
		return
	}
	_, err = tx.Exec(r.Context(), "INSERT INTO replay_audit(delivery_id,actor_id) VALUES($1,$2)", id, a.UserID)
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	platform.JSON(w, 200, internalhttp.ReplayResponse{Status: "queued"})
}
