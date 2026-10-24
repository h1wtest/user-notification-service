package notification

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/url"
	"platform-contracts/gen/go/events"
	"platform-contracts/gen/go/eventsv2"
	"platform-contracts/gen/go/internalhttp"
	"platform-contracts/gen/go/internalhttpv2"
	"strconv"
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
		s.Consumer, err = platform.NewConsumer(db, c, "notification-v1", []string{"identity.events.v1", "notification.commands.v1", "identity.events.v2", "notification.commands.v2", "billing.events.v1", "subscription.events.v1", "referral.events.v1", "external.events.v1"}, s.Apply)
		if err != nil {
			return nil, err
		}
		s.Consumer.ApplyV2 = s.ApplyV2
	}
	return s, nil
}
func (s *Service) Ready(ctx context.Context) error {
	if _, err := s.DB.Exec(ctx, "SELECT template_version,replay_version FROM delivery_jobs LIMIT 0; SELECT id FROM delivery_attempts LIMIT 0; SELECT event_id FROM inbox LIMIT 0; SELECT id FROM quarantine LIMIT 0; SELECT version FROM notification_templates LIMIT 0; SELECT id FROM notification_reminders LIMIT 0"); err != nil {
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
	To                  string     `json:"to"`
	Subject             string     `json:"subject"`
	Text                string     `json:"body"`
	ExpiresAt           *time.Time `json:"expires_at,omitempty"`
	UserID              string     `json:"user_id,omitempty"`
	EventType           string     `json:"event_type,omitempty"`
	Category            string     `json:"category,omitempty"`
	BindingVersion      int64      `json:"binding_version,omitempty"`
	FlowEventID         string     `json:"flow_event_id,omitempty"`
	RequiredPermission  string     `json:"required_permission,omitempty"`
	SubscriptionID      string     `json:"subscription_id,omitempty"`
	SubscriptionVersion int64      `json:"subscription_version,omitempty"`
	PeriodEnd           *time.Time `json:"period_end,omitempty"`
}

func (s *Service) Apply(ctx context.Context, e platform.Envelope) error {
	if err := platform.Validate(e); err != nil {
		return err
	}
	p := deliveryPlan{UserID: e.AggregateID, Category: eventCategory(e.EventType), Vars: map[string]string{"occurred_at": e.OccurredAt.UTC().Format(time.RFC3339)}}
	if strings.HasPrefix(e.EventType, "notification.") {
		raw, err := platform.Decrypt(s.Config.DeliveryKey, e.EventID, e.EncryptedPayload)
		if err != nil || events.ValidatePayload(e.EventType, raw) != nil {
			return platform.ErrPoison
		}
		switch e.EventType {
		case "notification.email_verification":
			var v events.EmailVerification
			if json.Unmarshal(raw, &v) != nil || v.UserID != e.AggregateID {
				return platform.ErrPoison
			}
			p.Target = v.Email
			p.ExpiresAt = &v.ExpiresAt
			p.Vars["code"] = v.Code
			p.Vars["verification_url"] = strings.TrimRight(s.Config.Origin, "/") + "/?user_id=" + url.QueryEscape(v.UserID)
			p.Vars["expires_at"] = v.ExpiresAt.UTC().Format(time.RFC3339)
		case "notification.password_reset":
			var v events.PasswordReset
			if json.Unmarshal(raw, &v) != nil || v.UserID != e.AggregateID {
				return platform.ErrPoison
			}
			p.Target = v.Email
			p.ExpiresAt = &v.ExpiresAt
			p.Vars["reset_url"] = strings.TrimRight(s.Config.Origin, "/") + "/?token=" + url.QueryEscape(v.Token)
			p.Vars["expires_at"] = v.ExpiresAt.UTC().Format(time.RFC3339)
		default:
			return platform.ErrPoison
		}
	} else if e.EventType == "identity.user_logged_in" {
		var v events.UserLoggedIn
		if json.Unmarshal(e.Payload, &v) != nil || v.UserID != e.AggregateID {
			return platform.ErrPoison
		}
		p.Vars["device_name"] = v.DeviceName
		p.Vars["ip_address"] = v.IPAddress
		p.Vars["occurred_at"] = v.LoggedInAt.UTC().Format(time.RFC3339)
	} else if _, ok := templateVariables[e.EventType]; !ok {
		return s.Jobs.Accept(ctx, e, nil)
	}
	intents, err := s.planIntents(ctx, e.EventID, e.EventType, p)
	if err != nil {
		return err
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
	m.HandleFunc("GET /internal/v2/templates", s.templates)
	m.HandleFunc("PUT /internal/v2/templates/{event_type}", s.putTemplate)
	m.HandleFunc("POST /internal/v2/templates/{event_type}/preview", s.previewTemplate)
	q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, ApplyV2: s.ApplyV2, Owner: "notification"}
	m.HandleFunc("GET /internal/v1/quarantine", q.List)
	m.HandleFunc("POST /internal/v1/quarantine/{id}/replay", q.Replay)
}
func (s *Service) list(w http.ResponseWriter, r *http.Request, user string) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			platform.Error(w, 400, "invalid_limit")
			return
		}
		limit = n
	}
	var before *time.Time
	var beforeID *string
	if values, exists := r.URL.Query()["cursor"]; exists {
		if len(values) != 1 {
			platform.Error(w, 400, "invalid_cursor")
			return
		}
		created, id, err := parseDeliveryCursor(values[0])
		if err != nil {
			platform.Error(w, 400, "invalid_cursor")
			return
		}
		before, beforeID = &created, &id
	}
	rows, err := s.DB.Query(r.Context(), `SELECT id,user_id,channel,status,attempts,next_attempt_at,last_error,message_id,created_at,delivered_at,ambiguous FROM delivery_jobs WHERE ($1='' OR user_id::text=$1) AND ($2::timestamptz IS NULL OR (created_at,id)<($2::timestamptz,$3::uuid)) ORDER BY created_at DESC,id DESC LIMIT $4`, user, before, beforeID, limit+1)
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
		if delivered != nil {
			utc := delivered.UTC()
			delivered = &utc
		}
		out = append(out, internalhttp.Delivery{ID: id, UserID: uid, Channel: ch, Status: status, Attempts: int64(attempts), NextAttemptAt: next.UTC(), LastError: last, MessageID: msg, CreatedAt: created.UTC(), DeliveredAt: delivered, Ambiguous: ambiguous})
	}
	if rows.Err() != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		w.Header().Set("X-Next-Cursor", encodeDeliveryCursor(last.CreatedAt, last.ID))
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
	if _, ok := s.admin(w, r, "notifications:read"); ok {
		s.list(w, r, "")
	}
}
func (s *Service) replay(w http.ResponseWriter, r *http.Request) {
	a, ok := s.admin(w, r, "notifications:replay")
	if !ok {
		return
	}
	var input internalhttpv2.CommandRequest
	if !platform.Decode(w, r, &input) {
		return
	}
	input.Reason = strings.TrimSpace(input.Reason)
	if input.Reason == "" || len(input.Reason) > 500 {
		platform.Error(w, 400, "reason_required")
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
	var recipient string
	var replayVersion int64
	err = tx.QueryRow(r.Context(), "UPDATE delivery_jobs SET status='retry',attempts=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error=NULL,replay_version=replay_version+1 WHERE id=$1 AND status='failed' RETURNING user_id::text,replay_version", id).Scan(&recipient, &replayVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		platform.Error(w, 409, "delivery_not_failed")
		return
	}
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	_, err = tx.Exec(r.Context(), "INSERT INTO replay_audit(delivery_id,actor_id,reason) VALUES($1,$2,$3)", id, a.UserID, input.Reason)
	if err == nil {
		payload, encodeErr := json.Marshal(eventsv2.DeliveryReplayed{DeliveryID: id, UserID: recipient, ActorID: a.UserID, Reason: input.Reason})
		if encodeErr != nil {
			err = encodeErr
		} else {
			eventID, idErr := uuid.NewV7()
			if idErr != nil {
				err = idErr
			} else {
				err = platform.EnqueueV2(r.Context(), tx, eventsv2.Envelope{SchemaVersion: 2, EventID: eventID.String(), EventType: "notification.delivery_replayed", Producer: "notification", AggregateType: "delivery", AggregateID: id, AggregateVersion: replayVersion, OccurredAt: time.Now().UTC(), Payload: payload})
			}
		}
	}
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		platform.Error(w, 503, "database_unavailable")
		return
	}
	platform.JSON(w, 200, internalhttp.ReplayResponse{Status: "queued"})
}
