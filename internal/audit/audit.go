package audit

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/url"
	"platform-contracts/gen/go/internalhttp"
	"strings"
	"time"
	"user-notification-service/internal/platform"
)

type Service struct {
	DB       *pgxpool.Pool
	Config   platform.Config
	Client   *platform.Client
	Consumer *platform.Consumer
	Audit    Repository
}

func New(c platform.Config, db *pgxpool.Pool, client *platform.Client) (platform.App, error) {
	s := &Service{DB: db, Config: c, Client: client, Audit: &PostgresAudit{DB: db}}
	var err error
	s.Consumer, err = platform.NewConsumer(db, c, "audit-v1", []string{"identity.events.v1"}, s.Apply)
	return s, err
}
func (s *Service) Ready(ctx context.Context) error {
	if _, err := s.DB.Exec(ctx, "SELECT id FROM audit_events LIMIT 0; SELECT id FROM admin_commands LIMIT 0; SELECT event_id FROM inbox LIMIT 0; SELECT id FROM quarantine LIMIT 0"); err != nil {
		return err
	}
	if err := s.Client.DependencyReady(ctx, s.Config.IdentityURL); err != nil {
		return err
	}
	if err := s.Client.DependencyReady(ctx, s.Config.NotificationURL); err != nil {
		return err
	}
	return s.Consumer.Kafka.Ping(ctx)
}
func (s *Service) Run(ctx context.Context) error { return s.Consumer.Run(ctx) }
func (s *Service) Apply(ctx context.Context, e platform.Envelope) error {
	if err := platform.Validate(e); err != nil {
		return err
	}
	if e.EncryptedPayload != "" {
		return platform.ErrPoison
	}
	return s.Audit.Project(ctx, e)
}
func (s *Service) Routes(m *http.ServeMux) {
	m.HandleFunc("GET /api/v1/admin/users", s.users)
	m.HandleFunc("POST /api/v1/admin/users/{id}/ban", s.ban)
	m.HandleFunc("POST /api/v1/admin/users/{id}/unban", s.unban)
	m.HandleFunc("GET /api/v1/admin/audit", s.audit)
	m.HandleFunc("GET /api/v1/admin/deliveries", s.deliveries)
	m.HandleFunc("POST /api/v1/admin/deliveries/{id}/replay", s.replay)
	m.HandleFunc("GET /api/v1/admin/quarantine", s.quarantine)
	m.HandleFunc("POST /api/v1/admin/quarantine/{id}/replay", s.quarantineReplay)
}
func (s *Service) proxy(w http.ResponseWriter, r *http.Request, method, target string, body any) {
	status, b, err := s.Client.Call(r.Context(), method, target, body, r)
	if err != nil || status >= 500 {
		platform.Error(w, 503, "dependency_unavailable")
		return
	}
	if !json.Valid(b) {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	if status == http.StatusOK {
		if strings.HasSuffix(target, "/deliveries") {
			var result internalhttp.DeliveriesResponse
			if json.Unmarshal(b, &result) != nil {
				platform.Error(w, 503, "invalid_dependency_response")
				return
			}
			platform.JSON(w, status, result)
			return
		}
		if strings.HasSuffix(target, "/quarantine") {
			var result internalhttp.QuarantineResponse
			if json.Unmarshal(b, &result) != nil {
				platform.Error(w, 503, "invalid_dependency_response")
				return
			}
			platform.JSON(w, status, result)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func (s *Service) users(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "users:read", true); !ok {
		return
	}
	query := r.URL.Query().Get("query")
	if len(query) > 255 {
		platform.Error(w, 400, "query_too_long")
		return
	}
	status, b, err := s.Client.Call(r.Context(), "GET", s.Config.IdentityURL+"/internal/v1/admin/users?query="+url.QueryEscape(query), nil, r)
	if err != nil || status >= 500 {
		platform.Error(w, 503, "identity_unavailable")
		return
	}
	if status != 200 {
		platform.Error(w, status, "access_denied")
		return
	}
	var result internalhttp.UsersResponse
	if json.Unmarshal(b, &result) != nil {
		platform.Error(w, 503, "invalid_identity_response")
		return
	}
	if result.Users == nil {
		result.Users = []internalhttp.UserSummary{}
	}
	platform.JSON(w, 200, result.Users)
}
func (s *Service) ban(w http.ResponseWriter, r *http.Request)   { s.command(w, r, "ban") }
func (s *Service) unban(w http.ResponseWriter, r *http.Request) { s.command(w, r, "unban") }
func (s *Service) command(w http.ResponseWriter, r *http.Request, action string) {
	a, ok := s.Client.Authenticate(w, r, "users:write", true)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		platform.Error(w, 400, "invalid_user_id")
		return
	}
	var p internalhttp.BanRequest
	if !platform.Decode(w, r, &p) {
		return
	}
	if (p.Reason != nil && len(*p.Reason) > 500) || (action == "ban" && (p.Reason == nil || *p.Reason == "")) {
		platform.Error(w, 400, "invalid_reason")
		return
	}
	commandID, err := s.Audit.Request(r.Context(), a.UserID, action, id)
	if err != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	status, b, err := s.Client.Call(r.Context(), "POST", s.Config.IdentityURL+"/internal/v1/admin/users/"+id+"/"+action, p, r)
	outcome := "unknown"
	if err == nil && status < 500 {
		if status >= 200 && status < 300 {
			outcome = "succeeded"
		} else {
			outcome = "denied"
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	saveErr := s.Audit.Complete(ctx, commandID, outcome)
	if err != nil || saveErr != nil || status >= 500 || !json.Valid(b) {
		platform.Error(w, 503, "command_outcome_unknown")
		return
	}
	if status >= 200 && status < 300 {
		var result internalhttp.BanResponse
		if json.Unmarshal(b, &result) != nil {
			platform.Error(w, 503, "invalid_identity_response")
			return
		}
		platform.JSON(w, status, result)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func (s *Service) audit(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "audit:read", true); !ok {
		return
	}
	rows, err := s.DB.Query(r.Context(), `SELECT id,event_type,actor_id,user_id,created_at FROM (SELECT id,event_type,actor_id,user_id,created_at FROM audit_events UNION ALL SELECT id,'admin.'||action||'.'||status,actor_id,target_id,created_at FROM admin_commands) all_events ORDER BY created_at DESC LIMIT 200`)
	if err != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, event, user string
		var actor *string
		var created time.Time
		if rows.Scan(&id, &event, &actor, &user, &created) != nil {
			platform.Error(w, 503, "audit_unavailable")
			return
		}
		out = append(out, map[string]any{"id": id, "event_type": event, "actor_id": actor, "user_id": user, "created_at": created})
	}
	if rows.Err() != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	platform.JSON(w, 200, out)
}
func (s *Service) deliveries(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "deliveries:read", true); !ok {
		return
	}
	s.proxy(w, r, "GET", s.Config.NotificationURL+"/internal/v1/deliveries", nil)
}
func (s *Service) replay(w http.ResponseWriter, r *http.Request) { s.replayProxy(w, r, "deliveries") }
func (s *Service) quarantine(w http.ResponseWriter, r *http.Request) {
	owner := r.URL.Query().Get("owner")
	if owner == "audit" {
		q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, Owner: "audit", Local: true}
		q.List(w, r)
		return
	}
	if _, ok := s.Client.Authenticate(w, r, "deliveries:read", true); !ok {
		return
	}
	base := s.Config.NotificationURL
	if owner == "profile" {
		base = s.Config.ProfileURL
	} else if owner != "" && owner != "notification" {
		platform.Error(w, 400, "invalid_owner")
		return
	}
	s.proxy(w, r, "GET", base+"/internal/v1/quarantine", nil)
}
func (s *Service) quarantineReplay(w http.ResponseWriter, r *http.Request) {
	owner := r.URL.Query().Get("owner")
	if owner == "audit" {
		q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, Owner: "audit", Local: true}
		q.Replay(w, r)
		return
	}
	s.replayProxy(w, r, "quarantine")
}
func (s *Service) replayProxy(w http.ResponseWriter, r *http.Request, kind string) {
	a, ok := s.Client.Authenticate(w, r, "deliveries:replay", true)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		platform.Error(w, 400, "invalid_id")
		return
	}
	base := s.Config.NotificationURL
	owner := r.URL.Query().Get("owner")
	if kind == "quarantine" && owner == "profile" {
		base = s.Config.ProfileURL
	} else if owner != "" && owner != "notification" {
		platform.Error(w, 400, "invalid_owner")
		return
	}
	var input internalhttp.ReplayRequest
	if !platform.Decode(w, r, &input) {
		return
	}
	var body any = internalhttp.ReplayRequest{}
	if kind == "quarantine" {
		body = input
	} else if len(input.CorrectedEvent) > 0 {
		platform.Error(w, 400, "invalid_request")
		return
	}
	commandID, requestErr := s.Audit.Request(r.Context(), a.UserID, kind+".replay", id)
	if requestErr != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	status, b, err := s.Client.Call(r.Context(), "POST", base+"/internal/v1/"+kind+"/"+id+"/replay", body, r)
	outcome := "unknown"
	if err == nil && status < 500 {
		outcome = "denied"
		if status < 300 {
			outcome = "succeeded"
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	saved := s.Audit.Complete(ctx, commandID, outcome)
	if err != nil || saved != nil || status >= 500 || !json.Valid(b) {
		platform.Error(w, 503, "command_outcome_unknown")
		return
	}
	if status >= 200 && status < 300 {
		var result internalhttp.ReplayResponse
		if json.Unmarshal(b, &result) != nil {
			platform.Error(w, 503, "invalid_dependency_response")
			return
		}
		platform.JSON(w, status, result)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
