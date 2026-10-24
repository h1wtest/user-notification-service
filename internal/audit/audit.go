package audit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"platform-contracts/gen/go/internalhttp"
	"platform-contracts/gen/go/internalhttpv2"
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
	s.Consumer, err = platform.NewConsumer(db, c, "audit-v1", []string{"identity.events.v1", "identity.events.v2", "profile.events.v2", "catalog.events.v1", "billing.events.v1", "subscription.events.v1", "referral.events.v1", "external.events.v1", "notification.events.v2"}, s.Apply)
	if err == nil {
		s.Consumer.ApplyV2 = s.ApplyV2
	}
	return s, err
}
func (s *Service) Ready(ctx context.Context) error {
	if _, err := s.DB.Exec(ctx, "SELECT id,reason,idempotency_key,updated_at,result FROM admin_commands LIMIT 0; SELECT id,aggregate_id,aggregate_type,aggregate_version,producer FROM audit_events LIMIT 0; SELECT producer,aggregate_type,aggregate_id,version FROM audit_aggregate_versions LIMIT 0; SELECT event_id FROM audit_v2_receipts LIMIT 0; SELECT event_id FROM inbox LIMIT 0; SELECT id FROM quarantine LIMIT 0"); err != nil {
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
	m.HandleFunc("GET /api/v1/admin/users/{id}", s.user)
	m.HandleFunc("POST /api/v1/admin/users/{id}/ban", s.ban)
	m.HandleFunc("POST /api/v1/admin/users/{id}/unban", s.unban)
	m.HandleFunc("POST /api/v1/admin/users/{id}/revoke-sessions", s.revokeSessions)
	m.HandleFunc("GET /api/v1/admin/commands/{id}", s.commandStatus)
	m.HandleFunc("POST /api/v1/admin/commands/{id}/resume", s.commandResume)
	m.HandleFunc("GET /api/v1/admin/audit", s.audit)
	m.HandleFunc("GET /api/v1/admin/deliveries", s.deliveries)
	m.HandleFunc("POST /api/v1/admin/deliveries/{id}/replay", s.replay)
	m.HandleFunc("GET /api/v1/admin/templates", s.templates)
	m.HandleFunc("PUT /api/v1/admin/templates/{event_type}", s.publishTemplate)
	m.HandleFunc("POST /api/v1/admin/templates/{event_type}/preview", s.previewTemplate)
	m.HandleFunc("GET /api/v1/admin/quarantine", s.quarantine)
	m.HandleFunc("POST /api/v1/admin/quarantine/{id}/replay", s.quarantineReplay)
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
	status, raw, err := s.Client.Call(r.Context(), "GET", s.Config.IdentityURL+"/internal/v2/admin/users?query="+url.QueryEscape(query), nil, r)
	if !dependencyStatus(w, status, err, "identity_unavailable") {
		return
	}
	var result internalhttpv2.UsersResponse
	if json.Unmarshal(raw, &result) != nil {
		platform.Error(w, 503, "invalid_identity_response")
		return
	}
	if result.Users == nil {
		result.Users = []internalhttpv2.UserSummary{}
	}
	platform.JSON(w, 200, result.Users)
}
func (s *Service) user(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "users:read", true); !ok {
		return
	}
	id, ok := pathUUID(w, r, "invalid_user_id")
	if !ok {
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "GET", s.Config.IdentityURL+"/internal/v2/admin/users/"+id, nil, r)
	if !dependencyStatus(w, status, err, "identity_unavailable") {
		return
	}
	var result internalhttpv2.UserSummary
	if json.Unmarshal(raw, &result) != nil || result.ID != id {
		platform.Error(w, 503, "invalid_identity_response")
		return
	}
	platform.JSON(w, 200, result)
}
func dependencyStatus(w http.ResponseWriter, status int, err error, unavailable string, raw ...[]byte) bool {
	if err != nil || status >= 500 || status < 200 || status >= 300 && status < 400 {
		platform.Error(w, 503, unavailable)
		return false
	}
	if status != 200 {
		code := "access_denied"
		if status == 404 {
			code = "not_found"
		}
		if status == 409 {
			code = "version_conflict"
		}
		if status == 400 {
			code = "invalid_request"
		}
		if len(raw) > 0 {
			var owner internalhttpv2.ErrorResponse
			if json.Unmarshal(raw[0], &owner) == nil {
				switch owner.Code {
				case "invalid_template", "unknown_template_variable", "unsafe_template_url", "version_conflict", "delivery_not_failed", "delivery_not_found", "not_replayable", "invalid_reason", "record_still_invalid", "record_identity_mismatch", "record_topic_mismatch", "quarantine_not_found", "record_not_recoverable":
					code = owner.Code
				}
			}
		}
		platform.Error(w, status, code)
		return false
	}
	return true
}
func (s *Service) audit(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "audit:read", true); !ok {
		return
	}
	page, err := parsePage(r)
	if err != nil {
		platform.Error(w, 400, err.Error())
		return
	}
	items, err := s.Audit.List(r.Context(), page)
	if err != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	var next *string
	if len(items) > page.Limit {
		items = items[:page.Limit]
		last := items[len(items)-1]
		cursor := encodeCursor(last.CreatedAt, last.ID)
		next = &cursor
	}
	if items == nil {
		items = []AuditEvent{}
	}
	platform.JSON(w, 200, struct {
		Items      []AuditEvent `json:"items"`
		NextCursor *string      `json:"next_cursor"`
	}{items, next})
}
func (s *Service) deliveries(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:read", true); !ok {
		return
	}
	status, raw, headers, err := s.Client.CallWithHeaders(r.Context(), "GET", s.Config.NotificationURL+"/internal/v1/deliveries"+forwardQuery(r), nil, r)
	if !dependencyStatus(w, status, err, "notification_unavailable") {
		return
	}
	var result internalhttp.DeliveriesResponse
	if json.Unmarshal(raw, &result) != nil {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	if result == nil {
		result = internalhttp.DeliveriesResponse{}
	}
	if cursor := headers.Get("X-Next-Cursor"); cursor != "" {
		w.Header().Set("X-Next-Cursor", cursor)
	}
	platform.JSON(w, 200, result)
}
func forwardQuery(r *http.Request) string {
	values := url.Values{}
	for _, key := range []string{"cursor", "limit"} {
		if value := r.URL.Query().Get(key); value != "" {
			values.Set(key, value)
		}
	}
	if len(values) == 0 {
		return ""
	}
	return "?" + values.Encode()
}
func (s *Service) replay(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:replay", true); !ok {
		return
	}
	id, ok := pathUUID(w, r, "invalid_id")
	if !ok {
		return
	}
	var input internalhttpv2.CommandRequest
	if !platform.Decode(w, r, &input) {
		return
	}
	input.Reason = strings.TrimSpace(input.Reason)
	if input.Reason == "" || len(input.Reason) > 500 {
		platform.Error(w, 400, "invalid_reason")
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "POST", s.Config.NotificationURL+"/internal/v1/deliveries/"+id+"/replay", input, r)
	if !dependencyStatus(w, status, err, "notification_unavailable", raw) {
		return
	}
	var result internalhttpv2.ReplayResponse
	if json.Unmarshal(raw, &result) != nil || result.Status != "queued" {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	platform.JSON(w, 200, result)
}
func (s *Service) templates(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:read", true); !ok {
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "GET", s.Config.NotificationURL+"/internal/v2/templates", nil, r)
	if !dependencyStatus(w, status, err, "notification_unavailable") {
		return
	}
	var result internalhttpv2.TemplatesResponse
	if json.Unmarshal(raw, &result) != nil {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	platform.JSON(w, 200, result)
}
func (s *Service) publishTemplate(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:templates:write", true); !ok {
		return
	}
	eventType := r.PathValue("event_type")
	if eventType == "" || len(eventType) > 100 {
		platform.Error(w, 400, "invalid_event_type")
		return
	}
	var input internalhttpv2.TemplatePublish
	if !platform.Decode(w, r, &input) {
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "PUT", s.Config.NotificationURL+"/internal/v2/templates/"+url.PathEscape(eventType), input, r)
	if !dependencyStatus(w, status, err, "notification_unavailable", raw) {
		return
	}
	var result internalhttpv2.TemplateRevision
	if json.Unmarshal(raw, &result) != nil || result.EventType != eventType {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	platform.JSON(w, 200, result)
}
func (s *Service) previewTemplate(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:templates:write", true); !ok {
		return
	}
	eventType := r.PathValue("event_type")
	if eventType == "" || len(eventType) > 100 {
		platform.Error(w, 400, "invalid_event_type")
		return
	}
	var input internalhttpv2.TemplatePreviewRequest
	if !platform.Decode(w, r, &input) {
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "POST", s.Config.NotificationURL+"/internal/v2/templates/"+url.PathEscape(eventType)+"/preview", input, r)
	if !dependencyStatus(w, status, err, "notification_unavailable", raw) {
		return
	}
	var result internalhttpv2.TemplatePreviewResponse
	if json.Unmarshal(raw, &result) != nil {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	platform.JSON(w, 200, result)
}
func (s *Service) quarantine(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:read", true); !ok {
		return
	}
	owner := r.URL.Query().Get("owner")
	if owner == "audit" {
		q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, ApplyV2: s.ApplyV2, Owner: "audit", Local: true}
		q.List(w, r)
		return
	}
	base, ok := s.quarantineOwner(w, owner)
	if !ok {
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "GET", base+"/internal/v1/quarantine", nil, r)
	if !dependencyStatus(w, status, err, "dependency_unavailable") {
		return
	}
	var result internalhttp.QuarantineResponse
	if json.Unmarshal(raw, &result) != nil {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	if result == nil {
		result = internalhttp.QuarantineResponse{}
	}
	platform.JSON(w, 200, result)
}
func (s *Service) quarantineOwner(w http.ResponseWriter, owner string) (string, bool) {
	switch owner {
	case "", "notification":
		return s.Config.NotificationURL, true
	case "profile":
		return s.Config.ProfileURL, true
	default:
		platform.Error(w, 400, "invalid_owner")
		return "", false
	}
}
func (s *Service) quarantineReplay(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "notifications:replay", true); !ok {
		return
	}
	if _, ok := pathUUID(w, r, "invalid_id"); !ok {
		return
	}
	owner := r.URL.Query().Get("owner")
	if owner == "audit" {
		q := platform.QuarantineAPI{DB: s.DB, Config: s.Config, Client: s.Client, Apply: s.Apply, ApplyV2: s.ApplyV2, Owner: "audit", Local: true}
		q.Replay(w, r)
		return
	}
	base, ok := s.quarantineOwner(w, owner)
	if !ok {
		return
	}
	var input internalhttpv2.QuarantineReplayRequest
	if !platform.Decode(w, r, &input) {
		return
	}
	input.Reason = strings.TrimSpace(input.Reason)
	if input.Reason == "" || len(input.Reason) > 500 {
		platform.Error(w, 400, "invalid_reason")
		return
	}
	status, raw, err := s.Client.Call(r.Context(), "POST", base+"/internal/v1/quarantine/"+r.PathValue("id")+"/replay", input, r)
	if !dependencyStatus(w, status, err, "dependency_unavailable", raw) {
		return
	}
	var result internalhttpv2.ReplayResponse
	if json.Unmarshal(raw, &result) != nil || result.Status != "replayed" {
		platform.Error(w, 503, "invalid_dependency_response")
		return
	}
	platform.JSON(w, 200, result)
}
