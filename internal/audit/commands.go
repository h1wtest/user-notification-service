// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"platform-contracts/gen/go/internalhttpv2"
	"user-notification-service/internal/platform"
)

func (s *Service) ban(w http.ResponseWriter, r *http.Request)   { s.command(w, r, "ban") }
func (s *Service) unban(w http.ResponseWriter, r *http.Request) { s.command(w, r, "unban") }
func (s *Service) revokeSessions(w http.ResponseWriter, r *http.Request) {
	s.command(w, r, "revoke-sessions")
}

func (s *Service) command(w http.ResponseWriter, r *http.Request, action string) {
	permission := "users:write"
	if action == "revoke-sessions" {
		permission = "users:sessions:revoke"
	}
	actor, ok := s.Client.Authenticate(w, r, permission, true)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "invalid_user_id")
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
	key := r.Header.Get("Idempotency-Key")
	if input.IdempotencyKey != nil {
		if key != "" && key != *input.IdempotencyKey {
			platform.Error(w, 400, "idempotency_key_mismatch")
			return
		}
		key = *input.IdempotencyKey
	}
	if key == "" {
		key = uuid.NewString()
	}
	if strings.TrimSpace(key) != key || key == "" || len(key) > 200 {
		platform.Error(w, 400, "invalid_idempotency_key")
		return
	}
	command, fresh, err := s.Audit.Admit(r.Context(), actor.UserID, action, id, input.Reason, key)
	if errors.Is(err, ErrIdempotencyConflict) {
		platform.Error(w, 409, "idempotency_conflict")
		return
	}
	if err != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	w.Header().Set("Idempotency-Key", key)
	if !fresh {
		command, ok = s.recoverCommand(w, r, command, true)
		if !ok {
			return
		}
	} else {
		command = s.dispatchCommand(r, command)
	}
	writeCommand(w, command, false)
}

func moderationOutcome(command Command, status int, raw []byte, callErr error) (string, json.RawMessage) {
	if callErr != nil || status >= 500 {
		return "unknown", nil
	}
	if status >= 400 && status < 500 {
		var denied internalhttpv2.ErrorResponse
		if status != http.StatusRequestTimeout && json.Unmarshal(raw, &denied) == nil {
			return "denied", nil
		}
		return "unknown", nil
	}
	if status != http.StatusOK {
		return "unknown", nil
	}
	// Owner may return its persisted command record directly. The identity and
	// semantic result must match, not merely an arbitrary JSON 2xx body.
	var recorded internalhttpv2.CommandStatus
	if json.Unmarshal(raw, &recorded) == nil && recorded.CommandID == command.ID {
		if recorded.Status == "denied" {
			return "denied", nil
		}
		if recorded.Status == "succeeded" && validCommandResult(command, recorded.Result) {
			result, _ := json.Marshal(recorded.Result)
			return "succeeded", result
		}
		return "unknown", nil
	}
	if command.Action == "revoke-sessions" {
		var result internalhttpv2.CommandResult
		if json.Unmarshal(raw, &result) != nil || !validCommandResult(command, &result) {
			return "unknown", nil
		}
		safe, _ := json.Marshal(result)
		return "succeeded", safe
	}
	var banned internalhttpv2.BanResponse
	if json.Unmarshal(raw, &banned) != nil || banned.UserID != command.TargetID || banned.IsBanned != (command.Action == "ban") {
		return "unknown", nil
	}
	result := internalhttpv2.CommandResult{UserID: &banned.UserID, IsBanned: &banned.IsBanned}
	safe, _ := json.Marshal(result)
	return "succeeded", safe
}
func validCommandResult(command Command, result *internalhttpv2.CommandResult) bool {
	if result == nil {
		return false
	}
	if result.UserID != nil && *result.UserID != command.TargetID {
		return false
	}
	if command.Action == "revoke-sessions" {
		return result.Revoked != nil && *result.Revoked >= 0
	}
	return result.UserID != nil && result.IsBanned != nil && *result.IsBanned == (command.Action == "ban")
}

func (s *Service) commandStatus(w http.ResponseWriter, r *http.Request) {
	if !s.authenticateCommandLookup(w, r) {
		return
	}
	id, ok := pathUUID(w, r, "invalid_command_id")
	if !ok {
		return
	}
	command, err := s.Audit.Lookup(r.Context(), id)
	if errors.Is(err, ErrCommandNotFound) {
		platform.Error(w, 404, "command_not_found")
		return
	}
	if err != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	command, ok = s.recoverCommand(w, r, command, false)
	if !ok {
		return
	}
	writeCommand(w, command, true)
}

func (s *Service) commandResume(w http.ResponseWriter, r *http.Request) {
	if !s.authenticateCommandLookup(w, r) {
		return
	}
	id, ok := pathUUID(w, r, "invalid_command_id")
	if !ok {
		return
	}
	var input internalhttpv2.EmptyRequest
	if !platform.Decode(w, r, &input) {
		return
	}
	command, err := s.Audit.Lookup(r.Context(), id)
	if errors.Is(err, ErrCommandNotFound) {
		platform.Error(w, 404, "command_not_found")
		return
	}
	if err != nil {
		platform.Error(w, 503, "audit_unavailable")
		return
	}
	command, ok = s.recoverCommand(w, r, command, true)
	if !ok {
		return
	}
	writeCommand(w, command, false)
}

func commandPermission(action string) string {
	switch action {
	case "revoke-sessions":
		return "users:sessions:revoke"
	case "ban", "unban":
		return "users:write"
	default:
		return ""
	}
}

func (s *Service) authenticateCommandLookup(w http.ResponseWriter, r *http.Request) bool {
	actor, ok := s.Client.Authenticate(w, r, "", true)
	if !ok {
		return false
	}
	// Before revealing even absence, require a current command permission.
	// The persisted action and actor are checked separately by recoverCommand.
	for _, permission := range actor.Permissions {
		if permission == "users:write" || permission == "users:sessions:revoke" {
			return true
		}
	}
	platform.Error(w, 403, "permission_denied")
	return false
}

func (s *Service) dispatchCommand(r *http.Request, command Command) Command {
	input := internalhttpv2.CommandRequest{Reason: command.Reason, IdempotencyKey: &command.ID}
	path := command.Action
	if path == "revoke-sessions" {
		path = "sessions/revoke"
	}
	status, raw, callErr := s.Client.Call(r.Context(), "POST", s.Config.IdentityURL+"/internal/v2/admin/users/"+command.TargetID+"/"+path, input, r)
	outcome, result := moderationOutcome(command, status, raw, callErr)
	return s.saveCommandOutcome(command, outcome, result)
}

func (s *Service) saveCommandOutcome(command Command, status string, result json.RawMessage) Command {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if s.Audit.Finish(ctx, command.ID, status, result) != nil {
		command.Status = "unknown"
		command.Result = nil
		return command
	}
	// Concurrent reconcilers may already have committed a terminal outcome.
	// Read the persisted monotonic result rather than echoing our stale guess.
	if persisted, err := s.Audit.Lookup(ctx, command.ID); err == nil {
		return persisted
	}
	command.Status = "unknown"
	command.Result = nil
	return command
}

func (s *Service) recoverCommand(w http.ResponseWriter, r *http.Request, command Command, allowResume bool) (Command, bool) {
	permission := commandPermission(command.Action)
	if permission == "" {
		platform.Error(w, 403, "permission_denied")
		return command, false
	}
	actor, ok := s.Client.Authenticate(w, r, permission, true)
	if !ok {
		return command, false
	}
	if actor.UserID != command.ActorID {
		platform.Error(w, 403, "command_actor_mismatch")
		return command, false
	}
	if command.Status != "requested" && command.Status != "unknown" {
		return command, true
	}
	status, raw, callErr := s.Client.Call(r.Context(), "GET", s.Config.IdentityURL+"/internal/v2/admin/commands/"+command.ID, nil, r)
	if callErr != nil {
		return command, true
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		// Current authority may have changed after frontdoor introspection.
		// Do not expose the journal or redispatch when the owner denies lookup.
		platform.Error(w, status, "access_denied")
		return command, false
	}
	if status == 200 {
		var owner internalhttpv2.CommandStatus
		if json.Unmarshal(raw, &owner) != nil || owner.CommandID != command.ID {
			return command, true
		}
		if owner.Status == "denied" {
			return s.saveCommandOutcome(command, "denied", nil), true
		}
		if owner.Status == "succeeded" && validCommandResult(command, owner.Result) {
			result, _ := json.Marshal(owner.Result)
			return s.saveCommandOutcome(command, "succeeded", result), true
		}
		return command, true
	}
	if status != 404 {
		return command, true
	}
	var absence internalhttpv2.ErrorResponse
	if json.Unmarshal(raw, &absence) != nil || absence.Code != "command_not_found" {
		return command, true
	}
	if !allowResume || r.Method != http.MethodPost {
		return command, true
	}
	// Only authoritative absence allows resubmission. Recheck live mutation
	// authorization and original browser Origin/CSRF immediately before dispatch.
	actor, ok = s.Client.Authenticate(w, r, permission, true)
	if !ok {
		return command, false
	}
	if actor.UserID != command.ActorID {
		platform.Error(w, 403, "command_actor_mismatch")
		return command, false
	}
	return s.dispatchCommand(r, command), true
}
func writeCommand(w http.ResponseWriter, command Command, lookup bool) {
	response := internalhttpv2.CommandStatus{CommandID: command.ID, Status: command.Status}
	if len(command.Result) > 0 {
		var result internalhttpv2.CommandResult
		if json.Unmarshal(command.Result, &result) == nil && validCommandResult(command, &result) {
			response.Result = &result
		}
	}
	status := 200
	if !lookup && (command.Status == "requested" || command.Status == "unknown") {
		status = 202
	}
	platform.JSON(w, status, response)
}
func pathUUID(w http.ResponseWriter, r *http.Request, code string) (string, bool) {
	id := r.PathValue("id")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		platform.Error(w, 400, code)
		return "", false
	}
	return id, true
}
