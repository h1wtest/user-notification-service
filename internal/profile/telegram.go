package profile

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"platform-contracts/gen/go/businessv1"
	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

var (
	errTelegramLinkInvalid  = errors.New("telegram_link_invalid")
	errTelegramLinkConflict = errors.New("telegram_link_conflict")
	errTelegramNotLinked    = errors.New("telegram_link_required")
)

func telegramToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func validTelegramToken(token string) bool {
	if len(token) != 43 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == token
}

func (s *Service) telegramEnabled() bool {
	return s.Config.TelegramToken != "" && s.Config.TelegramWebhookSecret != ""
}

func telegramFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errTelegramLinkInvalid):
		platform.Error(w, http.StatusConflict, errTelegramLinkInvalid.Error())
	case errors.Is(err, errTelegramLinkConflict):
		platform.Error(w, http.StatusConflict, errTelegramLinkConflict.Error())
	case errors.Is(err, errTelegramNotLinked):
		platform.Error(w, http.StatusConflict, errTelegramNotLinked.Error())
	case errors.Is(err, pgx.ErrNoRows):
		platform.Error(w, http.StatusServiceUnavailable, "profile_pending")
	default:
		platform.Error(w, http.StatusServiceUnavailable, "database_unavailable")
	}
}

func (s *Service) link(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if !ok {
		return
	}
	if !s.telegramEnabled() {
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	if a.SessionID == "" {
		platform.Error(w, 401, "session_required")
		return
	}
	var request businessv1.EmptyRequest
	if !platform.Decode(w, r, &request) {
		return
	}
	result, err := s.telegram(r.Context(), "getMe", struct{}{})
	var bot struct {
		Username string `json:"username"`
	}
	if err != nil || json.Unmarshal(result, &bot) != nil || !validTelegramUsername(bot.Username) {
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	token, err := telegramToken()
	if err != nil {
		platform.Error(w, 503, "random_unavailable")
		return
	}
	expires := time.Now().UTC().Add(10 * time.Minute)
	if err = s.beginTelegramLink(r.Context(), a.UserID, a.SessionID, token, expires); err != nil {
		telegramFailure(w, err)
		return
	}
	platform.JSON(w, 200, businessv1.TelegramLinkResponse{LinkToken: token, ExpiresAt: expires, BotURL: "https://t.me/" + bot.Username + "?start=" + token})
}

func validTelegramUsername(username string) bool {
	if len(username) < 5 || len(username) > 32 {
		return false
	}
	for _, c := range username {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

func (s *Service) telegramStatus(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if !ok {
		return
	}
	state, err := s.readTelegramStatus(r.Context(), a.UserID, a.SessionID)
	if err != nil {
		telegramFailure(w, err)
		return
	}
	platform.JSON(w, 200, state)
}

func (s *Service) readTelegramStatus(ctx context.Context, user, session string) (businessv1.TelegramStatus, error) {
	var state businessv1.TelegramStatus
	err := s.DB.QueryRow(ctx, `SELECT p.telegram_chat_id IS NOT NULL,p.binding_version,
EXISTS(SELECT 1 FROM telegram_links l WHERE l.user_id=p.user_id AND l.session_id=$2 AND l.expires_at>now() AND l.binding_version=p.binding_version),
EXISTS(SELECT 1 FROM telegram_links l WHERE l.user_id=p.user_id AND l.session_id=$2 AND l.expires_at>now() AND l.binding_version=p.binding_version AND l.consumed_at IS NOT NULL AND l.candidate_chat_id IS NOT NULL),
EXISTS(SELECT 1 FROM telegram_unlinks u WHERE u.user_id=p.user_id AND u.expires_at>now() AND u.consumed_at IS NULL AND u.chat_id=p.telegram_chat_id AND u.binding_version=p.binding_version)
FROM profiles p WHERE p.user_id=$1`, user, session).Scan(&state.Linked, &state.BindingVersion, &state.LinkPending, &state.CandidateReady, &state.UnlinkPending)
	return state, err
}

func (s *Service) beginTelegramLink(ctx context.Context, user, session, token string, expires time.Time) error {
	if session == "" || !validTelegramToken(token) {
		return errTelegramLinkInvalid
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var chat *string
	var version int64
	if err = tx.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&chat, &version); err != nil {
		return err
	}
	if chat != nil {
		return errTelegramLinkConflict
	}
	hash := sha256.Sum256([]byte(token))
	if _, err = tx.Exec(ctx, "DELETE FROM telegram_links WHERE user_id=$1", user); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO telegram_links(token_hash,user_id,session_id,expires_at,binding_version) VALUES($1,$2,$3,$4,$5)", hash[:], user, session, expires, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) confirmTelegram(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "", false)
	if !ok {
		return
	}
	if !s.telegramEnabled() {
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	if a.SessionID == "" {
		platform.Error(w, 401, "session_required")
		return
	}
	var body businessv1.TelegramConfirmRequest
	if !platform.Decode(w, r, &body) {
		return
	}
	if !validTelegramToken(body.LinkToken) {
		platform.Error(w, 400, "invalid_link_token")
		return
	}
	if err := s.confirmTelegramLink(r.Context(), a.UserID, a.SessionID, body.LinkToken); err != nil {
		telegramFailure(w, err)
		return
	}
	state, err := s.readTelegramStatus(r.Context(), a.UserID, a.SessionID)
	if err != nil {
		telegramFailure(w, err)
		return
	}
	platform.JSON(w, 200, state)
}

func (s *Service) confirmTelegramLink(ctx context.Context, user, session, token string) error {
	if session == "" || !validTelegramToken(token) {
		return errTelegramLinkInvalid
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var current *string
	var version int64
	err = tx.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&current, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return errTelegramLinkInvalid
	}
	if err != nil {
		return err
	}
	if current != nil {
		return errTelegramLinkInvalid
	}
	hash := sha256.Sum256([]byte(token))
	var candidate string
	err = tx.QueryRow(ctx, `SELECT candidate_chat_id FROM telegram_links WHERE token_hash=$1 AND user_id=$2 AND session_id=$3 AND expires_at>now() AND consumed_at IS NOT NULL AND candidate_chat_id IS NOT NULL AND binding_version=$4 FOR UPDATE`, hash[:], user, session, version).Scan(&candidate)
	if errors.Is(err, pgx.ErrNoRows) {
		return errTelegramLinkInvalid
	}
	if err != nil {
		return err
	}
	var aggregate int64
	err = tx.QueryRow(ctx, "UPDATE profiles SET telegram_chat_id=$2,binding_version=binding_version+1,version=version+1,updated_at=now() WHERE user_id=$1 RETURNING binding_version,version", user, candidate).Scan(&version, &aggregate)
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "23505" {
		return errTelegramLinkConflict
	}
	if err != nil {
		return err
	}
	if err = clearTelegramHandshakes(ctx, tx, user); err != nil {
		return err
	}
	if err = enqueueTelegramBinding(ctx, tx, user, version, aggregate, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) unlinkTelegram(w http.ResponseWriter, r *http.Request) {
	// Identity, not a caller-controlled timestamp or a cached actor, decides
	// whether this current session reauthenticated within the last five minutes.
	a, ok := s.Client.Authenticate(w, r, "users:self:reauth", false)
	if !ok {
		return
	}
	if !s.telegramEnabled() {
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	var body businessv1.EmptyRequest
	if !platform.Decode(w, r, &body) {
		return
	}
	token, err := telegramToken()
	if err != nil {
		platform.Error(w, 503, "random_unavailable")
		return
	}
	expires := time.Now().UTC().Add(10 * time.Minute)
	chat, err := s.beginTelegramUnlink(r.Context(), a.UserID, token, expires)
	if err != nil {
		telegramFailure(w, err)
		return
	}
	_, err = s.telegram(r.Context(), "sendMessage", telegramUnlinkMessage{ChatID: chat, Text: "Confirm unlinking this Telegram chat from your account. This button expires in 10 minutes.", ReplyMarkup: telegramInlineKeyboard{InlineKeyboard: [][]telegramInlineButton{{{Text: "Confirm unlink", CallbackData: "unlink:" + token}}}}})
	if err != nil {
		hash := sha256.Sum256([]byte(token))
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 3*time.Second)
		defer cancel()
		if _, deleteErr := s.DB.Exec(cleanup, "DELETE FROM telegram_unlinks WHERE token_hash=$1 AND consumed_at IS NULL", hash[:]); deleteErr != nil {
			platform.Error(w, 503, "database_unavailable")
			return
		}
		platform.Error(w, 503, "telegram_unavailable")
		return
	}
	platform.JSON(w, 200, businessv1.TelegramUnlinkResponse{Status: "pending", ExpiresAt: expires})
}

// These types describe Telegram's external Bot API, not platform wire DTOs.
type telegramInlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}
type telegramInlineKeyboard struct {
	InlineKeyboard [][]telegramInlineButton `json:"inline_keyboard"`
}
type telegramUnlinkMessage struct {
	ChatID      string                 `json:"chat_id"`
	Text        string                 `json:"text"`
	ReplyMarkup telegramInlineKeyboard `json:"reply_markup"`
}

func (s *Service) beginTelegramUnlink(ctx context.Context, user, token string, expires time.Time) (string, error) {
	if !validTelegramToken(token) {
		return "", errTelegramLinkInvalid
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var chat *string
	var version int64
	if err = tx.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&chat, &version); err != nil {
		return "", err
	}
	if chat == nil {
		return "", errTelegramNotLinked
	}
	hash := sha256.Sum256([]byte(token))
	if _, err = tx.Exec(ctx, "DELETE FROM telegram_unlinks WHERE user_id=$1", user); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO telegram_unlinks(token_hash,user_id,chat_id,binding_version,expires_at) VALUES($1,$2,$3,$4,$5)", hash[:], user, *chat, version, expires); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return *chat, nil
}

func (s *Service) resetTelegram(w http.ResponseWriter, r *http.Request) {
	a, ok := s.Client.Authenticate(w, r, "notifications:bindings:reset", true)
	if !ok {
		return
	}
	user := r.PathValue("id")
	if _, err := uuid.Parse(user); err != nil {
		platform.Error(w, 400, "invalid_user_id")
		return
	}
	var body businessv1.TelegramResetRequest
	if !platform.Decode(w, r, &body) {
		return
	}
	reason := strings.TrimSpace(body.Reason)
	if reason == "" || utf8.RuneCountInString(reason) > 500 {
		platform.Error(w, 400, "invalid_reason")
		return
	}
	if err := s.forceResetTelegram(r.Context(), user, a.UserID, reason); err != nil {
		telegramFailure(w, err)
		return
	}
	platform.JSON(w, 200, businessv1.TelegramResetResponse{Status: "reset"})
}

func (s *Service) forceResetTelegram(ctx context.Context, user, actor, reason string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var locked string
	if err = tx.QueryRow(ctx, "SELECT user_id FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&locked); err != nil {
		return err
	}
	version, err := removeTelegramBinding(ctx, tx, user)
	if err != nil {
		return err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO profile_telegram_reset_audit(id,actor_id,user_id,reason,binding_version) VALUES($1,$2,$3,$4,$5)", id.String(), actor, user, reason, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type telegramSender struct {
	ID  int64 `json:"id"`
	Bot bool  `json:"is_bot"`
}
type telegramMessage struct {
	Text string         `json:"text"`
	From telegramSender `json:"from"`
	Chat struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
}
type telegramUpdate struct {
	UpdateID int64            `json:"update_id"`
	Message  *telegramMessage `json:"message"`
	Callback *struct {
		From    telegramSender   `json:"from"`
		Message *telegramMessage `json:"message"`
		Data    string           `json:"data"`
	} `json:"callback_query"`
}

func (s *Service) webhook(w http.ResponseWriter, r *http.Request) {
	secret := r.Header.Values("X-Telegram-Bot-Api-Secret-Token")
	if !s.telegramEnabled() || len(secret) != 1 || subtle.ConstantTimeCompare([]byte(secret[0]), []byte(s.Config.TelegramWebhookSecret)) != 1 {
		platform.Error(w, 403, "service_denied")
		return
	}
	var update telegramUpdate
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	decoder := json.NewDecoder(r.Body)
	if decoder.Decode(&update) != nil || update.UpdateID < 0 {
		platform.Error(w, 400, "invalid_update")
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		platform.Error(w, 400, "invalid_update")
		return
	}
	var err error
	if callback := update.Callback; callback != nil && callback.Message != nil {
		message := callback.Message
		if message.Chat.Type == "private" && !callback.From.Bot && callback.From.ID > 0 && callback.From.ID == message.Chat.ID && strings.HasPrefix(callback.Data, "unlink:") {
			token := strings.TrimPrefix(callback.Data, "unlink:")
			if validTelegramToken(token) {
				err = s.applyTelegramUnlink(r.Context(), update.UpdateID, token, strconv.FormatInt(message.Chat.ID, 10))
			}
		}
	} else if message := update.Message; message != nil && message.Chat.Type == "private" && !message.From.Bot && message.From.ID > 0 && message.From.ID == message.Chat.ID {
		parts := strings.Fields(message.Text)
		if len(parts) == 2 && parts[0] == "/start" && validTelegramToken(parts[1]) {
			err = s.setTelegramCandidate(r.Context(), update.UpdateID, parts[1], strconv.FormatInt(message.Chat.ID, 10))
		}
	}
	if err != nil {
		telegramFailure(w, err)
		return
	}
	platform.JSON(w, 200, struct {
		OK bool `json:"ok"`
	}{true})
}

func newTelegramUpdate(ctx context.Context, tx pgx.Tx, updateID int64) (bool, error) {
	tag, err := tx.Exec(ctx, "INSERT INTO telegram_updates(update_id) VALUES($1) ON CONFLICT DO NOTHING", updateID)
	return err == nil && tag.RowsAffected() == 1, err
}

func (s *Service) setTelegramCandidate(ctx context.Context, updateID int64, token, chat string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	fresh, err := newTelegramUpdate(ctx, tx, updateID)
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit(ctx)
	}
	hash := sha256.Sum256([]byte(token))
	var user string
	err = tx.QueryRow(ctx, "SELECT user_id FROM telegram_links WHERE token_hash=$1", hash[:]).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	var current *string
	var version int64
	if err = tx.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&current, &version); err != nil {
		return err
	}
	if current == nil {
		if _, err = tx.Exec(ctx, "UPDATE telegram_links SET consumed_at=now(),candidate_chat_id=$2 WHERE token_hash=$1 AND expires_at>now() AND consumed_at IS NULL AND binding_version=$3", hash[:], chat, version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Service) applyTelegramUnlink(ctx context.Context, updateID int64, token, chat string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	fresh, err := newTelegramUpdate(ctx, tx, updateID)
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit(ctx)
	}
	hash := sha256.Sum256([]byte(token))
	var user string
	err = tx.QueryRow(ctx, "SELECT user_id FROM telegram_unlinks WHERE token_hash=$1", hash[:]).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	var current *string
	var version int64
	if err = tx.QueryRow(ctx, "SELECT telegram_chat_id,binding_version FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&current, &version); err != nil {
		return err
	}
	if current == nil || *current != chat {
		return tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, "UPDATE telegram_unlinks SET consumed_at=now() WHERE token_hash=$1 AND user_id=$2 AND chat_id=$3 AND binding_version=$4 AND expires_at>now() AND consumed_at IS NULL", hash[:], user, chat, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if _, err = removeTelegramBinding(ctx, tx, user); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func clearTelegramHandshakes(ctx context.Context, tx pgx.Tx, user string) error {
	if _, err := tx.Exec(ctx, "DELETE FROM telegram_links WHERE user_id=$1", user); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "DELETE FROM telegram_unlinks WHERE user_id=$1", user)
	return err
}

// Caller holds the profile lock, so reset, confirm and bot callbacks share one
// serialization point. A new binding never inherits the old chat's opt-ins.
func removeTelegramBinding(ctx context.Context, tx pgx.Tx, user string) (int64, error) {
	var version, aggregate int64
	if err := tx.QueryRow(ctx, "UPDATE profiles SET telegram_chat_id=NULL,binding_version=binding_version+1,version=version+1,updated_at=now() WHERE user_id=$1 RETURNING binding_version,version", user).Scan(&version, &aggregate); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, "UPDATE notification_settings SET telegram_login_alerts=false,telegram_security_alerts=false,telegram_subscription=false,telegram_trading=false WHERE user_id=$1", user); err != nil {
		return 0, err
	}
	if err := clearTelegramHandshakes(ctx, tx, user); err != nil {
		return 0, err
	}
	if err := enqueueTelegramBinding(ctx, tx, user, version, aggregate, false); err != nil {
		return 0, err
	}
	return version, nil
}

func enqueueTelegramBinding(ctx context.Context, tx pgx.Tx, user string, bindingVersion, aggregateVersion int64, linked bool) error {
	payload, err := json.Marshal(eventsv2.TelegramBindingChanged{UserID: user, BindingVersion: bindingVersion, Linked: linked})
	if err != nil {
		return err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	return platform.EnqueueV2(ctx, tx, eventsv2.Envelope{SchemaVersion: 2, EventID: id.String(), EventType: "profile.telegram_binding_changed", Producer: "profile", AggregateType: "user", AggregateID: user, AggregateVersion: aggregateVersion, OccurredAt: time.Now().UTC(), Payload: payload})
}
