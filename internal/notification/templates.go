package notification

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"platform-contracts/gen/go/businessv1"
	"platform-contracts/gen/go/eventsv2"
	"time"
	"user-notification-service/internal/platform"
)

type TemplateText = businessv1.TemplateText
type Template struct {
	EventType string                  `json:"event_type"`
	Version   int64                   `json:"version"`
	Locales   map[string]TemplateText `json:"locales"`
}

var (
	errInvalidTemplate = errors.New("invalid_template")
	errUnknownVariable = errors.New("unknown_template_variable")
	errUnsafeURL       = errors.New("unsafe_template_url")
	errVersionConflict = errors.New("version_conflict")
	placeholder        = regexp.MustCompile(`\{\{([a-z][a-z0-9_]*)\}\}`)
	templateURL        = regexp.MustCompile(`(?i)(?:https?|javascript|data|vbscript|file)\s*:[^\s<>"']*`)
)

// Each event explicitly chooses its variables; credentials never become generic variables.
var templateVariables = map[string][]string{
	"notification.email_verification": {"code", "expires_at", "verification_url"},
	"notification.password_reset":     {"reset_url", "expires_at"},
	"identity.user_logged_in":         {"device_name", "ip_address", "occurred_at"},
	"identity.password_changed":       {"occurred_at"}, "identity.mfa_changed": {"occurred_at"}, "identity.session_revoked": {"occurred_at"}, "identity.user_banned": {"occurred_at"}, "identity.user_unbanned": {"occurred_at"},
	"billing.payment_accepted": {"order_id", "amount", "currency"}, "billing.order_fulfilled": {"order_id", "amount", "currency"},
	"subscription.changed": {"subscription_id", "period_end"}, "subscription.closed": {"subscription_id", "period_end"}, "subscription.compensated": {"subscription_id", "period_end"}, "subscription.expired": {"subscription_id", "period_end"}, "subscription.action_applied": {"subscription_id", "period_end"}, "subscription.action_rejected": {"reason"}, "subscription.expiry_reminder": {"subscription_id", "period_end", "days"},
	"referral.commission_posted": {"amount", "currency"}, "referral.commission_reversed": {"amount", "currency"}, "referral.payout_requested": {"amount", "currency", "payout_id", "state"}, "referral.payout_decided": {"amount", "currency", "payout_id", "state"}, "referral.payout_cancelled": {"amount", "currency", "payout_id", "state"},
	"external.readiness_changed": {"status", "message", "subscription_id"}, "external.trading_alert": {"status", "message", "subscription_id"},
}

func defaultTemplate(event string) Template {
	en, ru := "Account notification", "Уведомление аккаунта"
	body, bodyRU := "Your account state has changed.", "Состояние вашего аккаунта изменилось."
	switch event {
	case "notification.email_verification":
		en, ru = "Verify your email", "Подтвердите электронную почту"
		body, bodyRU = "Open the verification form: {{verification_url}}\nVerification code: {{code}}\nExpires: {{expires_at}}", "Подтвердите адрес в форме: {{verification_url}}\nКод подтверждения: {{code}}\nДействует до: {{expires_at}}"
	case "notification.password_reset":
		en, ru = "Reset your password", "Сброс пароля"
		body, bodyRU = "Reset your password: {{reset_url}}\nExpires: {{expires_at}}", "Сбросить пароль: {{reset_url}}\nДействует до: {{expires_at}}"
	case "identity.user_logged_in":
		en, ru = "New sign-in", "Новый вход"
		body, bodyRU = "Sign-in: {{occurred_at}}\nDevice: {{device_name}}\nIP: {{ip_address}}", "Вход: {{occurred_at}}\nУстройство: {{device_name}}\nIP: {{ip_address}}"
	default:
		if strings.HasPrefix(event, "subscription.") {
			en, ru = "Subscription update", "Изменение подписки"
		}
		if strings.HasPrefix(event, "billing.") {
			en, ru = "Payment update", "Изменение платежа"
		}
		if strings.HasPrefix(event, "referral.") {
			en, ru = "Referral update", "Изменение реферальной программы"
		}
		if strings.HasPrefix(event, "external.") {
			en, ru = "Service notification", "Уведомление сервиса"
		}
		titles := map[string][2]string{
			"identity.password_changed": {"Password changed", "Пароль изменён"}, "identity.mfa_changed": {"Two-factor authentication changed", "Двухфакторная аутентификация изменена"}, "identity.session_revoked": {"Session revoked", "Сессия отозвана"}, "identity.user_banned": {"Account blocked", "Аккаунт заблокирован"}, "identity.user_unbanned": {"Account unblocked", "Аккаунт разблокирован"},
			"billing.payment_accepted": {"Payment accepted", "Платёж принят"}, "billing.order_fulfilled": {"Order fulfilled", "Заказ выполнен"},
			"subscription.action_applied": {"Subscription action applied", "Изменение подписки применено"}, "subscription.action_rejected": {"Subscription action rejected", "Изменение подписки отклонено"}, "subscription.changed": {"Subscription changed", "Подписка изменена"}, "subscription.expired": {"Subscription expired", "Подписка истекла"}, "subscription.closed": {"Subscription closed", "Подписка закрыта"}, "subscription.compensated": {"Subscription compensated", "Период подписки компенсирован"}, "subscription.expiry_reminder": {"Subscription expires soon", "Подписка скоро истекает"},
			"referral.commission_posted": {"Referral commission credited", "Реферальная комиссия начислена"}, "referral.commission_reversed": {"Referral commission reversed", "Реферальная комиссия отменена"}, "referral.payout_requested": {"Payout requested", "Выплата запрошена"}, "referral.payout_decided": {"Payout decision", "Решение по выплате"}, "referral.payout_cancelled": {"Payout cancelled", "Выплата отменена"}, "external.readiness_changed": {"Service readiness changed", "Готовность сервиса изменилась"}, "external.trading_alert": {"Trading alert", "Торговое уведомление"},
		}
		if title, ok := titles[event]; ok {
			en, ru = title[0], title[1]
		}
		body, bodyRU = "", ""
		labels := map[string][2]string{"occurred_at": {"Time", "Время"}, "order_id": {"Order", "Заказ"}, "amount": {"Amount", "Сумма"}, "currency": {"Currency", "Валюта"}, "subscription_id": {"Subscription", "Подписка"}, "period_end": {"Period ends", "Окончание периода"}, "reason": {"Reason", "Причина"}, "days": {"Days remaining", "Осталось дней"}, "payout_id": {"Payout", "Выплата"}, "state": {"State", "Состояние"}, "status": {"Status", "Статус"}, "message": {"Message", "Сообщение"}}
		for _, v := range templateVariables[event] {
			label := labels[v]
			body += label[0] + ": {{" + v + "}}\n"
			bodyRU += label[1] + ": {{" + v + "}}\n"
		}
	}
	return Template{EventType: event, Version: 1, Locales: map[string]TemplateText{"en": {Subject: en, Body: body}, "ru": {Subject: ru, Body: bodyRU}}}
}
func safeTemplateText(text string) bool {
	normalized := strings.ToLower(html.UnescapeString(text))
	normalized = strings.Map(func(r rune) rune {
		if r <= 32 {
			return -1
		}
		return r
	}, normalized)
	for _, scheme := range []string{"javascript:", "data:", "vbscript:", "file:"} {
		if strings.Contains(normalized, scheme) {
			return false
		}
	}
	for _, raw := range templateURL.FindAllString(html.UnescapeString(text), -1) {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return false
		}
	}
	return true
}
func validateTemplate(t Template) error {
	allowed, ok := templateVariables[t.EventType]
	if !ok || len(t.Locales) != 2 {
		return errInvalidTemplate
	}
	for _, locale := range []string{"en", "ru"} {
		v, ok := t.Locales[locale]
		if !ok || strings.TrimSpace(v.Subject) == "" || strings.TrimSpace(v.Body) == "" || len(v.Subject) > 500 || len(v.Body) > 20000 || strings.ContainsAny(v.Subject, "\r\n") {
			return errInvalidTemplate
		}
		for _, text := range []string{v.Subject, v.Body} {
			if !safeTemplateText(text) {
				return errUnsafeURL
			}
			for _, m := range placeholder.FindAllStringSubmatch(text, -1) {
				found := false
				for _, a := range allowed {
					if a == m[1] {
						found = true
						break
					}
				}
				if !found {
					return errUnknownVariable
				}
			}
			rest := placeholder.ReplaceAllString(text, "")
			if strings.Contains(rest, "{{") || strings.Contains(rest, "}}") {
				return errInvalidTemplate
			}
		}
	}
	return nil
}
func renderTemplate(t Template, locale string, vars map[string]string) (string, string, error) {
	if err := validateTemplate(t); err != nil {
		return "", "", err
	}
	v, ok := t.Locales[locale]
	if !ok {
		v = t.Locales["en"]
	}
	var missing bool
	replace := func(m string) string {
		key := placeholder.FindStringSubmatch(m)[1]
		value, ok := vars[key]
		if !ok {
			missing = true
		}
		return html.EscapeString(strings.ReplaceAll(strings.ReplaceAll(value, "\r", ""), "\n", " "))
	}
	subject, body := placeholder.ReplaceAllStringFunc(v.Subject, replace), placeholder.ReplaceAllStringFunc(v.Body, replace)
	if missing {
		return "", "", errUnknownVariable
	}
	if !safeTemplateText(html.UnescapeString(subject)) || !safeTemplateText(html.UnescapeString(body)) {
		return "", "", errUnsafeURL
	}
	return subject, body, nil
}
func (s *Service) loadTemplate(ctx context.Context, event string) (Template, error) {
	if _, ok := templateVariables[event]; !ok {
		return Template{}, errInvalidTemplate
	}
	var v Template
	var raw []byte
	err := s.DB.QueryRow(ctx, "SELECT version,locales FROM notification_templates WHERE event_type=$1 ORDER BY version DESC LIMIT 1", event).Scan(&v.Version, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return defaultTemplate(event), nil
	}
	if err != nil {
		return v, err
	}
	v.EventType = event
	err = json.Unmarshal(raw, &v.Locales)
	return v, err
}
func (s *Service) publishTemplate(ctx context.Context, v Template, expected int64, actor string) (Template, error) {
	if err := validateTemplate(v); err != nil {
		return v, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return v, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "notification-template:"+v.EventType); err != nil {
		return v, err
	}
	var current int64
	if err = tx.QueryRow(ctx, "SELECT coalesce(max(version),1) FROM notification_templates WHERE event_type=$1", v.EventType).Scan(&current); err != nil {
		return v, err
	}
	if current != expected {
		return v, errVersionConflict
	}
	v.Version = current + 1
	raw, err := json.Marshal(v.Locales)
	if err != nil {
		return v, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO notification_templates(event_type,version,locales) VALUES($1,$2,$3)", v.EventType, v.Version, raw); err != nil {
		return v, err
	}
	payload, err := json.Marshal(eventsv2.TemplatePublished{EventType: v.EventType, Version: v.Version, ActorID: actor})
	if err != nil {
		return v, err
	}
	eventID, err := uuid.NewV7()
	if err != nil {
		return v, err
	}
	event := eventsv2.Envelope{SchemaVersion: 2, EventID: eventID.String(), EventType: "notification.template_published", Producer: "notification", AggregateType: "notification_template", AggregateID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("notification-template:"+v.EventType)).String(), AggregateVersion: v.Version - 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = platform.EnqueueV2(ctx, tx, event); err != nil {
		return v, err
	}
	return v, tx.Commit(ctx)
}
func (s *Service) templates(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.admin(w, r, "notifications:read"); !ok {
		return
	}
	keys := make([]string, 0, len(templateVariables))
	for key := range templateVariables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]businessv1.TemplateRevision, 0, len(keys))
	for _, key := range keys {
		v, err := s.loadTemplate(r.Context(), key)
		if err != nil {
			platform.Error(w, 503, "database_unavailable")
			return
		}
		items = append(items, templateWire(v))
	}
	platform.JSON(w, 200, businessv1.TemplatesResponse{Items: items})
}
func (s *Service) putTemplate(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.admin(w, r, "notifications:templates:write")
	if !ok {
		return
	}
	var in businessv1.TemplatePublish
	raw, decodeErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
	if decodeErr != nil || json.Unmarshal(raw, &in) != nil {
		platform.Error(w, 400, "invalid_template")
		return
	}
	v, err := s.publishTemplate(r.Context(), Template{EventType: r.PathValue("event_type"), Locales: map[string]TemplateText{"en": in.Locales.En, "ru": in.Locales.Ru}}, in.ExpectedVersion, actor.UserID)
	if err != nil {
		code := 503
		if errors.Is(err, errVersionConflict) {
			code = 409
		} else if errors.Is(err, errInvalidTemplate) || errors.Is(err, errUnknownVariable) || errors.Is(err, errUnsafeURL) {
			code = 400
		}
		if code == 503 {
			platform.Error(w, code, "database_unavailable")
		} else {
			platform.Error(w, code, err.Error())
		}
		return
	}
	platform.JSON(w, 200, templateWire(v))
}
func templateWire(v Template) businessv1.TemplateRevision {
	variables := append([]string{}, templateVariables[v.EventType]...)
	sort.Strings(variables)
	return businessv1.TemplateRevision{EventType: v.EventType, Version: v.Version, Locales: businessv1.TemplateLocales{En: v.Locales["en"], Ru: v.Locales["ru"]}, Variables: variables}
}

func (s *Service) previewTemplate(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.admin(w, r, "notifications:templates:write"); !ok {
		return
	}
	var in businessv1.TemplatePreviewRequest
	if !platform.Decode(w, r, &in) {
		return
	}
	t := Template{EventType: r.PathValue("event_type"), Locales: map[string]TemplateText{"en": in.Locales.En, "ru": in.Locales.Ru}}
	if err := validateTemplate(t); err != nil {
		platform.Error(w, 400, err.Error())
		return
	}
	vars, err := s.previewVariables(t.EventType)
	if err != nil {
		platform.Error(w, 503, "preview_configuration_unavailable")
		return
	}
	locales := make(map[string]TemplateText, 2)
	for _, locale := range []string{"en", "ru"} {
		subject, body, err := renderTemplate(t, locale, vars)
		if err != nil {
			platform.Error(w, 400, err.Error())
			return
		}
		locales[locale] = TemplateText{Subject: subject, Body: body}
	}
	platform.JSON(w, 200, businessv1.TemplatePreviewResponse{Locales: businessv1.TemplateLocales{En: locales["en"], Ru: locales["ru"]}})
}

// Samples are code-owned and never read an account, challenge or provider.
func (s *Service) previewVariables(event string) (map[string]string, error) {
	samples := map[string]string{
		"code": "654321", "expires_at": "2030-01-02T03:04:05Z", "occurred_at": "2030-01-01T03:04:05Z",
		"device_name": "Example <device> & browser", "ip_address": "192.0.2.1",
		"order_id": "00000000-0000-4000-8000-000000000001", "subscription_id": "00000000-0000-4000-8000-000000000002", "payout_id": "00000000-0000-4000-8000-000000000003",
		"amount": "12.34", "currency": "USD", "period_end": "2030-02-01T03:04:05Z", "days": "3",
		"reason": "Example reason", "state": "APPROVED", "status": "ready", "message": "Example <message> & details",
	}
	vars := make(map[string]string, len(templateVariables[event]))
	for _, name := range templateVariables[event] {
		if name == "verification_url" || name == "reset_url" {
			u, err := url.Parse(s.Config.Origin)
			if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(s.Config.Origin, "\r\n") {
				return nil, errUnsafeURL
			}
			vars[name] = strings.TrimRight(s.Config.Origin, "/") + "/?user_id=00000000-0000-4000-8000-000000000001"
			if name == "reset_url" {
				vars[name] = strings.TrimRight(s.Config.Origin, "/") + "/?token=example-not-a-valid-authentication-token"
			}
			continue
		}
		value, ok := samples[name]
		if !ok {
			return nil, errUnknownVariable
		}
		vars[name] = value
	}
	return vars, nil
}
