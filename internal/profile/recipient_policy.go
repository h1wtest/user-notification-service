package profile

import (
	"github.com/google/uuid"
	"net/http"
	"platform-contracts/gen/go/internalhttpv2"
	"user-notification-service/internal/platform"
)

func (s *Service) recipientPolicy(w http.ResponseWriter, r *http.Request) {
	if !platform.ServiceAuthorized(r, s.Config.NotificationToken) {
		platform.Error(w, 403, "service_denied")
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		platform.Error(w, 400, "invalid_user_id")
		return
	}
	var p internalhttpv2.RecipientPolicy
	p.UserID = id
	err := s.DB.QueryRow(r.Context(), `SELECT p.email,p.notification_locale,p.telegram_chat_id,p.binding_version,n.email_subscription,n.email_security_alerts,n.telegram_subscription,n.telegram_security_alerts,n.telegram_trading,n.email_login_alerts,n.telegram_login_alerts FROM profiles p JOIN notification_settings n USING(user_id) WHERE p.user_id=$1`, id).Scan(&p.Email, &p.Locale, &p.TelegramChatID, &p.BindingVersion, &p.EmailSubscription, &p.EmailSecurity, &p.TelegramSubscription, &p.TelegramSecurity, &p.TelegramTrading, &p.EmailLoginAlerts, &p.TelegramLoginAlerts)
	if err != nil {
		platform.Error(w, 503, "profile_pending")
		return
	}
	platform.JSON(w, 200, p)
}
