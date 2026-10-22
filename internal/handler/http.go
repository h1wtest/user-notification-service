package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"user-notification-service/internal/domain"
	"user-notification-service/internal/infrastructure/database"
)

type userContextKey string

const userIDKey userContextKey = "userID"

type HTTPHandler struct {
	repo      *database.PostgresRepo
	jwtSecret string
}

func NewHTTPHandler(repo *database.PostgresRepo, jwtSecret string) *HTTPHandler {
	return &HTTPHandler{
		repo:      repo,
		jwtSecret: jwtSecret,
	}
}

func (h *HTTPHandler) Router() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Accept", "Authorization", "Content-Type", "X-Trace-ID"},
	}))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": "user-notification-service",
		})
	})

	r.Route("/api/v1/users", func(r chi.Router) {
		r.Use(h.authMiddleware)

		r.Get("/me", h.getMe)
		r.Patch("/me", h.updateMe)
		r.Patch("/me/notifications", h.updateNotifications)
	})

	return r
}

func (h *HTTPHandler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, `{"error": "Missing or invalid Authorization header"}`, http.StatusUnauthorized)
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return []byte(h.jwtSecret), nil
		})

		if err != nil || !token.Valid {
			http.Error(w, `{"error": "Invalid or expired token"}`, http.StatusUnauthorized)
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			http.Error(w, `{"error": "Invalid token claims"}`, http.StatusUnauthorized)
			return
		}

		sub, ok := claims["sub"].(string)
		if !ok {
			http.Error(w, `{"error": "Missing sub in token"}`, http.StatusUnauthorized)
			return
		}

		userID, err := uuid.Parse(sub)
		if err != nil {
			http.Error(w, `{"error": "Invalid user ID UUID"}`, http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), userIDKey, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getUserID(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(userIDKey).(uuid.UUID)
	return id, ok
}

type GetMeResponse struct {
	Profile      *domain.UserProfile          `json:"profile"`
	Notification *domain.NotificationSettings `json:"notification_settings"`
}

func (h *HTTPHandler) getMe(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r.Context())
	if !ok {
		http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
		return
	}

	profile, err := h.repo.GetProfile(r.Context(), userID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if profile == nil {
		http.Error(w, `{"error": "Profile not found"}`, http.StatusNotFound)
		return
	}

	settings, err := h.repo.GetNotificationSettings(r.Context(), userID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(GetMeResponse{
		Profile:      profile,
		Notification: settings,
	})
}

type UpdateMeRequest struct {
	DisplayName *string `json:"display_name"`
	AvatarURL   *string `json:"avatar_url"`
	PhoneNumber *string `json:"phone_number"`
	TelegramID  *string `json:"telegram_id"`
}

func (h *HTTPHandler) updateMe(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r.Context())
	if !ok {
		http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var req UpdateMeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	profile, err := h.repo.GetProfile(r.Context(), userID)
	if err != nil || profile == nil {
		http.Error(w, `{"error": "Profile not found"}`, http.StatusNotFound)
		return
	}

	if req.DisplayName != nil {
		profile.DisplayName = req.DisplayName
	}
	if req.AvatarURL != nil {
		profile.AvatarURL = req.AvatarURL
	}
	if req.PhoneNumber != nil {
		profile.PhoneNumber = req.PhoneNumber
	}
	if req.TelegramID != nil {
		profile.TelegramID = req.TelegramID
	}

	if err := h.repo.UpdateProfile(r.Context(), profile); err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(profile)
}

type UpdateNotificationRequest struct {
	EmailLoginAlerts       *bool `json:"email_login_alerts"`
	EmailSecurityAlerts    *bool `json:"email_security_alerts"`
	EmailNewsDigests       *bool `json:"email_news_digests"`
	TelegramLoginAlerts    *bool `json:"telegram_login_alerts"`
	TelegramSecurityAlerts *bool `json:"telegram_security_alerts"`
}

func (h *HTTPHandler) updateNotifications(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r.Context())
	if !ok {
		http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
		return
	}

	var req UpdateNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	settings, err := h.repo.GetNotificationSettings(r.Context(), userID)
	if err != nil || settings == nil {
		http.Error(w, `{"error": "Settings not found"}`, http.StatusNotFound)
		return
	}

	if req.EmailLoginAlerts != nil {
		settings.EmailLoginAlerts = *req.EmailLoginAlerts
	}
	if req.EmailSecurityAlerts != nil {
		settings.EmailSecurityAlerts = *req.EmailSecurityAlerts
	}
	if req.EmailNewsDigests != nil {
		settings.EmailNewsDigests = *req.EmailNewsDigests
	}
	if req.TelegramLoginAlerts != nil {
		settings.TelegramLoginAlerts = *req.TelegramLoginAlerts
	}
	if req.TelegramSecurityAlerts != nil {
		settings.TelegramSecurityAlerts = *req.TelegramSecurityAlerts
	}

	if err := h.repo.UpdateNotificationSettings(r.Context(), settings); err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(settings)
}
