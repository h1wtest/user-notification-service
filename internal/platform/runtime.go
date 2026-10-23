package platform

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"platform-contracts/gen/go/events"
	"platform-contracts/gen/go/internalhttp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Envelope = events.Envelope

type Config struct {
	Kind, DSN, Port, Origin, Token, NotificationToken, AdminToken, IdentityURL, ProfileURL, NotificationURL, Cert, Key, CA string
	Brokers                                                                                                                []string
	DeliveryKey, QuarantineKey                                                                                             []byte
	KafkaCA, KafkaUsername, KafkaPassword                                                                                  string
	SMTPHost, SMTPPort, SMTPFrom, SMTPUser, SMTPPassword                                                                   string
	SMTPInsecure                                                                                                           bool
	TelegramToken, TelegramBase, TelegramWebhookSecret                                                                     string
}

func Env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func Load(kind string) (Config, error) {
	ports := map[string]string{"profile": "8002", "notification": "8003", "admin": "8004", "worker": "8005"}
	c := Config{Kind: kind, DSN: os.Getenv("DATABASE_URL"), Port: Env("HTTP_PORT", ports[kind]), Origin: os.Getenv("PUBLIC_ORIGIN"), Token: os.Getenv("INTERNAL_SERVICE_TOKEN"), NotificationToken: os.Getenv("NOTIFICATION_INTERNAL_TOKEN"), AdminToken: os.Getenv("ADMIN_INTERNAL_TOKEN"), IdentityURL: Env("IDENTITY_URL", "https://identity-service:8001"), ProfileURL: Env("PROFILE_URL", "https://profile-service:8002"), NotificationURL: Env("NOTIFICATION_URL", "https://notification-service:8003"), Cert: os.Getenv("TLS_CERT_FILE"), Key: os.Getenv("TLS_KEY_FILE"), CA: os.Getenv("INTERNAL_CA_FILE"), Brokers: strings.Split(Env("KAFKA_BROKERS", "redpanda:29092"), ","), SMTPHost: Env("SMTP_HOST", "mailpit"), SMTPPort: Env("SMTP_PORT", "1025"), SMTPFrom: Env("SMTP_FROM", "no-reply@sso.local"), SMTPUser: os.Getenv("SMTP_USER"), SMTPPassword: os.Getenv("SMTP_PASSWORD"), SMTPInsecure: os.Getenv("SMTP_ALLOW_INSECURE") == "true", TelegramToken: os.Getenv("TELEGRAM_BOT_TOKEN"), TelegramBase: Env("TELEGRAM_API_BASE", "https://api.telegram.org"), TelegramWebhookSecret: os.Getenv("TELEGRAM_WEBHOOK_SECRET")}
	if c.DSN == "" {
		return c, errors.New("DATABASE_URL required")
	}
	var err error
	c.QuarantineKey, err = loadKey("QUARANTINE_ENCRYPTION_KEY")
	if err != nil {
		return c, err
	}
	if kind == "notification" || kind == "worker" {
		c.DeliveryKey, err = loadKey("DELIVERY_ENCRYPTION_KEY")
		if err != nil {
			return c, err
		}
	}
	c.KafkaCA, c.KafkaUsername, c.KafkaPassword = os.Getenv("KAFKA_TLS_CA_FILE"), os.Getenv("KAFKA_SASL_USERNAME"), os.Getenv("KAFKA_SASL_PASSWORD")
	if kind != "worker" && (c.KafkaCA == "" || c.KafkaUsername == "" || c.KafkaPassword == "") {
		return c, errors.New("Kafka TLS and SASL credentials required")
	}
	return c, nil
}
func loadKey(name string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(os.Getenv(name))
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("%s must be base64 32 bytes", name)
	}
	return key, nil
}
func Encrypt(key []byte, aad string, data []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(g.Seal(nonce, nonce, data, []byte(aad))), nil
}
func Decrypt(key []byte, aad, data string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(b) < g.NonceSize() {
		return nil, errors.New("invalid ciphertext")
	}
	return g.Open(nil, b[:g.NonceSize()], b[g.NonceSize():], []byte(aad))
}
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func Error(w http.ResponseWriter, status int, code string) {
	JSON(w, status, internalhttp.ErrorResponse{Error: code, Code: code})
}
func Decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16384)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		Error(w, 400, "invalid_request")
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		Error(w, 400, "invalid_request")
		return false
	}
	return true
}
func ServiceAuthorized(r *http.Request, token string) bool {
	return token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Service-Token")), []byte(token)) == 1
}

type Actor = internalhttp.IntrospectResponse
type Client struct {
	HTTP   *http.Client
	Config Config
}

func NewClient(c Config) (*Client, error) {
	pem, err := os.ReadFile(c.CA)
	if err != nil {
		return nil, errors.New("INTERNAL_CA_FILE required")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid internal CA")
	}
	return &Client{Config: c, HTTP: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ResponseHeaderTimeout: 4 * time.Second, MaxIdleConnsPerHost: 8}}}, nil
}
func ForwardActor(dst, src *http.Request) {
	for _, h := range []string{"Cookie", "Authorization", "X-CSRF-Token", "Origin", "Traceparent"} {
		dst.Header.Del(h)
		for _, value := range src.Header.Values(h) {
			dst.Header.Add(h, value)
		}
	}
	dst.Header.Del("X-Actor-Session")
	if ck, err := src.Cookie("sso_session"); err == nil && ck.Value != "" {
		dst.Header.Set("X-Actor-Session", ck.Value)
	}
	dst.Header.Del("X-Actor-Token")
	if authorization := src.Header.Get("Authorization"); strings.HasPrefix(authorization, "Bearer ") {
		if token := strings.TrimPrefix(authorization, "Bearer "); strings.TrimSpace(token) != "" {
			dst.Header.Set("X-Actor-Token", token)
		}
	}
}
func (c *Client) Call(ctx context.Context, method, target string, body any, source *http.Request) (int, []byte, error) {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != "https" {
		return 503, nil, errors.New("internal HTTPS required")
	}
	var reader io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return 503, nil, e
		}
		reader = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 503, nil, err
	}
	req.Header.Set("X-Service-Token", c.Config.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if source != nil {
		ForwardActor(req, source)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 503, nil, errors.New("dependency unavailable")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}
func (c *Client) Authenticate(w http.ResponseWriter, r *http.Request, permission string, mfa bool) (Actor, bool) {
	var actor Actor
	cookie := ""
	if ck, err := r.Cookie("sso_session"); err == nil {
		cookie = ck.Value
	}
	auth := r.Header.Get("Authorization")
	token := ""
	if len(r.Header.Values("Authorization")) > 0 {
		if !strings.HasPrefix(auth, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")) == "" {
			Error(w, 401, "invalid_authorization")
			return actor, false
		}
		token = strings.TrimPrefix(auth, "Bearer ")
	}
	if cookie == "" && token == "" {
		Error(w, 401, "unauthenticated")
		return actor, false
	}
	csrf, origin, method := r.Header.Get("X-CSRF-Token"), r.Header.Get("Origin"), r.Method
	body := internalhttp.IntrospectRequest{SessionCookie: &cookie, AccessToken: &token, CSRFToken: &csrf, Method: &method, Origin: &origin, Permission: &permission, RequireMFA: &mfa}
	status, b, err := c.Call(r.Context(), "POST", c.Config.IdentityURL+"/internal/v1/introspect", body, r)
	if err != nil || status >= 500 {
		Error(w, 503, "identity_unavailable")
		return actor, false
	}
	if status != 200 {
		if status != 403 {
			status = 401
		}
		Error(w, status, "access_denied")
		return actor, false
	}
	if json.Unmarshal(b, &actor) != nil || !actor.Active || actor.UserID == "" {
		Error(w, 401, "unauthenticated")
		return actor, false
	}
	if mfa && !actor.MFAVerified {
		Error(w, 403, "mfa_required")
		return actor, false
	}
	if permission != "" {
		allowed := false
		for _, p := range actor.Permissions {
			if p == permission {
				allowed = true
			}
		}
		if !allowed {
			Error(w, 403, "permission_denied")
			return actor, false
		}
	}
	return actor, true
}

type App interface {
	Routes(*http.ServeMux)
	Run(context.Context) error
	Ready(context.Context) error
}

func Serve(kind string, build func(Config, *pgxpool.Pool, *Client) (App, error)) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	migrating := len(os.Args) == 2 && os.Args[1] == "migrate"
	rekeying := len(os.Args) == 2 && os.Args[1] == "rekey"
	var c Config
	var err error
	if migrating || rekeying {
		c.DSN = os.Getenv("DATABASE_URL")
		if c.DSN == "" {
			err = errors.New("DATABASE_URL required")
		}
	} else {
		c, err = Load(kind)
	}
	if err != nil {
		slog.Error("configuration invalid", "reason", err.Error())
		os.Exit(1)
	}
	db, err := pgxpool.New(ctx, c.DSN)
	if err != nil {
		slog.Error("database configuration invalid")
		os.Exit(1)
	}
	defer db.Close()
	owner := kind
	if owner == "worker" {
		owner = "notification"
	}
	if migrating {
		if err = Migrate(ctx, db, owner); err != nil {
			slog.Error("migration failed", "reason", err.Error())
			os.Exit(1)
		}
		return
	}
	if rekeying {
		legacy, keyErr := loadKey("LEGACY_ENCRYPTION_KEY")
		if keyErr == nil {
			c.QuarantineKey, keyErr = loadKey("QUARANTINE_ENCRYPTION_KEY")
		}
		if keyErr == nil && owner == "notification" {
			c.DeliveryKey, keyErr = loadKey("DELIVERY_ENCRYPTION_KEY")
		}
		if keyErr == nil {
			keyErr = Rekey(ctx, db, owner, legacy, c.DeliveryKey, c.QuarantineKey)
		}
		if keyErr != nil {
			slog.Error("owner rekey failed", "reason", keyErr.Error())
			os.Exit(1)
		}
		return
	}
	if c.Cert == "" || c.Key == "" || c.Token == "" || c.Origin == "" {
		slog.Error("TLS, origin, service identity required")
		os.Exit(1)
	}
	client, err := NewClient(c)
	if err != nil {
		slog.Error("internal TLS invalid")
		os.Exit(1)
	}
	app, err := build(c, db, client)
	if err != nil {
		slog.Error("startup failed", "reason", err.Error())
		os.Exit(1)
	}
	telemetryLifetime, stopTelemetry := context.WithCancel(context.Background())
	defer stopTelemetry()
	InitTelemetry(telemetryLifetime, kind)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { JSON(w, 200, map[string]bool{"live": true}) })
	ready := func(ctx context.Context) error {
		var marker string
		if err := db.QueryRow(ctx, "SELECT owner FROM service_schema WHERE owner=$1 AND version=1", owner).Scan(&marker); err != nil {
			return err
		}
		return app.Ready(ctx)
	}
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if ready(ctx) != nil {
			Error(w, 503, "not_ready")
			return
		}
		JSON(w, 200, map[string]bool{"ready": true})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		state := 0
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if ready(ctx) == nil {
			state = 1
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		WriteMetrics(w)
		fmt.Fprintf(w, "service_ready %d\ndb_pool_acquired_connections %d\n", state, db.Stat().AcquiredConns())
		var quarantined int64
		_ = db.QueryRow(ctx, "SELECT count(*) FROM quarantine WHERE replayed_at IS NULL").Scan(&quarantined)
		fmt.Fprintf(w, "quarantine_pending %d\n", quarantined)
		if owner == "notification" {
			rows, err := db.Query(ctx, "SELECT status,count(*) FROM delivery_jobs GROUP BY status")
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var status string
					var n int64
					if rows.Scan(&status, &n) == nil {
						fmt.Fprintf(w, "delivery_jobs_total{status=%s} %d\n", strconv.Quote(status), n)
					}
				}
			}
			attemptRows, err := db.Query(ctx, "SELECT status,count(*) FROM delivery_attempts GROUP BY status")
			if err == nil {
				counts := map[string]int64{"sent": 0, "retry": 0, "failed": 0}
				valid := true
				for attemptRows.Next() {
					var status string
					var count int64
					if attemptRows.Scan(&status, &count) != nil {
						valid = false
						break
					}
					counts[status] = count
				}
				if attemptRows.Err() != nil {
					valid = false
				}
				attemptRows.Close()
				if valid {
					for _, status := range []string{"sent", "retry", "failed"} {
						fmt.Fprintf(w, "delivery_attempts_total{status=%s} %d\n", strconv.Quote(status), counts[status])
					}
				}
			}
			var age float64
			_ = db.QueryRow(ctx, "SELECT coalesce(extract(epoch from now()-min(created_at)),0) FROM delivery_jobs WHERE status IN ('pending','retry','leased')").Scan(&age)
			fmt.Fprintf(w, "delivery_oldest_age_seconds %f\n", age)
		}
	})
	app.Routes(mux)
	server := &http.Server{Addr: ":" + c.Port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		bounded, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		ctx, trace, finish := StartSpan(bounded, r.Header.Get("Traceparent"), r.Method+" http")
		r = r.WithContext(ctx)
		r.Header.Set("Traceparent", trace)
		rw := &ResponseStatus{ResponseWriter: w}
		defer func() {
			status := rw.Status
			if status == 0 {
				status = 200
			}
			finish(status >= 500)
			RecordHTTP(r.Method, r.Pattern, status, time.Since(start))
			slog.Info("http request", "method", r.Method, "route", r.Pattern, "status", status, "duration_ms", time.Since(start).Milliseconds(), "traceparent", trace)
		}()
		w.Header().Set("X-Request-ID", randomHex(16))
		w.Header().Set("Traceparent", trace)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(rw, r)
	}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	errorsCh := make(chan error, 2)
	appDone := make(chan struct{})
	go func() { defer close(appDone); errorsCh <- app.Run(ctx) }()
	go func() { errorsCh <- server.ListenAndServeTLS(c.Cert, c.Key) }()
	failed := false
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("service stopped", "reason", err.Error())
			failed = true
		}
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	select {
	case <-appDone:
	case <-time.After(20 * time.Second):
		slog.Error("background work drain timed out")
		failed = true
	}
	stopTelemetry()
	telemetryDrain, telemetryCancel := context.WithTimeout(context.Background(), 4*time.Second)
	WaitTelemetry(telemetryDrain)
	telemetryCancel()
	if failed {
		os.Exit(1)
	}
}
