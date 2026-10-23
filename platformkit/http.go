package platformkit

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const bodyLimit = 1 << 20

type Actor struct {
	UserID, SessionID, Email string
	Roles, Permissions       []string
	MFAVerified, Verified    bool
}

type Client struct {
	HTTP   *http.Client
	Config Config
}

func NewClient(c Config) (*Client, error) {
	pem, err := os.ReadFile(c.CA)
	if err != nil {
		return nil, errors.New("internal CA required")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid internal CA")
	}
	return &Client{Config: c, HTTP: &http.Client{Timeout: 5 * time.Second, CheckRedirect: noRedirect, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ResponseHeaderTimeout: 4 * time.Second, TLSHandshakeTimeout: 3 * time.Second, MaxIdleConns: 64, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second}}}, nil
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func JSON(w http.ResponseWriter, status int, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		raw = []byte(`{"error":"encoding_failed","code":"encoding_failed"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(append(raw, '\n'))
}
func Error(w http.ResponseWriter, status int, code string) {
	JSON(w, status, struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}{code, code})
}

func Decode(w http.ResponseWriter, r *http.Request, target any) bool {
	if r.Body == nil {
		Error(w, 400, "invalid_request")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
	raw, err := io.ReadAll(r.Body)
	if err != nil || decodeStrict(raw, target) != nil {
		Error(w, 400, "invalid_request")
		return false
	}
	return true
}

// decodeStrict rejects duplicate keys before decoding; DisallowUnknownFields by
// itself would accept the last duplicate, including security-relevant fields.
func decodeStrict(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > bodyLimit || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("invalid JSON body")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := uniqueValue(d, reflect.TypeOf(target)); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("multiple JSON values")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		return err
	}
	return nil
}

func uniqueValue(d *json.Decoder, target reflect.Type) error {
	for target != nil && target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if target == reflect.TypeFor[json.RawMessage]() {
		target = nil
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]struct{})
		for d.More() {
			token, err = d.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if _, exists := keys[key]; exists {
				return errors.New("duplicate object key")
			}
			keys[key] = struct{}{}
			var member reflect.Type
			if target != nil {
				switch target.Kind() {
				case reflect.Struct:
					var found bool
					member, found = exactJSONField(target, key)
					if !found {
						return errors.New("unknown or noncanonical object member")
					}
				case reflect.Map:
					member = target.Elem()
				}
			}
			if err = uniqueValue(d, member); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			var member reflect.Type
			if target != nil && (target.Kind() == reflect.Slice || target.Kind() == reflect.Array) {
				member = target.Elem()
			}
			if err = uniqueValue(d, member); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}

// encoding/json deliberately accepts case-insensitive struct field aliases.
// A strict wire boundary must bind exact member names without case-folding
// user-owned map keys. Explicit fields win over promoted anonymous fields.
func exactJSONField(target reflect.Type, key string) (reflect.Type, bool) {
	for i := range target.NumField() {
		field := target.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "-" {
			continue
		}
		if name == "" {
			if field.Anonymous {
				continue
			}
			name = field.Name
		}
		if name == key {
			return field.Type, true
		}
	}
	var match reflect.Type
	for i := range target.NumField() {
		field := target.Field(i)
		if !field.Anonymous || field.Tag.Get("json") != "" {
			continue
		}
		typeOf := field.Type
		if typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if typeOf == target || typeOf.Kind() != reflect.Struct {
			continue
		}
		if promoted, found := exactJSONField(typeOf, key); found {
			if match != nil {
				return nil, false
			}
			match = promoted
		}
	}
	return match, match != nil
}

func forwarded(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/internal/") && r.Header.Get("X-Service-Token") != ""
}

func actorCredentials(r *http.Request) (session, token string, err error) {
	if len(r.Header.Values("Authorization")) > 1 {
		return "", "", errors.New("ambiguous authorization")
	}
	if authorization := r.Header.Get("Authorization"); authorization != "" {
		if !strings.HasPrefix(authorization, "Bearer ") {
			return "", "", errors.New("invalid bearer")
		}
		token = strings.TrimPrefix(authorization, "Bearer ")
		if token == "" || strings.ContainsAny(token, " \t\r\n") {
			return "", "", errors.New("invalid bearer")
		}
	}
	for _, cookie := range r.Cookies() {
		if cookie.Name == "sso_session" {
			if session != "" {
				return "", "", errors.New("ambiguous session")
			}
			session = cookie.Value
		}
	}
	// The owner must authenticate the workload caller before Authenticate on an
	// internal route. Never accept these headers at a public API boundary.
	if forwarded(r) {
		if session == "" {
			session = r.Header.Get("X-Actor-Session")
		}
		if token == "" {
			token = r.Header.Get("X-Actor-Token")
		}
	}
	return session, token, nil
}

func ForwardActor(dst, src *http.Request) {
	for key := range dst.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-actor-") {
			dst.Header.Del(key)
		}
	}
	for _, header := range []string{"Cookie", "Authorization", "X-CSRF-Token", "Origin", "Traceparent"} {
		dst.Header.Del(header)
		for _, value := range src.Header.Values(header) {
			dst.Header.Add(header, value)
		}
	}
	session, token, err := actorCredentials(src)
	if err != nil {
		return
	}
	if session != "" {
		dst.Header.Set("X-Actor-Session", session)
	}
	if token != "" {
		dst.Header.Set("X-Actor-Token", token)
	}
}

func (c *Client) Call(ctx context.Context, method, target string, body any, source *http.Request) (int, []byte, error) {
	if c == nil || c.HTTP == nil || c.Config.Token == "" || !httpsURL(target) {
		return 503, nil, errors.New("HTTPS workload client required")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 503, nil, errors.New("invalid dependency request")
		}
		if len(raw) > bodyLimit {
			return 503, nil, errors.New("dependency request too large")
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 503, nil, errors.New("invalid dependency target")
	}
	if source != nil {
		ForwardActor(req, source)
	}
	parent := traceparentFrom(ctx)
	if parent == "" && source != nil {
		parent = source.Header.Get("Traceparent")
	}
	spanCtx, span := startSpan(ctx, parent, 3, "")
	req = req.WithContext(spanCtx)
	req.Header.Set("Traceparent", span.header())
	spanStatus := http.StatusServiceUnavailable
	defer func() {
		if span.sampled {
			span.item.Attributes = []spanAttribute{{Key: "http.request.method", Value: spanValue{String: metricMethod(method)}}, {Key: "http.response.status_code", Value: spanValue{Int: strconv.Itoa(spanStatus)}}}
			span.finish(metricMethod(method)+" http.client", spanStatus >= 500)
		}
	}()
	req.Header.Set("X-Service-Token", c.Config.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := *c.HTTP
	client.CheckRedirect = noRedirect
	resp, err := client.Do(req)
	if err != nil {
		return 503, nil, errors.New("dependency unavailable")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, bodyLimit+1))
	if err != nil || len(raw) > bodyLimit {
		return 503, nil, errors.New("invalid dependency response")
	}
	spanStatus = resp.StatusCode
	return resp.StatusCode, raw, nil
}

func (c *Client) Authenticate(w http.ResponseWriter, r *http.Request, permission string, admin bool) (Actor, bool) {
	session, token, err := actorCredentials(r)
	if err != nil || session == "" && token == "" {
		Error(w, 401, "unauthenticated")
		return Actor{}, false
	}
	// v2 explicitly retains the existing v1 credential request shape. No
	// endpoint downgrade/fallback: v1's permission model cannot authorize v2.
	body := struct {
		Session    string `json:"session_cookie"`
		Token      string `json:"access_token"`
		CSRF       string `json:"csrf_token"`
		Method     string `json:"method"`
		Origin     string `json:"origin"`
		Permission string `json:"permission"`
		MFA        bool   `json:"require_mfa"`
	}{session, token, r.Header.Get("X-CSRF-Token"), r.Method, r.Header.Get("Origin"), permission, admin}
	status, raw, err := c.Call(r.Context(), "POST", strings.TrimRight(c.Config.IdentityURL, "/")+"/internal/v2/introspect", body, r)
	if err != nil || status >= 500 || status != 200 && status != 401 && status != 403 {
		Error(w, 503, "identity_unavailable")
		return Actor{}, false
	}
	if status != 200 {
		Error(w, status, "access_denied")
		return Actor{}, false
	}
	var decision struct {
		Active            bool       `json:"active"`
		UserID            string     `json:"user_id"`
		SessionID         string     `json:"session_id"`
		Email             string     `json:"email"`
		Roles             []string   `json:"roles"`
		Permissions       []string   `json:"permissions"`
		MFA               bool       `json:"mfa_verified"`
		Verified          bool       `json:"is_verified"`
		ReauthenticatedAt *time.Time `json:"reauthenticated_at,omitempty"`
	}
	if decodeStrict(raw, &decision) != nil || decision.UserID == "" || decision.SessionID == "" {
		Error(w, 503, "identity_unavailable")
		return Actor{}, false
	}
	if !decision.Active {
		Error(w, 401, "unauthenticated")
		return Actor{}, false
	}
	if admin && !decision.MFA {
		Error(w, 403, "mfa_required")
		return Actor{}, false
	}
	if permission != "" {
		allowed := false
		for _, p := range decision.Permissions {
			if p == permission {
				allowed = true
				break
			}
		}
		if !allowed {
			Error(w, 403, "permission_denied")
			return Actor{}, false
		}
	}
	return Actor{UserID: decision.UserID, SessionID: decision.SessionID, Email: decision.Email, Roles: decision.Roles, Permissions: decision.Permissions, MFAVerified: decision.MFA, Verified: decision.Verified}, true
}
