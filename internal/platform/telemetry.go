package platform

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type span struct {
	TraceID      string         `json:"traceId"`
	SpanID       string         `json:"spanId"`
	ParentSpanID string         `json:"parentSpanId,omitempty"`
	Name         string         `json:"name"`
	Kind         int            `json:"kind"`
	Start        string         `json:"startTimeUnixNano"`
	End          string         `json:"endTimeUnixNano"`
	Attributes   []any          `json:"attributes"`
	Status       map[string]any `json:"status"`
}

var spans = make(chan span, 256)
var telemetryOnce sync.Once
var telemetryDone = make(chan struct{})

func randomHex(n int) string { b := make([]byte, n); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func StartSpan(ctx context.Context, parent, name string) (context.Context, string, func(bool)) {
	parts := strings.Split(parent, "-")
	trace, parentID := randomHex(16), ""
	if len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16 && parts[1] != strings.Repeat("0", 32) && parts[2] != strings.Repeat("0", 16) {
		if _, err := hex.DecodeString(parts[1] + parts[2]); err == nil {
			trace, parentID = parts[1], parts[2]
		}
	}
	id := randomHex(8)
	start := time.Now()
	header := "00-" + trace + "-" + id + "-01"
	return ctx, header, func(failed bool) {
		code := 1
		if failed {
			code = 2
		}
		item := span{TraceID: trace, SpanID: id, ParentSpanID: parentID, Name: name, Kind: 1, Start: strconv.FormatInt(start.UnixNano(), 10), End: strconv.FormatInt(time.Now().UnixNano(), 10), Attributes: []any{}, Status: map[string]any{"code": code}}
		select {
		case spans <- item:
		default:
		}
	}
}
func InitTelemetry(lifetime context.Context, service string) {
	telemetryOnce.Do(func() {
		endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if endpoint == "" {
			close(telemetryDone)
			return
		}
		go func() {
			defer close(telemetryDone)
			client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			export := func(ctx context.Context, batch []span) {
				payload := map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": []any{map[string]any{"key": "service.name", "value": map[string]string{"stringValue": Env("OTEL_SERVICE_NAME", service)}}}}, "scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "platform.go"}, "spans": batch}}}}}
				body, err := json.Marshal(payload)
				if err != nil {
					return
				}
				req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(endpoint, "/")+"/v1/traces", bytes.NewReader(body))
				if err != nil {
					return
				}
				req.Header.Set("Content-Type", "application/json")
				response, err := client.Do(req)
				if err == nil {
					_ = response.Body.Close()
				}
			}
			for {
				select {
				case item := <-spans:
					batch := []span{item}
					for len(batch) < 64 {
						select {
						case next := <-spans:
							batch = append(batch, next)
						default:
							goto send
						}
					}
				send:
					requestCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					export(requestCtx, batch)
					cancel()
				case <-lifetime.Done():
					flush, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					for {
						batch := make([]span, 0, 64)
						for len(batch) < 64 {
							select {
							case item := <-spans:
								batch = append(batch, item)
							default:
								goto flushBatch
							}
						}
					flushBatch:
						if len(batch) == 0 {
							return
						}
						export(flush, batch)
						if flush.Err() != nil {
							return
						}
					}
				}
			}
		}()
	})
}
func WaitTelemetry(ctx context.Context) {
	select {
	case <-telemetryDone:
	case <-ctx.Done():
	}
}

type ResponseStatus struct {
	http.ResponseWriter
	Status int
}

func (w *ResponseStatus) WriteHeader(status int) {
	if w.Status == 0 {
		w.Status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *ResponseStatus) Write(b []byte) (int, error) {
	if w.Status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
