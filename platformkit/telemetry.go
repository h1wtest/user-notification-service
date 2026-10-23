package platformkit

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This is the public technical adapter counterpart of internal/platform's
// dependency-free OTLP/HTTP exporter. Runtime state belongs to one Serve call.
type telemetryKey struct{}
type traceKey struct{}
type routeKey struct{}
type routeObservation struct{ request atomic.Pointer[http.Request] }

// RequestWithContext retains leaf ServeMux pattern attribution across technical
// deadline wrappers. Like ServeMux, wrappers must not mutate a request concurrently.
func RequestWithContext(r *http.Request, ctx context.Context) *http.Request {
	observation, observed := r.Context().Value(routeKey{}).(*routeObservation)
	if observed {
		ctx = context.WithValue(ctx, routeKey{}, observation)
	}
	next := r.WithContext(ctx)
	if observed {
		observation.request.Store(next)
	}
	return next
}

type traceContext struct{ traceID, spanID, flags string }
type spanValue struct {
	String string `json:"stringValue,omitempty"`
	Int    string `json:"intValue,omitempty"`
}
type spanAttribute struct {
	Key   string    `json:"key"`
	Value spanValue `json:"value"`
}
type spanLink struct {
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
}
type otlpSpan struct {
	TraceID      string          `json:"traceId"`
	SpanID       string          `json:"spanId"`
	ParentSpanID string          `json:"parentSpanId,omitempty"`
	Name         string          `json:"name"`
	Kind         int             `json:"kind"`
	Start        string          `json:"startTimeUnixNano"`
	End          string          `json:"endTimeUnixNano"`
	Attributes   []spanAttribute `json:"attributes"`
	Links        []spanLink      `json:"links,omitempty"`
	Status       struct {
		Code int `json:"code"`
	} `json:"status"`
}
type telemetry struct {
	service, endpoint string
	queue             chan otlpSpan
	stop, done        chan struct{}
	lifetime          context.Context
	cancel            context.CancelFunc
	once              sync.Once
	mu                sync.RWMutex
	closed            bool
	dropped           atomic.Uint64
	metrics           runtimeMetrics
}
type activeSpan struct {
	owner   *telemetry
	trace   traceContext
	item    otlpSpan
	sampled bool
}

func randomTraceHex(n int) string {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		panic("trace entropy unavailable")
	}
	return hex.EncodeToString(raw)
}
func parseTraceparent(raw string) (traceContext, bool) {
	// v00 is fixed length; future versions may carry extension fields.
	if len(raw) < 55 || raw[2] != '-' || raw[35] != '-' || raw[52] != '-' || raw[:2] == "ff" || raw[:2] == "00" && len(raw) != 55 || len(raw) > 55 && (raw[55] != '-' || len(raw) == 56) {
		return traceContext{}, false
	}
	for _, part := range []string{raw[:2], raw[3:35], raw[36:52], raw[53:55]} {
		for _, c := range part {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return traceContext{}, false
			}
		}
	}
	if raw[3:35] == strings.Repeat("0", 32) || raw[36:52] == strings.Repeat("0", 16) {
		return traceContext{}, false
	}
	return traceContext{raw[3:35], raw[36:52], raw[53:55]}, true
}
func (t traceContext) header() string {
	if t.traceID == "" {
		return ""
	}
	return "00-" + t.traceID + "-" + t.spanID + "-" + t.flags
}
func withTelemetry(ctx context.Context, t *telemetry) context.Context {
	return context.WithValue(ctx, telemetryKey{}, t)
}
func telemetryFrom(ctx context.Context) *telemetry {
	t, _ := ctx.Value(telemetryKey{}).(*telemetry)
	return t
}
func traceparentFrom(ctx context.Context) string {
	t, _ := ctx.Value(traceKey{}).(traceContext)
	return t.header()
}
func startSpan(ctx context.Context, parent string, kind int, link string) (context.Context, *activeSpan) {
	current, ok := parseTraceparent(parent)
	if parent == "" {
		current, ok = ctx.Value(traceKey{}).(traceContext)
	}
	parentID := ""
	if ok {
		parentID = current.spanID
	} else {
		current = traceContext{traceID: randomTraceHex(16), flags: "01"}
	}
	current.spanID = randomTraceHex(8)
	flag, _ := strconv.ParseUint(current.flags, 16, 8)
	owner := telemetryFrom(ctx)
	s := &activeSpan{owner: owner, trace: current, sampled: owner != nil && owner.endpoint != "" && flag&1 != 0}
	if s.sampled {
		s.item = otlpSpan{TraceID: current.traceID, SpanID: current.spanID, ParentSpanID: parentID, Kind: kind, Start: strconv.FormatInt(time.Now().UnixNano(), 10), Attributes: []spanAttribute{}}
		if linked, valid := parseTraceparent(link); valid {
			s.item.Links = []spanLink{{TraceID: linked.traceID, SpanID: linked.spanID}}
		}
	}
	return context.WithValue(ctx, traceKey{}, current), s
}
func (s *activeSpan) header() string { return s.trace.header() }
func (s *activeSpan) finish(name string, failed bool) {
	if s.owner == nil || !s.sampled {
		return
	}
	s.item.Name, s.item.End, s.item.Status.Code = name, strconv.FormatInt(time.Now().UnixNano(), 10), 1
	if failed {
		s.item.Status.Code = 2
	}
	s.owner.mu.RLock()
	defer s.owner.mu.RUnlock()
	if s.owner.closed {
		return
	}
	select {
	case s.owner.queue <- s.item:
	default:
		s.owner.dropped.Add(1)
	}
}
func newTelemetry(service, endpoint string) *telemetry {
	lifetime, cancel := context.WithCancel(context.Background())
	t := &telemetry{service: service, endpoint: strings.TrimRight(endpoint, "/"), stop: make(chan struct{}), done: make(chan struct{}), lifetime: lifetime, cancel: cancel}
	if endpoint == "" {
		close(t.done)
	} else {
		t.queue = make(chan otlpSpan, 256)
		go t.run()
	}
	return t
}
func (t *telemetry) close(ctx context.Context) {
	budget, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	interrupt := context.AfterFunc(budget, t.cancel)
	defer interrupt()
	t.once.Do(func() { t.mu.Lock(); t.closed = true; close(t.stop); t.mu.Unlock() })
	select {
	case <-t.done:
	case <-budget.Done():
		t.cancel()
		<-t.done
	}
	t.cancel()
}
func (t *telemetry) run() {
	defer close(t.done)
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: noRedirect}
	defer client.CloseIdleConnections()
	export := func(ctx context.Context, batch []otlpSpan) {
		// No URLs, headers, bodies, error strings, IDs, or user-controlled names.
		payload := map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": []spanAttribute{{Key: "service.name", Value: spanValue{String: t.service}}}}, "scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "platform.go"}, "spans": batch}}}}}
		raw, err := json.Marshal(payload)
		if err != nil {
			return
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint+"/v1/traces", bytes.NewReader(raw))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
		}
	}
	batch := func(first *otlpSpan) []otlpSpan {
		items := make([]otlpSpan, 0, 64)
		if first != nil {
			items = append(items, *first)
		}
		for len(items) < 64 {
			select {
			case item := <-t.queue:
				items = append(items, item)
			default:
				return items
			}
		}
		return items
	}
	for {
		if t.lifetime.Err() != nil {
			return
		}
		// Stop has priority over a ready queue: no normal exports after shutdown.
		select {
		case <-t.stop:
			goto flush
		default:
		}
		select {
		case <-t.stop:
			goto flush
		case first := <-t.queue:
			request, cancel := context.WithTimeout(t.lifetime, 3*time.Second)
			export(request, batch(&first))
			cancel()
		}
	}
flush:
	for t.lifetime.Err() == nil {
		items := batch(nil)
		if len(items) == 0 {
			return
		}
		export(t.lifetime, items)
	}
}

func telemetryHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parent := r.Header.Get("Traceparent")
		if len(r.Header.Values("Traceparent")) != 1 {
			parent = "invalid"
		}
		ctx, span := startSpan(r.Context(), parent, 2, "")
		observation := &routeObservation{}
		ctx = context.WithValue(ctx, routeKey{}, observation)
		r = r.WithContext(ctx)
		observation.request.Store(r)
		r.Header.Set("Traceparent", span.header())
		w.Header().Set("Traceparent", span.header())
		status := &telemetryResponse{ResponseWriter: w}
		start := time.Now()
		defer func() {
			code := status.status
			if code == 0 {
				code = 200
			}
			method, route := metricMethod(r.Method), observation.request.Load().Pattern
			if route == "" {
				route = "unmatched"
			}
			if span.sampled {
				span.item.Attributes = []spanAttribute{{Key: "http.request.method", Value: spanValue{String: method}}, {Key: "http.route", Value: spanValue{String: route}}, {Key: "http.response.status_code", Value: spanValue{Int: strconv.Itoa(code)}}}
			}
			span.finish(route, code >= 500)
			if t := telemetryFrom(ctx); t != nil {
				t.metrics.recordHTTP(method, route, code, time.Since(start))
			}
		}()
		next.ServeHTTP(status, r)
	})
}

type telemetryResponse struct {
	http.ResponseWriter
	status int
}

func (w *telemetryResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *telemetryResponse) WriteHeader(code int) {
	if code >= 100 && code < 200 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *telemetryResponse) Write(raw []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(raw)
}

func publicationRecord(ctx context.Context, p publication) (*kgo.Record, *activeSpan) {
	var metadata struct {
		Traceparent string `json:"traceparent"`
	}
	if len(p.Raw) <= bodyLimit {
		_ = json.Unmarshal(p.Raw, &metadata)
	}
	_, span := startSpan(ctx, metadata.Traceparent, 4, metadata.Traceparent)
	return &kgo.Record{Topic: p.Topic, Key: []byte(p.Key), Value: p.Raw, Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte(span.header())}}}, span
}
func consumerSpan(ctx context.Context, record *kgo.Record) (context.Context, *activeSpan) {
	var metadata struct {
		Traceparent string `json:"traceparent"`
	}
	if len(record.Value) <= bodyLimit {
		_ = json.Unmarshal(record.Value, &metadata)
	}
	parent := metadata.Traceparent
	var count int
	for _, header := range record.Headers {
		if strings.EqualFold(header.Key, "traceparent") {
			parent = string(header.Value)
			count++
		}
	}
	if count > 1 {
		parent = "invalid"
	}
	return startSpan(ctx, parent, 5, metadata.Traceparent)
}
func runtimeTelemetry(service string) *telemetry {
	return newTelemetry(service, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
}
