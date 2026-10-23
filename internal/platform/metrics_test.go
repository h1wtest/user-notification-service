package platform

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestHTTPDurationExportsObservedHistogramBuckets(t *testing.T) {
	RecordHTTP("GET", "GET /api/v1/users/me", 200, 25*time.Millisecond)
	var output bytes.Buffer
	WriteMetrics(&output)
	text := output.String()
	if !strings.Contains(text, `http_request_duration_seconds_bucket{method="GET",route="GET /api/v1/users/me",status="200",le="+Inf"} 1`) {
		t.Fatalf("duration histogram observation missing: %s", text)
	}
}
