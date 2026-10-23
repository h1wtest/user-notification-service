package platform

import (
	"bytes"
	"strings"
	"testing"
)

func TestRevokedPartitionDoesNotKeepStaleLagSample(t *testing.T) {
	RecordLag("test.revoked", 3, 4)
	var before bytes.Buffer
	WriteMetrics(&before)
	if !strings.Contains(before.String(), `kafka_sampled_committed_lag{topic="test.revoked",partition="3"} 4`) {
		t.Fatal("durable lag sample missing")
	}
	ClearLag(map[string][]int32{"test.revoked": {3}})
	var after bytes.Buffer
	WriteMetrics(&after)
	if strings.Contains(after.String(), `topic="test.revoked"`) {
		t.Fatal("revoked assignment kept stale sample")
	}
}
