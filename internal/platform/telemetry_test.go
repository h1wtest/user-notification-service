package platform

import (
	"context"
	"strings"
	"testing"
)

func TestTracePropagationPreservesValidParentAndRejectsZeroIdentity(t *testing.T) {
	parent := "00-11111111111111111111111111111111-2222222222222222-01"
	_, header, finish := StartSpan(context.Background(), parent, "test")
	finish(false)
	if !strings.HasPrefix(header, "00-11111111111111111111111111111111-") || header == parent {
		t.Fatal("child did not retain trace and replace span")
	}
	_, header, finish = StartSpan(context.Background(), "00-00000000000000000000000000000000-0000000000000000-01", "test")
	finish(true)
	if strings.HasPrefix(header, "00-00000000000000000000000000000000-") {
		t.Fatal("invalid zero trace propagated")
	}
}
