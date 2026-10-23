package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequiredDependencyFailureMakesNotReady(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	c := &Client{Config: Config{IdentityURL: server.URL}, HTTP: server.Client()}
	if c.DependencyReady(context.Background(), server.URL) == nil {
		t.Fatal("unavailable identity reported ready")
	}
}
