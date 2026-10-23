package notification

import (
	"context"
	"net"
	"testing"
	"time"

	"user-notification-service/internal/platform"
)

func TestSMTPContextCancellationClosesBlockedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		_, _ = conn.Read(make([]byte, 1))
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	s := New(nil, platform.Config{SMTPHost: host, SMTPPort: port, SMTPFrom: "no-reply@example.test", SMTPInsecure: true}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := s.sendSMTP(ctx, Job{MessageID: "<stable@sso.local>"}, Body{To: "user@example.test", Subject: "test"})
		done <- err
	}()
	<-accepted
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled SMTP unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("SMTP ignored cancellation and blocked shutdown")
	}
}
