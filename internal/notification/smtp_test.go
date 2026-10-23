package notification

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestSMTPTemporaryRejectionRemainsRetryable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		fmt.Fprint(conn, "220 test ESMTP\r\n")
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "EHLO"):
				fmt.Fprint(conn, "250 test\r\n")
			case strings.HasPrefix(line, "MAIL FROM"):
				fmt.Fprint(conn, "451 temporary outage\r\n")
				return
			default:
				fmt.Fprint(conn, "250 ok\r\n")
			}
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	s := New(nil, platform.Config{SMTPHost: host, SMTPPort: port, SMTPFrom: "no-reply@example.test", SMTPInsecure: true}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	permanent, reason, err := s.sendSMTP(ctx, Job{MessageID: "<stable@example.test>"}, Body{To: "user@example.test", Subject: "Login", Text: "Body"})
	if err == nil || permanent || reason != "smtp_temporary" {
		t.Fatalf("temporary failure lost: %v %s %v", permanent, reason, err)
	}
	<-done
}
func TestSMTPRejectsHeaderInjectionBeforeConnecting(t *testing.T) {
	s := New(nil, platform.Config{SMTPFrom: "no-reply@example.test"}, nil)
	permanent, reason, err := s.sendSMTP(context.Background(), Job{MessageID: "<valid@example.test>"}, Body{To: "user@example.test", Subject: "Login\r\nBcc: victim@example.test"})
	if err == nil || !permanent || reason != "invalid_headers" {
		t.Fatal("SMTP header injection accepted")
	}
}
