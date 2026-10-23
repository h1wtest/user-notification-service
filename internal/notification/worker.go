package notification

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"
	"user-notification-service/internal/platform"
)

type Job struct {
	ID, EventID, Channel, EncryptedBody, MessageID, LeaseToken, Traceparent string
	Attempts                                                                int
}

func (s *PostgresJobs) Claim(ctx context.Context) (Job, error) {
	var j Job
	err := s.DB.QueryRow(ctx, `WITH exhausted AS (UPDATE delivery_jobs SET status='failed',ambiguous=ambiguous OR status='leased',lease_until=NULL,lease_token=NULL,last_error='attempts_exhausted' WHERE attempts>=8 AND status IN ('pending','retry','leased') AND (status<>'leased' OR lease_until<now()) RETURNING id,attempts), exhausted_audit AS (INSERT INTO delivery_attempts(job_id,attempt,status) SELECT id,attempts,'failed' FROM exhausted), candidate AS (SELECT id FROM delivery_jobs WHERE attempts<8 AND ((status IN ('pending','retry') AND next_attempt_at<=now()) OR (status='leased' AND lease_until<now())) ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE delivery_jobs j SET status='leased',attempts=attempts+1,lease_token=$1,lease_until=now()+interval '60 seconds',ambiguous=j.ambiguous OR j.status='leased' FROM candidate c WHERE j.id=c.id RETURNING j.id,j.event_id,j.channel,j.encrypted_body,j.message_id,j.lease_token,j.attempts,coalesce(j.traceparent,'')`, uuid.NewString()).Scan(&j.ID, &j.EventID, &j.Channel, &j.EncryptedBody, &j.MessageID, &j.LeaseToken, &j.Attempts, &j.Traceparent)
	return j, err
}
func backoff(attempt int) time.Duration {
	if attempt > 8 {
		attempt = 8
	}
	base := time.Second * time.Duration(1<<attempt)
	var b [8]byte
	_, _ = rand.Read(b[:])
	return base + time.Duration(binary.LittleEndian.Uint64(b[:])%uint64(base/2+1))
}
func (s *PostgresJobs) Finish(ctx context.Context, j Job, success, permanent bool, reason string) error {
	status := "retry"
	var delivered *time.Time
	next := time.Now().Add(backoff(j.Attempts))
	if success {
		status = "sent"
		now := time.Now()
		delivered = &now
		reason = ""
	} else if permanent || j.Attempts >= 8 {
		status = "failed"
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE delivery_jobs SET status=$3,next_attempt_at=$4,lease_until=NULL,lease_token=NULL,last_error=nullif($5,''),delivered_at=$6,ambiguous=ambiguous OR ($3<>'sent' AND $5='provider_outcome_unknown'),encrypted_body=CASE WHEN $3='sent' THEN '' ELSE encrypted_body END WHERE id=$1 AND lease_token=$2 AND status='leased'`, j.ID, j.LeaseToken, status, next, reason, delivered)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("delivery lease lost")
	}
	if _, err = tx.Exec(ctx, "INSERT INTO delivery_attempts(job_id,attempt,status) VALUES($1,$2,$3)", j.ID, j.Attempts, status); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Service) Work(ctx context.Context) error {
	for ctx.Err() == nil {
		j, err := s.Jobs.Claim(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			if !platform.Pause(ctx, 300*time.Millisecond) {
				return nil
			}
			continue
		}
		if err != nil {
			if !platform.Pause(ctx, time.Second) {
				return nil
			}
			continue
		}
		spanCtx, trace, finishSpan := platform.StartSpan(ctx, j.Traceparent, "delivery."+j.Channel)
		j.Traceparent = trace
		raw, err := platform.Decrypt(s.Config.DeliveryKey, j.ID, j.EncryptedBody)
		var body Body
		permanent := false
		reason := ""
		if err != nil || json.Unmarshal(raw, &body) != nil {
			err = errors.New("invalid encrypted job")
			permanent = true
			reason = "invalid_job"
		} else if body.ExpiresAt != nil && !time.Now().Before(*body.ExpiresAt) {
			err = errors.New("intent expired")
			permanent = true
			reason = "intent_expired"
		} else {
			deadline, cancel := context.WithTimeout(spanCtx, 15*time.Second)
			provider := s.Providers[j.Channel]
			if provider == nil {
				permanent, reason, err = true, "invalid_channel", errors.New("unknown delivery channel")
			} else {
				permanent, reason, err = provider.Send(deadline, j, body)
			}
			cancel()
		}
		finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		saveErr := s.Jobs.Finish(finishCtx, j, err == nil, permanent, reason)
		cancel()
		finishSpan(err != nil || saveErr != nil)
	}
	return nil
}
func (s *Service) sendSMTP(ctx context.Context, j Job, b Body) (bool, string, error) {
	from, err := mail.ParseAddress(s.Config.SMTPFrom)
	if err != nil {
		return true, "invalid_sender", err
	}
	to, err := mail.ParseAddress(b.To)
	if err != nil {
		return true, "invalid_recipient", err
	}
	if strings.ContainsAny(b.Subject, "\r\n") || strings.ContainsAny(j.MessageID, "\r\n") {
		return true, "invalid_headers", errors.New("invalid headers")
	}
	address := net.JoinHostPort(s.Config.SMTPHost, s.Config.SMTPPort)
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return false, "smtp_unavailable", err
	}
	defer conn.Close()
	stopCancellation := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancellation()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	client, err := smtp.NewClient(conn, s.Config.SMTPHost)
	if err != nil {
		return smtpError(err, false)
	}
	defer client.Close()
	if !s.Config.SMTPInsecure {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return true, "smtp_tls_required", errors.New("STARTTLS unavailable")
		}
		if err = client.StartTLS(&tls.Config{ServerName: s.Config.SMTPHost, MinVersion: tls.VersionTLS12}); err != nil {
			return false, "smtp_tls_failed", err
		}
	}
	if s.Config.SMTPUser != "" {
		if err = client.Auth(smtp.PlainAuth("", s.Config.SMTPUser, s.Config.SMTPPassword, s.Config.SMTPHost)); err != nil {
			return smtpError(err, false)
		}
	}
	if err = client.Mail(from.Address); err != nil {
		return smtpError(err, false)
	}
	if err = client.Rcpt(to.Address); err != nil {
		return smtpError(err, false)
	}
	writer, err := client.Data()
	if err != nil {
		return smtpError(err, false)
	}
	message := "From: " + from.String() + "\r\nTo: " + to.String() + "\r\nSubject: " + b.Subject + "\r\nMessage-ID: " + j.MessageID + "\r\nDate: " + time.Now().UTC().Format(time.RFC1123Z) + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n" + strings.ReplaceAll(strings.ReplaceAll(b.Text, "\r\n", "\n"), "\n", "\r\n")
	if _, err = io.Copy(writer, bufio.NewReader(strings.NewReader(message))); err != nil {
		return smtpError(err, true)
	}
	if err = writer.Close(); err != nil {
		return smtpError(err, true)
	}
	_ = client.Quit()
	return false, "", nil
}
func smtpError(err error, ambiguous bool) (bool, string, error) {
	var protocol *textproto.Error
	if errors.As(err, &protocol) {
		if protocol.Code >= 500 {
			return true, "smtp_rejected", err
		}
		return false, "smtp_temporary", err
	}
	if ambiguous {
		return false, "provider_outcome_unknown", err
	}
	return false, "smtp_unavailable", err
}
func (s *Service) sendTelegram(ctx context.Context, j Job, b Body) (bool, string, error) {
	if s.Config.TelegramToken == "" {
		return true, "telegram_not_configured", errors.New("telegram unavailable")
	}
	if _, err := strconv.ParseInt(b.To, 10, 64); err != nil {
		return true, "invalid_chat", err
	}
	payload, _ := json.Marshal(map[string]any{"chat_id": b.To, "text": b.Subject + "\n\n" + b.Text})
	request, err := http.NewRequestWithContext(ctx, "POST", s.Config.TelegramBase+"/bot"+s.Config.TelegramToken+"/sendMessage", strings.NewReader(string(payload)))
	if err != nil {
		return true, "telegram_configuration", errors.New("invalid endpoint")
	}
	if request.URL.Scheme != "https" {
		return true, "telegram_tls_required", errors.New("HTTPS required")
	}
	request.Header.Set("Content-Type", "application/json")
	if j.Traceparent != "" {
		request.Header.Set("Traceparent", j.Traceparent)
	}
	response, err := s.Client.HTTP.Do(request)
	if err != nil {
		return false, "provider_outcome_unknown", errors.New("telegram unavailable")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 65536))
	var result struct {
		OK        bool `json:"ok"`
		ErrorCode int  `json:"error_code"`
	}
	if err != nil || json.Unmarshal(data, &result) != nil {
		return false, "provider_outcome_unknown", errors.New("invalid telegram response")
	}
	if result.OK && response.StatusCode == 200 {
		return false, "", nil
	}
	permanent := response.StatusCode == 400 || response.StatusCode == 401 || response.StatusCode == 403
	return permanent, "telegram_rejected", fmt.Errorf("telegram status %d", response.StatusCode)
}
