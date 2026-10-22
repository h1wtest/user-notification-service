package mailer

import (
	"bytes"
	"fmt"
	"html/template"
	"net"
	"net/smtp"
	"strconv"
	"time"
)

type Mailer struct {
	smtpHost string
	smtpPort int
	from     string
}

func New(smtpHost string, smtpPort int, from string) *Mailer {
	return &Mailer{
		smtpHost: smtpHost,
		smtpPort: smtpPort,
		from:     from,
	}
}

func (m *Mailer) SendVerificationEmail(to string, code string) error {
	subject := "Код подтверждения регистрации"
	body, err := renderVerificationTemplate(code)
	if err != nil {
		return fmt.Errorf("failed to render verification email template: %w", err)
	}

	return m.sendHTML(to, subject, body)
}

func (m *Mailer) SendLoginAlertEmail(to, ip, userAgent, device string, loginTime time.Time) error {
	subject := "Новый вход в ваш аккаунт"
	body, err := renderLoginAlertTemplate(ip, userAgent, device, loginTime)
	if err != nil {
		return fmt.Errorf("failed to render login alert template: %w", err)
	}

	return m.sendHTML(to, subject, body)
}

func (m *Mailer) sendHTML(to, subject, htmlBody string) error {
	addr := net.JoinHostPort(m.smtpHost, strconv.Itoa(m.smtpPort))

	headers := make(map[string]string)
	headers["From"] = m.from
	headers["To"] = to
	headers["Subject"] = subject
	headers["MIME-Version"] = "1.0"
	headers["Content-Type"] = "text/html; charset=UTF-8"

	message := ""
	for k, v := range headers {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + htmlBody

	// Local development mailer (Mailpit doesn't require AUTH)
	return smtp.SendMail(addr, nil, m.from, []string{to}, []byte(message))
}

const verificationHTML = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Подтверждение регистрации</title>
</head>
<body style="margin: 0; padding: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0f172a; color: #f8fafc;">
    <div style="max-width: 520px; margin: 0 auto; background-color: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 32px; box-shadow: 0 4px 12px rgba(0,0,0,0.3);">
        <h2 style="margin-top: 0; color: #38bdf8; font-size: 24px; font-weight: 700; text-align: center;">Добро пожаловать!</h2>
        <p style="font-size: 15px; color: #cbd5e1; line-height: 1.6; text-align: center;">
            Вы успешно зарегистрировали аккаунт. Для подтверждения адреса электронной почты введите следующий одноразовый код:
        </p>
        <div style="margin: 32px auto; text-align: center;">
            <span style="display: inline-block; font-size: 36px; font-weight: 800; letter-spacing: 8px; color: #0284c7; background-color: #0f172a; padding: 16px 28px; border-radius: 8px; border: 1px solid #0284c7; font-family: monospace;">{{.Code}}</span>
        </div>
        <p style="font-size: 13px; color: #94a3b8; text-align: center; margin-bottom: 0;">
            Код действует 24 часа. Если вы не регистрировались на нашей платформе, просто проигнорируйте это письмо.
        </p>
    </div>
</body>
</html>
`

const loginAlertHTML = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Уведомление о входе</title>
</head>
<body style="margin: 0; padding: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #0f172a; color: #f8fafc;">
    <div style="max-width: 520px; margin: 0 auto; background-color: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 32px; box-shadow: 0 4px 12px rgba(0,0,0,0.3);">
        <h2 style="margin-top: 0; color: #fbbf24; font-size: 22px; font-weight: 700;">Новый вход в ваш аккаунт</h2>
        <p style="font-size: 15px; color: #cbd5e1; line-height: 1.6;">
            Мы зафиксировали вход в ваш аккаунт с новыми параметрами:
        </p>
        <div style="background-color: #0f172a; border-radius: 8px; padding: 16px; margin: 20px 0; font-size: 14px; border-left: 4px solid #fbbf24;">
            <p style="margin: 4px 0; color: #94a3b8;"><strong>Устройство:</strong> <span style="color: #f1f5f9;">{{.Device}}</span></p>
            <p style="margin: 4px 0; color: #94a3b8;"><strong>IP адрес:</strong> <span style="color: #f1f5f9;">{{.IP}}</span></p>
            <p style="margin: 4px 0; color: #94a3b8;"><strong>Браузер / Агент:</strong> <span style="color: #f1f5f9;">{{.UserAgent}}</span></p>
            <p style="margin: 4px 0; color: #94a3b8;"><strong>Время:</strong> <span style="color: #f1f5f9;">{{.Time}}</span></p>
        </div>
        <p style="font-size: 13px; color: #f87171; line-height: 1.5;">
            Если это были не вы, немедленно войдите в Личный Кабинет и завершите подозрительную сессию во вкладке «Безопасность».
        </p>
    </div>
</body>
</html>
`

func renderVerificationTemplate(code string) (string, error) {
	tmpl, err := template.New("verify").Parse(verificationHTML)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, map[string]string{"Code": code}); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func renderLoginAlertTemplate(ip, userAgent, device string, loginTime time.Time) (string, error) {
	tmpl, err := template.New("login_alert").Parse(loginAlertHTML)
	if err != nil {
		return "", err
	}
	if device == "" {
		device = "Неизвестное устройство"
	}
	var buf bytes.Buffer
	data := map[string]string{
		"IP":        ip,
		"UserAgent": userAgent,
		"Device":    device,
		"Time":      loginTime.Format("02.01.2006 15:04:05 UTC"),
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
