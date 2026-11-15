// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package mail

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strconv"
	"strings"
	"time"
)

const smtpTimeout = 15 * time.Second

type smtpSender struct {
	host               string
	port               int
	username, password string
	from               string
	startTLS           bool
	fromAddr           *mail.Address
	fromErr            error
}

// NewSMTP returns a Sender that talks to one SMTP server. With startTLS the send fails unless the
// server offers STARTTLS; it never falls back to plain text. A username turns on PLAIN auth.
func NewSMTP(host string, port int, username, password, from string, startTLS bool) Sender {
	s := &smtpSender{host: host, port: port, username: username, password: password, from: from, startTLS: startTLS}
	s.fromAddr, s.fromErr = mail.ParseAddress(from)
	return s
}

func (s *smtpSender) Send(ctx context.Context, m Message) error {
	if s.fromErr != nil {
		return fmt.Errorf("mail: invalid from address: %w", s.fromErr)
	}
	for _, v := range []string{m.To, m.Subject} {
		if strings.ContainsAny(v, "\r\n") {
			return errors.New("mail: header value contains a line break")
		}
	}
	to, err := mail.ParseAddress(m.To)
	if err != nil {
		return fmt.Errorf("mail: invalid recipient: %w", err)
	}
	msg := s.build(to, m)

	ctx, cancel := context.WithTimeout(ctx, smtpTimeout)
	defer cancel()
	addr := net.JoinHostPort(s.host, strconv.Itoa(s.port))
	conn, err := (&net.Dialer{Timeout: smtpTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("mail: connect: %w", err)
	}
	defer conn.Close()
	// The deadline bounds every read and write; cancelling the context closes the connection.
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	c, err := smtp.NewClient(conn, s.host)
	if err != nil {
		return fmt.Errorf("mail: greeting: %w", err)
	}
	defer c.Close()
	if err := c.Hello("localhost"); err != nil {
		return fmt.Errorf("mail: hello: %w", err)
	}
	if s.startTLS {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return errors.New("mail: server does not offer STARTTLS")
		}
		if err := c.StartTLS(&tls.Config{ServerName: s.host, MinVersion: tls.VersionTLS12}); err != nil {
			return fmt.Errorf("mail: starttls: %w", err)
		}
	}
	if s.username != "" {
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("mail: server does not offer AUTH")
		}
		if err := c.Auth(plainAuth{s.username, s.password, s.host}); err != nil {
			return fmt.Errorf("mail: auth: %w", err)
		}
	}
	if err := c.Mail(s.fromAddr.Address); err != nil {
		return fmt.Errorf("mail: sender: %w", err)
	}
	if err := c.Rcpt(to.Address); err != nil {
		return fmt.Errorf("mail: recipient: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("mail: data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("mail: write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("mail: send: %w", err)
	}
	_ = c.Quit()
	return nil
}

func (s *smtpSender) build(to *mail.Address, m Message) []byte {
	domain := "localhost"
	if i := strings.LastIndex(s.fromAddr.Address, "@"); i >= 0 {
		domain = s.fromAddr.Address[i+1:]
	}
	var id [12]byte
	_, _ = rand.Read(id[:])
	body := strings.ReplaceAll(strings.ReplaceAll(m.Text, "\r\n", "\n"), "\n", "\r\n")
	if !strings.HasSuffix(body, "\r\n") {
		body += "\r\n"
	}
	var b strings.Builder
	h := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	h("Date", time.Now().Format(time.RFC1123Z))
	h("Message-ID", "<"+hex.EncodeToString(id[:])+"@"+domain+">")
	h("From", s.fromAddr.String())
	h("To", to.String())
	h("Subject", mime.QEncoding.Encode("utf-8", m.Subject))
	h("MIME-Version", "1.0")
	h("Content-Type", "text/plain; charset=UTF-8")
	h("Content-Transfer-Encoding", "8bit")
	b.WriteString("\r\n")
	b.WriteString(body)
	return []byte(b.String())
}

// plainAuth is PLAIN without net/smtp.PlainAuth's refusal to run over an unencrypted link,
// so that starttls false against a trusted relay can still authenticate. The caller chose that.
type plainAuth struct{ user, pass, host string }

func (a plainAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "PLAIN", []byte("\x00" + a.user + "\x00" + a.pass), nil
}

func (a plainAuth) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return nil, errors.New("unexpected server challenge")
	}
	return nil, nil
}
