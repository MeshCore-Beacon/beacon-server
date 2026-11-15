// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package mail

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecorder(t *testing.T) {
	var r Recorder
	var s Sender = &r
	if err := s.Send(context.Background(), Message{To: "a@x.org", Subject: "one"}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	r.FailWith(boom)
	if err := s.Send(context.Background(), Message{To: "b@x.org"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	r.FailWith(nil)
	_ = s.Send(context.Background(), Message{To: "c@x.org"})
	got := r.Messages()
	if len(got) != 2 || got[0].To != "a@x.org" || got[1].To != "c@x.org" {
		t.Fatalf("messages: %+v", got)
	}
	got[0].To = "changed"
	if r.Messages()[0].To != "a@x.org" {
		t.Fatal("Messages must return a copy")
	}
}

func TestRecorderConcurrent(t *testing.T) {
	var r Recorder
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Send(context.Background(), Message{To: "a@x.org"}) }()
	}
	wg.Wait()
	if n := len(r.Messages()); n != 50 {
		t.Fatalf("got %d", n)
	}
}

type captured struct {
	from, rcpt string
	data       string
}

// fakeSMTP accepts one connection, speaks just enough SMTP to take one message, and
// answers rejectRcpt to RCPT TO when set.
func fakeSMTP(t *testing.T, rejectRcpt bool) (port int, done <-chan captured) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ch := make(chan captured, 1)
	go func() {
		defer close(ch)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		br := bufio.NewReader(c)
		w := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
		var got captured
		w("220 fake ESMTP")
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				w("250 fake")
			case strings.HasPrefix(cmd, "MAIL FROM:"):
				got.from = strings.TrimSpace(line[len("MAIL FROM:"):])
				w("250 ok")
			case strings.HasPrefix(cmd, "RCPT TO:"):
				if rejectRcpt {
					w("550 no such user")
					continue
				}
				got.rcpt = strings.TrimSpace(line[len("RCPT TO:"):])
				w("250 ok")
			case cmd == "DATA":
				w("354 go")
				var b strings.Builder
				for {
					l, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if l == ".\r\n" {
						break
					}
					b.WriteString(l)
				}
				got.data = b.String()
				w("250 queued")
			case cmd == "QUIT":
				w("221 bye")
				ch <- got
				return
			case cmd == "RSET":
				w("250 ok")
			default:
				w("502 unsupported")
			}
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, ch
}

func TestSMTPDelivers(t *testing.T) {
	port, done := fakeSMTP(t, false)
	s := NewSMTP("127.0.0.1", port, "", "", "Beacon <no-reply@example.org>", false)
	err := s.Send(context.Background(), Message{To: "user@example.net", Subject: "Verify your email", Text: "hello\nline two\n"})
	if err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.from != "<no-reply@example.org>" || got.rcpt != "<user@example.net>" {
		t.Fatalf("envelope: %+v", got)
	}
	for _, want := range []string{
		"From: \"Beacon\" <no-reply@example.org>\r\n",
		"To: <user@example.net>\r\n",
		"Subject: Verify your email\r\n",
		"Date: ", "Message-ID: <", "@example.org>\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n",
		"\r\n\r\nhello\r\nline two\r\n",
	} {
		if !strings.Contains(got.data, want) {
			t.Errorf("message lacks %q:\n%s", want, got.data)
		}
	}
	if strings.Contains(strings.ReplaceAll(got.data, "\r\n", ""), "\n") {
		t.Error("bare LF in message")
	}
}

func TestSMTPEncodesSubject(t *testing.T) {
	port, done := fakeSMTP(t, false)
	s := NewSMTP("127.0.0.1", port, "", "", "no-reply@example.org", false)
	if err := s.Send(context.Background(), Message{To: "u@example.net", Subject: "Vérifiez votre courriel", Text: "x"}); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if !strings.Contains(got.data, "Subject: =?utf-8?q?V=C3=A9rifiez_votre_courriel?=\r\n") {
		t.Fatalf("subject not encoded:\n%s", got.data)
	}
}

func TestSMTPRejectedRecipient(t *testing.T) {
	port, _ := fakeSMTP(t, true)
	s := NewSMTP("127.0.0.1", port, "", "", "no-reply@example.org", false)
	if err := s.Send(context.Background(), Message{To: "u@example.net", Subject: "s", Text: "x"}); err == nil {
		t.Fatal("want an error when the server rejects the recipient")
	}
}

func TestSMTPStartTLSRequired(t *testing.T) {
	port, _ := fakeSMTP(t, false)
	s := NewSMTP("127.0.0.1", port, "", "", "no-reply@example.org", true)
	err := s.Send(context.Background(), Message{To: "u@example.net", Subject: "s", Text: "x"})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "starttls") {
		t.Fatalf("err = %v, want a STARTTLS error", err)
	}
}

func TestSMTPRejectsHeaderInjection(t *testing.T) {
	s := NewSMTP("127.0.0.1", 1, "", "", "no-reply@example.org", false)
	for name, m := range map[string]Message{
		"subject": {To: "u@example.net", Subject: "a\r\nBcc: x@y.z", Text: "x"},
		"to":      {To: "u@example.net\r\nBcc: x@y.z", Subject: "s", Text: "x"},
		"bad to":  {To: "not an address", Subject: "s", Text: "x"},
	} {
		if err := s.Send(context.Background(), m); err == nil || strings.Contains(err.Error(), "connect") {
			t.Errorf("%s: err = %v, want a validation error before dialling", name, err)
		}
	}
}

func TestSMTPHonoursContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { // accepts and never greets
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			time.Sleep(3 * time.Second)
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	s := NewSMTP("127.0.0.1", port, "", "", "no-reply@example.org", false)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.Send(ctx, Message{To: "u@example.net", Subject: "s", Text: "x"}); err == nil {
		t.Fatal("want an error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("send ignored the context: %v", time.Since(start))
	}
}

func TestWithDisplayName(t *testing.T) {
	cases := []struct{ from, name, want string }{
		{"Beacon <no-reply@x.org>", "Mesh", "Beacon <no-reply@x.org>"},
		{"no-reply@x.org", "Beacon - Ottawa", "\"Beacon - Ottawa\" <no-reply@x.org>"},
		{"<no-reply@x.org>", "Mesh", "\"Mesh\" <no-reply@x.org>"},
		{"no-reply@x.org", "A \"q\" (b), c", "\"A \\\"q\\\" (b), c\" <no-reply@x.org>"},
		{"no-reply@x.org", "Réseau", "=?utf-8?q?R=C3=A9seau?= <no-reply@x.org>"},
		{"not an address", "Mesh", "not an address"},
	}
	for _, c := range cases {
		if got := WithDisplayName(c.from, c.name); got != c.want {
			t.Errorf("WithDisplayName(%q,%q) = %q, want %q", c.from, c.name, got, c.want)
		}
	}
	got := WithDisplayName("no-reply@x.org", "Mesh\r\nBcc: evil@x.org")
	if strings.ContainsAny(got, "\r\n") {
		t.Errorf("line break in %q", got)
	}
}

func TestSMTPAccentedNameAndSubject(t *testing.T) {
	port, done := fakeSMTP(t, false)
	s := NewSMTP("127.0.0.1", port, "", "", WithDisplayName("no-reply@example.org", "Réseau Mesh"), false)
	if err := s.Send(context.Background(), Message{To: "u@example.net", Subject: "Confirm your email address for Réseau Mesh", Text: "x"}); err != nil {
		t.Fatal(err)
	}
	got := <-done
	for _, want := range []string{
		"From: =?utf-8?q?R=C3=A9seau_Mesh?= <no-reply@example.org>\r\n",
		"Subject: =?utf-8?q?Confirm_your_email_address_for_R=C3=A9seau_Mesh?=\r\n",
	} {
		if !strings.Contains(got.data, want) {
			t.Errorf("lacks %q:\n%s", want, got.data)
		}
	}
}
