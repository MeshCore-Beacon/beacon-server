// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/background"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestAlertSenderNeedsAChannel(t *testing.T) {
	pu, _ := url.Parse("https://beacon.example.org")
	s, r := newAlertSender(config.ResolvedAlerts{}, true, mail.Discard{}, nil, pu)
	if s != nil || r != nil {
		t.Fatal("a sender with no channel")
	}
	if tasks := background.AlertTasks(nil, s); len(tasks) != 0 {
		t.Fatalf("tasks without a sender: %d", len(tasks))
	}
	// Email without a mailer, or anything without accounts, sends nothing.
	if s, _ := newAlertSender(config.ResolvedAlerts{Email: true}, true, nil, nil, pu); s != nil {
		t.Fatal("email without a mailer")
	}
	if s, _ := newAlertSender(config.ResolvedAlerts{Email: true, Push: true, PushID: "inst_abc", PushSecret: "x"}, false, mail.Discard{}, nil, pu); s != nil {
		t.Fatal("alerts without accounts")
	}
	if s, r := newAlertSender(config.ResolvedAlerts{Email: true}, true, mail.Discard{}, nil, pu); s == nil || r != nil {
		t.Fatal("email only")
	}
}

func TestAlertSenderPushLogsTheNoticeAndNeverTheSecret(t *testing.T) {
	pu, _ := url.Parse("https://beacon.example.org")
	buf := captureLog(t)
	ra := config.ResolvedAlerts{Push: true, PushServer: config.OfficialPushServer, PushID: "inst_abc", PushSecret: "topsecret", OfficialServer: true}
	s, r := newAlertSender(ra, true, nil, nil, pu)
	if s == nil || r == nil || r.PushID() != "inst_abc" {
		t.Fatal("push sender not built")
	}
	out := buf.String()
	if strings.Count(out, "push server (push.meshcorebeacon.ca)") != 1 || strings.Contains(out, "not the official") || strings.Contains(out, "topsecret") ||
		strings.Contains(out, "reports") || !strings.Contains(out, "never readable by the push server") {
		t.Fatalf("%s", out)
	}
	buf.Reset()
	ra.PushServer, ra.OfficialServer = "https://push.example.org", false
	newAlertSender(ra, true, nil, nil, pu)
	if !strings.Contains(buf.String(), "push server (push.example.org)") || strings.Contains(buf.String(), "meshcorebeacon.ca") ||
		!strings.Contains(buf.String(), "WARNING") || !strings.Contains(buf.String(), "official Beacon app will not receive") || strings.Contains(buf.String(), "topsecret") {
		t.Fatalf("%s", buf.String())
	}
}

func TestRetiredPushCredentialsWarnOnce(t *testing.T) {
	buf := captureLog(t)
	warnRetiredPushCredentials(func(string) (string, bool) { return "", false })
	if buf.Len() != 0 {
		t.Fatalf("warned without the variable: %s", buf.String())
	}
	warnRetiredPushCredentials(func(k string) (string, bool) { return "/run/secrets/fcm.json", k == "BEACON_PUSH_CREDENTIALS" })
	out := buf.String()
	if strings.Count(out, "BEACON_PUSH_CREDENTIALS") != 1 || !strings.Contains(out, "2.3.0") || strings.Contains(out, "fcm.json") {
		t.Fatalf("%s", out)
	}
}
