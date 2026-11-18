// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"strings"
	"testing"
)

func TestAlertsChannelsNeedAccounts(t *testing.T) {
	push := AlertsPushConfig{Enabled: true, ID: "inst_abc"}
	for _, tc := range []struct {
		name   string
		a      AlertsConfig
		mode   string
		err    string
		email  bool
		pushOn bool
	}{
		{"off", AlertsConfig{}, "local", "", false, false},
		{"email local", AlertsConfig{Email: AlertsEmailConfig{Enabled: true}}, "local", "", true, false},
		{"email portal", AlertsConfig{Email: AlertsEmailConfig{Enabled: true}}, "portal", "accounts.mode local", false, false},
		{"email off", AlertsConfig{Email: AlertsEmailConfig{Enabled: true}}, "off", "accounts.mode local", false, false},
		{"push portal", AlertsConfig{Push: push}, "portal", "", false, true},
		{"push local", AlertsConfig{Push: push}, "local", "", false, true},
		{"push without accounts", AlertsConfig{Push: push}, "", "accounts.mode local or portal", false, false},
		{"both", AlertsConfig{Email: AlertsEmailConfig{Enabled: true}, Push: push}, "local", "", true, true},
	} {
		r, err := tc.a.ResolveWithSecret(tc.mode, "secret")
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%s: %v", tc.name, err)
			}
			continue
		}
		if err != nil || r.Email != tc.email || r.Push != tc.pushOn {
			t.Errorf("%s: %+v %v", tc.name, r, err)
		}
	}
}

func TestAlertsPushNeedsIDAndSecret(t *testing.T) {
	_, err := AlertsConfig{Push: AlertsPushConfig{Enabled: true}}.ResolveWithSecret("local", "secret")
	if err == nil || !strings.Contains(err.Error(), "alerts.push.id") || !strings.Contains(err.Error(), "https://push.meshcorebeacon.ca/register") {
		t.Fatalf("no id: %v", err)
	}
	_, err = AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc", Server: "https://push.example.org"}}.ResolveWithSecret("local", "")
	if err == nil || !strings.Contains(err.Error(), "BEACON_PUSH_SECRET") || !strings.Contains(err.Error(), "https://push.example.org/register") {
		t.Fatalf("no secret: %v", err)
	}
	// Loading the file does not need the secret; startup does.
	if _, err := (AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc"}}).Resolve("local"); err != nil {
		t.Fatalf("resolve without secret: %v", err)
	}
	for _, bad := range []string{"inst.abc", "inst abc", strings.Repeat("i", 200)} {
		if _, err := (AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: bad}}).ResolveWithSecret("local", "s"); err == nil {
			t.Errorf("id %q accepted", bad)
		}
	}
	if _, err := (AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc"}}).ResolveWithSecret("local", "a b"); err == nil {
		t.Error("secret with a space accepted")
	}
	// The secret never appears in an error.
	_, err = AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst.x"}}.ResolveWithSecret("local", "topsecret")
	if err == nil || strings.Contains(err.Error(), "topsecret") {
		t.Fatalf("%v", err)
	}
}

func TestAlertsPushServer(t *testing.T) {
	r, err := AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc"}}.ResolveWithSecret("local", "s")
	if err != nil || r.PushServer != "https://push.meshcorebeacon.ca" || !r.OfficialServer || r.PushID != "inst_abc" || r.PushSecret != "s" {
		t.Fatalf("default: %+v %v", r, err)
	}
	for in, want := range map[string]string{"https://push.meshcorebeacon.ca/": "https://push.meshcorebeacon.ca", "https://push.example.org:8443": "https://push.example.org:8443"} {
		r, err := AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc", Server: in}}.ResolveWithSecret("local", "s")
		if err != nil || r.PushServer != want {
			t.Errorf("%s: %+v %v", in, r, err)
		}
	}
	r, _ = AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc", Server: "https://push.example.org"}}.ResolveWithSecret("local", "s")
	if r.OfficialServer {
		t.Error("another server read as the official one")
	}
	for _, bad := range []string{"http://push.meshcorebeacon.ca", "https://push.example.org/v1", "https://u:p@push.example.org", "https://push.example.org?x=1",
		"https://push.example.org#f", "push.example.org", "https://"} {
		if _, err := (AlertsConfig{Push: AlertsPushConfig{Enabled: true, ID: "inst_abc", Server: bad}}).ResolveWithSecret("local", "s"); err == nil ||
			!strings.Contains(err.Error(), "alerts.push.server") {
			t.Errorf("%s: %v", bad, err)
		}
	}
	// With push off the rest of the block is not inspected.
	if _, err := (AlertsConfig{Push: AlertsPushConfig{Server: "http://x"}}).ResolveWithSecret("off", ""); err != nil {
		t.Errorf("push off: %v", err)
	}
}

// The old alerts.delivery key is refused with a pointer to what replaced it.
func TestAlertsOldDeliveryKeyRefused(t *testing.T) {
	_, err := loadText(t, "alerts:\n  delivery: push\n")
	if err == nil || !strings.Contains(err.Error(), "alerts.delivery") || !strings.Contains(err.Error(), "alerts.email.enabled") ||
		!strings.Contains(err.Error(), "alerts.push.enabled") {
		t.Fatalf("%v", err)
	}
	cfg, err := loadText(t, `mail:
  host: smtp.example.org
  from: "Beacon <no-reply@example.org>"
accounts:
  mode: local
  public_url: https://beacon.example.org
alerts:
  email:
    enabled: true
  push:
    enabled: true
    id: inst_abc
`)
	if err != nil || !cfg.Alerts.Email.Enabled || !cfg.Alerts.Push.Enabled || cfg.Alerts.Push.ID != "inst_abc" {
		t.Fatalf("%+v %v", cfg.Alerts, err)
	}
}
