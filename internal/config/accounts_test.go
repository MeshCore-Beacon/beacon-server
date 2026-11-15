// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"strings"
	"testing"
)

func localAccounts() AccountsConfig {
	return AccountsConfig{
		Mode:      "local",
		PublicURL: "https://beacon.example.org",
		SMTP:      SMTPConfig{Host: "smtp.example.org", From: "Beacon <no-reply@example.org>"},
	}
}

func TestAccountsDefaultsOff(t *testing.T) {
	r, err := AccountsConfig{}.Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if r.Enabled || r.PublicURL != nil {
		t.Fatalf("accounts must default to off: %+v", r)
	}
}

func TestAccountsLocalDefaults(t *testing.T) {
	r, err := localAccounts().Resolve("pw")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Enabled || !r.RegistrationOpen || !r.CookieSecure || r.SMTPPort != 587 || !r.SMTPStartTLS || r.SMTPPassword != "pw" {
		t.Fatalf("bad defaults: %+v", r)
	}
	if r.PublicURL.Host != "beacon.example.org" {
		t.Fatalf("public url: %v", r.PublicURL)
	}
}

func TestAccountsExplicitValues(t *testing.T) {
	no := false
	c := localAccounts()
	c.Registration = "closed"
	c.SMTP.Port = 2525
	c.SMTP.StartTLS = &no
	r, err := c.Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if r.RegistrationOpen || r.SMTPPort != 2525 || r.SMTPStartTLS {
		t.Fatalf("%+v", r)
	}
}

func TestAccountsRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		mut func(*AccountsConfig)
		key string
	}{
		"unknown mode":         {func(c *AccountsConfig) { c.Mode = "oauth" }, "accounts.mode"},
		"unknown registration": {func(c *AccountsConfig) { c.Registration = "maybe" }, "accounts.registration"},
		"no public url":        {func(c *AccountsConfig) { c.PublicURL = "" }, "accounts.public_url"},
		"relative url":         {func(c *AccountsConfig) { c.PublicURL = "beacon.example.org" }, "accounts.public_url"},
		"ftp url":              {func(c *AccountsConfig) { c.PublicURL = "ftp://beacon.example.org" }, "accounts.public_url"},
		"path":                 {func(c *AccountsConfig) { c.PublicURL = "https://beacon.example.org/app" }, "accounts.public_url"},
		"query":                {func(c *AccountsConfig) { c.PublicURL = "https://beacon.example.org/?a=b" }, "accounts.public_url"},
		"http remote":          {func(c *AccountsConfig) { c.PublicURL = "http://beacon.example.org" }, "accounts.public_url"},
		"no smtp host":         {func(c *AccountsConfig) { c.SMTP.Host = "" }, "accounts.smtp.host"},
		"no smtp from":         {func(c *AccountsConfig) { c.SMTP.From = "" }, "accounts.smtp.from"},
		"bad smtp from":        {func(c *AccountsConfig) { c.SMTP.From = "not an address" }, "accounts.smtp.from"},
		"bad port":             {func(c *AccountsConfig) { c.SMTP.Port = 70000 }, "accounts.smtp.port"},
	} {
		c := localAccounts()
		tc.mut(&c)
		if _, err := c.Resolve(""); err == nil || !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%s: err = %v, want it to name %s", name, err, tc.key)
		}
	}
}

func TestAccountsLocalhostHTTP(t *testing.T) {
	for _, u := range []string{"http://localhost:5173", "http://127.0.0.1:8080/"} {
		c := localAccounts()
		c.PublicURL = u
		r, err := c.Resolve("")
		if err != nil {
			t.Fatalf("%s: %v", u, err)
		}
		if r.CookieSecure {
			t.Errorf("%s: cookie must not be Secure", u)
		}
	}
}

func TestAccountsOffIgnoresRest(t *testing.T) {
	// A half-filled section is fine while accounts are off.
	if _, err := (AccountsConfig{SMTP: SMTPConfig{Host: "x"}}).Resolve(""); err != nil {
		t.Fatal(err)
	}
}

func portalAccounts() AccountsConfig {
	return AccountsConfig{Mode: "portal", PublicURL: "https://beacon.example.org",
		Portal: PortalConfig{URL: "https://portal.example.org/", ClientID: "beacon-analyzer"}}
}

func TestAccountsPortal(t *testing.T) {
	r, err := portalAccounts().ResolveWithSecrets("", "sec")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Enabled || !r.Portal || r.RegistrationOpen || r.PortalURL.Host != "portal.example.org" || r.PortalClientID != "beacon-analyzer" || r.PortalSecret != "sec" {
		t.Fatalf("%+v", r)
	}
	// Loading the config validates without the secret; startup demands it.
	if _, err := portalAccounts().Resolve(""); err != nil {
		t.Fatal(err)
	}
	if _, err := portalAccounts().ResolveWithSecrets("", ""); err == nil || !strings.Contains(err.Error(), "BEACON_PORTAL_SECRET") {
		t.Fatalf("%v", err)
	}
}

func TestAccountsPortalRejects(t *testing.T) {
	for name, mut := range map[string]func(*AccountsConfig){
		"http url":        func(c *AccountsConfig) { c.Portal.URL = "http://portal.example.org" },
		"no url":          func(c *AccountsConfig) { c.Portal.URL = "" },
		"credentials":     func(c *AccountsConfig) { c.Portal.URL = "https://a:b@portal.example.org" },
		"query":           func(c *AccountsConfig) { c.Portal.URL = "https://portal.example.org/?a=b" },
		"no client id":    func(c *AccountsConfig) { c.Portal.ClientID = "" },
		"no public url":   func(c *AccountsConfig) { c.PublicURL = "" },
		"http public url": func(c *AccountsConfig) { c.PublicURL = "http://beacon.example.org" },
	} {
		c := portalAccounts()
		mut(&c)
		if _, err := c.ResolveWithSecrets("", "sec"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := (AccountsConfig{Mode: "other"}).Resolve(""); err == nil {
		t.Error("unknown mode accepted")
	}
}

func TestAlertsDelivery(t *testing.T) {
	for _, tc := range []struct {
		delivery, mode, want string
		err                  bool
	}{
		{"", "local", "off", false}, {"", "off", "off", false}, {"off", "off", "off", false},
		{"push", "off", "", true}, {"email", "off", "", true}, {"push", "", "", true}, {"push", "portal", "push", false}, {"push", "local", "push", false},
		{"email", "local", "email", false}, {"email", "portal", "", true}, {"sms", "local", "", true},
	} {
		r, err := AlertsConfig{Delivery: tc.delivery}.Resolve(tc.mode)
		if (err != nil) != tc.err || r.Delivery != tc.want {
			t.Errorf("%+v: %q %v", tc, r.Delivery, err)
		}
	}
}

// Email alerts need local accounts with an SMTP host; accounts validation already refuses local mode without one.
func TestAlertsEmailNeedsSMTPHost(t *testing.T) {
	a := localAccounts()
	a.SMTP.Host = ""
	if _, err := a.Resolve(""); err == nil || !strings.Contains(err.Error(), "accounts.smtp.host") {
		t.Fatalf("local accounts without smtp host: %v", err)
	}
}
