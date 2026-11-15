// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"net/mail"
	"net/url"
	"strings"
)

// AccountsConfig controls user accounts. Off by default; with it off no account route is served.
type AccountsConfig struct {
	Mode         string     `yaml:"mode"`         // "off" (default) or "local"
	PublicURL    string     `yaml:"public_url"`   // where users reach the web app; used in mailed links
	Registration string     `yaml:"registration"` // "open" (default) or "closed"
	SMTP         SMTPConfig `yaml:"smtp"`
}

// SMTPConfig is the outgoing mail server. The password is never in YAML: it comes from BEACON_SMTP_PASSWORD.
type SMTPConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"` // default 587
	Username string `yaml:"username"`
	From     string `yaml:"from"`
	StartTLS *bool  `yaml:"starttls"` // default true
}

// ResolvedAccounts is AccountsConfig with defaults applied and values parsed.
type ResolvedAccounts struct {
	Enabled          bool
	PublicURL        *url.URL
	RegistrationOpen bool
	SMTPHost         string
	SMTPPort         int
	SMTPUsername     string
	SMTPPassword     string
	SMTPFrom         string
	SMTPStartTLS     bool
	// CookieSecure is false only for a plain-http localhost public URL, for local development.
	CookieSecure bool
}

// Resolve applies defaults and validates. A section with mode off is not inspected further.
func (a AccountsConfig) Resolve(smtpPassword string) (ResolvedAccounts, error) {
	switch a.Mode {
	case "", "off":
		return ResolvedAccounts{}, nil
	case "local":
	default:
		return ResolvedAccounts{}, fmt.Errorf("accounts.mode must be off or local")
	}
	r := ResolvedAccounts{
		Enabled:      true,
		SMTPHost:     strings.TrimSpace(a.SMTP.Host),
		SMTPPort:     a.SMTP.Port,
		SMTPUsername: a.SMTP.Username,
		SMTPPassword: smtpPassword,
		SMTPFrom:     strings.TrimSpace(a.SMTP.From),
		SMTPStartTLS: true,
	}
	switch a.Registration {
	case "", "open":
		r.RegistrationOpen = true
	case "closed":
	default:
		return ResolvedAccounts{}, fmt.Errorf("accounts.registration must be open or closed")
	}
	if a.SMTP.StartTLS != nil {
		r.SMTPStartTLS = *a.SMTP.StartTLS
	}
	if r.SMTPPort == 0 {
		r.SMTPPort = 587
	}
	if r.SMTPPort < 1 || r.SMTPPort > 65535 {
		return ResolvedAccounts{}, fmt.Errorf("accounts.smtp.port must be between 1 and 65535")
	}

	u, err := parsePublicURL(a.PublicURL)
	if err != nil {
		return ResolvedAccounts{}, err
	}
	r.PublicURL = u
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1"
	if u.Scheme == "http" && !local {
		return ResolvedAccounts{}, fmt.Errorf("accounts.public_url must be https unless the host is localhost or 127.0.0.1")
	}
	r.CookieSecure = !(u.Scheme == "http" && local)

	if r.SMTPHost == "" {
		return ResolvedAccounts{}, fmt.Errorf("accounts.smtp.host: required when accounts.mode is local")
	}
	if r.SMTPFrom == "" {
		return ResolvedAccounts{}, fmt.Errorf("accounts.smtp.from: required when accounts.mode is local")
	}
	if _, err := mail.ParseAddress(r.SMTPFrom); err != nil {
		return ResolvedAccounts{}, fmt.Errorf("accounts.smtp.from: not a valid address")
	}
	return r, nil
}

func parsePublicURL(s string) (*url.URL, error) {
	if s == "" {
		return nil, fmt.Errorf("accounts.public_url: required when accounts.mode is local")
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return nil, fmt.Errorf("accounts.public_url must be an absolute http or https URL")
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("accounts.public_url must have no path, query, fragment or credentials")
	}
	u.Path = ""
	return u, nil
}
