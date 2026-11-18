// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"net/mail"
	"net/url"
	"strings"

	"gopkg.in/yaml.v3"
)

// AccountsConfig controls user accounts. Off by default; with it off no account route is served.
type AccountsConfig struct {
	Mode         string       `yaml:"mode"`         // "off" (default), "local" or "portal"
	PublicURL    string       `yaml:"public_url"`   // where users reach the web app; used in mailed links
	Registration string       `yaml:"registration"` // "open" (default) or "closed"
	Portal       PortalConfig `yaml:"portal"`
	// Mail is the top-level mail block; Load copies it in, since account mail is sent through it.
	Mail MailConfig `yaml:"-"`
	// MovedSMTP catches the old accounts.smtp block so startup can say where it went.
	MovedSMTP *yaml.Node `yaml:"smtp"`
}

// PortalConfig is the sign-in portal used when mode is portal. The client secret is never in YAML: it comes from
// BEACON_PORTAL_SECRET.
type PortalConfig struct {
	URL      string `yaml:"url"`       // https base URL of the portal
	ClientID string `yaml:"client_id"` // this instance's client id at the portal
}

// MailConfig is the top-level mail block: the outgoing mail server for account mail and email alerts. The password
// is never in YAML: it comes from BEACON_SMTP_PASSWORD.
type MailConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"` // default 587
	Username string `yaml:"username"`
	From     string `yaml:"from"`
	StartTLS *bool  `yaml:"starttls"` // default true
}

// ResolvedAccounts is AccountsConfig with defaults applied and values parsed.
type ResolvedAccounts struct {
	Enabled bool
	// Portal is true when users sign in through the portal; there are no passwords or email addresses then.
	Portal           bool
	PortalURL        *url.URL
	PortalClientID   string
	PortalSecret     string
	PublicURL        *url.URL
	RegistrationOpen bool
	SMTPHost         string
	SMTPPort         int
	SMTPUsername     string
	SMTPPassword     string
	SMTPFrom         string
	SMTPStartTLS     bool
	// InstanceName is what mail calls this instance; main sets it from VITE_APP_NAME. Empty reads as "Beacon".
	InstanceName string
	// CookieSecure is false only for a plain-http localhost public URL, for local development.
	CookieSecure bool
}

// Resolve applies defaults and validates. A section with mode off is not inspected further. It does not need the
// portal secret, so config loading can validate without it; ResolveWithSecrets is the startup call.
func (a AccountsConfig) Resolve(smtpPassword string) (ResolvedAccounts, error) {
	return a.resolve(smtpPassword, "", false)
}

// ResolveWithSecrets is Resolve with the SMTP password and the portal client secret from the environment; portal
// mode needs the secret.
func (a AccountsConfig) ResolveWithSecrets(smtpPassword, portalSecret string) (ResolvedAccounts, error) {
	return a.resolve(smtpPassword, portalSecret, true)
}

func (a AccountsConfig) resolve(smtpPassword, portalSecret string, needSecret bool) (ResolvedAccounts, error) {
	if a.MovedSMTP != nil {
		return ResolvedAccounts{}, fmt.Errorf("accounts.smtp has moved: put those settings in a top-level mail block")
	}
	switch a.Mode {
	case "", "off":
		return ResolvedAccounts{}, nil
	case "local", "portal":
	default:
		return ResolvedAccounts{}, fmt.Errorf("accounts.mode must be off, local or portal")
	}
	portal := a.Mode == "portal"
	r := ResolvedAccounts{
		Enabled:      true,
		Portal:       portal,
		SMTPHost:     strings.TrimSpace(a.Mail.Host),
		SMTPPort:     a.Mail.Port,
		SMTPUsername: a.Mail.Username,
		SMTPPassword: smtpPassword,
		SMTPFrom:     strings.TrimSpace(a.Mail.From),
		SMTPStartTLS: true,
	}
	switch a.Registration {
	case "", "open":
		r.RegistrationOpen = !portal
	case "closed":
	default:
		return ResolvedAccounts{}, fmt.Errorf("accounts.registration must be open or closed")
	}
	if a.Mail.StartTLS != nil {
		r.SMTPStartTLS = *a.Mail.StartTLS
	}
	if r.SMTPPort == 0 {
		r.SMTPPort = 587
	}
	if !portal && (r.SMTPPort < 1 || r.SMTPPort > 65535) {
		return ResolvedAccounts{}, fmt.Errorf("mail.port must be between 1 and 65535")
	}

	u, err := parsePublicURL(a.PublicURL, a.Mode)
	if err != nil {
		return ResolvedAccounts{}, err
	}
	r.PublicURL = u
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1"
	if u.Scheme == "http" && !local {
		return ResolvedAccounts{}, fmt.Errorf("accounts.public_url must be https unless the host is localhost or 127.0.0.1")
	}
	r.CookieSecure = !(u.Scheme == "http" && local)

	if portal {
		pu, err := url.Parse(strings.TrimSpace(a.Portal.URL))
		if err != nil || pu.Scheme != "https" || pu.Hostname() == "" || pu.User != nil || pu.RawQuery != "" || pu.Fragment != "" {
			return ResolvedAccounts{}, fmt.Errorf("accounts.portal.url must be an https URL with no credentials, query or fragment")
		}
		r.PortalURL = pu
		r.PortalClientID = strings.TrimSpace(a.Portal.ClientID)
		if r.PortalClientID == "" {
			return ResolvedAccounts{}, fmt.Errorf("accounts.portal.client_id: required when accounts.mode is portal")
		}
		if needSecret && portalSecret == "" {
			return ResolvedAccounts{}, fmt.Errorf("BEACON_PORTAL_SECRET: required when accounts.mode is portal")
		}
		r.PortalSecret = portalSecret
		return r, nil
	}

	if r.SMTPHost == "" {
		return ResolvedAccounts{}, fmt.Errorf("mail.host: required when accounts.mode is local")
	}
	if r.SMTPFrom == "" {
		return ResolvedAccounts{}, fmt.Errorf("mail.from: required when accounts.mode is local")
	}
	if _, err := mail.ParseAddress(r.SMTPFrom); err != nil {
		return ResolvedAccounts{}, fmt.Errorf("mail.from: not a valid address")
	}
	return r, nil
}

func parsePublicURL(s, mode string) (*url.URL, error) {
	if s == "" {
		return nil, fmt.Errorf("accounts.public_url: required when accounts.mode is %s", mode)
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
