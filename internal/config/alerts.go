// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"
)

// OfficialPushServer is the push server the Beacon app receives notifications through.
const OfficialPushServer = "https://push.meshcorebeacon.ca"

// AlertsConfig picks the channels alerts go out by. Both are off by default.
type AlertsConfig struct {
	Email AlertsEmailConfig `yaml:"email"`
	Push  AlertsPushConfig  `yaml:"push"`
	// MovedDelivery catches the old alerts.delivery key so startup can say what replaced it.
	MovedDelivery yaml.Node `yaml:"delivery"`
}

// AlertsEmailConfig sends alerts by mail. It needs local accounts and the top-level mail block.
type AlertsEmailConfig struct {
	Enabled bool `yaml:"enabled"`
}

// AlertsPushConfig sends alerts as push notifications to the Beacon app. The Push secret is never in YAML: it comes
// from BEACON_PUSH_SECRET.
type AlertsPushConfig struct {
	Enabled bool   `yaml:"enabled"`
	ID      string `yaml:"id"`
	Server  string `yaml:"server"` // default OfficialPushServer
}

// ResolvedAlerts is AlertsConfig checked against the accounts mode.
type ResolvedAlerts struct {
	Email bool
	Push  bool
	// PushServer is the push server's https origin, with no trailing slash.
	PushServer     string
	PushID         string
	PushSecret     string
	OfficialServer bool
}

// Any is true when alerts go out by at least one channel.
func (r ResolvedAlerts) Any() bool { return r.Email || r.Push }

const maxPushID = 64

// Resolve validates without the Push secret, so config loading can run without it; ResolveWithSecret is the startup
// call.
func (a AlertsConfig) Resolve(accountsMode string) (ResolvedAlerts, error) {
	return a.resolve(accountsMode, "", false)
}

// ResolveWithSecret is Resolve with the Push secret from the environment; push needs it.
func (a AlertsConfig) ResolveWithSecret(accountsMode, pushSecret string) (ResolvedAlerts, error) {
	return a.resolve(accountsMode, pushSecret, true)
}

func (a AlertsConfig) resolve(accountsMode, secret string, needSecret bool) (ResolvedAlerts, error) {
	if a.MovedDelivery.Kind != 0 {
		return ResolvedAlerts{}, fmt.Errorf("alerts.delivery has been replaced: set alerts.email.enabled and alerts.push.enabled instead")
	}
	accounts := accountsMode == "local" || accountsMode == "portal"
	var r ResolvedAlerts
	if a.Email.Enabled {
		// Portal users have no address to send to.
		if accountsMode != "local" {
			return ResolvedAlerts{}, fmt.Errorf("alerts.email.enabled needs accounts.mode local: alerts are sent to account email addresses")
		}
		r.Email = true
	}
	if !a.Push.Enabled {
		return r, nil
	}
	// Rules hang on a user's Atlas, so with accounts off nothing could ever be sent.
	if !accounts {
		return ResolvedAlerts{}, fmt.Errorf("alerts.push.enabled needs accounts.mode local or portal")
	}
	server, err := pushServer(a.Push.Server)
	if err != nil {
		return ResolvedAlerts{}, err
	}
	register := server + "/register"
	id := strings.TrimSpace(a.Push.ID)
	if id == "" {
		return ResolvedAlerts{}, fmt.Errorf("alerts.push.id: required when alerts.push.enabled is true; register this server at %s to get a Push ID and Push secret", register)
	}
	if len(id) > maxPushID || strings.ContainsRune(id, '.') || strings.IndexFunc(id, badTokenRune) >= 0 {
		return ResolvedAlerts{}, fmt.Errorf("alerts.push.id is not a Push ID: copy it from %s", register)
	}
	secret = strings.TrimSpace(secret)
	if needSecret {
		if secret == "" {
			return ResolvedAlerts{}, fmt.Errorf("BEACON_PUSH_SECRET: required when alerts.push.enabled is true; it is the Push secret you got when you registered this server at %s", register)
		}
		if strings.IndexFunc(secret, badTokenRune) >= 0 {
			return ResolvedAlerts{}, fmt.Errorf("BEACON_PUSH_SECRET is not a Push secret: copy it again from %s", register)
		}
	}
	r.Push, r.PushServer, r.PushID, r.PushSecret, r.OfficialServer = true, server, id, secret, server == OfficialPushServer
	return r, nil
}

func badTokenRune(r rune) bool {
	return r > unicode.MaxASCII || unicode.IsSpace(r) || unicode.IsControl(r)
}

func pushServer(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return OfficialPushServer, nil
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("alerts.push.server must be an https address with no path, query or credentials, such as %s", OfficialPushServer)
	}
	return "https://" + strings.ToLower(u.Host), nil
}
