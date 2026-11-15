// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import "fmt"

// AlertsConfig controls alert delivery. Off by default.
type AlertsConfig struct {
	// Delivery is "off" (default), "push" or "email". Push needs the credential file named by
	// BEACON_PUSH_CREDENTIALS; email needs local accounts and their mail setup.
	Delivery string `yaml:"delivery"`
}

// ResolvedAlerts is AlertsConfig with the mode checked against the accounts mode.
type ResolvedAlerts struct {
	// Delivery is "off", "push" or "email".
	Delivery string
}

// Resolve validates the delivery mode. Email needs an address to send to, so it cannot be combined with portal
// sign-in, where users have none.
func (a AlertsConfig) Resolve(accountsMode string) (ResolvedAlerts, error) {
	switch a.Delivery {
	case "", "off":
		return ResolvedAlerts{Delivery: "off"}, nil
	case "push", "email":
		// Rules hang on a user's Atlas, so with accounts off nothing could ever be sent.
		if accountsMode == "" || accountsMode == "off" {
			return ResolvedAlerts{}, fmt.Errorf("alerts.delivery %s needs accounts.mode local or portal", a.Delivery)
		}
	}
	switch a.Delivery {
	case "push":
		return ResolvedAlerts{Delivery: "push"}, nil
	case "email":
		if accountsMode == "portal" {
			return ResolvedAlerts{}, fmt.Errorf("alerts.delivery email needs accounts.mode local: portal users have no email address")
		}
		return ResolvedAlerts{Delivery: "email"}, nil
	}
	return ResolvedAlerts{}, fmt.Errorf("alerts.delivery must be off, push or email")
}
