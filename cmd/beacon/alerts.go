// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"log/slog"
	"net/url"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
)

// pushNotice is logged once at startup while push is on, so operators know what the push server sees.
func pushNotice(server string) string {
	host := server
	if u, err := url.Parse(server); err == nil && u.Host != "" {
		host = u.Host
	}
	return "Alert push notifications are delivered through the MeshCore Beacon push server (" + host + "). " +
		"The push server identifies this server as the sender of every notification, enforces rate limits, and records usage " +
		"(server, time, recipient handle, size) to detect and act on abuse. Servers that abuse it may be rate-limited, " +
		"suspended, or banned. Notification contents are encrypted for each device and never readable by the push server."
}

// warnRetiredPushCredentials flags BEACON_PUSH_CREDENTIALS, the Firebase key push used before the push server.
func warnRetiredPushCredentials(lookup func(string) (string, bool)) {
	if _, ok := lookup("BEACON_PUSH_CREDENTIALS"); ok {
		slog.Warn("BEACON_PUSH_CREDENTIALS is unused since 2.3.0 (push goes through the push server) and can be removed", "component", "startup")
	}
}

// alertChannels is what alerts go out by on this server: email needs the account mailer, and neither works
// without accounts.
func alertChannels(ra config.ResolvedAlerts, accountsOn bool, mailSender mail.Sender) (email, push bool) {
	if !accountsOn {
		return false, false
	}
	return ra.Email && mailSender != nil, ra.Push
}

// newAlertSender builds the outbox sender for the channels on, and the push server client when push is on; nil
// when no channel is on. Neither the Push secret nor anything derived from it is logged.
func newAlertSender(ra config.ResolvedAlerts, accountsOn bool, mailSender mail.Sender, store *db.Store, publicURL *url.URL) (*alerts.Sender, *alerts.Relay) {
	email, push := alertChannels(ra, accountsOn, mailSender)
	if !email && !push {
		return nil, nil
	}
	cfg := alerts.SenderConfig{PublicURL: publicURL}
	if email {
		cfg.Mail = mailSender
	}
	var relay *alerts.Relay
	if push {
		slog.Warn(pushNotice(ra.PushServer), "component", "startup")
		if !ra.OfficialServer {
			slog.Warn("WARNING: alerts.push.server is not the official MeshCore Beacon push server: the official Beacon app will not receive these notifications",
				"component", "startup", "server", ra.PushServer)
		}
		relay = alerts.NewRelay(ra.PushServer, ra.PushID, ra.PushSecret, nil)
		cfg.Push, cfg.PushID = relay, ra.PushID
	}
	slog.Info("alert channels", "component", "startup", "email", email, "push", push)
	return alerts.NewSender(db.AlertEval{Store: store}, cfg), relay
}
