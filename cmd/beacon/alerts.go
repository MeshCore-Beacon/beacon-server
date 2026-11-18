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
const pushNotice = "Alert push notifications are delivered through the MeshCore Beacon push relay (push.meshcorebeacon.ca). " +
	"The relay identifies this server as the sender of every notification, enforces rate limits, and records usage " +
	"(server, time, recipient handle, size) to detect and act on abuse. Servers that abuse the relay may be rate-limited, " +
	"suspended, or banned. Notification contents are encrypted and not readable by the relay unless a user reports a notification."

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
		slog.Warn(pushNotice, "component", "startup")
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
