// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"log/slog"
	"net/url"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
)

// newAlertSender builds the outbox sender for the delivery mode, or nil when nothing can be sent: delivery off
// or email with no mailer. Push without credentials is an error, so a server never advertises
// alerts it cannot send. The credentials path is never logged.
func newAlertSender(delivery string, mailSender mail.Sender, store *db.Store, publicURL *url.URL, credentialsPath string) (*alerts.Sender, error) {
	switch delivery {
	case "email":
		if mailSender == nil {
			return nil, nil
		}
		slog.Info("alert delivery", "component", "startup", "mode", "email")
		return alerts.NewSender(db.AlertEval{Store: store}, "email", mailSender, nil, publicURL, nil), nil
	case "push":
		if credentialsPath == "" {
			return nil, errors.New("alerts.delivery is push but BEACON_PUSH_CREDENTIALS is not set: set it to the Google service-account key file, or set alerts.delivery to off")
		}
		fcm, err := alerts.LoadFCM(credentialsPath)
		if err != nil {
			return nil, err
		}
		slog.Info("alert delivery", "component", "startup", "mode", "push")
		return alerts.NewSender(db.AlertEval{Store: store}, "push", nil, fcm, publicURL, nil), nil
	}
	return nil, nil
}
