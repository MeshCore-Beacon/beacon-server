// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"log/slog"
	"net/url"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// newAlertSender builds the outbox sender for the delivery mode, or nil when nothing can be sent: delivery off,
// email without the account mail queue, or push without credentials. With a nil sender no alert rule is evaluated
// and nothing tries to send; the routes and rules still work. The credentials path is never logged.
func newAlertSender(delivery string, svc *users.Service, store *db.Store, publicURL *url.URL, credentialsPath string) (*alerts.Sender, error) {
	switch delivery {
	case "email":
		if svc == nil {
			return nil, nil
		}
		slog.Info("alert delivery", "component", "startup", "mode", "email")
		return alerts.NewSender(db.AlertEval{Store: store}, "email", svc, nil, publicURL, nil), nil
	case "push":
		if credentialsPath == "" {
			slog.Warn("alerts.delivery is push but BEACON_PUSH_CREDENTIALS is not set: alert rules can be stored, nothing is evaluated or sent", "component", "startup")
			return nil, nil
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
