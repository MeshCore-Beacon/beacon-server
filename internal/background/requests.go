// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"log/slog"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

const requestsOfflineInterval = time.Hour

// RequestsMaintStore is what the offline rule needs; *db.Store satisfies it.
type RequestsMaintStore interface {
	DisableOfflineRequests(ctx context.Context, cutoff, now time.Time, reason string) (int, error)
}

// RequestTasks disables telemetry requests for repeaters no observer has heard for days days. The user starts a
// disabled request again by hand.
func RequestTasks(store RequestsMaintStore, days int) []Task {
	return []Task{{Name: "requests_offline", Interval: requestsOfflineInterval, Run: func(ctx context.Context) error {
		now := pollingNow()
		reason := polling.ReasonOffline(days)
		n, err := store.DisableOfflineRequests(ctx, now.AddDate(0, 0, -days), now, reason)
		if n > 0 {
			slog.Info("telemetry requests disabled for offline repeaters", "component", "background", "repeaters", n)
		}
		return err
	}}}
}
