// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"log"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

// ViewRefreshTask returns a Task that refreshes all materialized views.
func ViewRefreshTask(store *db.Store, interval time.Duration) Task {
	return Task{
		Name:     "view_refresh",
		Interval: interval,
		Run: func(ctx context.Context) error {
			if err := store.RefreshHourlyStats(ctx); err != nil {
				log.Printf("background[view_refresh]: hourly stats: %v", err)
			}
			if err := store.RefreshTopNodes(ctx); err != nil {
				log.Printf("background[view_refresh]: top nodes: %v", err)
			}
			if err := store.RefreshRadioPresets(ctx); err != nil {
				log.Printf("background[view_refresh]: radio presets: %v", err)
			}
			return nil
		},
	}
}

// CleanupTask returns a Task that prunes old telemetry and packet rows.
func CleanupTask(store *db.Store, telemetryRetention, packetRetention, interval time.Duration) Task {
	return Task{
		Name:     "cleanup",
		Interval: interval,
		Run: func(ctx context.Context) error {
			if err := store.DeleteOldTelemetry(ctx, time.Now().Add(-telemetryRetention)); err != nil {
				return err
			}
			if err := store.DeleteOldPackets(ctx, time.Now().Add(-packetRetention)); err != nil {
				return err
			}
			return nil
		},
	}
}
