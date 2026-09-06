// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

type viewRefresher interface {
	RefreshHourlyStats(context.Context) error
	RefreshTopNodes(context.Context) error
	RefreshTopObservers(context.Context) error
	RefreshPayloadBreakdown(context.Context) error
	RefreshTopTalkers(context.Context) error
	RefreshTopAdvertisers(context.Context) error
	RefreshRadioPresets(context.Context) error
}

// ViewRefreshTask returns a Task that refreshes all materialized views.
func ViewRefreshTask(store viewRefresher, interval time.Duration) Task {
	return Task{
		Name:     "view_refresh",
		Interval: interval,
		Run: func(ctx context.Context) error {
			var errs []error
			for _, view := range []struct {
				name    string
				refresh func(context.Context) error
			}{
				{"hourly stats", store.RefreshHourlyStats},
				{"top nodes", store.RefreshTopNodes},
				{"top observers", store.RefreshTopObservers},
				{"payload breakdown", store.RefreshPayloadBreakdown},
				{"top talkers", store.RefreshTopTalkers},
				{"top advertisers", store.RefreshTopAdvertisers},
				{"radio presets", store.RefreshRadioPresets},
			} {
				if err := view.refresh(ctx); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", view.name, err))
				}
			}
			return errors.Join(errs...)
		},
	}
}

// CleanupTask returns a Task that prunes old telemetry, packet, and node rows.
func CleanupTask(store *db.Store, telemetryRetention, packetRetention, nodeDeleteAfter, interval time.Duration) Task {
	return Task{
		Name:     "cleanup",
		Interval: interval,
		Run: func(ctx context.Context) error {
			if err := store.DeleteOldTelemetry(ctx, time.Now().Add(-telemetryRetention)); err != nil {
				return err
			}
			// One cutoff for all three so the IATA tables stay in step
			// with the packets they mirror.
			cutoff := time.Now().Add(-packetRetention)
			if err := store.DeleteOldPackets(ctx, cutoff); err != nil {
				return err
			}
			if err := store.DeleteOldChannelIATAs(ctx, cutoff); err != nil {
				return err
			}
			if err := store.DeleteOldTraceIATAs(ctx, cutoff); err != nil {
				return err
			}
			if err := store.DeleteOldNodes(ctx, time.Now().Add(-nodeDeleteAfter)); err != nil {
				return err
			}
			return nil
		},
	}
}

// reconfirmBatchSize bounds per-tick reconfirm work; at hourly ticks a 16M-row
// table gets fully re-checked roughly daily.
const reconfirmBatchSize = 750_000

// ReconfirmTask returns a Task that prunes aged routes first, then reconfirms
// stale and ambiguous resolved paths and neighbors, so known_routes only ever
// has one writer at a time.
func ReconfirmTask(store *db.Store, routeRetention, routeGrace time.Duration, routeMinObservations int64, interval time.Duration) Task {
	return Task{
		Name:     "reconfirm",
		Interval: interval,
		Run: func(ctx context.Context) error {
			now := time.Now()
			if err := store.DeleteOldRoutes(ctx, now.Add(-routeRetention), routeMinObservations, now.Add(-routeGrace)); err != nil {
				return fmt.Errorf("route retention: %w", err)
			}
			if err := store.ReconfirmRoutes(ctx, reconfirmBatchSize); err != nil {
				return fmt.Errorf("routes: %w", err)
			}
			if err := store.ReconfirmNeighbors(ctx); err != nil {
				return fmt.Errorf("neighbors: %w", err)
			}
			return nil
		},
	}
}
