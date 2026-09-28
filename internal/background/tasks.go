// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/google/uuid"
)

type viewRefresher interface {
	RefreshHourlyStats(context.Context) error
	RefreshTopNodes(context.Context) error
	RefreshTopObservers(context.Context) error
	RefreshPayloadBreakdown(context.Context) error
	RefreshTopTalkers(context.Context) error
	RefreshTopAdvertisers(context.Context) error
	RefreshRadioPresets(context.Context) error
	RefreshObserverActivity(context.Context) error
	RefreshSignalStats(context.Context) error
	RefreshPathStats(context.Context) error
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
				{"observer activity", store.RefreshObserverActivity},
				{"signal stats", store.RefreshSignalStats},
				{"path stats", store.RefreshPathStats},
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

// Limit lock lifetime while retaining the per-run work budget.
const (
	reconfirmRunLimit     = 750_000
	reconfirmBatchSize    = 1_000
	reconfirmBatchTimeout = 5 * time.Second
)

type routeMaintainer interface {
	DeleteOldRoutes(context.Context, time.Time, int64, time.Time) error
	ReconfirmRoutes(context.Context, int32, time.Time) (int64, error)
	ReconfirmNeighbors(context.Context) error
}

type observerCleaner interface {
	DeleteOldObservers(context.Context, time.Time) ([]uuid.UUID, error)
}

// ObserverCleanupTask ages out unreferenced observers through the presence
// coalescer, then invalidates any cached details for the deleted IDs.
func ObserverCleanupTask(store observerCleaner, deleteAfter, interval time.Duration, onDelete func(context.Context, uuid.UUID)) Task {
	return Task{
		Name: "observer_cleanup", Interval: interval,
		Run: func(ctx context.Context) error {
			if deleteAfter <= 0 {
				return nil
			}
			ids, err := store.DeleteOldObservers(ctx, time.Now().Add(-deleteAfter))
			if err != nil {
				return err
			}
			if onDelete != nil {
				for _, id := range ids {
					onDelete(ctx, id)
				}
			}
			return nil
		},
	}
}

// ReconfirmTask returns a Task that prunes aged routes first, then reconfirms
// stale and ambiguous resolved paths and neighbors in order.
func ReconfirmTask(store routeMaintainer, routeRetention, routeGrace time.Duration, routeMinObservations int64, interval time.Duration) Task {
	return Task{
		Name:     "reconfirm",
		Interval: interval,
		Run: func(ctx context.Context) error {
			now := time.Now()
			if err := store.DeleteOldRoutes(ctx, now.Add(-routeRetention), routeMinObservations, now.Add(-routeGrace)); err != nil {
				return fmt.Errorf("route retention: %w", err)
			}
			for remaining := int64(reconfirmRunLimit); remaining > 0; {
				limit := min(int64(reconfirmBatchSize), remaining)
				batchCtx, cancel := context.WithTimeout(ctx, reconfirmBatchTimeout)
				n, err := store.ReconfirmRoutes(batchCtx, int32(limit), now)
				cancel()
				if err != nil {
					return fmt.Errorf("routes: %w", err)
				}
				remaining -= n
				if n < limit {
					break
				}
			}
			if err := store.ReconfirmNeighbors(ctx); err != nil {
				return fmt.Errorf("neighbors: %w", err)
			}
			return nil
		},
	}
}
