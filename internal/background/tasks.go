// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/google/uuid"
)

type viewRefresher interface {
	RefreshRadioPresets(context.Context) error
}

// ViewRefreshTask rebuilds the radio preset view; historical stats come from hourly rollups.
func ViewRefreshTask(store viewRefresher, interval time.Duration) Task {
	return Task{
		Name:     "view_refresh",
		Interval: interval,
		Run: func(ctx context.Context) error {
			if err := store.RefreshRadioPresets(ctx); err != nil {
				return fmt.Errorf("radio presets: %w", err)
			}
			return nil
		},
	}
}

type cleanupStore interface {
	DeleteOldScopeRepeats(ctx context.Context, cutoff time.Time) error
	DeleteOldTelemetry(ctx context.Context, cutoff time.Time) error
	OldestMissingRollupHour(ctx context.Context) (time.Time, bool, error)
	DeleteOldPackets(ctx context.Context, cutoff time.Time) error
	DeleteOldChannelIATAs(ctx context.Context, cutoff time.Time) error
	DeleteOldTraceIATAs(ctx context.Context, cutoff time.Time) error
	DeleteOldTraceTags(ctx context.Context, cutoff time.Time) error
	DeleteOldRollups(ctx context.Context, cutoff time.Time) error
	DeleteOldNodes(ctx context.Context, cutoff time.Time) error
}

// CleanupConfig holds the retention windows CleanupTask enforces.
type CleanupConfig struct {
	TelemetryRetention time.Duration
	PacketRetention    time.Duration
	RollupRetention    time.Duration
	NodeDeleteAfter    time.Duration
	Interval           time.Duration
}

// Raw rows wait for unrolled hours, but not forever: past the cap those hours become partial.
const (
	rawHoldbackMargin = 35 * time.Minute // ingest clamps heard_at to ±30 min of last_heard_at
	rawHoldbackWarn   = 6 * time.Hour
	rawHoldbackCap    = 24 * time.Hour
	traceTagLag       = 30 * time.Minute
)

// CleanupTask returns a Task that prunes old telemetry, packets, rollups and nodes.
func CleanupTask(store cleanupStore, cfg CleanupConfig) Task {
	return cleanupTask(store, cfg, time.Now)
}

func cleanupTask(store cleanupStore, cfg CleanupConfig, now func() time.Time) Task {
	return Task{
		Name:     "cleanup",
		Interval: cfg.Interval,
		Run: func(ctx context.Context) error {
			t := now()
			if err := store.DeleteOldScopeRepeats(ctx, t.Add(-48*time.Hour)); err != nil {
				return err
			}
			if err := store.DeleteOldTelemetry(ctx, t.Add(-cfg.TelemetryRetention)); err != nil {
				return err
			}
			cutoff, err := rawCutoff(ctx, store, t.Add(-cfg.PacketRetention))
			if err != nil {
				return err
			}
			// One cutoff so the IATA and trace tables stay in step with the packets they mirror.
			for _, del := range []func(context.Context, time.Time) error{
				store.DeleteOldPackets, store.DeleteOldChannelIATAs, store.DeleteOldTraceIATAs,
			} {
				if err := del(ctx, cutoff); err != nil {
					return err
				}
			}
			// Tag times are hearing times, up to the clamp behind their packets' last_heard_at;
			// trailing by it keeps a re-heard packet's tag from being deleted out from under it.
			if err := store.DeleteOldTraceTags(ctx, cutoff.Add(-traceTagLag)); err != nil {
				return err
			}
			if err := store.DeleteOldRollups(ctx, t.Add(-cfg.RollupRetention)); err != nil {
				return err
			}
			return store.DeleteOldNodes(ctx, t.Add(-cfg.NodeDeleteAfter))
		},
	}
}

// rawCutoff holds packet deletion behind the oldest unrolled hour, up to rawHoldbackCap.
func rawCutoff(ctx context.Context, store cleanupStore, retentionCutoff time.Time) (time.Time, error) {
	oldest, ok, err := store.OldestMissingRollupHour(ctx)
	if err != nil || !ok {
		return retentionCutoff, err
	}
	held := oldest.Add(-rawHoldbackMargin)
	if !held.Before(retentionCutoff) {
		return retentionCutoff, nil
	}
	holdback := retentionCutoff.Sub(held)
	if holdback > rawHoldbackCap {
		held, holdback = retentionCutoff.Add(-rawHoldbackCap), rawHoldbackCap
	}
	if holdback > rawHoldbackWarn {
		slog.Warn("packet cleanup held back by unrolled analytics hours", "component", "background",
			"holdback", holdback.Round(time.Minute), "oldest_missing_hour", oldest.UTC())
	}
	return held, nil
}

// ReconfirmTick is how often the reconfirm sweep runs: the hourly row budget is spent in slices this long.
const ReconfirmTick = 5 * time.Minute

type routeReconfirmer interface {
	ReconfirmSweep(context.Context, int) (db.ReconfirmResult, error)
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

// ReconfirmTask returns a Task that walks known_routes in primary-key order, deleting routes
// with a departed or now-ambiguous hop. Each tick checks rowsPerHour's share of the hour.
func ReconfirmTask(store routeReconfirmer, rowsPerHour int, tick time.Duration) Task {
	budget := max(1, rowsPerHour*int(tick/time.Minute)/60)
	return Task{
		Name:     "reconfirm",
		Interval: tick,
		Run: func(ctx context.Context) error {
			res, err := store.ReconfirmSweep(ctx, budget)
			if err != nil {
				return fmt.Errorf("routes: %w", err)
			}
			metrics.JobRows.WithLabelValues("reconfirm").Add(float64(res.Deleted))
			slog.Debug("route reconfirm", "component", "background", "scanned", res.Scanned,
				"deleted", res.Deleted, "skipped", res.Skipped, "timed_out", res.TimedOut, "wrapped", res.Wrapped)
			return nil
		},
	}
}
