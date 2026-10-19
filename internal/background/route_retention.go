// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

// RoutesGraceKey is the background_state key of the grace walk's watermark.
const RoutesGraceKey = "routes_grace"

// routeRetentionBatch is the row budget of one retention or grace statement.
const routeRetentionBatch int32 = 10_000

// fullPassEvery forces a grace walk from the retention cutoff, catching anything the watermark skipped.
const fullPassEvery = 7 * 24 * time.Hour

type routesGraceState struct {
	Watermark       time.Time `json:"watermark"`        // floor_ms(G) or the earliest unresolved candidate
	MinObservations int64     `json:"min_observations"` // a change forces a full pass
	Grace           string    `json:"grace"`            // time.Duration.String(); a change forces a full pass
	LastFullPass    time.Time `json:"last_full_pass"`   // the full pass's now, truncated to the hour
}

type routeRetainer interface {
	DeleteExpiredRoutes(context.Context, db.RouteKey, time.Time, int32) (int64, db.RouteKey, error)
	DeleteGraceRoutesSlice(context.Context, db.RouteKey, time.Time, int64, int32) (db.GraceSliceResult, error)
	LoadState(context.Context, string, any) (bool, error)
	SaveState(context.Context, string, any) error
	ReconfirmNeighbors(context.Context) error
}

// RouteRetentionTask deletes routes last seen before the retention cutoff, then routes last seen
// before the grace cutoff with fewer than minObs observations, then stale neighbor edges.
func RouteRetentionTask(store routeRetainer, retention, grace time.Duration, minObs int64, interval time.Duration) Task {
	return routeRetentionTask(store, retention, grace, minObs, interval, time.Now, routeRetentionBatch)
}

func routeRetentionTask(store routeRetainer, retention, grace time.Duration, minObs int64, interval time.Duration, now func() time.Time, batch int32) Task {
	return Task{
		Name:     "route_retention",
		Interval: interval,
		Run: func(ctx context.Context) error {
			t := now()
			// The exact cutoffs floored to µs, as pgx sends them (lossless). Every comparison below uses
			// these values, so the walk's stop test agrees with the window end the SQL computes.
			R, G := t.Add(-retention).Truncate(time.Microsecond), t.Add(-grace).Truncate(time.Microsecond)

			var expired int64
			for after := (db.RouteKey{}); ; {
				selected, last, err := store.DeleteExpiredRoutes(ctx, after, R, batch)
				if err != nil {
					return fmt.Errorf("route retention: %w", err)
				}
				expired += selected
				if selected < int64(batch) {
					break
				}
				after = last
			}

			var st routesGraceState
			ok, err := store.LoadState(ctx, RoutesGraceKey, &st)
			if err != nil {
				return fmt.Errorf("route grace state: %w", err)
			}
			full := !ok || st.MinObservations != minObs || st.Grace != grace.String() || t.Before(st.LastFullPass) || t.Sub(st.LastFullPass) >= fullPassEvery
			// Never from RouteKey{} (-infinity): routes before R are gone, so the walk starts at floor_ms(R).
			start := R.Truncate(time.Millisecond)
			lastFull := st.LastFullPass
			if full {
				lastFull = t.Truncate(time.Hour)
			} else if st.Watermark.After(start) {
				start = st.Watermark
			}

			watermark, candidates, deleted, err := graceWalk(ctx, store, start, G, minObs, batch)
			if err != nil {
				return fmt.Errorf("route grace: %w", err)
			}
			if err := store.SaveState(ctx, RoutesGraceKey, routesGraceState{
				Watermark: watermark, MinObservations: minObs, Grace: grace.String(), LastFullPass: lastFull,
			}); err != nil {
				return fmt.Errorf("route grace state: %w", err)
			}
			slog.Debug("route retention", "component", "background", "expired", expired,
				"grace_candidates", candidates, "grace_deleted", deleted, "full_pass", full, "watermark", watermark)

			if err := store.ReconfirmNeighbors(ctx); err != nil {
				return fmt.Errorf("neighbors: %w", err)
			}
			return nil
		},
	}
}

// graceWalk runs the grace slices from start until a window reaches ceil_ms(G): a full batch stays
// in its window, a short one moves to the next. It returns min(floor_ms(G), earliest unresolved
// candidate), where the next incremental walk starts. A window that does not advance is an error,
// never a retry of the same statement.
func graceWalk(ctx context.Context, store routeRetainer, start, G time.Time, minObs int64, batch int32) (watermark time.Time, candidates, deleted int64, err error) {
	watermark = G.Truncate(time.Millisecond)
	after := db.RouteKey{TS: start}
	for {
		res, err := store.DeleteGraceRoutesSlice(ctx, after, G, minObs, batch)
		if err != nil {
			return time.Time{}, 0, 0, err
		}
		candidates += res.Candidates
		deleted += res.Deleted
		if res.Unresolved != nil && res.Unresolved.Before(watermark) {
			watermark = *res.Unresolved
		}
		switch {
		case res.Candidates == int64(batch):
			after = res.Last
		case !res.WindowEnd.Before(G):
			return watermark, candidates, deleted, nil
		case !res.WindowEnd.After(after.TS):
			return time.Time{}, 0, 0, fmt.Errorf("grace walk made no progress at %v (window end %v, cutoff %v)",
				after.TS, res.WindowEnd, G)
		default:
			after = db.RouteKey{TS: res.WindowEnd}
		}
	}
}
