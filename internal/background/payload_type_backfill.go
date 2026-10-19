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
)

const (
	payloadTypeBackfillSpan    int64 = 1_000_000 // ids a batch reads at most
	payloadTypeBackfillRows    int32 = 50_000    // rows a batch writes at most
	payloadTypeBackfillBatches       = 10        // batches per run
)

type payloadTypeBackfiller interface {
	ObservationIDRange(context.Context) (int64, int64, error)
	HasUntypedObservations(context.Context, int64) (bool, error)
	BackfillObservationPayloadTypes(context.Context, int64, int64, int32) (int64, int64, error)
	LoadState(context.Context, string, any) (bool, error)
	SaveState(context.Context, string, any) error
}

// PayloadTypeBackfillTask types observations stored without a payload type, a bounded id range at a time.
func PayloadTypeBackfillTask(store payloadTypeBackfiller, interval time.Duration) Task {
	return payloadTypeBackfillTask(store, interval, payloadTypeBackfillSpan, payloadTypeBackfillRows, payloadTypeBackfillBatches)
}

// payloadTypeBackfillTask walks packet_observations by id, at most batches statements per run, up
// to the max id read at the run's start. Each batch reads at most span ids and types at most
// maxRows of them; when it hits maxRows it continues after the last row it picked. It saves the
// next id after every batch, so a failed or interrupted run resumes where it stopped, and it saves done when a batch
// slot finds the walk past the max (at once for an empty table). Once done, a run only reads the state.
//
// Before its first batch, a task value asks once whether any observation up to the max lacks a
// payload type. When none does it saves done at once instead of walking the whole id range. That
// answer stays true: ingest writes payload_type on every observation it stores, so no untyped row
// can appear later, and the walk would have typed nothing. Done only switches the adverts reader to
// the statement that filters on the observation's own type, which returns the same rows once every
// observation carries one; it just switches sooner. When the probe finds an untyped row, or fails
// (logged once), the task walks as before and never probes again.
func payloadTypeBackfillTask(store payloadTypeBackfiller, interval time.Duration, span int64, maxRows int32, batches int) Task {
	probed := false
	return Task{
		Name:     "payload_type_backfill",
		Interval: interval,
		Run: func(ctx context.Context) error {
			var st db.ObservationPayloadTypeState
			if _, err := store.LoadState(ctx, db.ObservationPayloadTypeKey, &st); err != nil {
				return fmt.Errorf("payload type backfill: %w", err)
			}
			if st.Done {
				return nil
			}
			minID, maxID, err := store.ObservationIDRange(ctx)
			if err != nil {
				return fmt.Errorf("payload type backfill: %w", err)
			}
			if maxID > 0 && !probed {
				probed = true
				untyped, err := store.HasUntypedObservations(ctx, maxID)
				switch {
				case err != nil:
					slog.Warn("observation payload type probe failed; walking the id range", "component", "background", "error", err)
				case !untyped:
					next := maxID + 1
					if err := store.SaveState(ctx, db.ObservationPayloadTypeKey, db.ObservationPayloadTypeState{NextID: next, Done: true}); err != nil {
						return fmt.Errorf("payload type backfill: %w", err)
					}
					slog.Info("observation payload types complete", "component", "background", "next_id", next, "probe", true)
					return nil
				}
			}
			next := max(st.NextID, minID)
			var rows int64
			defer func() {
				metrics.JobRows.WithLabelValues("payload_type_backfill").Add(float64(rows))
				slog.Debug("observation payload type backfill", "component", "background", "next_id", next, "rows", rows)
			}()
			for range batches {
				if maxID == 0 || next > maxID {
					if err := store.SaveState(ctx, db.ObservationPayloadTypeKey, db.ObservationPayloadTypeState{NextID: next, Done: true}); err != nil {
						return fmt.Errorf("payload type backfill: %w", err)
					}
					slog.Info("observation payload types complete", "component", "background", "next_id", next)
					return nil
				}
				n, to, err := store.BackfillObservationPayloadTypes(ctx, next, min(next+span, maxID+1), maxRows)
				if err != nil {
					return fmt.Errorf("payload type backfill: %w", err)
				}
				rows += n
				next = to
				if err := store.SaveState(ctx, db.ObservationPayloadTypeKey, db.ObservationPayloadTypeState{NextID: next}); err != nil {
					return fmt.Errorf("payload type backfill: %w", err)
				}
			}
			return nil
		},
	}
}
