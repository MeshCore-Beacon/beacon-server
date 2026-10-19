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
	payloadTypeBackfillSpan    int64 = 1_000_000 // ids per batch
	payloadTypeBackfillBatches       = 10        // batches per run
)

type payloadTypeBackfiller interface {
	ObservationIDRange(context.Context) (int64, int64, error)
	BackfillObservationPayloadTypes(context.Context, int64, int64) (int64, error)
	LoadState(context.Context, string, any) (bool, error)
	SaveState(context.Context, string, any) error
}

// PayloadTypeBackfillTask types observations stored without a payload type, a bounded id range at a time.
func PayloadTypeBackfillTask(store payloadTypeBackfiller, interval time.Duration) Task {
	return payloadTypeBackfillTask(store, interval, payloadTypeBackfillSpan, payloadTypeBackfillBatches)
}

// payloadTypeBackfillTask walks packet_observations by id in spans of span ids, at most batches
// spans per run, up to the max id read at the run's start. It saves the next id after every
// batch, so a failed or interrupted run resumes where it stopped, and it saves done when a batch
// slot finds the walk past the max (at once for an empty table). Once done, a run only reads the state.
func payloadTypeBackfillTask(store payloadTypeBackfiller, interval time.Duration, span int64, batches int) Task {
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
				to := min(next+span, maxID+1)
				n, err := store.BackfillObservationPayloadTypes(ctx, next, to)
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
