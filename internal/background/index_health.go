// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// unusedIndexLogEvery is how often the unused-index list is logged after the first run.
const unusedIndexLogEvery = 7 * 24 * time.Hour

type indexHealthStore interface {
	MeasureIndexHealth(context.Context) ([]db.IndexHealth, error)
	UnusedIndexes(context.Context) ([]db.UnusedIndex, error)
}

// IndexHealthTask exports each index's size and its bloat ratio against the baseline taken when
// the index was first seen or last rebuilt. On its first run and then weekly it logs the indexes
// with no scans. It applies no thresholds and rebuilds nothing: alerts read the metrics.
func IndexHealthTask(store indexHealthStore, interval time.Duration) Task {
	var exported map[string]bool
	var lastUnusedLog time.Time
	return Task{
		Name:     "index_health",
		Interval: interval,
		Run: func(ctx context.Context) error {
			health, err := store.MeasureIndexHealth(ctx)
			if err != nil {
				return fmt.Errorf("index health: %w", err)
			}
			seen := make(map[string]bool, len(health))
			for _, h := range health {
				seen[h.Name] = true
				metrics.IndexBytes.WithLabelValues(h.Name).Set(float64(h.Bytes))
				if h.HasRatio {
					metrics.IndexBloat.WithLabelValues(h.Name).Set(h.Ratio)
				} else {
					metrics.IndexBloat.DeleteLabelValues(h.Name)
				}
			}
			for name := range exported {
				if !seen[name] {
					metrics.IndexBytes.DeleteLabelValues(name)
					metrics.IndexBloat.DeleteLabelValues(name)
				}
			}
			exported = seen

			if !lastUnusedLog.IsZero() && time.Since(lastUnusedLog) < unusedIndexLogEvery {
				return nil
			}
			unused, err := store.UnusedIndexes(ctx)
			if err != nil {
				return fmt.Errorf("unused indexes: %w", err)
			}
			lastUnusedLog = time.Now()
			if len(unused) > 0 {
				slog.Info("indexes with no scans since the last statistics reset", "component", "background",
					"indexes", formatUnusedIndexes(unused))
			}
			return nil
		},
	}
}

// formatUnusedIndexes renders indexes as "[name (size), ...]".
func formatUnusedIndexes(unused []db.UnusedIndex) string {
	parts := make([]string, len(unused))
	for i, u := range unused {
		parts[i] = fmt.Sprintf("%s (%s)", u.Name, formatBytes(u.Bytes))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// PgStatsTask exports PostgreSQL's WAL, checkpoint and transaction ID age counters and how much of each sequence is
// used. A failing read logs once per distinct error and never fails the task, so a missing view does not spam; the
// two reads fail independently.
func PgStatsTask(store interface {
	PgActivity(context.Context) (metrics.PgActivity, error)
	SequenceUse(context.Context) ([]metrics.SequenceUse, error)
}, interval time.Duration) Task {
	var lastErr, lastSeqErr string
	return Task{
		Name:     "pg_stats",
		Interval: interval,
		Run: func(ctx context.Context) error {
			if a, err := store.PgActivity(ctx); err != nil {
				if msg := err.Error(); msg != lastErr {
					lastErr = msg
					slog.Warn("postgres activity unavailable", "component", "background", "error", err)
				}
			} else {
				lastErr = ""
				metrics.SetPgActivity(a)
			}
			if seqs, err := store.SequenceUse(ctx); err != nil {
				if msg := err.Error(); msg != lastSeqErr {
					lastSeqErr = msg
					slog.Warn("sequence headroom unavailable", "component", "background", "error", err)
				}
			} else {
				lastSeqErr = ""
				metrics.SetSequenceUse(seqs)
			}
			return nil
		},
	}
}
