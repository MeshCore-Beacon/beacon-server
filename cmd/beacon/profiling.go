// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/MeshCore-Beacon/beacon-server/internal/profiling"
	"github.com/jackc/pgx/v5/pgxpool"
)

func configureProfiling(ctx context.Context, pool *pgxpool.Pool) *profiling.Recorder {
	r, err := profiling.Start(ctx, os.Getenv("BEACON_CPU_PROFILE_DIR"), os.Getenv("BEACON_CPU_PROFILE_UNTIL"), []string{"reconfirm"}, func() any {
		s := pool.Stat()
		return struct {
			Acquired, Idle, Total, Max                int32
			Acquires, EmptyAcquires, CanceledAcquires int64
			AcquireDurationNS                         int64
		}{
			s.AcquiredConns(), s.IdleConns(), s.TotalConns(), s.MaxConns(),
			s.AcquireCount(), s.EmptyAcquireCount(), s.CanceledAcquireCount(), int64(s.AcquireDuration()),
		}
	})
	if err != nil {
		slog.Warn("CPU profiling unavailable", "component", "profiling", "error", err)
	}
	return r
}
