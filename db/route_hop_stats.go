// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// hopStatsTTL is how long the store keeps known_routes' hop statistics before reading them again. They change only
// when the table is analyzed.
const hopStatsTTL = 5 * time.Minute

// hopStatsRetry is how soon a failed read is tried again; until then the global hop page keeps its last statistics.
const hopStatsRetry = 30 * time.Second

// hopStatsReadTimeout bounds one read of the statistics. The read runs on its own context, so a request that goes
// away does not fail it.
const hopStatsReadTimeout = 3 * time.Second

// hopStatsCache holds the planner's statistics for known_routes.hop_count, read at most once per hopStatsTTL.
type hopStatsCache struct {
	mu      sync.Mutex
	at      time.Time                       // when the last read started
	reading bool                            // a read is in flight
	stats   *sqlc.GetKnownRoutesHopStatsRow // nil until a read succeeds
}

// hopGroupSmall reports whether ListKnownRoutesGlobalHop should read the whole hop group (see hopGroupIsSmall).
// When the statistics are due, the first caller reads them while the others answer from the last ones; without
// statistics, or until a first read succeeds, it reports false: today's statement.
func (s *Store) hopGroupSmall(ctx context.Context, hop, limit int32) bool {
	c := &s.hopStats
	c.mu.Lock()
	refresh := !c.reading && (c.at.IsZero() || time.Since(c.at) >= hopStatsTTL)
	if refresh {
		c.reading, c.at = true, time.Now()
	}
	c.mu.Unlock()
	if refresh {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hopStatsReadTimeout)
		r, err := s.q.GetKnownRoutesHopStats(rctx)
		cancel()
		c.mu.Lock()
		c.reading = false
		if err != nil {
			c.at = c.at.Add(hopStatsRetry - hopStatsTTL) // try again soon, not on every page
			slog.Warn("db: reading known_routes hop statistics failed; global hop pages keep their last statistics",
				"component", "db", "error", err)
		} else {
			c.stats = &r
		}
		c.mu.Unlock()
	}
	c.mu.Lock()
	st := c.stats
	c.mu.Unlock()
	return st != nil && hopGroupIsSmall(*st, hop, limit)
}

// hopGroupIsSmall reports whether the statistics estimate hop's group under the real break-even between reading
// the group from the hop index (cost about the group size) and walking the keyset index (about
// limit * rows / group rows): 0.7 * sqrt(limit * rows), at least 1000. The planner's own break-even sits far lower,
// so it walks for groups where reading them is several times cheaper. Near the bound both cost about the same, so
// an estimate that lands on either side of it costs little. The estimate follows the planner's: the hop's
// most-common-value frequency, else the share of each remaining value, at most the least common listed one.
func hopGroupIsSmall(st sqlc.GetKnownRoutesHopStatsRow, hop, limit int32) bool {
	if !st.HasStats || st.Reltuples <= 0 {
		return false
	}
	freq := -1.0
	sum := 0.0
	for i, v := range st.HopValues {
		if i >= len(st.HopFreqs) {
			break
		}
		sum += st.HopFreqs[i]
		if v == hop {
			freq = st.HopFreqs[i]
		}
	}
	if freq < 0 {
		distinct := st.NDistinct
		if distinct < 0 {
			distinct = -distinct * st.Reltuples
		}
		freq = (1 - sum - st.NullFrac) / math.Max(distinct-float64(len(st.HopFreqs)), 1)
		// As the planner: a value outside the list is no more common than the least common one in it.
		if n := min(len(st.HopValues), len(st.HopFreqs)); n > 0 {
			freq = math.Min(freq, st.HopFreqs[n-1])
		}
	}
	freq = math.Min(math.Max(freq, 0), 1)
	bound := math.Max(1000, 0.7*math.Sqrt(st.Reltuples*float64(limit)))
	return freq*st.Reltuples < bound
}
