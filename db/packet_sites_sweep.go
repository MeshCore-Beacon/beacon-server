// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// packetSitesSweepIndexes are the time-ordered indexes the front sweep keeps dense.
var packetSitesSweepIndexes = []string{"idx_packet_sites_site", "idx_packet_sites_site_type", "idx_packet_sites_site_scope"}

// SweepResult is the entries the front sweep visited on each index.
type SweepResult struct{ SiteLive, TypeLive, ScopeLive int64 }

// sweepSettings run the sweep with plain index scans only: a bitmap scan never sets a dead-entry hint. JIT is off:
// with scans disabled a plan's cost can pass the JIT thresholds, and the sweep runs every 30 seconds.
const sweepSettings = "SET LOCAL enable_bitmapscan = off; SET LOCAL enable_seqscan = off; SET LOCAL jit = off; " +
	"SET LOCAL statement_timeout = '5s'; SET LOCAL lock_timeout = '1s'"

func sweepLookback(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

// SweepPacketSitesFronts runs the read-only front sweep in its own transaction on one maintenance connection, with
// bitmap and seq scans off, statement_timeout 5s and lock_timeout 1s.
func (s *Store) SweepPacketSitesFronts(ctx context.Context, lookback time.Duration) (SweepResult, error) {
	if s.pool == nil {
		return SweepResult{}, errors.New("packet_sites sweep: store has no dedicated pool")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return SweepResult{}, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // read-only
	if _, err := tx.Exec(ctx, sweepSettings); err != nil {
		return SweepResult{}, err
	}
	row, err := sqlc.New(tx).SweepPacketSitesFronts(ctx, sweepLookback(lookback))
	if err != nil {
		return SweepResult{}, err
	}
	return SweepResult(row), tx.Commit(ctx)
}

// PacketSitesSweepPlanOK plans the sweep (EXPLAIN (FORMAT JSON) under the same settings, parameters bound) and
// reports whether it reads packet_sites only through index scans and uses each of the three time-ordered indexes.
// detail lists the scans, or what is wrong.
func (s *Store) PacketSitesSweepPlanOK(ctx context.Context) (ok bool, detail string, err error) {
	if s.pool == nil {
		return false, "", errors.New("packet_sites sweep: store has no dedicated pool")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // read-only
	return sweepPlanOK(ctx, tx)
}

// sweepPlanOK runs the plan check inside tx.
func sweepPlanOK(ctx context.Context, tx pgx.Tx) (bool, string, error) {
	if _, err := tx.Exec(ctx, sweepSettings); err != nil {
		return false, "", err
	}
	rec := &sweepTextRecorder{}
	_, _ = sqlc.New(rec).SweepPacketSitesFronts(ctx, sweepLookback(2*time.Minute))
	var plan []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+rec.sql, rec.args...).Scan(&plan); err != nil {
		return false, "", err
	}
	var doc []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(plan, &doc); err != nil || len(doc) == 0 {
		return false, "", fmt.Errorf("sweep plan: %w", err)
	}
	var scans, bad []string
	used := map[string]bool{}
	var walk func(n map[string]any)
	walk = func(n map[string]any) {
		if n["Relation Name"] == "packet_sites" {
			node, _ := n["Node Type"].(string)
			idx, _ := n["Index Name"].(string)
			scans = append(scans, node+" on "+idx)
			if node == "Index Scan" || node == "Index Only Scan" {
				used[idx] = true
			} else {
				bad = append(bad, node+" on packet_sites")
			}
		}
		if kids, ok := n["Plans"].([]any); ok {
			for _, k := range kids {
				if m, ok := k.(map[string]any); ok {
					walk(m)
				}
			}
		}
	}
	walk(doc[0].Plan)
	for _, idx := range packetSitesSweepIndexes {
		if !used[idx] {
			bad = append(bad, "no index scan on "+idx)
		}
	}
	slices.Sort(scans)
	scans = slices.Compact(scans)
	if len(bad) > 0 {
		return false, strings.Join(bad, "; ") + " (plan: " + strings.Join(scans, ", ") + ")", nil
	}
	return true, strings.Join(scans, ", "), nil
}

// sweepTextRecorder captures one statement's text and arguments without running it.
type sweepTextRecorder struct {
	sql  string
	args []any
}

func (r *sweepTextRecorder) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sql, r.args = sql, args
	return pgconn.CommandTag{}, errNotRun
}

func (r *sweepTextRecorder) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.sql, r.args = sql, args
	return nil, errNotRun
}

func (r *sweepTextRecorder) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	r.sql, r.args = sql, args
	return notRunRow{}
}

var errNotRun = errors.New("statement recorded, not run")

type notRunRow struct{}

func (notRunRow) Scan(...any) error { return errNotRun }

// PacketSitesTotalBytes returns packet_sites' heap and index size.
func (s *Store) PacketSitesTotalBytes(ctx context.Context) (int64, error) {
	return s.q.PacketSitesTotalBytes(ctx)
}
