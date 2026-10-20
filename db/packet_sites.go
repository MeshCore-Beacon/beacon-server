// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	packetSitesStateKey = "packet_sites"
	// packetSitesSlice is the observation ids one fill covers, in the backfill, the reconcile and the audit.
	packetSitesSlice int64 = 500_000
	// packetSitesFillAttempts bounds the retries of a fill that lost a packet to a concurrent cascade (23503).
	packetSitesFillAttempts = 3
	// packetSitesBackfillPause separates backfill slices, so the one-off backfill leaves room for other work.
	packetSitesBackfillPause = 100 * time.Millisecond
)

// PacketSitesState is background_state['packet_sites']: the one-off backfill's progress (newest first, from
// WriterFromID down) and the reconcile watermarks. Nothing in it says the table is exact; each process earns that.
type PacketSitesState struct {
	WriterFromID       int64      `json:"writer_from_id"`
	BackfilledDownToID int64      `json:"backfilled_down_to_id"`
	Complete           bool       `json:"complete"`
	ReconPrevHi        int64      `json:"recon_prev_hi"`
	ReconHi            int64      `json:"recon_hi"`
	ReconAt            *time.Time `json:"recon_at,omitempty"`
	FirstVerifiedAt    *time.Time `json:"first_verified_at,omitempty"`
}

// PacketSitesSession holds the packet_sites lock on one maintenance connection and the state it read after taking
// the lock; every statement and state write goes through it.
type PacketSitesSession struct {
	conn  *pgxpool.Conn
	q     *sqlc.Queries
	state PacketSitesState
	found bool // the state existed when the lock was taken
	pause time.Duration

	// fillStmt runs one fill; tests replace it. fills lists the (after, through] ranges filled, and retries counts
	// 23503 retries, for tests.
	fillStmt func(ctx context.Context, after, through int64) (int64, error)
	fills    [][2]int64
	retries  int
}

// BeginPacketSites takes the packet_sites lock for this schema on a dedicated connection and reads the persisted
// state under it. ok is false when another session holds the lock.
func (s *Store) BeginPacketSites(ctx context.Context) (*PacketSitesSession, bool, error) {
	if s.pool == nil {
		return nil, false, errors.New("packet_sites: store has no dedicated pool")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, hashtext(current_schema()))", packetSitesReconcileLockKey).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	p := &PacketSitesSession{conn: conn, q: sqlc.New(conn), pause: packetSitesBackfillPause}
	p.fillStmt = func(ctx context.Context, after, through int64) (int64, error) {
		return p.q.FillPacketSitesSlice(ctx, sqlc.FillPacketSitesSliceParams{AfterID: after, ThroughID: through})
	}
	if p.found, err = loadState(ctx, p.q, packetSitesStateKey, &p.state); err != nil {
		p.Close(ctx)
		return nil, false, fmt.Errorf("packet_sites state: %w", err)
	}
	return p, true, nil
}

// Close releases the lock and the connection. If the unlock fails, the connection is closed, which drops the lock.
func (p *PacketSitesSession) Close(ctx context.Context) {
	if p.conn == nil {
		return
	}
	if _, err := p.conn.Exec(ctx, "SELECT pg_advisory_unlock($1, hashtext(current_schema()))", packetSitesReconcileLockKey); err != nil {
		_ = p.conn.Conn().Close(ctx)
	}
	p.conn.Release()
	p.conn = nil
}

// State returns the state as of the session's last read or write.
func (p *PacketSitesSession) State() PacketSitesState { return p.state }

func (p *PacketSitesSession) save(ctx context.Context) error {
	if err := saveState(ctx, p.q, packetSitesStateKey, p.state); err != nil {
		return err
	}
	p.found = true
	return nil
}

// Init returns the state read under the lock or, on first use, starts the backfill and both reconcile watermarks
// at the newest observation id: the writer maintains every row above it.
func (p *PacketSitesSession) Init(ctx context.Context) (PacketSitesState, error) {
	if p.found {
		return p.state, nil
	}
	hi, err := p.q.MaxObservationID(ctx)
	if err != nil {
		return PacketSitesState{}, err
	}
	p.state = PacketSitesState{WriterFromID: hi, BackfilledDownToID: hi, Complete: hi == 0, ReconPrevHi: hi, ReconHi: hi}
	return p.state, p.save(ctx)
}

// fill runs one idempotent fill of (after, through], retrying only when a packet was cascaded away between the
// statement's snapshot and its foreign key check (SQLSTATE 23503).
func (p *PacketSitesSession) fill(ctx context.Context, after, through int64) (int64, error) {
	p.fills = append(p.fills, [2]int64{after, through})
	var err error
	for attempt := 1; attempt <= packetSitesFillAttempts; attempt++ {
		var n int64
		if n, err = p.fillStmt(ctx, after, through); err == nil {
			return n, nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" || attempt == packetSitesFillAttempts {
			break
		}
		p.retries++
	}
	return 0, err
}

// Backfill fills packet_sites from WriterFromID down, one slice of packetSitesSlice observation ids at a time,
// newest first, until it passes the oldest observation (then Complete) or budget is spent. Progress is saved after
// every slice, and a failed slice is retried from the same place on the next call.
func (p *PacketSitesSession) Backfill(ctx context.Context, budget time.Duration) (PacketSitesState, error) {
	deadline := time.Now().Add(budget)
	for !p.state.Complete && time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return p.state, err
		}
		lo, err := p.q.MinObservationID(ctx)
		if err != nil {
			return p.state, err
		}
		if lo == 0 || p.state.BackfilledDownToID < lo {
			p.state.Complete = true
			if err := p.save(ctx); err != nil {
				return p.state, err
			}
			slog.Info("packet_sites backfill complete", "component", "readmodel", "writer_from_id", p.state.WriterFromID)
			break
		}
		from := max(p.state.BackfilledDownToID-packetSitesSlice, lo-1)
		if _, err := p.fill(ctx, from, p.state.BackfilledDownToID); err != nil {
			return p.state, fmt.Errorf("packet_sites backfill (%d, %d]: %w", from, p.state.BackfilledDownToID, err)
		}
		p.state.BackfilledDownToID = from
		if err := p.save(ctx); err != nil {
			return p.state, err
		}
		if p.pause > 0 && !sleepCtx(ctx, p.pause) {
			return p.state, ctx.Err()
		}
	}
	return p.state, nil
}

// sleepCtx waits d or until ctx ends; it reports whether ctx is still live.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Reconcile fills the observation ids that arrived since the run before last, newest slice first, so each run
// overlaps the previous one by one interval (an id taken before a run read the maximum may commit after its
// snapshot), and advances the watermarks monotonically. It returns the rows written: anything above zero was
// written by a path that bypassed the read model.
func (p *PacketSitesSession) Reconcile(ctx context.Context) (int64, error) {
	hi, err := p.q.MaxObservationID(ctx)
	if err != nil {
		return 0, err
	}
	var rows int64
	for top := hi; top > p.state.ReconPrevHi; top -= packetSitesSlice {
		after := max(top-packetSitesSlice, p.state.ReconPrevHi)
		n, err := p.fill(ctx, after, top)
		if err != nil {
			return rows, fmt.Errorf("packet_sites reconcile (%d, %d]: %w", after, top, err)
		}
		rows += n
	}
	now := time.Now().UTC()
	st := p.state
	st.ReconPrevHi, st.ReconHi = st.ReconHi, max(hi, st.ReconHi)
	st.ReconAt = &now
	p.state = st
	return rows, p.save(ctx)
}

// Audit refills one slice of packetSitesSlice ids below ReconPrevHi, the ids no reconcile will see again; start
// picks its lower bound in [lo, hi]. It returns the rows written: anything above zero was missed by every writer
// and reconcile.
func (p *PacketSitesSession) Audit(ctx context.Context, start func(lo, hi int64) int64) (int64, error) {
	lo, err := p.q.MinObservationID(ctx)
	if err != nil {
		return 0, err
	}
	top := p.state.ReconPrevHi
	if lo == 0 || top < lo {
		return 0, nil
	}
	if top-(lo-1) <= packetSitesSlice {
		return p.fill(ctx, lo-1, top)
	}
	a := min(max(start(lo-1, top-packetSitesSlice), lo-1), top-packetSitesSlice)
	return p.fill(ctx, a, a+packetSitesSlice)
}
