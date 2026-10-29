// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	packetSitesStateKey = "packet_sites"
	// packetSitesSlice is the observation ids one fill covers in the reconcile, and the observations in the backfill's
	// default slice (readmodels.fill.packet_sites_slice) and in the audit's and verification's sampled slice.
	packetSitesSlice     int64 = 500_000
	packetSitesBootSlice int64 = 50_000
	// packetSitesFillAttempts bounds the retries of a fill that lost a packet to a concurrent cascade (23503).
	packetSitesFillAttempts = 3
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
	store *Store
	conn  *pgxpool.Conn
	q     *sqlc.Queries
	state PacketSitesState
	found bool // the state existed when the lock was taken

	// The backfill's pacing (see FillPacing): pause is the shortest wait after a slice, duty the share of time it may
	// be busy, slice the observations one slice covers. progress logs it once a minute; nil logs nothing.
	pause    time.Duration
	duty     float64
	slice    int64
	progress *FillProgress
	sliced   bool         // this session's Backfill has filled a slice
	ids      *idHistogram // the statistics progress counts in, kept for the whole fill so its units never change
	sample   int64        // observations in the audit's and verification's sampled slice; tests shrink it

	// fillStmt runs one fill; tests replace it. fills lists the (after, through] ranges filled, and retries counts
	// 23503 retries, for tests.
	fillStmt      func(ctx context.Context, after, through int64) (int64, error)
	fills         [][2]int64
	checks        [][2]int64 // the (after, through] ranges Verify checked, for tests
	retries       int
	backfillSpan  int64 // observations per boot write, independent of ID gaps
	backfillLimit time.Duration
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
	if _, err := conn.Exec(ctx, fillPlanMode); err != nil {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1, hashtext(current_schema()))", packetSitesReconcileLockKey)
		conn.Release()
		return nil, false, err
	}
	pace := s.FillPacing()
	p := &PacketSitesSession{store: s, conn: conn, q: sqlc.New(conn),
		pause: pace.MinPause, duty: pace.Duty, slice: pace.PacketSitesSlice, progress: &s.packetSitesProgress,
		ids: &s.packetSitesIDs, sample: packetSitesSlice}
	p.fillStmt = func(ctx context.Context, after, through int64) (int64, error) {
		return p.q.FillPacketSitesSlice(ctx, sqlc.FillPacketSitesSliceParams{AfterID: after, ThroughID: through})
	}
	if p.found, err = loadState(ctx, p.q, packetSitesStateKey, &p.state); err != nil {
		p.Close(ctx)
		return nil, false, fmt.Errorf("packet_sites state: %w", err)
	}
	analyzeAtStart(ctx, conn, "packet_sites")
	return p, true, nil
}

// Close releases the lock and the connection. If the unlock fails, the connection is closed, which drops the lock.
func (p *PacketSitesSession) Close(ctx context.Context) {
	if p.conn == nil {
		return
	}
	_, resetErr := p.conn.Exec(ctx, "RESET plan_cache_mode")
	if _, err := p.conn.Exec(ctx, "SELECT pg_advisory_unlock($1, hashtext(current_schema()))", packetSitesReconcileLockKey); err != nil || resetErr != nil {
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
	if p.progress != nil {
		p.progress.Reset() // a new fill (first install or rebuild) times itself from its own start
	}
	return p.state, p.save(ctx)
}

// fill runs one idempotent fill of (after, through], retrying only when a packet was cascaded away between the
// statement's snapshot and its foreign key check (SQLSTATE 23503).
func (p *PacketSitesSession) fill(ctx context.Context, after, through int64) (int64, error) {
	return p.fillWith(ctx, after, through, p.fillStmt)
}

// fillWith runs stmt as fill runs FillPacketSitesSlice.
func (p *PacketSitesSession) fillWith(ctx context.Context, after, through int64, stmt func(ctx context.Context, after, through int64) (int64, error)) (int64, error) {
	p.fills = append(p.fills, [2]int64{after, through})
	var err error
	for attempt := 1; attempt <= packetSitesFillAttempts; attempt++ {
		var n int64
		if n, err = stmt(ctx, after, through); err == nil {
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

// Backfill fills packet_sites from WriterFromID down, one slice of observations at a time (the session's slice,
// packetSitesSlice by default), newest first, until it passes the oldest observation (then Complete) or budget is
// spent. A slice is the next slice observations by id, so a long, sparse id range costs what its rows cost. Progress
// is saved after every slice, and a failed slice is retried from the same place on the next call. After each slice
// it waits as FillPacing.Wait does, so it is busy at most duty of the time. Its progress log and ETA count
// estimated observations, not ids.
func (p *PacketSitesSession) Backfill(ctx context.Context, budget time.Duration) (PacketSitesState, error) {
	deadline := time.Now().Add(budget)
	if p.backfillSpan <= 0 {
		p.backfillSpan = packetSitesBootSlice
		if p.slice > 0 {
			p.backfillSpan = min(p.backfillSpan, p.slice)
		}
	}
	for !p.state.Complete && time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return p.state, err
		}
		began := time.Now()
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
			if p.progress != nil {
				p.progress.Reset()
			}
			break
		}
		if p.progress != nil && !p.progress.Started() {
			p.loadIDs(ctx)
			p.progress.Start(p.ids.rowsOrIDs(p.state.BackfilledDownToID, p.state.WriterFromID))
		}
		// A slice holds slice observations, however far apart their ids are: retention leaves a few old observations
		// spread over a long id range, which would otherwise take a paced slice per slice ids.
		from, err := p.q.ObservationIDBelow(ctx, sqlc.ObservationIDBelowParams{ThroughID: p.state.BackfilledDownToID, N: p.backfillSpan})
		if err != nil {
			return p.state, err
		}
		from = max(from, lo-1)
		timeout := backfillTimeout(p.backfillLimit, deadline)
		_, err = p.fillWith(ctx, from, p.state.BackfilledDownToID, func(ctx context.Context, after, through int64) (int64, error) {
			return backfillWrite(ctx, p.conn, timeout, func() (int64, error) { return p.fillStmt(ctx, after, through) })
		})
		if err != nil {
			if backfillTimedOut(ctx, err) && timeout < backfillMaxTimeout(p.backfillLimit) {
				return p.state, nil
			}
			if backfillTimedOut(ctx, err) && p.backfillSpan > 1 {
				p.backfillSpan = max(1, p.backfillSpan/2)
				continue
			}
			return p.state, fmt.Errorf("packet_sites backfill (%d, %d]: %w", from, p.state.BackfilledDownToID, err)
		}
		p.state.BackfilledDownToID = from
		if err := p.save(ctx); err != nil {
			return p.state, err
		}
		if !p.sliced {
			// A new table now holds a slice's rows; plan the next slices on them.
			analyzeAfterFirstSlice(ctx, p.conn, "packet_sites")
			p.sliced = true
		}
		if p.progress != nil && p.progress.Due() {
			done, left := p.ids.rowsOrIDs(from, p.state.WriterFromID), p.ids.rowsOrIDs(lo-1, from)
			p.progress.LogLeft("packet_sites backfill progress", done, left,
				"ids_done", p.state.WriterFromID-from, "ids_remaining", from-(lo-1),
				"observations_done", done, "observations_remaining", left,
				"backfilled_down_to_id", from, "writer_from_id", p.state.WriterFromID)
		}
		pace := FillPacing{Duty: p.duty, MinPause: p.pause}
		if !pace.Wait(ctx, time.Since(began)) {
			return p.state, ctx.Err()
		}
	}
	return p.state, nil
}

// loadSampleIDs reads the statistics a sampled slice is picked with, outside any transaction, so a failed read
// only falls back to picking by ids.
func (p *PacketSitesSession) loadSampleIDs(ctx context.Context) idHistogram {
	ids, err := loadObservationIDHistogram(ctx, p.conn)
	if err != nil {
		slog.Warn("packet_sites: cannot read observation statistics; the sampled slice is picked by ids", "component", "readmodel", "error", err)
		return idHistogram{}
	}
	return ids
}

// sampleSlice picks a slice of p.sample observations in (after, through], starting u of the way through the
// range by row (see idHistogram.sliceStart); a range holding fewer observations is sampled whole.
func (p *PacketSitesSession) sampleSlice(ctx context.Context, q sqlc.Querier, ids idHistogram, after, through int64, u float64) (int64, int64, error) {
	from := ids.sliceStart(after, through, p.sample, u)
	to, err := q.ObservationIDAbove(ctx, sqlc.ObservationIDAboveParams{AfterID: from, Skip: p.sample - 1})
	if err != nil {
		return 0, 0, err
	}
	if to == 0 || to > through {
		to = through
	}
	return from, to, nil
}

// loadIDs reads the observation id statistics the progress log counts in, when its clock starts; without them
// (a failed read, or no ANALYZE yet) progress counts ids.
func (p *PacketSitesSession) loadIDs(ctx context.Context) {
	h, err := loadObservationIDHistogram(ctx, p.conn)
	if err != nil {
		slog.Warn("packet_sites backfill: cannot read observation statistics; progress counts ids", "component", "readmodel", "error", err)
		h = idHistogram{}
	}
	*p.ids = h
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

// Audit refills one sampled slice (packetSitesSlice observations) at or below ReconPrevHi, the ids no reconcile will see
// again, each pair at its latest hearing among all the packet's observations. The slice starts u (in [0, 1)) of the
// way through those observations by row, so every observation is as likely to be audited however sparse its ids
// are. It returns the rows written: anything above zero was missed by every writer and reconcile.
func (p *PacketSitesSession) Audit(ctx context.Context, u float64) (int64, error) {
	lo, err := p.q.MinObservationID(ctx)
	if err != nil {
		return 0, err
	}
	top := p.state.ReconPrevHi
	if lo == 0 || top < lo {
		return 0, nil
	}
	after, through, err := p.sampleSlice(ctx, p.q, p.loadSampleIDs(ctx), lo-1, top, u)
	if err != nil {
		return 0, err
	}
	return p.fillWith(ctx, after, through, func(ctx context.Context, after, through int64) (int64, error) {
		return p.q.FillPacketSitesAudit(ctx, sqlc.FillPacketSitesAuditParams{AfterID: after, ThroughID: through})
	})
}

const (
	// packetSitesVerifySites is how many of the busiest recent sites the verification pages.
	packetSitesVerifySites = 5
	// packetSitesVerifyLimit is the verification's page size.
	packetSitesVerifyLimit = 50
	// packetSitesReadOnlyCheckIDs is how many of the newest observation ids a read-only server checks exactly.
	packetSitesReadOnlyCheckIDs int64 = 20_000
)

// PacketSitesVerification is the outcome of one verification pass.
type PacketSitesVerification struct {
	Missing, Wrong, Extra int64 // CheckPacketSitesSlice over the sampled slices
	PagesCompared         int
	PageDiffs             []string // "sites [AAA BBB] page 2: row 7 has <hash>@<ts>, the observations give <hash>@<ts>"
}

// OK reports a clean pass.
func (v PacketSitesVerification) OK() bool {
	return v.Missing == 0 && v.Wrong == 0 && v.Extra == 0 && len(v.PageDiffs) == 0
}

func (v *PacketSitesVerification) add(c sqlc.CheckPacketSitesSliceRow) {
	v.Missing += c.Missing
	v.Wrong += c.Wrong
	v.Extra += c.Extra
}

// Verify is the first-time verification. The caller has just reconciled (Verify does not reconcile again, which
// would shrink the next run's overlap). It VACUUMs and ANALYZEs the table once per process (index-only scans need
// its visibility map; a retry every interval does not repeat it), then in one read-only snapshot runs the exact check
// over the newest slice of observation ids and a random slice of the observations below it, picked by row, and compares region pages of the busiest
// recent sites with pages read straight from the observations. The first pass is persisted as FirstVerifiedAt. The
// caller retries a failed pass every interval.
func (p *PacketSitesSession) Verify(ctx context.Context) (PacketSitesVerification, error) {
	if p.store == nil || p.store.packetSitesVacuums.Load() == 0 {
		if _, err := p.conn.Exec(ctx, "VACUUM (ANALYZE) packet_sites"); err != nil {
			return PacketSitesVerification{}, fmt.Errorf("packet_sites vacuum: %w", err)
		}
		if p.store != nil {
			p.store.packetSitesVacuums.Add(1)
		}
	}
	ids := p.loadSampleIDs(ctx)
	tx, err := p.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return PacketSitesVerification{}, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // read-only
	q := sqlc.New(tx)
	hi := p.state.ReconHi
	lo, err := q.MinObservationID(ctx)
	if err != nil {
		return PacketSitesVerification{}, err
	}
	ranges := [][2]int64{{max(hi-packetSitesSlice, 0), hi}}
	if lo > 0 && hi-packetSitesSlice >= lo {
		// A random slice of the observations below, picked by row: in a long, sparse old id range a slice of ids
		// would hold almost nothing.
		after, through, err := p.sampleSlice(ctx, q, ids, lo-1, hi-packetSitesSlice, rand.Float64())
		if err != nil {
			return PacketSitesVerification{}, err
		}
		ranges = append(ranges, [2]int64{after, through})
	}
	var v PacketSitesVerification
	p.checks = append(p.checks, ranges...)
	for _, r := range ranges {
		c, err := q.CheckPacketSitesSlice(ctx, sqlc.CheckPacketSitesSliceParams{AfterID: r[0], ThroughID: r[1]})
		if err != nil {
			return v, err
		}
		v.add(c)
	}
	if err := comparePacketSitePages(ctx, q, max(hi-packetSitesSlice, 0), &v); err != nil {
		return v, err
	}
	if err := tx.Rollback(ctx); err != nil {
		return v, err
	}
	if !v.OK() {
		slog.Error("packet_sites verification failed; region packet pages answer 503 until a later pass succeeds", "component", "readmodel",
			"missing", v.Missing, "wrong", v.Wrong, "extra", v.Extra, "pages", v.PagesCompared, "diffs", len(v.PageDiffs),
			"first", v.PageDiffs[:min(len(v.PageDiffs), 3)])
		return v, nil
	}
	if p.state.FirstVerifiedAt == nil {
		now := time.Now().UTC()
		p.state.FirstVerifiedAt = &now
		if err := p.save(ctx); err != nil {
			return v, err
		}
		slog.Info("packet_sites first-time verification passed", "component", "readmodel", "pages", v.PagesCompared)
	}
	return v, nil
}

// comparePacketSitePages pages the busiest sites among observations above after (each alone, and the top three
// together), from the first row and from page 1's 26th row, and compares each page's keys with the keys read from
// the observations down to the page's last key.
func comparePacketSitePages(ctx context.Context, q sqlc.Querier, after int64, v *PacketSitesVerification) error {
	samples, err := q.ListPacketSiteSamples(ctx, sqlc.ListPacketSiteSamplesParams{AfterID: after, RowLimit: packetSitesVerifySites})
	if err != nil {
		return err
	}
	var lists [][]string
	for _, s := range samples {
		lists = append(lists, []string{s.Iata})
	}
	if len(samples) >= 3 {
		lists = append(lists, []string{samples[0].Iata, samples[1].Iata, samples[2].Iata})
	}
	for _, sites := range lists {
		var cursor *api.PacketCursor
		for page := 1; page <= 2; page++ {
			f, _ := newPacketFilter(nil, nil, nil, time.Time{}, time.Time{}, 0, cursor, packetSitesVerifyLimit)
			rows, _, err := listSiteRows(ctx, q, f, sites)
			if err != nil {
				return err
			}
			floor := pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
			if len(rows) == int(f.pageLimit) {
				floor = rows[len(rows)-1].SiteHeardAt
			}
			truth, err := q.TruthPacketsBySitesPage(ctx, sqlc.TruthPacketsBySitesPageParams{
				Iatas: sites, FloorTs: floor, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit,
			})
			if err != nil {
				return err
			}
			v.PagesCompared++
			for i := range max(len(rows), len(truth)) {
				var got, want string
				if i < len(rows) {
					got = fmt.Sprintf("%x@%s", rows[i].PacketHash, rows[i].SiteHeardAt.Time.UTC().Format(time.RFC3339Nano))
				}
				if i < len(truth) {
					want = fmt.Sprintf("%x@%s", truth[i].PacketHash, truth[i].SiteHeardAt.Time.UTC().Format(time.RFC3339Nano))
				}
				if got != want {
					v.PageDiffs = append(v.PageDiffs, fmt.Sprintf("sites %v page %d: row %d has %s, the observations give %s", sites, page, i+1, got, want))
					break
				}
			}
			if len(rows) <= packetSitesVerifyLimit/2 {
				break
			}
			r := rows[packetSitesVerifyLimit/2]
			cursor = &api.PacketCursor{At: r.SiteHeardAt.Time, Hash: r.PacketHash}
		}
	}
	return nil
}

// PacketSitesReadOnlyCheck is what a read-only server reads to decide whether to trust the table another process
// maintains.
type PacketSitesReadOnlyCheck struct {
	State        PacketSitesState
	Verification PacketSitesVerification // the exact check over the newest observation ids
	Age          time.Duration           // since the writer last saved the state, on the database clock
	Found        bool                    // the state exists
}

// CheckPacketSitesReadOnly reads the persisted packet_sites state and its age, and runs the exact check over the
// newest packetSitesReadOnlyCheckIDs observation ids. It writes nothing.
func (s *Store) CheckPacketSitesReadOnly(ctx context.Context) (PacketSitesReadOnlyCheck, error) {
	row, err := s.q.GetBackgroundStateAge(ctx, packetSitesStateKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return PacketSitesReadOnlyCheck{}, nil
	}
	if err != nil {
		return PacketSitesReadOnlyCheck{}, err
	}
	out := PacketSitesReadOnlyCheck{Found: true, Age: time.Duration(row.AgeSeconds * float64(time.Second))}
	if err := json.Unmarshal(row.Value, &out.State); err != nil {
		return out, fmt.Errorf("packet_sites state: %w", err)
	}
	// Up to the newest observation, not the writer's last watermark: a writer that bypasses the read model (an older
	// binary after a partial rollback) shows here at the next check.
	hi, err := s.q.MaxObservationID(ctx)
	if err != nil {
		return out, err
	}
	hi = max(hi, out.State.ReconHi)
	c, err := s.q.CheckPacketSitesSlice(ctx, sqlc.CheckPacketSitesSliceParams{AfterID: max(hi-packetSitesReadOnlyCheckIDs, 0), ThroughID: hi})
	out.Verification.add(c)
	return out, err
}
