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
	"reflect"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cmiStateKey = "cmi"
	// cmiBackfillBatch is the message ids one fill of the reconcile's message part covers, and the backfill's
	// default batch (readmodels.fill.cmi_batch).
	cmiBackfillBatch int64 = 10_000
	cmiBootBatch     int64 = 1_000
	// cmiObsSlice is the observation ids one fill of the reconcile's observation part covers.
	cmiObsSlice int64 = 500_000
	// cmiFillAttempts bounds the retries of a fill that lost a message to a concurrent cascade (23503).
	cmiFillAttempts = 3
	// cmiVerifyMessages is the width of each message range the verification checks exactly.
	cmiVerifyMessages int64 = 2_000
)

// CmiState is background_state['cmi']: the one-off backfill's progress and the reconcile watermarks. Nothing in it
// says the table is exact; each process earns that at boot.
type CmiState struct {
	BackfillMaxID   int64      `json:"backfill_max_id"`
	BackfilledToID  int64      `json:"backfilled_to_id"`
	Complete        bool       `json:"complete"`
	ObsPrevHi       int64      `json:"obs_prev_hi"`
	ObsHi           int64      `json:"obs_hi"`
	MsgPrevHi       int64      `json:"msg_prev_hi"`
	MsgHi           int64      `json:"msg_hi"`
	FirstVerifiedAt *time.Time `json:"first_verified_at,omitempty"`
}

// CmiSession holds the cmi lock on one maintenance connection; every statement and state write goes through it.
type CmiSession struct {
	conn  *pgxpool.Conn
	q     *sqlc.Queries
	state CmiState
	// fills and retries count fill statements and 23503 retries, for tests.
	fills, retries int
	backfillSpan   int64
	backfillLimit  time.Duration
	// The backfill's pacing (see FillPacing): pause is the shortest wait after a batch, duty the share of time it may
	// be busy, batch the message ids one batch covers. progress logs it once a minute; nil logs nothing.
	pause    time.Duration
	duty     float64
	batch    int64
	progress *FillProgress
	batched  bool // this session's Backfill has filled a batch
}

// BeginCmi takes the cmi lock for this schema on a dedicated connection. ok is false when another session holds it.
func (s *Store) BeginCmi(ctx context.Context) (*CmiSession, bool, error) {
	if s.pool == nil {
		return nil, false, errors.New("cmi: store has no dedicated pool")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, hashtext(current_schema()))", cmiReconcileLockKey).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	if _, err := conn.Exec(ctx, fillPlanMode); err != nil {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1, hashtext(current_schema()))", cmiReconcileLockKey)
		conn.Release()
		return nil, false, err
	}
	analyzeAtStart(ctx, conn, "channel_message_iatas")
	pace := s.FillPacing()
	return &CmiSession{conn: conn, q: sqlc.New(conn),
		pause: pace.MinPause, duty: pace.Duty, batch: pace.CmiBatch, progress: &s.cmiProgress}, true, nil
}

// Close releases the lock and the connection. If the unlock fails, the connection is closed, which drops the lock.
func (c *CmiSession) Close(ctx context.Context) {
	if c.conn == nil {
		return
	}
	_, resetErr := c.conn.Exec(ctx, "RESET plan_cache_mode")
	if _, err := c.conn.Exec(ctx, "SELECT pg_advisory_unlock($1, hashtext(current_schema()))", cmiReconcileLockKey); err != nil || resetErr != nil {
		_ = c.conn.Conn().Close(ctx)
	}
	c.conn.Release()
	c.conn = nil
}

// State returns the state as of the session's last read or write.
func (c *CmiSession) State() CmiState { return c.state }

func (c *CmiSession) save(ctx context.Context) error {
	return saveState(ctx, c.q, cmiStateKey, c.state)
}

// Init loads the persisted state or, on first use, records where the backfill must reach and starts both
// reconcile ranges at the current maxima.
func (c *CmiSession) Init(ctx context.Context) (CmiState, error) {
	var st CmiState
	found, err := loadState(ctx, c.q, cmiStateKey, &st)
	if err != nil {
		return CmiState{}, fmt.Errorf("cmi state: %w", err)
	}
	if found {
		c.state = st
		return st, nil
	}
	if c.progress != nil {
		c.progress.Reset() // a new fill (first install or rebuild) times itself from its own start
	}
	msgHi, err := c.q.MaxChannelMessageID(ctx)
	if err != nil {
		return CmiState{}, err
	}
	obsHi, err := c.q.MaxObservationID(ctx)
	if err != nil {
		return CmiState{}, err
	}
	first, err := c.q.NextChannelMessageID(ctx, 0)
	if err != nil {
		return CmiState{}, err
	}
	c.state = CmiState{
		BackfillMaxID: msgHi, BackfilledToID: max(first-1, 0), Complete: msgHi == 0,
		ObsPrevHi: obsHi, ObsHi: obsHi, MsgPrevHi: msgHi, MsgHi: msgHi,
	}
	return c.state, c.save(ctx)
}

// fill runs one idempotent fill, retrying only when a message was cascaded away between the statement's snapshot
// and its foreign key check (SQLSTATE 23503).
func (c *CmiSession) fill(ctx context.Context, run func() (int64, error)) (int64, error) {
	var err error
	for attempt := 1; attempt <= cmiFillAttempts; attempt++ {
		c.fills++
		var n int64
		if n, err = run(); err == nil {
			return n, nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23503" || attempt == cmiFillAttempts {
			break
		}
		c.retries++
	}
	return 0, err
}

func (c *CmiSession) fillMessages(ctx context.Context, after, through int64) (int64, error) {
	return c.fill(ctx, func() (int64, error) {
		return c.q.FillChannelMessageIATAsByMessages(ctx, sqlc.FillChannelMessageIATAsByMessagesParams{AfterID: after, ThroughID: through})
	})
}

// Backfill fills cmi oldest to newest in batches of message ids (the session's batch, cmiBackfillBatch by default),
// jumping id gaps, until it reaches BackfillMaxID (then Complete) or budget is spent. Progress is saved after every
// batch. After each batch it waits as FillPacing.Wait does, so it is busy at most duty of the time.
func (c *CmiSession) Backfill(ctx context.Context, budget time.Duration) (CmiState, error) {
	deadline := time.Now().Add(budget)
	if c.backfillSpan <= 0 {
		c.backfillSpan = cmiBootBatch
		if c.batch > 0 {
			c.backfillSpan = min(c.backfillSpan, c.batch)
		}
	}
	for !c.state.Complete && time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return c.state, err
		}
		began := time.Now()
		next, err := c.q.NextChannelMessageID(ctx, c.state.BackfilledToID)
		if err != nil {
			return c.state, err
		}
		if next == 0 || next > c.state.BackfillMaxID {
			c.state.Complete = true
			if err := c.save(ctx); err != nil {
				return c.state, err
			}
			slog.Info("cmi backfill complete", "component", "readmodel", "max_id", c.state.BackfillMaxID)
			if c.progress != nil {
				c.progress.Reset()
			}
			break
		}
		if c.progress != nil {
			c.progress.Start(c.state.BackfilledToID)
		}
		hi := min(next-1+c.backfillSpan, c.state.BackfillMaxID)
		timeout := backfillTimeout(c.backfillLimit, deadline)
		_, err = c.fill(ctx, func() (int64, error) {
			return backfillWrite(ctx, c.conn, timeout, func() (int64, error) {
				return c.q.FillChannelMessageIATAsByMessages(ctx, sqlc.FillChannelMessageIATAsByMessagesParams{AfterID: next - 1, ThroughID: hi})
			})
		})
		if err != nil {
			// A short remaining cycle is not evidence that the batch is too large.
			// Leave its watermark and size intact for the next full budget.
			if backfillTimedOut(ctx, err) && timeout < backfillMaxTimeout(c.backfillLimit) {
				return c.state, nil
			}
			if backfillTimedOut(ctx, err) && hi-(next-1) > 1 {
				c.backfillSpan = max(1, (hi-(next-1))/2)
				continue
			}
			return c.state, fmt.Errorf("cmi backfill (%d, %d]: %w", next-1, hi, err)
		}
		c.state.BackfilledToID = hi
		if err := c.save(ctx); err != nil {
			return c.state, err
		}
		if !c.batched {
			// A new table now holds a batch's rows; plan the next batches on them.
			analyzeAfterFirstSlice(ctx, c.conn, "channel_message_iatas")
			c.batched = true
		}
		if c.progress != nil && c.progress.Due() {
			c.logProgress(ctx)
		}
		// The pause leaves room for ingest's writes and WAL, and stretches with the batch's cost on a slow disk.
		pace := FillPacing{Duty: c.duty, MinPause: c.pause}
		if !pace.Wait(ctx, time.Since(began)) {
			return c.state, ctx.Err()
		}
	}
	return c.state, nil
}

// logProgress logs the backfill's progress: the message ids done from the oldest message and those left.
func (c *CmiSession) logProgress(ctx context.Context) {
	first, err := c.q.NextChannelMessageID(ctx, 0)
	if err != nil || first == 0 {
		first = c.state.BackfilledToID + 1 // unknown: report none done rather than fail the batch
	}
	c.progress.Log("cmi backfill progress", c.state.BackfilledToID, max(c.state.BackfilledToID-(first-1), 0), c.state.BackfillMaxID-c.state.BackfilledToID,
		"backfilled_to_id", c.state.BackfilledToID, "max_id", c.state.BackfillMaxID)
}

// Reconcile fills the observation ids and the message ids that arrived since the run before last, so each range
// overlaps the previous run by one interval, and advances both watermarks monotonically. It returns the rows the
// two parts inserted: anything above zero was written by a path that bypassed the read model.
func (c *CmiSession) Reconcile(ctx context.Context) (int64, error) {
	obsHi, err := c.q.MaxObservationID(ctx)
	if err != nil {
		return 0, err
	}
	msgHi, err := c.q.MaxChannelMessageID(ctx)
	if err != nil {
		return 0, err
	}
	var rows int64
	for lo := c.state.ObsPrevHi; lo < obsHi; lo += cmiObsSlice {
		hi := min(lo+cmiObsSlice, obsHi)
		n, err := c.fill(ctx, func() (int64, error) {
			return c.q.FillChannelMessageIATAsByObservations(ctx, sqlc.FillChannelMessageIATAsByObservationsParams{AfterID: lo, ThroughID: hi})
		})
		if err != nil {
			return rows, fmt.Errorf("cmi reconcile observations (%d, %d]: %w", lo, hi, err)
		}
		rows += n
	}
	for after := c.state.MsgPrevHi; ; {
		next, err := c.q.NextChannelMessageID(ctx, after)
		if err != nil {
			return rows, err
		}
		if next == 0 || next > msgHi {
			break
		}
		hi := min(next-1+cmiBackfillBatch, msgHi)
		n, err := c.fillMessages(ctx, next-1, hi)
		if err != nil {
			return rows, fmt.Errorf("cmi reconcile messages (%d, %d]: %w", next-1, hi, err)
		}
		rows += n
		after = hi
	}
	st := c.state
	st.ObsPrevHi, st.ObsHi = st.ObsHi, max(obsHi, st.ObsHi)
	st.MsgPrevHi, st.MsgHi = st.MsgHi, max(msgHi, st.MsgHi)
	c.state = st
	return rows, c.save(ctx)
}

// CmiVerification is the outcome of one verification pass.
type CmiVerification struct {
	Missing, Extra int64 // the exact check over the sampled message ranges
	PagesCompared  int
	PageDiffs      []string // "channel 7 iatas [YUL] cursor 0: legacy 51 rows, cmi 50 rows, first difference at 12"
}

// OK reports a clean pass.
func (v CmiVerification) OK() bool { return v.Missing == 0 && v.Extra == 0 && len(v.PageDiffs) == 0 }

// verifyRanges returns the newest cmiVerifyMessages-wide message range below MsgHi and one random range of the same
// width below it, as (after, through] pairs.
func (c *CmiSession) verifyRanges(first int64) [][2]int64 {
	hi := c.state.MsgHi
	ranges := [][2]int64{{max(hi-cmiVerifyMessages, 0), hi}}
	if lo := max(first-1, 0); hi-cmiVerifyMessages-lo > cmiVerifyMessages {
		start := lo + rand.Int64N(hi-cmiVerifyMessages-lo-cmiVerifyMessages)
		ranges = append(ranges, [2]int64{start, start + cmiVerifyMessages})
	}
	return ranges
}

// checkRanges runs the exact check over ranges through q, counting observations up to ObsHi.
func (c *CmiSession) checkRanges(ctx context.Context, q sqlc.Querier, ranges [][2]int64) (CmiVerification, error) {
	var v CmiVerification
	for _, r := range ranges {
		row, err := q.CheckChannelMessageIATAs(ctx, sqlc.CheckChannelMessageIATAsParams{ObsThrough: c.state.ObsHi, AfterID: r[0], ThroughID: r[1]})
		if err != nil {
			return v, err
		}
		v.Missing += row.Missing
		v.Extra += row.Extra
	}
	return v, nil
}

// Verify is the first-time verification: it reconciles, VACUUMs and ANALYZEs the table (the index-only scans need
// its visibility map), then in one read-only snapshot runs the exact check over the newest messages and one random
// range below them, and compares today's and the read model's pages on the busiest recent sites and scopes, from
// the newest message and from a deeper cursor. The first pass is persisted as FirstVerifiedAt. An in-flight Add can
// make one attempt differ; the caller retries every interval.
func (c *CmiSession) Verify(ctx context.Context) (CmiVerification, error) {
	if _, err := c.Reconcile(ctx); err != nil {
		return CmiVerification{}, err
	}
	if _, err := c.conn.Exec(ctx, "VACUUM (ANALYZE) channel_message_iatas"); err != nil {
		return CmiVerification{}, fmt.Errorf("cmi vacuum: %w", err)
	}
	tx, err := c.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return CmiVerification{}, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // read-only
	q := sqlc.New(tx)
	first, err := q.NextChannelMessageID(ctx, 0)
	if err != nil {
		return CmiVerification{}, err
	}
	v, err := c.checkRanges(ctx, q, c.verifyRanges(first))
	if err != nil {
		return v, err
	}
	if err := comparePages(ctx, q, max(c.state.MsgHi-cmiVerifyMessages, 0), &v); err != nil {
		return v, err
	}
	if err := tx.Rollback(ctx); err != nil {
		return v, err
	}
	if !v.OK() {
		slog.Error("cmi verification failed; region and scope message pages answer 503 until a later pass succeeds", "component", "readmodel",
			"missing", v.Missing, "extra", v.Extra, "pages", v.PagesCompared, "diffs", len(v.PageDiffs), "first", v.PageDiffs[:min(len(v.PageDiffs), 3)])
		return v, nil
	}
	if c.state.FirstVerifiedAt == nil {
		now := time.Now().UTC()
		c.state.FirstVerifiedAt = &now
		if err := c.save(ctx); err != nil {
			return v, err
		}
		slog.Info("cmi first-time verification passed", "component", "readmodel", "pages", v.PagesCompared)
	}
	return v, nil
}

const (
	cmiVerifySites  = 5
	cmiVerifyScopes = 3
	cmiVerifyLimit  = 50
)

// comparePages compares the original statements with the read model's on the busiest sites and scopes among messages
// above after: channel and all-channel pages, from cursor 0 and from page 1's 26th id.
func comparePages(ctx context.Context, q sqlc.Querier, after int64, v *CmiVerification) error {
	type request struct {
		channel *int32
		iatas   []string
		scope   string
	}
	var requests []request
	sites, err := q.ListChannelMessageIATASamples(ctx, sqlc.ListChannelMessageIATASamplesParams{AfterID: after, RowLimit: cmiVerifySites})
	if err != nil {
		return err
	}
	for _, s := range sites {
		requests = append(requests, request{channel: &s.ChannelID, iatas: []string{s.Iata}}, request{iatas: []string{s.Iata}})
	}
	scopes, err := q.ListChannelMessageScopeSamples(ctx, sqlc.ListChannelMessageScopeSamplesParams{AfterID: after, RowLimit: cmiVerifyScopes})
	if err != nil {
		return err
	}
	for _, s := range scopes {
		requests = append(requests, request{channel: &s.ChannelID, scope: s.Name}, request{scope: s.Name})
	}
	for _, r := range requests {
		cursors := []int64{0}
		for i, cursor := 0, int64(0); i < len(cursors); i++ {
			cursor = cursors[i]
			legacy, err := listChannelMessagesLegacy(ctx, q, r.channel, time.Time{}, cmiVerifyLimit, r.iatas, r.scope, cursor)
			if err != nil {
				return err
			}
			model, err := listChannelMessages(ctx, q, true, r.channel, time.Time{}, cmiVerifyLimit, r.iatas, r.scope, cursor)
			if err != nil {
				return err
			}
			v.PagesCompared++
			if !reflect.DeepEqual(legacy, model) {
				at := 0
				for at < min(len(legacy.Items), len(model.Items)) && reflect.DeepEqual(legacy.Items[at], model.Items[at]) {
					at++
				}
				where := "all channels"
				if r.channel != nil {
					where = fmt.Sprintf("channel %d", *r.channel)
				}
				v.PageDiffs = append(v.PageDiffs, fmt.Sprintf("%s iatas %v scope %q cursor %d: legacy %d rows, cmi %d rows, first difference at %d",
					where, r.iatas, r.scope, cursor, len(legacy.Items), len(model.Items), at))
			}
			if cursor == 0 && len(legacy.Items) > cmiVerifyLimit/2 {
				cursors = append(cursors, legacy.Items[cmiVerifyLimit/2].ID)
			}
		}
	}
	return nil
}

// CmiReadOnlyCheck is what a read-only server reads to decide whether to trust the table another process maintains.
type CmiReadOnlyCheck struct {
	State        CmiState
	Verification CmiVerification // the exact check over the newest messages
	Age          time.Duration   // since the writer last saved the state, on the database clock
	Found        bool            // the state exists
}

// CheckCmiReadOnly reads the persisted cmi state, its age, and runs the exact check over the newest messages. It
// writes nothing, so a read-only server can decide whether to trust the table another process maintains.
func (s *Store) CheckCmiReadOnly(ctx context.Context) (CmiReadOnlyCheck, error) {
	row, err := s.q.GetBackgroundStateAge(ctx, cmiStateKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return CmiReadOnlyCheck{}, nil
	}
	if err != nil {
		return CmiReadOnlyCheck{}, err
	}
	out := CmiReadOnlyCheck{Found: true, Age: time.Duration(row.AgeSeconds * float64(time.Second))}
	if err := json.Unmarshal(row.Value, &out.State); err != nil {
		return out, fmt.Errorf("cmi state: %w", err)
	}
	c := &CmiSession{state: out.State}
	out.Verification, err = c.checkRanges(ctx, s.q, [][2]int64{{max(out.State.MsgHi-cmiVerifyMessages, 0), out.State.MsgHi}})
	return out, err
}
