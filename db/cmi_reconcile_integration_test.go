// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"math"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

const cmiBase = int64(3_000_000_000)

// unseeded stores a message the way an older binary did: no site rows.
func (f *cmiFixture) unseeded(id int64, channel int32, hash []byte) {
	f.t.Helper()
	f.exec("INSERT INTO channel_messages (id, channel_id, packet_hash, sender_name, content, sent_at) VALUES ($1, $2, $3, 's', 'c', now())", id, channel, hash)
}

// check runs the exact check over every message and observation.
func (f *cmiFixture) check() sqlc.CheckChannelMessageIATAsRow {
	f.t.Helper()
	row, err := f.store.q.CheckChannelMessageIATAs(f.ctx, sqlc.CheckChannelMessageIATAsParams{ObsThrough: math.MaxInt64, AfterID: 0, ThroughID: math.MaxInt64})
	if err != nil {
		f.t.Fatal(err)
	}
	return row
}

func (f *cmiFixture) count() int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM channel_message_iatas").Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *cmiFixture) begin() *CmiSession {
	f.t.Helper()
	c, ok, err := f.store.BeginCmi(f.ctx)
	if err != nil || !ok {
		f.t.Fatalf("BeginCmi: ok %v, %v", ok, err)
	}
	f.t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func (f *cmiFixture) reconcile(c *CmiSession) int64 {
	f.t.Helper()
	n, err := c.Reconcile(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return n
}

func TestCmiInitAndBackfillPostgres(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		f := newCmiFixture(t)
		c := f.begin()
		st, err := c.Init(f.ctx)
		if err != nil || st != (CmiState{Complete: true}) {
			t.Fatalf("Init = %+v, %v", st, err)
		}
		if st, err = c.Backfill(f.ctx, time.Minute); err != nil || !st.Complete {
			t.Fatalf("Backfill = %+v, %v", st, err)
		}
		if got := f.check(); got.Missing != 0 || got.Extra != 0 {
			t.Fatalf("check = %+v", got)
		}
	})

	f := newCmiFixture(t)
	var lastObs int64
	ids := []int64{1, 2, 3, 4, 5, 40_001, 40_002, 40_003}
	for i, off := range ids {
		h := f.packet(string(rune('a'+i)), 3)
		f.obs(f.pool, h, "YUL", true)
		lastObs = f.obs(f.pool, h, []string{"YOW", "YYZ"}[i%2], true)
		f.unseeded(cmiBase+off, 7, h)
	}
	c := f.begin()
	st, err := c.Init(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	maxID := cmiBase + 40_003
	want := CmiState{BackfillMaxID: maxID, BackfilledToID: cmiBase, ObsPrevHi: lastObs, ObsHi: lastObs, MsgPrevHi: maxID, MsgHi: maxID}
	if st != want || c.State() != want {
		t.Fatalf("Init = %+v, want %+v", st, want)
	}
	var loaded CmiState
	if ok, err := f.store.LoadState(f.ctx, cmiStateKey, &loaded); !ok || err != nil || loaded != want {
		t.Fatalf("persisted %+v (%v, %v), want %+v", loaded, ok, err, want)
	}
	var raw string
	if err := f.pool.QueryRow(f.ctx, "SELECT value->>'backfill_max_id' FROM background_state WHERE key = 'cmi'").Scan(&raw); err != nil || raw != "3000040003" {
		t.Fatalf("backfill_max_id stored as %q, %v", raw, err)
	}
	if missing := f.check().Missing; missing != 16 {
		t.Fatalf("missing before the backfill = %d, want 16", missing)
	}

	st, err = c.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete || st.BackfilledToID != maxID {
		t.Fatalf("Backfill = %+v, %v", st, err)
	}
	if c.fills != 2 {
		t.Fatalf("the backfill ran %d fills, want 2 (the id gap is jumped)", c.fills)
	}
	if got := f.check(); got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("check = %+v", got)
	}
	before := f.count()
	if _, err := c.Backfill(f.ctx, time.Minute); err != nil || f.count() != before || c.fills != 2 {
		t.Fatalf("a second backfill: %v, rows %d -> %d, fills %d", err, before, f.count(), c.fills)
	}

	// One session per schema at a time; schemas do not contend.
	if other, ok, err := f.store.BeginCmi(f.ctx); ok || err != nil {
		if other != nil {
			other.Close(f.ctx)
		}
		t.Fatalf("second session in the schema: ok %v, %v", ok, err)
	}
	g := newCmiFixture(t)
	g.begin()
}

func TestCmiMessageRangeReconcilePostgres(t *testing.T) {
	f := newCmiFixture(t)
	old := f.packet("old", 3)
	f.obs(f.pool, old, "YUL", true)
	f.obs(f.pool, old, "YOW", true)
	f.obs(f.pool, old, "YYZ", false)
	live := f.packet("live", 0)
	f.obs(f.pool, live, "YUL", true)
	f.message(f.store, 7, live, false)

	c := f.begin()
	if _, err := c.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if n := f.reconcile(c); n != 0 {
		t.Fatalf("clean reconcile repaired %d", n)
	}
	st := c.State()

	// An older binary imports a key and stores a message for the old packet, without sites.
	var m int64
	if err := f.pool.QueryRow(f.ctx, "INSERT INTO channel_messages (channel_id, packet_hash, sent_at) VALUES (8, $1, now()) RETURNING id", old).Scan(&m); err != nil {
		t.Fatal(err)
	}
	obsMax, err := f.store.q.MaxObservationID(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.q.FillChannelMessageIATAsByObservations(f.ctx, sqlc.FillChannelMessageIATAsByObservationsParams{AfterID: st.ObsPrevHi, ThroughID: obsMax}); err != nil {
		t.Fatal(err)
	}
	if missing := f.check().Missing; missing != 3 {
		t.Fatalf("the observation range alone left %d missing, want 3", missing)
	}
	if n := f.reconcile(c); n != 3 {
		t.Fatalf("reconcile repaired %d, want 3", n)
	}
	if got := f.check(); got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("check = %+v", got)
	}
	after := c.State()
	if after.MsgHi != m || after.MsgPrevHi != st.MsgHi {
		t.Fatalf("message watermarks %d/%d, want %d/%d", after.MsgPrevHi, after.MsgHi, st.MsgHi, m)
	}
	if n := f.reconcile(c); n != 0 {
		t.Fatalf("next run repaired %d", n)
	}
}

func TestCmiObservationRangeReconcilePostgres(t *testing.T) {
	f := newCmiFixture(t)
	p := f.packet("P", 3)
	f.obs(f.pool, p, "YUL", true)
	f.message(f.store, 7, p, false)
	c := f.begin()
	if _, err := c.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	f.reconcile(c)
	prev := c.State()

	// Hearings stored without their Add: a new site, and an untyped observation at another.
	f.obs(f.pool, p, "YOW", true)
	f.obs(f.pool, p, "YYZ", false)
	if n := f.reconcile(c); n != 2 {
		t.Fatalf("reconcile repaired %d, want 2", n)
	}
	if got := f.check(); got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("check = %+v", got)
	}
	st := c.State()
	if st.ObsPrevHi != prev.ObsHi || st.ObsHi <= prev.ObsHi {
		t.Fatalf("observation watermarks %+v after %+v", st, prev)
	}
	for range 2 {
		before := c.State()
		if n := f.reconcile(c); n != 0 {
			t.Fatalf("idle run repaired %d", n)
		}
		after := c.State()
		if after.ObsHi < before.ObsHi || after.ObsPrevHi < before.ObsPrevHi || after.MsgHi < before.MsgHi || after.MsgPrevHi < before.MsgPrevHi {
			t.Fatalf("watermarks moved back: %+v -> %+v", before, after)
		}
	}
}

func TestCmiBackfillRetries23503Postgres(t *testing.T) {
	f := newCmiFixture(t)
	var hashes [][]byte
	for i, site := range []string{"YUL", "YOW", "YYZ"} {
		h := f.packet(site, 0)
		f.obs(f.pool, h, site, true)
		f.unseeded(cmiBase+int64(i)+1, 7, h)
		hashes = append(hashes, h)
	}
	c := f.begin()
	if _, err := c.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	tx, err := conn.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if _, err := tx.Exec(f.ctx, "DELETE FROM packets WHERE packet_hash = $1", hashes[0]); err != nil {
		t.Fatal(err)
	}
	type result struct {
		st  CmiState
		err error
	}
	done := make(chan result, 1)
	pid := c.conn.Conn().PgConn().PID()
	go func() {
		st, err := c.Backfill(f.ctx, time.Minute)
		done <- result{st, err}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		if err := f.pool.QueryRow(f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the backfill never waited on the deleting transaction")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || !r.st.Complete {
		t.Fatalf("Backfill = %+v, %v", r.st, r.err)
	}
	if c.retries != 1 {
		t.Fatalf("retries = %d, want 1 (the first attempt fails with 23503)", c.retries)
	}
	want := []string{"3000000002/YOW/7/0", "3000000003/YYZ/7/0"}
	if got := f.rows(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	if got := f.check(); got.Missing != 0 || got.Extra != 0 {
		t.Fatalf("check = %+v", got)
	}
}
