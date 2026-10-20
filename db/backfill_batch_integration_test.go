// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestBackfillWriteBoundsPostgres(t *testing.T) {
	f := newPsFixture(t)
	p := f.begin()
	if _, err := p.conn.Exec(f.ctx, "SET statement_timeout = '2s'"); err != nil {
		t.Fatal(err)
	}
	_, err := backfillWrite(f.ctx, p.conn, 20*time.Millisecond, func() (int64, error) {
		_, err := p.conn.Exec(f.ctx, `INSERT INTO background_state(key, value) VALUES ('timeout-test', '{}');
			SELECT pg_sleep(1)`)
		return 1, err
	})
	if !backfillTimedOut(f.ctx, err) {
		t.Fatalf("expected server statement timeout, got %v", err)
	}
	var count int
	if err := p.conn.QueryRow(f.ctx, "SELECT count(*) FROM background_state WHERE key = 'timeout-test'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("canceled write persisted or session unusable: count %d, %v", count, err)
	}
	var timeout string
	if err := p.conn.QueryRow(f.ctx, "SHOW statement_timeout").Scan(&timeout); err != nil || timeout != "2s" {
		t.Fatalf("timeout leaked: %q, %v", timeout, err)
	}
	if other, ok, err := f.store.BeginPacketSites(f.ctx); err != nil || ok {
		if other != nil {
			other.Close(f.ctx)
		}
		t.Fatalf("timeout lost session advisory lock: acquired %v, %v", ok, err)
	}
	// A subsequent attempt commits normally on the same connection.
	if _, err := backfillWrite(f.ctx, p.conn, time.Second, func() (int64, error) {
		tag, err := p.conn.Exec(f.ctx, "INSERT INTO background_state(key,value) VALUES ('timeout-test','{}')")
		return tag.RowsAffected(), err
	}); err != nil {
		t.Fatal(err)
	}
	// A larger requested limit must not relax an operator's smaller setting.
	if _, err := p.conn.Exec(f.ctx, "SET statement_timeout = '10ms'"); err != nil {
		t.Fatal(err)
	}
	_, err = backfillWrite(f.ctx, p.conn, time.Second, func() (int64, error) {
		_, err := p.conn.Exec(f.ctx, "SELECT pg_sleep(1)")
		return 0, err
	})
	if !backfillTimedOut(f.ctx, err) {
		t.Fatalf("operator timeout was relaxed: %v", err)
	}
}

func TestCmiBackfillShrinksPostgres(t *testing.T) {
	f := newCmiFixture(t)
	for _, id := range []int64{1, 600} {
		h := f.packet(string(rune('a'+id)), 3)
		f.obs(f.pool, h, "YUL", true)
		f.unseeded(id, 7, h)
	}
	c := f.begin()
	c.pause = -1
	c.backfillLimit = 50 * time.Millisecond
	if _, err := c.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	held, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(context.Background()) //nolint:errcheck
	if _, err := held.Exec(f.ctx, "SELECT id FROM channel_messages WHERE id = 600 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	st, err := c.Backfill(f.ctx, time.Second)
	if !backfillTimedOut(f.ctx, err) || st.Complete || st.BackfilledToID <= 0 || st.BackfilledToID >= 600 {
		t.Fatalf("expected a committed smaller prefix before the locked message: %+v, %v", st, err)
	}
	var saved CmiState
	if found, err := f.store.LoadState(f.ctx, cmiStateKey, &saved); err != nil || !found || saved.BackfilledToID != st.BackfilledToID {
		t.Fatalf("saved watermark %+v, %v, %v", saved, found, err)
	}
	if err := held.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	st, err = c.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete || st.BackfilledToID != 600 {
		t.Fatalf("resume: %+v, %v", st, err)
	}
	if check := f.check(); check.Missing != 0 || check.Extra != 0 {
		t.Fatalf("range skipped or duplicated after retry: %+v", check)
	}
}

func TestPacketSitesBackfillShrinksPostgres(t *testing.T) {
	f := newPsFixture(t)
	stream := f.packets(10)
	f.unsited(stream[:30]...)
	p := f.begin()
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	original := p.fillStmt
	var canceled int
	p.fillStmt = func(ctx context.Context, after, through int64) (int64, error) {
		if through-after > 8 {
			canceled++
			return 0, &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}
		}
		return original(ctx, after, through)
	}
	st, err := p.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete || canceled == 0 || p.backfillSpan > 8 {
		t.Fatalf("backfill did not shrink and complete: %+v, span %d, canceled %d, %v", st, p.backfillSpan, canceled, err)
	}
	f.wantClean("after timeout retries")
	if n, err := sqlc.New(f.pool).FillPacketSitesSlice(f.ctx, sqlc.FillPacketSitesSliceParams{AfterID: 0, ThroughID: f.maxID()}); err != nil || n != 0 {
		t.Fatalf("retry left incomplete rows: wrote %d, %v", n, err)
	}
}
