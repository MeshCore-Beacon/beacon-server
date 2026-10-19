// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCmiFirstVerificationPostgres(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		f := newCmiFixture(t)
		c := f.begin()
		if _, err := c.Init(f.ctx); err != nil {
			t.Fatal(err)
		}
		v, err := c.Verify(f.ctx)
		if err != nil || !v.OK() || v.PagesCompared != 0 {
			t.Fatalf("Verify = %+v, %v", v, err)
		}
	})

	f := newCmiFixture(t)
	buildCmiMessages(f)
	f.exec("TRUNCATE channel_message_iatas") // as on a first install: the backfill fills it
	c := f.begin()
	if _, err := c.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	v, err := c.Verify(f.ctx)
	if err != nil || !v.OK() || v.PagesCompared < 10 {
		t.Fatalf("Verify = %+v, %v", v, err)
	}
	first := c.State().FirstVerifiedAt
	var loaded CmiState
	if _, err := f.store.LoadState(f.ctx, cmiStateKey, &loaded); err != nil || first == nil || loaded.FirstVerifiedAt == nil || !loaded.FirstVerifiedAt.Equal(*first) {
		t.Fatalf("first_verified_at %v persisted as %+v (%v)", first, loaded, err)
	}
	var reltuples float64
	if err := f.pool.QueryRow(f.ctx, "SELECT reltuples FROM pg_class WHERE oid = 'channel_message_iatas'::regclass").Scan(&reltuples); err != nil || reltuples <= 0 {
		t.Fatalf("reltuples = %v (%v): no ANALYZE", reltuples, err)
	}

	// Damage below the watermarks, where no reconcile looks: the newest message of the busiest sampled pair loses
	// a site.
	samples, err := f.store.q.ListChannelMessageIATASamples(f.ctx, sqlc.ListChannelMessageIATASamplesParams{AfterID: 0, RowLimit: 1})
	if err != nil || len(samples) != 1 {
		t.Fatalf("samples %v, %v", samples, err)
	}
	f.exec(`DELETE FROM channel_message_iatas WHERE (message_id, iata) = (
  SELECT message_id, iata FROM channel_message_iatas WHERE channel_id = $1 AND iata = $2 ORDER BY message_id DESC LIMIT 1)`, samples[0].ChannelID, samples[0].Iata)
	v, err = c.Verify(f.ctx)
	if err != nil || v.Missing != 1 || v.Extra != 0 || len(v.PageDiffs) == 0 || v.OK() {
		t.Fatalf("damaged Verify = %+v, %v", v, err)
	}
	if got := c.State().FirstVerifiedAt; got == nil || !got.Equal(*first) {
		t.Fatalf("first_verified_at moved: %v -> %v", first, got)
	}
	t.Logf("diffs: %v", v.PageDiffs)

	// Repair, then the read-only check on a read-only pool.
	f.exec("TRUNCATE channel_message_iatas")
	f.exec("DELETE FROM background_state WHERE key = 'cmi'")
	c.Close(f.ctx)
	c = f.begin()
	if _, err := c.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Verify(f.ctx); err != nil || !v.OK() {
		t.Fatalf("Verify after the rebuild = %+v, %v", v, err)
	}
	cfg := f.pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	ro, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	st, rv, err := New(ro, ro, 0, 0).CheckCmiReadOnly(f.ctx)
	if err != nil || !rv.OK() || !st.Complete || st.FirstVerifiedAt == nil {
		t.Fatalf("CheckCmiReadOnly = %+v, %+v, %v", st, rv, err)
	}
}
