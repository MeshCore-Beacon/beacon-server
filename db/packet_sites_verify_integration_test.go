// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPacketSitesFirstVerificationPostgres(t *testing.T) {
	empty := newPsFixture(t)
	e := empty.begin()
	if _, err := e.Init(empty.ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := e.Verify(empty.ctx); err != nil || !v.OK() || v.PagesCompared != 0 || e.State().FirstVerifiedAt == nil {
		t.Fatalf("empty database: %+v, %v, %+v", v, err, e.State())
	}

	f := newSiteFixture(t)
	p := f.begin()
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	v, err := p.Verify(f.ctx)
	if err != nil || !v.OK() || v.PagesCompared < 10 {
		t.Fatalf("Verify = %+v, %v", v, err)
	}
	first := p.State().FirstVerifiedAt
	var saved PacketSitesState
	if found, err := f.store.LoadState(f.ctx, "packet_sites", &saved); err != nil || !found || saved.FirstVerifiedAt == nil || !saved.FirstVerifiedAt.Equal(*first) {
		t.Fatalf("persisted %+v, %v", saved, err)
	}
	var tuples float64
	var visible int32
	if err := f.pool.QueryRow(f.ctx, "SELECT reltuples, relallvisible FROM pg_class WHERE oid = 'packet_sites'::regclass").Scan(&tuples, &visible); err != nil || tuples <= 0 || visible <= 0 {
		t.Fatalf("after VACUUM ANALYZE: reltuples %v, relallvisible %d, %v", tuples, visible, err)
	}

	// The newest row at the busiest site moved back: the exact check and the site's page both see it.
	f.exec(`UPDATE packet_sites SET site_heard_at = site_heard_at - interval '1 day'
WHERE (packet_hash, iata) = (SELECT packet_hash, iata FROM packet_sites WHERE iata = 'AAA' ORDER BY site_heard_at DESC, packet_hash DESC LIMIT 1)`)
	v, err = p.Verify(f.ctx)
	if err != nil || v.OK() || v.Wrong != 1 || len(v.PageDiffs) == 0 || !strings.Contains(strings.Join(v.PageDiffs, "\n"), "[AAA]") {
		t.Fatalf("damaged Verify = %+v, %v", v, err)
	}
	if !p.State().FirstVerifiedAt.Equal(*first) {
		t.Fatal("FirstVerifiedAt moved")
	}

	// A read-only server checks the newest observations and writes nothing.
	check := func() PacketSitesReadOnlyCheck {
		tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background()) //nolint:errcheck // read-only
		c, err := New(tx, nil, 0, 0).CheckPacketSitesReadOnly(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if c := check(); !c.Found || !c.State.Complete || c.Verification.Wrong != 1 || c.Age > time.Minute {
		t.Fatalf("read-only check of the damaged table %+v", c)
	}
	f.exec(`UPDATE packet_sites SET site_heard_at = site_heard_at + interval '1 day'
WHERE site_heard_at < (SELECT max(heard_at) FROM packet_observations po WHERE po.packet_hash = packet_sites.packet_hash AND po.iata = packet_sites.iata)`)
	if c := check(); !c.Verification.OK() {
		t.Fatalf("read-only check of the repaired table %+v", c)
	}
	if c, err := New(empty.pool, nil, 0, 0).CheckPacketSitesReadOnly(f.ctx); err != nil || !c.Found {
		t.Fatalf("empty: %+v, %v", c, err)
	}
	if c, err := New(newPsFixture(t).pool, nil, 0, 0).CheckPacketSitesReadOnly(f.ctx); err != nil || c.Found {
		t.Fatalf("no state: %+v, %v", c, err)
	}
}
