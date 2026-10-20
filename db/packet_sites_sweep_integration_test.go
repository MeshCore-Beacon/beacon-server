// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSweepPacketSitesFrontsPlanPostgres(t *testing.T) {
	f := newSiteFixture(t)
	// 200 rows heard in the last minute, a third of them scoped.
	now := time.Now().UTC()
	for i := range 50 {
		h := f.sitePacket(3000+i, now.Add(-time.Minute))
		for s, site := range siteFixtureSites {
			f.sited(psObs(h, 1+s*3, site, now.Add(-time.Duration(i)*time.Second)))
		}
	}
	f.exec("VACUUM ANALYZE packet_sites")
	ok, detail, err := f.store.PacketSitesSweepPlanOK(f.ctx)
	if err != nil || !ok {
		t.Fatalf("plan ok %v (%s), %v", ok, detail, err)
	}
	for _, idx := range []string{"idx_packet_sites_site", "idx_packet_sites_site_type", "idx_packet_sites_site_scope"} {
		if !strings.Contains(detail, idx) {
			t.Errorf("detail %q does not name %s", detail, idx)
		}
	}
	start := time.Now()
	res, err := f.store.SweepPacketSitesFronts(f.ctx, 2*time.Minute)
	if err != nil || res.SiteLive != 200 || res.TypeLive != 200 || res.ScopeLive == 0 || time.Since(start) > 5*time.Second {
		t.Fatalf("sweep %+v, %v in %v", res, err, time.Since(start))
	}

	// Without one of the three indexes the plan check fails and names it.
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // the drop is undone
	if _, err := tx.Exec(f.ctx, "DROP INDEX idx_packet_sites_site_type"); err != nil {
		t.Fatal(err)
	}
	ok, detail, err = sweepPlanOK(f.ctx, tx)
	if err != nil || ok || !strings.Contains(detail, "idx_packet_sites_site_type") {
		t.Fatalf("without the type index: ok %v, %q, %v", ok, detail, err)
	}
	_ = tx.Rollback(f.ctx)

	// A row moved forward seconds ago: after a sweep its old entry in idx_packet_sites_site is marked dead.
	var hasPageinspect bool
	if err := f.pool.QueryRow(f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pageinspect')").Scan(&hasPageinspect); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, "CREATE EXTENSION IF NOT EXISTS pageinspect"); !hasPageinspect || err != nil {
		t.Logf("pageinspect unavailable (%v); dead-entry check skipped", err)
		return
	}
	f.sited(psObs(f.sitePacket(3100, now), 2, "AAA", now.Add(-10*time.Second)))
	f.sited(psObs(psHash(10_000+3100), 3, "AAA", now.Add(-5*time.Second)))
	dead := func() int {
		var n int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM generate_series(1, pg_relation_size('idx_packet_sites_site') / 8192 - 1) b,
LATERAL bt_page_items('idx_packet_sites_site', b::int) WHERE dead`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := dead()
	// The hint lands only once the old version is dead to every snapshot; other sessions on the cluster can hold
	// that back for a moment.
	for range 50 {
		if _, err := f.store.SweepPacketSitesFronts(f.ctx, 2*time.Minute); err != nil {
			t.Fatal(err)
		}
		if dead() > before {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	var held bool
	if err := f.pool.QueryRow(f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid())").Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held {
		t.Skip("another session holds the xmin horizon back; dead-entry check skipped")
	}
	t.Fatalf("no dead entry in idx_packet_sites_site after sweeps (before %d)", before)
}

func TestIndexHealthPacketSitesFreshBaselinePostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := New(pool, pool, 0, 0)
	fill := func(from, to int) {
		mustPoolExec(t, ctx, pool, `INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
SELECT int8send(i), i % 16, 0, i % 4, '\x00', '\x11', now(), now() FROM generate_series(`+strconv.Itoa(from)+`, `+strconv.Itoa(to)+`) i;
INSERT INTO packet_sites (packet_hash, iata, site_heard_at, first_heard_at, payload_type, route_type, scope_id)
SELECT int8send(i), s, now() - i * interval '1 ms', now(), i % 16, i % 4, CASE WHEN i % 3 = 0 THEN 1 END
FROM generate_series(`+strconv.Itoa(from)+`, `+strconv.Itoa(to)+`) i, unnest('{AAA,BBB,CCC,DDD}'::bpchar[]) s;
ANALYZE packet_sites; ANALYZE packets;`)
	}
	fill(1, 1000)
	for _, idx := range []string{"packet_sites_pkey", "idx_packet_sites_site", "idx_packet_sites_site_type", "idx_packet_sites_site_scope"} {
		if h := indexHealthOf(t, ctx, store, idx); h.HasRatio {
			t.Errorf("%s with 4,000 rows has a ratio: %+v", idx, h)
		}
		if _, ok := savedBaselines(t, ctx, store)[idx]; ok {
			t.Errorf("%s got a baseline below %d rows", idx, freshBaselineMinRows)
		}
	}
	if h := indexHealthOf(t, ctx, store, "packets_pkey"); !h.HasRatio {
		t.Errorf("other indexes keep their ratio: %+v", h)
	}
	fill(1001, 30_000)
	var bytes int64
	var tuples float64
	if err := pool.QueryRow(ctx, "SELECT pg_relation_size('idx_packet_sites_site'), reltuples FROM pg_class WHERE relname = 'idx_packet_sites_site' AND relnamespace = current_schema()::regnamespace").Scan(&bytes, &tuples); err != nil {
		t.Fatal(err)
	}
	h := indexHealthOf(t, ctx, store, "idx_packet_sites_site")
	if want := float64(bytes) / tuples / freshBytesPerRow["idx_packet_sites_site"]; !h.HasRatio || h.Ratio != want {
		t.Fatalf("ratio %+v, want %v", h, want)
	}
	if b := savedBaselines(t, ctx, store)["idx_packet_sites_site"]; b.BytesPerRow != freshBytesPerRow["idx_packet_sites_site"] || b.Filenode == 0 {
		t.Fatalf("baseline %+v", b)
	}
	mustPoolExec(t, ctx, pool, "REINDEX INDEX idx_packet_sites_site")
	if h := indexHealthOf(t, ctx, store, "idx_packet_sites_site"); !h.HasRatio || h.Ratio != 1 {
		t.Fatalf("after REINDEX %+v, want ratio 1", h)
	}
}
