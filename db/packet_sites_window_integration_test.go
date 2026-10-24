// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// windowFixture is psFixture with 3,000 packets first heard 80 s apart over 2.8 days, all heard at AAA and every
// third at BBB, mostly within seconds of the first hearing; a few at a site exactly 30 minutes (and 1 µs more) later
// or earlier, 45 minutes earlier, hours or days later; groups of three tied on one microsecond at AAA; and three
// packets at CCC.
type windowFixture struct {
	*siteFixture
	first func(i int) time.Time
}

func newWindowFixture(t *testing.T) *windowFixture {
	t.Helper()
	f := &windowFixture{siteFixture: &siteFixture{psFixture: newPsFixture(t), usTie: map[string]bool{}}}
	f.first = func(i int) time.Time { return psT0.Add(time.Duration(i) * 80 * time.Second) }
	rng := rand.New(rand.NewPCG(13, 17))
	lag := func(i int) time.Duration {
		switch {
		case i%97 == 0:
			return 60*time.Hour + time.Duration(i)*time.Second // heard there days later
		case i%50 == 7:
			return siteSkew
		case i%50 == 8:
			return siteSkew + time.Microsecond
		case i%50 == 9:
			return -siteSkew
		case i%50 == 10:
			return -siteSkew - time.Microsecond
		case i%50 == 11:
			return 2 * time.Hour
		case i%50 == 12:
			return 26 * time.Hour
		case i%50 == 13:
			return -45 * time.Minute
		}
		return time.Duration(rng.Int64N(int64(10*time.Second))) - 5*time.Second
	}
	for i := range 3000 {
		h := f.sitePacket(i, f.first(i))
		at := f.first(i).Add(lag(i))
		if i%200 >= 100 && i%200 < 103 { // three packets on one microsecond at AAA
			at = f.first(i - i%200 + 100).Add(time.Second)
		}
		f.sited(psObs(h, 1, "AAA", at))
		if i%3 == 0 {
			f.sited(psObs(h, 4, "BBB", f.first(i).Add(lag(i+1))))
		}
		if i == 400 || i == 1400 || i == 2400 {
			f.sited(psObs(h, 7, "CCC", at))
		}
	}
	f.exec("VACUUM ANALYZE packet_sites") // autovacuum keeps the table's visibility map current in production
	f.exec("ANALYZE")
	return f
}

// Windows far from the newest rows, at the 30-minute edges and across ties, walked by pageCursor in both plan modes:
// every page equals the truth from the observations, and the until page reads about a page and the 30 minutes above
// until, not the rows above it.
func TestPacketsBySitesFarWindowPostgres(t *testing.T) {
	f := newWindowFixture(t)
	day := 24 * time.Hour
	edge := f.first(1507)                // a packet heard at AAA exactly 30 minutes after it was first heard
	early := f.first(1509)               // ... exactly 30 minutes before
	tie := f.first(1100).Add(-time.Hour) // just below a tied group
	windows := []struct{ since, until time.Time }{
		{until: psT0.Add(day)},
		{until: edge},
		{until: edge.Add(-time.Microsecond)},
		{until: early},
		{until: tie},
		{since: psT0.Add(2 * day)},
		{since: early},
		{since: early.Add(time.Microsecond)},
		{since: edge},
		{since: psT0.Add(day), until: psT0.Add(day + time.Hour)},
		{since: psT0.Add(day + 12*time.Hour), until: psT0.Add(day)}, // empty
		{until: psT0.Add(-time.Hour)},                               // before every packet
	}
	type shape struct {
		sites  []string
		pt     []int16
		scopes []string
	}
	shapes := []shape{
		{sites: []string{"AAA"}}, {sites: []string{"AAA", "BBB"}}, {sites: []string{"BBB", "CCC", "ZZZ"}},
		{sites: []string{"AAA", "BBB"}, pt: []int16{4, 5}}, {sites: []string{"AAA", "BBB"}, scopes: []string{"#a"}},
		{sites: []string{"CCC"}},
	}
	rows := 0
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		if _, err := tx.Exec(f.ctx, "SET LOCAL plan_cache_mode = "+mode); err != nil {
			t.Fatal(err)
		}
		s := f.trusted(tx)
		for _, w := range windows {
			for _, sh := range shapes {
				r := siteReq{sites: sh.sites, pt: sh.pt, scopes: sh.scopes, since: w.since, until: w.until}
				rows += f.walk(s, tx, r, 50)
			}
			rows += f.walk(s, tx, siteReq{sites: []string{"AAA", "BBB"}, since: w.since, until: w.until}, 7)
		}
		rows += f.walk(s, tx, siteReq{sites: []string{"AAA", "BBB"}, until: psT0.Add(day)}, 1)
		_ = tx.Rollback(f.ctx)
	}
	if rows < 50_000 {
		t.Fatalf("walks returned %d rows; the fixture is too thin", rows)
	}

	// Deep cursors: a page from a position far below until, and one above it, equals the truth there.
	s := f.trusted(f.pool)
	r := siteReq{sites: []string{"AAA", "BBB"}, until: psT0.Add(2 * day)}
	for _, at := range []time.Time{psT0.Add(6 * time.Hour), psT0.Add(2*day + 40*time.Minute), psT0.Add(3 * day)} {
		hash := make([]byte, 32)
		pc := &api.PacketCursor{At: at, Hash: hash}
		page, _ := f.page(s, r, 0, pc, 50)
		want := f.truth(f.pool, r, at, hash, 51)
		if len(want) > 50 != page.HasMore || len(page.Items) != min(len(want), 50) {
			t.Fatalf("cursor %v: %d rows (more %v), truth %d", at, len(page.Items), page.HasMore, len(want))
		}
		for i, it := range page.Items {
			if it.PacketHash != want[i].hash {
				t.Fatalf("cursor %v row %d: %s, truth %s", at, i, it.PacketHash, want[i].hash)
			}
		}
	}

	// The until page reads the 30 minutes above until, the rows heard more than 30 minutes late above it and the
	// page, not the 1.8 days of rows above until.
	fl, _ := newPacketFilter(nil, nil, nil, time.Time{}, psT0.Add(day), 0, nil, 50)
	var above int64
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM packet_sites WHERE iata = 'AAA' AND site_heard_at > $1", psT0.Add(day)).Scan(&above); err != nil {
		t.Fatal(err)
	}
	if n := windowRowsRead(t, f.pool, []string{"AAA"}, fl); above < 1500 || n > 200 {
		t.Fatalf("the until page read %d packet_sites rows (%d rows above until)", n, above)
	}
	// So does a page whose cursor already sits below until.
	fl, _ = newPacketFilter(nil, nil, nil, time.Time{}, psT0.Add(day), 0, &api.PacketCursor{At: psT0.Add(12 * time.Hour), Hash: []byte{}}, 50)
	if n := windowRowsRead(t, f.pool, []string{"AAA"}, fl); n > 200 {
		t.Fatalf("a deep until page read %d packet_sites rows", n)
	}
	// A type pair no packet at AAA carries (payload type 0 only comes with route type 0 in the fixture): the typed forms
	// probe each pair's range, where reading each site's or scope's range and filtering would walk all of it. Beyond
	// that they read only the rows heard over 30 minutes late above until + 30 minutes.
	for _, tc := range []struct {
		scopes []string
		until  time.Time
	}{{until: psT0.Add(2 * day)}, {scopes: []string{"#a"}}, {scopes: []string{"#a"}, until: psT0.Add(2 * day)}} {
		fl, _ := newPacketFilter([]int16{0}, []int16{1}, tc.scopes, time.Time{}, tc.until, 0, nil, 50)
		var late int64
		if !tc.until.IsZero() {
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM packet_sites WHERE iata = 'AAA' AND site_heard_at > $1
  AND site_heard_at - first_heard_at > interval '30 minutes'`, tc.until.Add(siteSkew)).Scan(&late); err != nil {
				t.Fatal(err)
			}
		}
		if n := typedWindowRowsRead(t, f.pool, []string{"AAA"}, fl); n > late+10 {
			t.Fatalf("scopes %v until %v: a type pair with no packet read %d packet_sites rows (%d late above until)", tc.scopes, tc.until, n, late)
		}
		page, _ := f.page(s, siteReq{sites: []string{"AAA"}, pt: []int16{0}, rt: []int16{1}, scopes: tc.scopes, until: tc.until}, 0, nil, 50)
		if len(page.Items) != 0 || page.HasMore {
			t.Fatalf("scopes %v: %+v", tc.scopes, page)
		}
	}
	// A since window ends each site at since - 30 minutes: a sparse filter reads the window, not the history below.
	fl, _ = newPacketFilter([]int16{15}, nil, nil, psT0.Add(2*day+12*time.Hour), time.Time{}, 0, nil, 50)
	var window int64
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM packet_sites WHERE iata = 'AAA' AND site_heard_at >= $1", psT0.Add(2*day+11*time.Hour)).Scan(&window); err != nil {
		t.Fatal(err)
	}
	if n := windowRowsRead(t, f.pool, []string{"AAA"}, fl); n > window+60 {
		t.Fatalf("a sparse since page read %d packet_sites rows, its window holds %d", n, window)
	}
}

// typedWindowRowsRead is packetSitesRowsRead for the typed window statement on fl (the scoped one with scopes), without
// the re-check.
func typedWindowRowsRead(t *testing.T, db sqlc.DBTX, sites []string, fl packetFilter) int64 {
	t.Helper()
	var text string
	lo, hi, mainTS, mainHash := windowBounds(fl)
	if len(fl.scopes) > 0 {
		_, _ = sqlc.New(&queryRecorder{text: &text}).ListPacketsBySitesScopedTypedWindow(context.Background(), sqlc.ListPacketsBySitesScopedTypedWindowParams{})
		return packetSitesRowsRead(t, db, text, sites, fl.scopes, fl.payloadTypes, fl.routeTypes, mainTS, mainHash, lo, fl.since, fl.until, false,
			fl.cursorTS, fl.cursorHash, fl.pageLimit, hi)
	}
	_, _ = sqlc.New(&queryRecorder{text: &text}).ListPacketsBySitesTypedWindow(context.Background(), sqlc.ListPacketsBySitesTypedWindowParams{})
	return packetSitesRowsRead(t, db, text, sites, fl.payloadTypes, fl.routeTypes, mainTS, mainHash, lo, fl.since, fl.until, false,
		fl.cursorTS, fl.cursorHash, fl.pageLimit, hi)
}
