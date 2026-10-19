// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

// siteRow is one packet_sites row: a packet heard at a site, at its newest time there.
type siteRow struct {
	hash string
	site string
	at   int
}

// walkData holds 3 sites and 40 packets. Packets x1 and x2 are heard at two sites: their newest rows (sites A and
// C) are the two newest in the data, and their older site-B rows sit just above y, site B's next packet.
func walkData() []siteRow {
	data := []siteRow{
		{"00000000000000f1", "A", 1000}, {"00000000000000f1", "B", 900},
		{"00000000000000f2", "C", 990}, {"00000000000000f2", "B", 890},
		{"00000000000000f3", "B", 880},
	}
	sites := []string{"A", "C", "B"}
	for i := 0; i < 37; i++ {
		data = append(data, siteRow{fmt.Sprintf("%016x", 0x100+i), sites[i%3], 870 - 10*i})
	}
	return data
}

type keyed struct {
	hash string
	at   int
}

func ts(at int) string { return fmt.Sprintf("%06d", at) }

// before reports whether (at, hash) sorts after the cursor in (at DESC, hash DESC) order, i.e. is below it.
func below(at int, hash string, cur []string) bool {
	if cur == nil {
		return true
	}
	a := ts(at)
	return a < cur[0] || (a == cur[0] && hash < cur[1])
}

func page(ks []keyed, n int) planguard.RowSet {
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].at != ks[j].at {
			return ks[i].at > ks[j].at
		}
		return ks[i].hash > ks[j].hash
	})
	if len(ks) > n {
		ks = ks[:n]
	}
	rs := planguard.RowSet{Columns: []string{"packet_hash", "site_heard_at"}}
	for _, k := range ks {
		rs.Rows = append(rs.Rows, []*string{s(k.hash), s(ts(k.at))})
	}
	return rs
}

// oracleFetch: each packet's newest row over the sites, ordered by (site_heard_at DESC, hash DESC), below the cursor.
func oracleFetch(data []siteRow) planguard.PageFetcher {
	return func(_ context.Context, cur []string, limit int) (planguard.RowSet, error) {
		newest := map[string]int{}
		for _, r := range data {
			if r.at > newest[r.hash] {
				newest[r.hash] = r.at
			}
		}
		var ks []keyed
		for h, at := range newest {
			if below(at, h, cur) {
				ks = append(ks, keyed{h, at})
			}
		}
		return page(ks, limit+1), nil
	}
}

// perSite returns each site's first n rows below the cursor (optionally only rows of packets no site has shown).
func perSite(data []siteRow, cur []string, n int, recheck func(hash string) bool) map[string][]siteRow {
	bySite := map[string][]siteRow{}
	for _, r := range data {
		if below(r.at, r.hash, cur) && (recheck == nil || recheck(r.hash)) {
			bySite[r.site] = append(bySite[r.site], r)
		}
	}
	for site, rs := range bySite {
		sort.Slice(rs, func(i, j int) bool { return rs[i].at > rs[j].at })
		if len(rs) > n {
			bySite[site] = rs[:n]
		}
	}
	return bySite
}

// notShown reports whether no row of the packet is at or above the cursor.
func notShown(data []siteRow, cur []string) func(string) bool {
	return func(hash string) bool {
		for _, r := range data {
			if r.hash == hash && !below(r.at, r.hash, cur) {
				return false
			}
		}
		return true
	}
}

func groupPage(bySite map[string][]siteRow, keep func(string) bool, n int) planguard.RowSet {
	newest := map[string]int{}
	for _, rs := range bySite {
		for _, r := range rs {
			if r.at > newest[r.hash] {
				newest[r.hash] = r.at
			}
		}
	}
	var ks []keyed
	for h, at := range newest {
		if keep == nil || keep(h) {
			ks = append(ks, keyed{h, at})
		}
	}
	return page(ks, n)
}

// rmFetch: the readmodels statement's logic, a per-site LIMIT and then the cross-site re-check.
func rmFetch(data []siteRow) planguard.PageFetcher {
	return func(_ context.Context, cur []string, limit int) (planguard.RowSet, error) {
		return groupPage(perSite(data, cur, limit+1, nil), notShown(data, cur), limit+1), nil
	}
}

// endFetch: the end-state logic, the re-check inside each per-site range.
func endFetch(data []siteRow) planguard.PageFetcher {
	return func(_ context.Context, cur []string, limit int) (planguard.RowSet, error) {
		return groupPage(perSite(data, cur, limit+1, notShown(data, cur)), nil, limit+1), nil
	}
}

func clean(w planguard.WalkOutcome) bool {
	return w.FirstWrong == 0 && w.Missing == 0 && w.Duplicates == 0 && w.OutOfOrder == 0 && w.Stuck == ""
}

func TestWalkModeFindsReadmodelsSkip(t *testing.T) {
	ctx := context.Background()
	data := walkData()
	cursor := []string{"site_heard_at", "packet_hash"}
	identity := []string{"packet_hash"}
	oracle, stuck, err := planguard.Walk(ctx, oracleFetch(data), cursor, 2000, 2000)
	if err != nil || stuck != "" {
		t.Fatalf("oracle: %v %q", err, stuck)
	}
	if len(oracle.Rows) != 40 {
		t.Fatalf("oracle has %d rows, want 40", len(oracle.Rows))
	}

	walk := func(f planguard.PageFetcher, limit int) planguard.WalkOutcome {
		t.Helper()
		rs, stuck, err := planguard.Walk(ctx, f, cursor, limit, 2000)
		if err != nil {
			t.Fatal(err)
		}
		w, err := planguard.CompareWalk(oracle, rs, identity)
		if err != nil {
			t.Fatal(err)
		}
		w.Stuck = stuck
		return w
	}
	rm1 := walk(rmFetch(data), 1)
	if rm1.Missing == 0 || rm1.FirstWrong == 0 {
		t.Errorf("readmodels at limit 1: %+v, want rows missing and a first wrong row", rm1)
	}
	if rm1.FirstWrong != 3 {
		t.Errorf("readmodels at limit 1: first wrong row %d, want 3 (site B's next packet is skipped)", rm1.FirstWrong)
	}
	if end1 := walk(endFetch(data), 1); !clean(end1) || end1.Rows != 40 {
		t.Errorf("end state at limit 1: %+v, want all zero over 40 rows", end1)
	}
	// The control: at limit 7 the readmodels statement happens to be right.
	if rm7 := walk(rmFetch(data), 7); !clean(rm7) || rm7.Rows != 40 {
		t.Errorf("readmodels at limit 7: %+v, want all zero over 40 rows", rm7)
	}
	if end7 := walk(endFetch(data), 7); !clean(end7) || end7.Rows != 40 {
		t.Errorf("end state at limit 7: %+v, want all zero over 40 rows", end7)
	}
	// MaxRows caps the walk.
	if rs, _, err := planguard.Walk(ctx, endFetch(data), cursor, 3, 10); err != nil || len(rs.Rows) != 10 {
		t.Errorf("max rows 10: %d rows, %v", len(rs.Rows), err)
	}
}

func TestCompareWalkCounts(t *testing.T) {
	cols := []string{"packet_hash", "site_heard_at"}
	ref := rows(cols, []string{"a", "5"}, []string{"b", "4"}, []string{"c", "3"}, []string{"d", "2"})
	id := []string{"packet_hash"}
	if w := compareWalk(t, ref, ref, id); !clean(w) || w.Rows != 4 {
		t.Errorf("same walk: %+v", w)
	}
	dup := rows(cols, []string{"a", "5"}, []string{"b", "4"}, []string{"b", "4"}, []string{"c", "3"}, []string{"d", "2"})
	if w := compareWalk(t, ref, dup, id); w.Duplicates != 1 || w.FirstWrong != 3 || w.Missing != 0 {
		t.Errorf("duplicate row: %+v", w)
	}
	swap := rows(cols, []string{"a", "5"}, []string{"c", "3"}, []string{"b", "4"}, []string{"d", "2"})
	if w := compareWalk(t, ref, swap, id); w.OutOfOrder != 1 || w.FirstWrong != 2 || w.Missing != 0 {
		t.Errorf("rows out of order: %+v", w)
	}
	val := rows(cols, []string{"a", "5"}, []string{"b", "9"}, []string{"c", "3"}, []string{"d", "2"})
	if w := compareWalk(t, ref, val, id); w.FirstWrong != 2 || w.Missing != 0 {
		t.Errorf("value changed: %+v", w)
	}
	short := rows(cols, []string{"a", "5"}, []string{"b", "4"})
	if w := compareWalk(t, ref, short, id); w.Missing != 2 || w.FirstWrong != 3 {
		t.Errorf("walk stopped early: %+v", w)
	}
}

func compareWalk(t *testing.T, ref, cand planguard.RowSet, key []string) planguard.WalkOutcome {
	t.Helper()
	w, err := planguard.CompareWalk(ref, cand, key)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestCompareWalkRefDuplicates(t *testing.T) {
	cols := []string{"packet_hash", "site_heard_at"}
	ref := rows(cols, []string{"a", "5"}, []string{"a", "4"})
	if _, err := planguard.CompareWalk(ref, ref, []string{"packet_hash"}); err == nil || !strings.Contains(err.Error(), "rows 1 and 2") {
		t.Errorf("ref with a repeated identity: err %v, want an error naming rows 1 and 2", err)
	}
	if _, err := planguard.CompareWalk(ref, ref, []string{"nope"}); err == nil {
		t.Error("identity column missing from the ref: no error")
	}
}

func TestWalkStopsOnStuckCursor(t *testing.T) {
	calls := 0
	same := func(_ context.Context, cur []string, limit int) (planguard.RowSet, error) {
		calls++
		rs := planguard.RowSet{Columns: []string{"packet_hash", "site_heard_at"}}
		for i := 0; i <= limit; i++ {
			rs.Rows = append(rs.Rows, []*string{s(fmt.Sprintf("h%d", i)), s("000100")})
		}
		return rs, nil
	}
	rs, stuck, err := planguard.Walk(context.Background(), same, []string{"site_heard_at", "packet_hash"}, 2, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if stuck != "cursor did not advance at page 2" {
		t.Errorf("stuck %q, want %q", stuck, "cursor did not advance at page 2")
	}
	if calls != 2 || len(rs.Rows) != 4 {
		t.Errorf("%d fetches, %d rows; want 2 fetches and 4 rows", calls, len(rs.Rows))
	}

	// A cursor that cycles through two values is stuck too.
	n := 0
	cycle := func(_ context.Context, cur []string, limit int) (planguard.RowSet, error) {
		n++
		rs := planguard.RowSet{Columns: []string{"packet_hash", "site_heard_at"}}
		for i := 0; i <= limit; i++ {
			rs.Rows = append(rs.Rows, []*string{s(fmt.Sprintf("h%d", n%2)), s("000100")})
		}
		return rs, nil
	}
	if _, stuck, err := planguard.Walk(context.Background(), cycle, []string{"site_heard_at", "packet_hash"}, 1, 2000); err != nil || !strings.HasPrefix(stuck, "cursor did not advance at page 3") {
		t.Errorf("cycling cursor: %q, %v", stuck, err)
	}

	// A NULL in a cursor column, or a missing cursor column, is an error.
	nullCur := func(_ context.Context, cur []string, limit int) (planguard.RowSet, error) {
		return planguard.RowSet{Columns: []string{"packet_hash", "site_heard_at"}, Rows: [][]*string{{s("a"), nil}, {s("b"), nil}}}, nil
	}
	if _, _, err := planguard.Walk(context.Background(), nullCur, []string{"site_heard_at", "packet_hash"}, 1, 2000); err == nil {
		t.Error("NULL cursor value: no error")
	}
	if _, _, err := planguard.Walk(context.Background(), nullCur, []string{"nope"}, 1, 2000); err == nil {
		t.Error("missing cursor column: no error")
	}
}
