// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// packetFixture: scopes #a, #b, #c and #empty (no packets); 600 packets over 40 tied milliseconds with
// microsecond-tie groups inside each, mixed types, a quarter unscoped, first_heard_at up to 6 s before
// last_heard_at; 21 packets (i % 29 = 1) with first_heard_at 300 µs after last_heard_at; two observations per
// packet on distinct observers, and for the 10 packets with i % 60 = 0 both observations share the newest heard_at.
const packetFixture = `
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES
  (1, '#a', '\x01', '\x01'), (2, '#b', '\x02', '\x02'), (3, '#c', '\x03', '\x03'), (4, '#empty', '\x04', '\x04');
INSERT INTO observers (id, public_key, display_name)
SELECT ('00000000-0000-0000-0000-00000000030' || n)::uuid, int2send(n::int2), 'obs' || n FROM generate_series(1, 4) n;
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, scope_id, raw_payload, raw_header,
                     parsed_payload, first_heard_at, last_heard_at, observation_count)
SELECT decode(md5(i::text), 'hex'), (i * 7) % 16, 0, (i * 3) % 4, CASE WHEN i % 4 = 0 THEN NULL ELSE 1 + i % 3 END,
       '\x00', '\x00',
       CASE WHEN (i * 7) % 16 = 4 THEN jsonb_build_object('appData', jsonb_build_object('name', 'n' || i)) END,
       CASE WHEN i % 29 = 1 THEN x.l + interval '300 microseconds' ELSE x.l - (i % 7) * interval '1 second' END,
       x.l, 2
FROM generate_series(1, 600) i,
     LATERAL (SELECT '2026-01-10 00:00:00+00'::timestamptz + (i % 40) * interval '1 millisecond'
                     + (i % 3) * interval '1 microsecond') x(l);
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes)
SELECT packet_hash, ('00000000-0000-0000-0000-00000000030' || (1 + i % 4))::uuid, 'YVR',
       last_heard_at - CASE WHEN i % 60 = 0 THEN interval '0' ELSE interval '1 millisecond' END, 65, 1, 1, '\x01'
FROM (SELECT i, decode(md5(i::text), 'hex') AS packet_hash FROM generate_series(1, 600) i) g JOIN packets USING (packet_hash);
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes)
SELECT packet_hash, ('00000000-0000-0000-0000-00000000030' || (1 + (i + 1) % 4))::uuid, 'YYZ', last_heard_at, 66, 1, 2, '\x0203'
FROM (SELECT i, decode(md5(i::text), 'hex') AS packet_hash FROM generate_series(1, 600) i) g JOIN packets USING (packet_hash);
ANALYZE packets;`

var packetBase = time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)

// fixtureHash is packet i's hash in packetFixture.
func fixtureHash(i int) []byte {
	h := md5.Sum(fmt.Appendf(nil, "%d", i))
	return h[:]
}

// packetsGlobalTx is a rolled-back transaction holding the end-state schema and packetFixture.
func packetsGlobalTx(t *testing.T) (context.Context, pgx.Tx, *Store) {
	t.Helper()
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	applyPacketsEndState(t, ctx, tx)
	mustExec(t, ctx, tx, packetFixture)
	return ctx, tx, routeStore(tx)
}

type packetShape struct {
	name         string
	pt, rt       []int16
	scopes       []string
	since, until time.Time
}

func optTS(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

// legacyPacketRows runs one of today's statements with the arguments today's Store passed it.
func legacyPacketRows(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, sh packetShape, cursor int64, pageLimit int32) []packetRow {
	t.Helper()
	var cts pgtype.Timestamptz
	if cursor > 0 {
		cts = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	return legacyRows[packetRow](t, ctx, tx, sql, sh.pt, sh.rt, optTS(sh.since), optTS(sh.until), cts, pageLimit, sh.scopes)
}

// newPacketRows runs the dispatcher's core for one request; no rows when the filter cannot match.
func newPacketRows(t *testing.T, ctx context.Context, s *Store, sh packetShape, cursor int64, pc *api.PacketCursor, limit int32) []packetRow {
	t.Helper()
	f, ok := newPacketFilter(sh.pt, sh.rt, sh.scopes, sh.since, sh.until, cursor, pc, limit)
	if !ok {
		return nil
	}
	rows, err := s.listPacketRows(ctx, f)
	if err != nil {
		t.Fatalf("%s: %v", sh.name, err)
	}
	return rows
}

func packetKeys(rows []packetRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%s/%x", r.LastHeardAt.Time.Format("15:04:05.000000"), r.PacketHash[:3])
	}
	return out
}

// sameKeyRuns reports whether got and raw have the same last_heard_at sequence and, for every run of equal
// last_heard_at, the same hash set. A run the LIMIT cut (the last one of a full page) is compared by length only.
func sameKeyRuns(got, raw []packetRow, full bool) bool {
	if len(got) != len(raw) {
		return false
	}
	for i := range got {
		if !got[i].LastHeardAt.Time.Equal(raw[i].LastHeardAt.Time) {
			return false
		}
	}
	for i := 0; i < len(got); {
		j := i
		for j < len(got) && got[j].LastHeardAt.Time.Equal(got[i].LastHeardAt.Time) {
			j++
		}
		if j < len(got) || !full {
			a, b := hashSet(got[i:j]), hashSet(raw[i:j])
			if !slices.Equal(a, b) {
				return false
			}
		}
		i = j
	}
	return true
}

func hashSet(rows []packetRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r.PacketHash)
	}
	slices.Sort(out)
	return out
}

func globalShapes() []packetShape {
	bogus := make([]string, 5000)
	for i := range bogus {
		bogus[i] = fmt.Sprintf("#bogus%d", i)
	}
	all16x4 := packetShape{name: "all 16x4 explicit"}
	for i := int16(0); i < 16; i++ {
		all16x4.pt = append(all16x4.pt, i)
	}
	all16x4.rt = []int16{0, 1, 2, 3}
	oneFirst := packetBase.Add(7*time.Millisecond + time.Microsecond) // packet 7: first_heard_at = last_heard_at
	return []packetShape{
		{name: "unfiltered"},
		{name: "pt 4", pt: []int16{4}},
		{name: "pt 4,4 rt 0,0,1", pt: []int16{4, 4}, rt: []int16{0, 0, 1}},
		{name: "pt 99,4", pt: []int16{99, 4}},
		{name: "pt 99", pt: []int16{99}},
		{name: "pt empty", pt: []int16{}},
		{name: "rt 3", rt: []int16{3}},
		{name: "rt 3,7", rt: []int16{3, 7}},
		{name: "web 13 types", pt: []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15}},
		all16x4,
		{name: "scopes #a", scopes: []string{"#a"}},
		{name: "scopes #a,#b,#a,#nope", scopes: []string{"#a", "#b", "#a", "#nope"}},
		{name: "scopes #empty", scopes: []string{"#empty"}},
		{name: "scopes #nope", scopes: []string{"#nope"}},
		{name: "scopes untrimmed", scopes: []string{" #a"}},
		{name: "scopes #a,#c pt fib", scopes: []string{"#a", "#c"}, pt: []int16{1, 2, 3, 5, 8, 13}},
		{name: "window since until", since: packetBase.Add(-3 * time.Second), until: packetBase.Add(20 * time.Millisecond)},
		{name: "window since only", since: packetBase.Add(14*time.Millisecond + 200*time.Microsecond)},
		{name: "window since early", since: packetBase.Add(-4 * time.Second)},
		{name: "window until only", until: packetBase.Add(-2 * time.Second)},
		{name: "window typed", since: packetBase.Add(-5 * time.Second), pt: []int16{4, 9}, rt: []int16{1, 2}},
		{name: "window scoped", since: packetBase.Add(-5 * time.Second), scopes: []string{"#a", "#b"}},
		{name: "window since = until", since: oneFirst, until: oneFirst},
		{name: "window since > until", since: oneFirst, until: oneFirst.Add(-time.Microsecond)},
		{name: "5000 bogus scopes", scopes: bogus},
	}
}

// Shapes × cursors × limits × both plan modes: rows equal today's statement with the tiebreak, in order, on all
// columns; against today's raw statement the key sequence and the set per key run are equal (tie order).
func TestListPacketsGlobalMatchesLegacyPostgres(t *testing.T) {
	ctx, tx, s := packetsGlobalTx(t)
	cursors := []int64{
		0, -5,
		packetBase.Add(17 * time.Millisecond).UnixMilli(), // a tied millisecond
		packetBase.Add(30 * time.Millisecond).UnixMilli(), // just above a millisecond of microsecond-tie groups
		packetBase.Add(-time.Millisecond).UnixMilli(),
		packetBase.Add(40 * time.Millisecond).UnixMilli(),
		math.MaxInt64,
	}
	nonEmpty := 0
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
		for _, sh := range globalShapes() {
			for _, c := range cursors {
				for _, limit := range []int32{1, 2, 3, 7, 51} {
					want := legacyPacketRows(t, ctx, tx, legacyListPacketsDet, sh, c, limit+1)
					got := newPacketRows(t, ctx, s, sh, c, nil, limit)
					if !sameRows(got, want) {
						t.Fatalf("%s %s cursor=%d limit=%d:\n got  %v\n want %v", mode, sh.name, c, limit, packetKeys(got), packetKeys(want))
					}
					raw := legacyPacketRows(t, ctx, tx, legacyListPackets, sh, c, limit+1)
					if !sameKeyRuns(got, raw, len(raw) == int(limit)+1) {
						t.Fatalf("%s %s cursor=%d limit=%d vs raw:\n got %v\n raw %v", mode, sh.name, c, limit, packetKeys(got), packetKeys(raw))
					}
					if len(got) > 0 {
						nonEmpty++
					}
				}
			}
		}
	}
	if nonEmpty < 500 {
		t.Fatalf("only %d non-empty comparisons; the fixture does not exercise the statements", nonEmpty)
	}
}

// Three or more packets in one millisecond at a page boundary.
func TestPacketsGlobalCursorTiesPostgres(t *testing.T) {
	ctx, tx, s := packetsGlobalTx(t)
	for _, sh := range []packetShape{
		{name: "unfiltered"},
		{name: "pt 4", pt: []int16{4}},
		{name: "scopes #a", scopes: []string{"#a"}},
		{name: "window", since: packetBase.Add(-4 * time.Second)},
	} {
		all := legacyPacketRows(t, ctx, tx, legacyListPacketsDet, sh, 0, 100000)
		// Smallest page that ends inside a millisecond with at least one more row after the boundary.
		limit := 0
		for i := 1; i < len(all); i++ {
			if all[i].LastHeardAt.Time.UnixMilli() == all[i-1].LastHeardAt.Time.UnixMilli() && i >= 2 {
				limit = i
				break
			}
		}
		if limit == 0 {
			t.Fatalf("%s: no tied millisecond in %d rows", sh.name, len(all))
		}
		page1 := newPacketRows(t, ctx, s, sh, 0, nil, int32(limit))
		if !sameRows(page1, all[:limit+1]) {
			t.Fatalf("%s page 1: %v", sh.name, packetKeys(page1))
		}
		last := page1[limit-1]
		lastMS := last.LastHeardAt.Time.UnixMilli()

		// Legacy cursor: today's page, which skips the rest of the tied millisecond.
		legacyNext := legacyPacketRows(t, ctx, tx, legacyListPacketsDet, sh, lastMS, int32(limit)+1)
		gotLegacy := newPacketRows(t, ctx, s, sh, lastMS, nil, int32(limit))
		if !sameRows(gotLegacy, legacyNext) {
			t.Fatalf("%s legacy cursor:\n got  %v\n want %v", sh.name, packetKeys(gotLegacy), packetKeys(legacyNext))
		}
		if len(gotLegacy) > 0 && gotLegacy[0].LastHeardAt.Time.UnixMilli() >= lastMS {
			t.Fatalf("%s legacy cursor did not skip the tied millisecond", sh.name)
		}

		// pageCursor: exactly the skipped tied rows, then the legacy page's rows.
		var skipped []packetRow
		for _, r := range all[limit:] {
			if r.LastHeardAt.Time.UnixMilli() != lastMS {
				break
			}
			skipped = append(skipped, r)
		}
		if len(skipped) == 0 {
			t.Fatalf("%s: boundary is not inside a tied millisecond", sh.name)
		}
		want := append(slices.Clone(skipped), legacyNext...)
		if len(want) > limit+1 {
			want = want[:limit+1]
		}
		gotPC := newPacketRows(t, ctx, s, sh, 0, &api.PacketCursor{At: last.LastHeardAt.Time, Hash: last.PacketHash}, int32(limit))
		if !sameRows(gotPC, want) {
			t.Fatalf("%s pageCursor:\n got  %v\n want %v", sh.name, packetKeys(gotPC), packetKeys(want))
		}

		// A limit-1 pageCursor walk returns every row once, in order. A legacy cursor passed alongside is ignored.
		var walked []packetRow
		var pc *api.PacketCursor
		for range len(all) + 2 {
			var page []packetRow
			if pc == nil {
				page = newPacketRows(t, ctx, s, sh, 0, nil, 1)
			} else {
				page = newPacketRows(t, ctx, s, sh, lastMS, pc, 1)
			}
			if len(page) == 0 {
				break
			}
			walked = append(walked, page[0])
			pc = &api.PacketCursor{At: page[0].LastHeardAt.Time, Hash: page[0].PacketHash}
		}
		if !sameRows(walked, all) {
			t.Fatalf("%s limit-1 walk: %d rows, want %d", sh.name, len(walked), len(all))
		}
	}
}

// An inverted row inside [since, until] appears once, at its last_heard_at position, also when only the inverted
// branch can reach it.
func TestListPacketsWindowInvertedRowsPostgres(t *testing.T) {
	ctx, tx, s := packetsGlobalTx(t)
	since := packetBase.Add(14*time.Millisecond + 200*time.Microsecond)
	inverted := fixtureHash(494) // last_heard_at base+14ms+2µs < since <= first_heard_at = last_heard_at + 300µs
	var lha, fha time.Time
	if err := tx.QueryRow(ctx, `SELECT last_heard_at, first_heard_at FROM packets WHERE packet_hash = $1`, inverted).Scan(&lha, &fha); err != nil {
		t.Fatal(err)
	}
	if !lha.Before(since) || fha.Before(since) {
		t.Fatalf("fixture: packet 494 last %v first %v, since %v", lha, fha, since)
	}
	for _, sh := range []packetShape{
		{name: "since", since: since},
		{name: "since scoped", since: since, scopes: []string{"#a", "#b", "#c"}},
	} {
		want := legacyPacketRows(t, ctx, tx, legacyListPacketsDet, sh, 0, 100000)
		for _, limit := range []int32{1, 3, 100000 - 1} {
			got := newPacketRows(t, ctx, s, sh, 0, nil, limit)
			if !sameRows(got, want[:min(len(want), int(limit)+1)]) {
				t.Fatalf("%s limit %d:\n got  %v\n want %v", sh.name, limit, packetKeys(got), packetKeys(want))
			}
		}
		n := 0
		for _, r := range want {
			if bytes.Equal(r.PacketHash, inverted) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s: inverted packet appears %d times", sh.name, n)
		}
		pc := &api.PacketCursor{At: lha.Add(time.Microsecond), Hash: inverted}
		got := newPacketRows(t, ctx, s, sh, 0, pc, 3)
		if len(got) == 0 || !bytes.Equal(got[0].PacketHash, inverted) {
			t.Fatalf("%s pageCursor just above the inverted row: %v", sh.name, packetKeys(got))
		}
	}
	for _, sh := range []packetShape{
		{name: "until only", until: packetBase.Add(-2 * time.Second)},
		{name: "since only", since: since},
		{name: "since = until", since: fha, until: fha},
	} {
		want := legacyPacketRows(t, ctx, tx, legacyListPacketsDet, sh, 0, 100000)
		got := newPacketRows(t, ctx, s, sh, 0, nil, 100000-1)
		if len(want) == 0 || !sameRows(got, want) {
			t.Fatalf("%s:\n got  %v\n want %v", sh.name, packetKeys(got), packetKeys(want))
		}
	}
}

// Ties on the newest heard_at resolve to the highest observation id.
func TestPacketsLatestObserverTiePostgres(t *testing.T) {
	ctx, tx, s := packetsGlobalTx(t)
	rows := newPacketRows(t, ctx, s, packetShape{name: "all"}, 0, nil, 1000)
	byHash := map[string]packetRow{}
	for _, r := range rows {
		byHash[string(r.PacketHash)] = r
	}
	checked := 0
	for i := 60; i <= 600; i += 60 {
		h := fixtureHash(i)
		var want uuid.UUID
		var tied int
		if err := tx.QueryRow(ctx, `
SELECT (SELECT observer_id FROM packet_observations WHERE packet_hash = $1 ORDER BY heard_at DESC, id DESC LIMIT 1),
       (SELECT count(*) FROM packet_observations WHERE packet_hash = $1
          AND heard_at = (SELECT max(heard_at) FROM packet_observations WHERE packet_hash = $1))::int`, h).Scan(&want, &tied); err != nil {
			t.Fatal(err)
		}
		if tied != 2 {
			t.Fatalf("packet %d: %d observations on the newest heard_at, want 2", i, tied)
		}
		r, ok := byHash[string(h)]
		if !ok {
			t.Fatalf("packet %d missing", i)
		}
		if r.LatestObserverID != want {
			t.Errorf("packet %d: latest observer %v, want %v (highest id)", i, r.LatestObserverID, want)
		}
		checked++
	}
	if checked != 10 {
		t.Fatalf("checked %d packets, want 10", checked)
	}
}
