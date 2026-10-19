// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// keysetFixture: 300 nodes and 120 observers. Row i sits in block (i-1)/size at position r = (i-1)%size; rows
// r = 0..2 of every block share one millisecond (12 tied groups per table), r = 0 and r = 1 also share one
// microsecond in blocks 0..2 (3 exact pairs). Observers are within minutes of now() so status splits online and
// offline; 20 (i % 6 = 0) have NULL last_status_at, the rest have last_status_at 60-120 s before last_seen.
const keysetFixture = `
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES
  (1, '#test', '\x01', '\x02'), (2, '#other', '\x03', '\x04');
INSERT INTO nodes (id, public_key, node_type, name, last_seen, default_scope_id)
SELECT md5('node' || i)::uuid,
       CASE WHEN i % 10 = 0 THEN '\x0a'::bytea || sha256(int4send(i)) ELSE sha256(int4send(i)) END,
       1 + i % 4, 'node-' || i,
       '2026-03-01'::timestamptz - (b * 25 + CASE WHEN r < 3 THEN 0 ELSE r END) * interval '1 second'
         + CASE WHEN r < 3 AND NOT (b < 3 AND r = 1) THEN r ELSE 0 END * interval '1 microsecond',
       CASE WHEN i % 3 = 0 THEN 1 END
FROM generate_series(1, 300) i, LATERAL (SELECT (i - 1) / 25 AS b, (i - 1) % 25 AS r) k;
INSERT INTO node_iatas (node_id, iata) SELECT md5('node' || i)::uuid, 'YVR' FROM generate_series(1, 300) i WHERE i % 2 = 0;
INSERT INTO node_iatas (node_id, iata) SELECT md5('node' || i)::uuid, 'YYJ' FROM generate_series(1, 300) i WHERE i % 5 = 0;
INSERT INTO observers (id, public_key, display_name, observer_type, last_iata, last_seen, last_status_at)
SELECT md5('observer' || i)::uuid, sha256(int4send(100000 + i)), 'obs-' || i,
       CASE WHEN i % 3 = 0 THEN 'repeater' ELSE 'companion' END,
       CASE WHEN i % 2 = 0 THEN 'YVR' ELSE 'YYZ' END,
       ls, CASE WHEN i % 6 = 0 THEN NULL ELSE ls - interval '1 minute' - (i % 4) * interval '20 seconds' END
FROM generate_series(1, 120) i,
     LATERAL (SELECT (i - 1) / 10 AS b, (i - 1) % 10 AS r) k,
     LATERAL (SELECT date_trunc('second', now()) - interval '1 second'
                     - (b * 10 + CASE WHEN r < 3 THEN 0 ELSE r END) * interval '5 seconds'
                     + CASE WHEN r < 3 AND NOT (b < 3 AND r = 1) THEN r ELSE 0 END * interval '1 microsecond') t(ls);
INSERT INTO observer_brokers (observer_id, broker_name) SELECT md5('observer' || i)::uuid, 'b1' FROM generate_series(1, 120) i;
INSERT INTO observer_brokers (observer_id, broker_name) SELECT md5('observer' || i)::uuid, 'b2' FROM generate_series(1, 120) i WHERE i % 4 = 0;
INSERT INTO observer_scopes (observer_id, scope_id) SELECT md5('observer' || i)::uuid, 1 FROM generate_series(1, 120) i WHERE i % 5 = 0;
INSERT INTO observer_scopes (observer_id, scope_id) SELECT md5('observer' || i)::uuid, 2 FROM generate_series(1, 120) i WHERE i % 7 = 0;
ANALYZE nodes, node_iatas, observers, observer_brokers, observer_scopes, transport_scopes;`

// keysetTx is a rolled-back transaction holding the schema after the small-table index drops and keysetFixture.
func keysetTx(t *testing.T) (context.Context, pgx.Tx, *Store) {
	t.Helper()
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	applyMigrationsTx(t, ctx, tx, smallTableMigrations...)
	mustExec(t, ctx, tx, keysetFixture)
	return ctx, tx, &Store{q: sqlc.New(tx)}
}

// keysetRow is one row of a table in (last_seen DESC, id DESC) order.
type keysetRow struct {
	ID           uuid.UUID
	LastSeen     time.Time
	LastStatusAt pgtype.Timestamptz
}

func keysetOrder(t *testing.T, ctx context.Context, tx pgx.Tx, sql string) []keysetRow {
	t.Helper()
	return legacyRows[keysetRow](t, ctx, tx, sql)
}

// tiedMillis are the epoch milliseconds that hold more than one row, newest first.
func tiedMillis(rows []keysetRow) []int64 {
	var out []int64
	for i := 1; i < len(rows); i++ {
		ms := rows[i].LastSeen.UnixMilli()
		if ms == rows[i-1].LastSeen.UnixMilli() && !slices.Contains(out, ms) {
			out = append(out, ms)
		}
	}
	return out
}

func keysetIDs(rows []keysetRow) []uuid.UUID {
	out := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func legacyCursorTS(cursor int64) pgtype.Timestamptz {
	if cursor > 0 {
		return pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	return pgtype.Timestamptz{}
}

type nodeShape struct {
	name           string
	nodeType       int16
	iatas          []string
	nameFilter     string
	scope          string
	prefix         string
	withNeighbours bool
}

func nodeShapes() []nodeShape {
	return []nodeShape{
		{name: "unfiltered"},
		{name: "unfiltered neighbours", withNeighbours: true},
		{name: "type 2", nodeType: 2},
		{name: "iatas YVR", iatas: []string{"YVR"}},
		{name: "name node-1", nameFilter: "node-1"},
		{name: "scope #test", scope: "#test"},
		{name: "pubkeyPrefix 0a", prefix: "0a"},
	}
}

func legacyNodeRows(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, sh nodeShape, cursor int64, pageLimit int32) []sqlc.ListNodesRow {
	t.Helper()
	return legacyRows[sqlc.ListNodesRow](t, ctx, tx, sql, sh.nodeType, sh.iatas, "any", "any", []byte(nil), sh.nameFilter,
		legacyCursorTS(cursor), pageLimit, sh.scope, sh.withNeighbours, sh.prefix)
}

func newNodeRows(t *testing.T, ctx context.Context, s *Store, sh nodeShape, cursor int64, pc *api.NodeCursor, limit int32) []sqlc.ListNodesRow {
	t.Helper()
	rows, err := s.q.ListNodes(ctx, listNodesParams(sh.nodeType, sh.iatas, nil, nil, nil, sh.prefix, sh.nameFilter, sh.scope, cursor, pc, limit, sh.withNeighbours))
	if err != nil {
		t.Fatalf("%s: %v", sh.name, err)
	}
	return rows
}

func nodeKeys(rows []sqlc.ListNodesRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%s/%s", r.LastSeen.Time.Format("15:04:05.000000"), r.ID.String()[:4])
	}
	return out
}

func nodeLastSeen(rows []sqlc.ListNodesRow) []time.Time {
	out := make([]time.Time, len(rows))
	for i, r := range rows {
		out[i] = r.LastSeen.Time
	}
	return out
}

// Legacy cursors return today's rows: the nil uuid sorts below every id, so (ms, nil) is today's "< ms".
func TestNodesLegacyCursorMatchesTodayPostgres(t *testing.T) {
	ctx, tx, s := keysetTx(t)
	all := keysetOrder(t, ctx, tx, "SELECT id, last_seen, NULL::timestamptz FROM nodes ORDER BY last_seen DESC, id DESC")
	tied := tiedMillis(all)
	if len(tied) != 12 {
		t.Fatalf("fixture: %d tied milliseconds, want 12", len(tied))
	}
	cursors := append([]int64{0, -5, all[3].LastSeen.Add(-500 * time.Millisecond).UnixMilli()}, tied...)
	nonEmpty := 0
	for _, setting := range planSettings {
		mustExec(t, ctx, tx, setting)
		for _, sh := range nodeShapes() {
			for _, c := range cursors {
				for _, limit := range []int32{1, 2, 51} {
					want := legacyNodeRows(t, ctx, tx, legacyListNodesDet, sh, c, limit+1)
					got := newNodeRows(t, ctx, s, sh, c, nil, limit)
					if !sameRows(got, want) {
						t.Fatalf("%q %s cursor=%d limit=%d:\n got  %v\n want %v", setting, sh.name, c, limit, nodeKeys(got), nodeKeys(want))
					}
					raw := legacyNodeRows(t, ctx, tx, legacyListNodes, sh, c, limit+1)
					if !slices.EqualFunc(nodeLastSeen(got), nodeLastSeen(raw), time.Time.Equal) {
						t.Fatalf("%q %s cursor=%d limit=%d vs raw:\n got %v\n raw %v", setting, sh.name, c, limit, nodeKeys(got), nodeKeys(raw))
					}
					if len(got) > 0 {
						nonEmpty++
					}
				}
			}
		}
	}
	mustExec(t, ctx, tx, "RESET plan_cache_mode; RESET enable_seqscan")
	if nonEmpty < 500 {
		t.Fatalf("only %d non-empty comparisons; the fixture does not exercise the statement", nonEmpty)
	}
}

// A page boundary inside a tied millisecond (and inside an exact-microsecond pair): a pageCursor page starts
// with the tied rows the legacy cursor skips; a limit-1 pageCursor walk returns every node once, in order.
func TestNodesPageCursorTiesPostgres(t *testing.T) {
	ctx, tx, s := keysetTx(t)
	all := keysetOrder(t, ctx, tx, "SELECT id, last_seen, NULL::timestamptz FROM nodes ORDER BY last_seen DESC, id DESC")
	if len(all) != 300 {
		t.Fatalf("fixture: %d nodes", len(all))
	}
	// The smallest page that ends inside a tied millisecond, and the smallest that ends between two rows with the
	// same microsecond, where only the id separates them.
	var limits []int
	for i := 1; i < len(all) && len(limits) < 2; i++ {
		sameMS := all[i].LastSeen.UnixMilli() == all[i-1].LastSeen.UnixMilli()
		if (len(limits) == 0 && sameMS) || (len(limits) == 1 && all[i].LastSeen.Equal(all[i-1].LastSeen)) {
			limits = append(limits, i)
		}
	}
	if len(limits) != 2 || all[limits[0]].LastSeen.Equal(all[limits[0]-1].LastSeen) {
		t.Fatalf("fixture: boundaries %v", limits)
	}
	none := nodeShape{name: "unfiltered"}
	for _, limit := range limits {
		page1, err := s.ListNodes(ctx, 0, nil, nil, nil, nil, "", "", "", 0, nil, int32(limit), false)
		if err != nil {
			t.Fatal(err)
		}
		last := all[limit-1]
		lastMS := last.LastSeen.UnixMilli()
		if !page1.HasMore || page1.NextCursor == nil || *page1.NextCursor != lastMS || page1.NextPageCursor == nil ||
			*page1.NextPageCursor != (api.NodeCursor{LastSeen: last.LastSeen, ID: last.ID}).String() {
			t.Fatalf("limit %d page 1 cursors: %+v", limit, page1)
		}
		for i, n := range page1.Items {
			if n.ID != all[i].ID {
				t.Fatalf("limit %d page 1 row %d: %s, want %s", limit, i, n.ID, all[i].ID)
			}
		}

		// Legacy cursor: today's page, which skips the rest of the tied millisecond.
		legacyNext := legacyNodeRows(t, ctx, tx, legacyListNodesDet, none, lastMS, int32(limit)+1)
		gotLegacy := newNodeRows(t, ctx, s, none, lastMS, nil, int32(limit))
		if !sameRows(gotLegacy, legacyNext) {
			t.Fatalf("limit %d legacy cursor:\n got  %v\n want %v", limit, nodeKeys(gotLegacy), nodeKeys(legacyNext))
		}
		if len(gotLegacy) == 0 || gotLegacy[0].LastSeen.Time.UnixMilli() >= lastMS {
			t.Fatalf("limit %d legacy cursor did not skip the tied millisecond", limit)
		}

		// pageCursor (sent with the legacy cursor, which it overrides): the skipped tied rows, then the legacy page.
		pc, err := api.ParseNodeCursor(*page1.NextPageCursor)
		if err != nil {
			t.Fatal(err)
		}
		var want []uuid.UUID
		for _, r := range all[limit:] {
			if r.LastSeen.UnixMilli() != lastMS {
				break
			}
			want = append(want, r.ID)
		}
		if len(want) == 0 {
			t.Fatalf("limit %d: boundary is not inside a tied millisecond", limit)
		}
		for _, r := range legacyNext {
			want = append(want, r.ID)
		}
		want = want[:min(len(want), limit+1)]
		gotPC := newNodeRows(t, ctx, s, none, lastMS, pc, int32(limit))
		gotIDs := make([]uuid.UUID, len(gotPC))
		for i, r := range gotPC {
			gotIDs[i] = r.ID
		}
		if !slices.Equal(gotIDs, want) {
			t.Fatalf("limit %d pageCursor page:\n got  %v\n want %v", limit, gotIDs, want)
		}
	}

	// A limit-1 walk following nextPageCursor, with nextCursor sent alongside: every node once, in order.
	var walked []uuid.UUID
	var cursor int64
	var pageCursor *api.NodeCursor
	for range len(all) + 2 {
		page, err := s.ListNodes(ctx, 0, nil, nil, nil, nil, "", "", "", cursor, pageCursor, 1, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range page.Items {
			walked = append(walked, n.ID)
		}
		if !page.HasMore {
			if page.NextPageCursor != nil || page.NextCursor != nil {
				t.Fatal("last page carries a cursor")
			}
			break
		}
		cursor = *page.NextCursor
		if pageCursor, err = api.ParseNodeCursor(*page.NextPageCursor); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(walked, keysetIDs(all)) {
		t.Fatalf("limit-1 walk: %d rows, want %d in (last_seen DESC, id DESC) order", len(walked), len(all))
	}
}

// legacyObserverRow is today's ListObservers row.
type legacyObserverRow struct {
	ID           uuid.UUID
	DisplayName  *string
	ObserverType *string
	LastStatusAt pgtype.Timestamptz
	RadioFreqMhz *float32
	RadioSf      *int16
	RadioBwKhz   *float32
	Scopes       []string
	Status       string
	Iata         string
}

type observerShape struct {
	name                                       string
	iatas                                      []string
	observerType, broker, status, nameF, scope string
}

func observerShapes() []observerShape {
	return []observerShape{
		{name: "unfiltered"},
		{name: "iatas YVR", iatas: []string{"YVR"}},
		{name: "type repeater", observerType: "repeater"},
		{name: "status online", status: "online"},
		{name: "status offline", status: "offline"},
		{name: "broker b2", broker: "b2"},
		{name: "name obs-1", nameF: "obs-1"},
		{name: "scope #test", scope: "#test"},
	}
}

func legacyObserverRows(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, sh observerShape, cts pgtype.Timestamptz, pageLimit int32) []legacyObserverRow {
	t.Helper()
	return legacyRows[legacyObserverRow](t, ctx, tx, sql, sh.iatas, sh.observerType, sh.broker, sh.status, sh.nameF, cts, pageLimit, sh.scope)
}

// newObserverRows runs today's columns of the new statement for one request.
func newObserverRows(t *testing.T, ctx context.Context, s *Store, sh observerShape, cursor int64, pc *api.ObserverCursor, limit int32) []legacyObserverRow {
	t.Helper()
	rows, err := s.q.ListObservers(ctx, listObserversParams(sh.iatas, sh.observerType, sh.broker, sh.status, sh.nameF, sh.scope, cursor, pc, limit))
	if err != nil {
		t.Fatalf("%s: %v", sh.name, err)
	}
	out := make([]legacyObserverRow, len(rows))
	for i, r := range rows {
		out[i] = legacyObserverRow{r.ID, r.DisplayName, r.ObserverType, r.LastStatusAt, r.RadioFreqMhz, r.RadioSf, r.RadioBwKhz, r.Scopes, r.Status, r.Iata}
	}
	return out
}

func observerIDs(rows []legacyObserverRow) []uuid.UUID {
	out := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func TestObserversLegacyCursorMatchesTodayPostgres(t *testing.T) {
	ctx, tx, s := keysetTx(t)
	all := keysetOrder(t, ctx, tx, "SELECT id, last_seen, last_status_at FROM observers ORDER BY last_seen DESC, id DESC")
	lastSeen := make(map[uuid.UUID]time.Time, len(all))
	for _, r := range all {
		lastSeen[r.ID] = r.LastSeen
	}
	tied := tiedMillis(all)
	if len(tied) != 12 {
		t.Fatalf("fixture: %d tied milliseconds, want 12", len(tied))
	}
	cursors := append([]int64{0, -5, all[3].LastSeen.Add(-2500 * time.Millisecond).UnixMilli()}, tied...)
	nonEmpty := 0
	for _, setting := range planSettings {
		mustExec(t, ctx, tx, setting)
		for _, sh := range observerShapes() {
			for _, c := range cursors {
				for _, limit := range []int32{1, 2, 51} {
					want := legacyObserverRows(t, ctx, tx, legacyListObserversDet, sh, legacyCursorTS(c), limit+1)
					got := newObserverRows(t, ctx, s, sh, c, nil, limit)
					if !sameRows(got, want) {
						t.Fatalf("%q %s cursor=%d limit=%d:\n got  %v\n want %v", setting, sh.name, c, limit, observerIDs(got), observerIDs(want))
					}
					raw := legacyObserverRows(t, ctx, tx, legacyListObservers, sh, legacyCursorTS(c), limit+1)
					seen := func(id uuid.UUID) time.Time { return lastSeen[id] }
					if !slices.EqualFunc(observerIDs(got), observerIDs(raw), func(a, b uuid.UUID) bool { return seen(a).Equal(seen(b)) }) {
						t.Fatalf("%q %s cursor=%d limit=%d vs raw:\n got %v\n raw %v", setting, sh.name, c, limit, observerIDs(got), observerIDs(raw))
					}
					if len(got) > 0 {
						nonEmpty++
					}
				}
			}
		}
	}
	mustExec(t, ctx, tx, "RESET plan_cache_mode; RESET enable_seqscan")
	if nonEmpty < 500 {
		t.Fatalf("only %d non-empty comparisons; the fixture does not exercise the statement", nonEmpty)
	}
}

// The legacy nextCursor now follows the ORDER BY column and is present whenever hasMore.
func TestObserversNextCursorFromLastSeenPostgres(t *testing.T) {
	ctx, tx, s := keysetTx(t)
	all := keysetOrder(t, ctx, tx, "SELECT id, last_seen, last_status_at FROM observers ORDER BY last_seen DESC, id DESC")
	if len(all) != 120 {
		t.Fatalf("fixture: %d observers", len(all))
	}
	const limit = 7

	// Store walk following nextCursor: exactly the rows a "last_seen < ms" walk reaches; the only rows missed
	// share the millisecond of a page's last row.
	var walked, want []uuid.UUID
	var boundaries []int64
	var cursor int64
	for range len(all) + 2 {
		page, err := s.ListObservers(ctx, nil, "", "", "", "", "", cursor, nil, limit)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Items {
			walked = append(walked, o.ID)
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == nil {
			t.Fatal("hasMore without nextCursor")
		}
		cursor = *page.NextCursor
		boundaries = append(boundaries, cursor)
	}
	for next := int64(0); ; {
		var page []keysetRow
		for _, r := range all {
			if next == 0 || r.LastSeen.Before(time.UnixMilli(next)) {
				page = append(page, r)
			}
		}
		if len(page) <= limit {
			want = append(want, keysetIDs(page)...)
			break
		}
		want = append(want, keysetIDs(page[:limit])...)
		next = page[limit-1].LastSeen.UnixMilli()
	}
	if !slices.Equal(walked, want) {
		t.Fatalf("nextCursor walk:\n got  %v\n want %v", walked, want)
	}
	missed := 0
	for _, r := range all {
		if !slices.Contains(walked, r.ID) {
			missed++
			if !slices.Contains(boundaries, r.LastSeen.UnixMilli()) {
				t.Errorf("observer %s missed outside a boundary millisecond", r.ID)
			}
		}
	}
	if missed == 0 {
		t.Fatal("fixture: no page boundary falls inside a tied millisecond")
	}

	// The pre-change walk (nextCursor = last_status_at, absent when NULL) misses more than half of them.
	var before []uuid.UUID
	var cts pgtype.Timestamptz
	for range len(all) + 2 {
		rows := legacyObserverRows(t, ctx, tx, legacyListObservers, observerShape{}, cts, limit+1)
		hasMore := len(rows) > limit
		if hasMore {
			rows = rows[:limit]
		}
		before = append(before, observerIDs(rows)...)
		if !hasMore || !rows[len(rows)-1].LastStatusAt.Valid {
			break
		}
		cts = pgtype.Timestamptz{Time: time.UnixMilli(rows[len(rows)-1].LastStatusAt.Time.UnixMilli()), Valid: true}
	}
	if len(all)-len(before) <= len(all)/2 {
		t.Fatalf("pre-change walk reached %d of %d observers; the fixture does not show the loss", len(before), len(all))
	}

	// A page whose last row has NULL last_status_at: hasMore with nextCursor from last_seen (was nil).
	k := slices.IndexFunc(all, func(r keysetRow) bool { return !r.LastStatusAt.Valid })
	if k < 0 || k+1 >= len(all) {
		t.Fatal("fixture: no observer with NULL last_status_at before the last row")
	}
	page, err := s.ListObservers(ctx, nil, "", "", "", "", "", 0, nil, int32(k+1))
	if err != nil {
		t.Fatal(err)
	}
	wantPC := (api.ObserverCursor{LastSeen: all[k].LastSeen, ID: all[k].ID}).String()
	if !page.HasMore || page.NextCursor == nil || *page.NextCursor != all[k].LastSeen.UnixMilli() ||
		page.NextPageCursor == nil || *page.NextPageCursor != wantPC {
		t.Fatalf("page ending on NULL last_status_at: hasMore=%t nextCursor=%v nextPageCursor=%v", page.HasMore, page.NextCursor, page.NextPageCursor)
	}

	// A limit-1 pageCursor walk: all 120 once, in (last_seen DESC, id DESC) order.
	var pcWalk []uuid.UUID
	var pc *api.ObserverCursor
	for range len(all) + 2 {
		page, err := s.ListObservers(ctx, nil, "", "", "", "", "", 0, pc, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Items {
			pcWalk = append(pcWalk, o.ID)
		}
		if !page.HasMore {
			break
		}
		if pc, err = api.ParseObserverCursor(*page.NextPageCursor); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(pcWalk, keysetIDs(all)) {
		t.Fatalf("limit-1 pageCursor walk: %d rows, want %d in order", len(pcWalk), len(all))
	}
}
