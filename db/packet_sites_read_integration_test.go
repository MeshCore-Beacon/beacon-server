// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// siteFixtureSites are the sites of the region fixture; ZZZ is a known code with no rows and NEW an unanalyzed site.
var siteFixtureSites = []string{"AAA", "BBB", "CCC", "DDD"}

// siteFixture is psFixture with the region fixture: 900 packets (payload type (i*7)%16, route type (i*3)%4, scope
// NULL for i%4 = 0 else #a or #b, first heard over 6 h) heard at 1-4 sites by 1-3 observers each over 6 h, written
// through the store's writer; 40 groups of three packets whose newest hearing at AAA shares a millisecond and 12
// groups that share a microsecond; 10 packets heard by two observers at one site in the same microsecond (usTie);
// ANALYZE; then 30 packets at NEW that the statistics never see.
type siteFixture struct {
	*psFixture
	usTie map[string]bool // hex hashes of the packets with two newest hearings at one site in one microsecond
}

func (f *psFixture) sitePacket(i int, first time.Time) []byte {
	f.t.Helper()
	var scope *int32
	if i%4 != 0 {
		v := int32(1 + i%2)
		scope = &v
	}
	h := psHash(10_000 + i)
	f.exec(`INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, scope_id, raw_payload, raw_header, first_heard_at, last_heard_at, observation_count, parsed_payload)
VALUES ($1, $2, 0, $3, $4, '\x00', '\x11', $5, $5, 0, jsonb_build_object('appData', jsonb_build_object('name', 'n' || $6::text)))`,
		h, int16(i*7%16), int16(i*3%4), scope, first, fmt.Sprint(i))
	return h
}

func newSiteFixture(t *testing.T) *siteFixture {
	t.Helper()
	f := &siteFixture{psFixture: newPsFixture(t), usTie: map[string]bool{}}
	rng := rand.New(rand.NewPCG(3, 5))
	span := int64(6 * time.Hour / time.Microsecond)
	for i := range 900 {
		first := psT0.Add(time.Duration(rng.Int64N(span)) * time.Microsecond)
		h := f.sitePacket(i, first)
		sites := rng.Perm(len(siteFixtureSites))[:1+rng.IntN(len(siteFixtureSites))]
		if i >= 100 && i < 280 && !slices.Contains(sites, 0) {
			sites = append(sites, 0) // the tie groups are at AAA
		}
		for _, s := range sites {
			base := psT0.Add(time.Duration(rng.Int64N(span)) * time.Microsecond)
			switch {
			case i >= 100 && i < 136: // 12 groups of three in one microsecond
				base = psT0.Add(time.Hour + time.Duration((i-100)/3)*time.Minute)
			case i >= 136 && i < 256: // 40 groups of three in one millisecond
				base = psT0.Add(2*time.Hour + time.Duration((i-136)/3)*time.Minute + time.Duration(1+(i-136)%3)*time.Microsecond)
			}
			if siteFixtureSites[s] != "AAA" && i >= 100 && i < 256 {
				base = base.Add(-7 * time.Hour) // keep the group's key at AAA
			}
			n := 1 + rng.IntN(3)
			if i >= 500 && i < 510 && s == sites[0] {
				n = 2
				f.usTie[hex.EncodeToString(h)] = true
			}
			for k := range n {
				at := base
				if k > 0 && !(i >= 500 && i < 510 && s == sites[0]) {
					at = base.Add(-time.Duration(rng.Int64N(int64(5*time.Second/time.Microsecond))) * time.Microsecond)
				}
				f.sited(psObs(h, 1+s*3+k, siteFixtureSites[s], at))
			}
		}
	}
	f.exec("ANALYZE")
	f.exec("ALTER TABLE packet_sites SET (autovacuum_enabled = false)")
	for i := 900; i < 930; i++ {
		h := f.sitePacket(i, psT0.Add(time.Duration(i)*time.Second))
		f.sited(psObs(h, 13, "NEW", psT0.Add(5*time.Hour+time.Duration(i)*time.Second)))
	}
	return f
}

// siteReq is one region request.
type siteReq struct {
	sites        []string
	pt, rt       []int16
	scopes       []string
	since, until time.Time
}

func (r siteReq) String() string {
	return fmt.Sprintf("sites %v pt %v rt %v scopes %v since %v until %v", r.sites, r.pt, r.rt, r.scopes, !r.since.IsZero(), !r.until.IsZero())
}

// key is a page key.
type key struct {
	hash string
	at   time.Time
}

// truthSitesPage reads page keys straight from the observations, independent of packet_sites.
const truthSitesPage = `SELECT po.packet_hash, max(po.heard_at) AS k
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE po.iata = ANY($1::bpchar[]) AND p.payload_type = ANY($2::smallint[]) AND p.route_type = ANY($3::smallint[])
  AND p.first_heard_at >= $4::timestamptz AND p.first_heard_at <= $5::timestamptz
  AND ($6::text[] IS NULL OR ts.name = ANY($6::text[]))
GROUP BY po.packet_hash
HAVING (max(po.heard_at), po.packet_hash) < ($7::timestamptz, $8::bytea)
ORDER BY 2 DESC, 1 DESC
LIMIT $9`

func (f *siteFixture) truth(q sqlc.DBTX, r siteReq, at time.Time, hash []byte, limit int) []key {
	f.t.Helper()
	fl, ok := newPacketFilter(r.pt, r.rt, r.scopes, r.since, r.until, 0, nil, 1)
	if !ok {
		return nil
	}
	var scopes []string
	if len(r.scopes) > 0 {
		scopes = r.scopes
	}
	var cursor any = "infinity"
	if !at.IsZero() {
		cursor = at
	}
	if hash == nil {
		hash = []byte{}
	}
	rows, err := q.Query(f.ctx, truthSitesPage, r.sites, fl.payloadTypes, fl.routeTypes, fl.since, fl.until, scopes, cursor, hash, limit)
	if err != nil {
		f.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (key, error) {
		var h []byte
		var k time.Time
		err := row.Scan(&h, &k)
		return key{hex.EncodeToString(h), k}, err
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

func (f *siteFixture) page(s *Store, r siteReq, cursor int64, pc *api.PacketCursor, limit int32) (api.PacketPage, []key) {
	f.t.Helper()
	page, err := s.ListPackets(f.ctx, r.pt, r.rt, r.sites, r.scopes, r.since, r.until, cursor, pc, limit)
	if err != nil {
		f.t.Fatalf("%v: %v", r, err)
	}
	if page.Items == nil {
		f.t.Fatalf("%v: nil items", r)
	}
	// Keys come from the site-local cursor of each row: re-read them from packet_sites' definition.
	keys := make([]key, len(page.Items))
	for i, it := range page.Items {
		keys[i] = key{hash: it.PacketHash}
	}
	return page, keys
}

// walk pages through r by pageCursor and checks each page against the truth for its cursor: the page is a prefix
// of the truth page, full unless it ends at a floor, and the concatenation equals the truth with no gap or duplicate.
func (f *siteFixture) walk(s *Store, q sqlc.DBTX, r siteReq, limit int32) int {
	f.t.Helper()
	all := f.truth(q, r, time.Time{}, nil, 100_000)
	var got []string
	var pc *api.PacketCursor
	var at time.Time
	var hash []byte
	for n := 0; ; n++ {
		if n > 5000 {
			f.t.Fatalf("%v limit %d: walk does not end", r, limit)
		}
		page, _ := f.page(s, r, 0, pc, limit)
		want := f.truth(q, r, at, hash, int(limit))
		if len(page.Items) > len(want) {
			f.t.Fatalf("%v limit %d page %d: %d rows, truth %d", r, limit, n, len(page.Items), len(want))
		}
		for i, it := range page.Items {
			if it.PacketHash != want[i].hash {
				f.t.Fatalf("%v limit %d page %d row %d: %s, truth %s", r, limit, n, i, it.PacketHash, want[i].hash)
			}
			got = append(got, it.PacketHash)
		}
		if len(page.Items) < int(limit) && len(page.Items) < len(want) && !page.HasMore {
			f.t.Fatalf("%v limit %d page %d: short page without more", r, limit, n)
		}
		if !page.HasMore {
			break
		}
		next, err := api.ParsePacketCursor(*page.NextPageCursor)
		if err != nil {
			f.t.Fatal(err)
		}
		// The next page starts at the last row, or below it at a floor of a bounded read.
		if len(page.Items) > 0 {
			last := want[len(page.Items)-1]
			lastHash, _ := hex.DecodeString(last.hash)
			if next.At.After(last.at) || next.At.Equal(last.at) && bytes.Compare(next.Hash, lastHash) > 0 {
				f.t.Fatalf("%v limit %d page %d: next cursor %v above the last row %v", r, limit, n, next, last)
			}
		}
		pc, at, hash = next, next.At, next.Hash
	}
	if len(got) != len(all) {
		f.t.Fatalf("%v limit %d: walk returned %d rows, truth %d", r, limit, len(got), len(all))
	}
	for i := range all {
		if got[i] != all[i].hash {
			f.t.Fatalf("%v limit %d: walk row %d %s, truth %s", r, limit, i, got[i], all[i].hash)
		}
	}
	return len(got)
}

// legacyWalk pages through r by the millisecond cursor alone: every page equals the truth below that millisecond
// (today's skip of rows tied with the cursor's millisecond), as a prefix when the page ends at a floor.
func (f *siteFixture) legacyWalk(s *Store, q sqlc.DBTX, r siteReq, limit int32) {
	f.t.Helper()
	var cursor int64
	for n := 0; ; n++ {
		if n > 5000 {
			f.t.Fatalf("%v: legacy walk does not end", r)
		}
		page, _ := f.page(s, r, cursor, nil, limit)
		var at time.Time
		if cursor > 0 {
			at = time.UnixMilli(cursor)
		}
		want := f.truth(q, r, at, nil, int(limit))
		if len(page.Items) > len(want) {
			f.t.Fatalf("%v legacy page %d: %d rows, truth %d", r, n, len(page.Items), len(want))
		}
		for i, it := range page.Items {
			if it.PacketHash != want[i].hash {
				f.t.Fatalf("%v legacy page %d row %d: %s, truth %s", r, n, i, it.PacketHash, want[i].hash)
			}
		}
		if !page.HasMore {
			return
		}
		cursor = *page.NextCursor
	}
}

func (f *siteFixture) trusted(db sqlc.DBTX) *Store {
	return New(db, nil, 0, 0, WithReadModels(earnedPacketSites()))
}

// Every dispatch shape against the truth, by pageCursor and by the millisecond cursor, in both plan modes.
func TestPacketsBySitesMatchesTruthPostgres(t *testing.T) {
	f := newSiteFixture(t)
	since, until := psT0.Add(2*time.Hour), psT0.Add(4*time.Hour)
	web := []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	type types struct{ pt, rt []int16 }
	// {""} and {ZZZ} are in the bogus-sites test; the window "until only" shares the since statement.
	siteSets := [][]string{{"AAA"}, {"AAA", "BBB"}, {"AAA", "BBB", "CCC", "DDD"}, {"AAA", "NEW"}, {"NEW"}}
	typeSets := []types{{}, {pt: []int16{4}}, {pt: []int16{4, 4}, rt: []int16{0, 0, 1}}, {rt: []int16{3}}, {pt: web}}
	scopeSets := [][]string{nil, {"#a"}, {"#a", "#b", "#none"}}
	windows := [][2]time.Time{{}, {since, until}}
	rows := 0
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		// A failed check must not leave the connection checked out: the pool's cleanup would wait for it.
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		if _, err := tx.Exec(f.ctx, "SET LOCAL plan_cache_mode = "+mode); err != nil {
			t.Fatal(err)
		}
		s := f.trusted(tx)
		for _, sites := range siteSets {
			for _, ty := range typeSets {
				for _, sc := range scopeSets {
					for _, w := range windows {
						r := siteReq{sites: sites, pt: ty.pt, rt: ty.rt, scopes: sc, since: w[0], until: w[1]}
						rows += f.walk(s, tx, r, 50)
					}
				}
			}
		}
		// Deep walks at limits 1 and 3 over the multi-site lists, and the millisecond cursor.
		for _, r := range []siteReq{
			{sites: siteSets[1]}, {sites: siteSets[2], until: until}, {sites: siteSets[1], pt: []int16{4}},
			{sites: siteSets[2], scopes: []string{"#a"}, since: since},
		} {
			rows += f.walk(s, tx, r, 3)
			f.legacyWalk(s, tx, r, 50)
		}
		rows += f.walk(s, tx, siteReq{sites: siteSets[1], since: since}, 1)
		rows += f.walk(s, tx, siteReq{sites: siteSets[2], until: until, pt: []int16{4}}, 200)
		_ = tx.Rollback(f.ctx)
	}
	if rows < 10_000 {
		t.Fatalf("walks returned %d rows in all; the fixture is too thin", rows)
	}
	t.Logf("%d rows walked", rows)
}

// latestObserverTruth is the observation of the packet with the newest hearing, ties broken by the highest id.
func (f *siteFixture) latestObserverTruth(hash string) string {
	f.t.Helper()
	var id string
	h, _ := hex.DecodeString(hash)
	if err := f.pool.QueryRow(f.ctx, "SELECT observer_id::text FROM packet_observations WHERE packet_hash = $1 ORDER BY heard_at DESC, id DESC LIMIT 1", h).Scan(&id); err != nil {
		f.t.Fatal(err)
	}
	return id
}

// Rows tied at a cursor boundary: the millisecond cursor skips ties as today; the page cursor returns each row once.
func TestPacketsBySitesCursorTiesPostgres(t *testing.T) {
	f := newSiteFixture(t)
	s := f.trusted(f.pool)
	T := psT0.Add(9*time.Hour + 123456*time.Microsecond)
	// Three packets at AAA in one microsecond.
	var tied []string
	for i := range 3 {
		h := f.sitePacket(2000+i, psT0)
		f.sited(psObs(h, 1, "AAA", T))
		tied = append(tied, hex.EncodeToString(h))
	}
	slices.Sort(tied)
	slices.Reverse(tied) // page order: packet_hash DESC
	r := siteReq{sites: []string{"AAA"}}
	p1, _ := f.page(s, r, 0, nil, 2)
	if len(p1.Items) != 2 || p1.Items[0].PacketHash != tied[0] || p1.Items[1].PacketHash != tied[1] {
		t.Fatalf("page 1 %v", p1.Items)
	}
	legacy, _ := f.page(s, r, *p1.NextCursor, nil, 2)
	for _, it := range legacy.Items {
		if it.PacketHash == tied[2] {
			t.Fatalf("the millisecond cursor returned the row tied with it: %v", legacy.Items)
		}
	}
	pc, err := api.ParsePacketCursor(*p1.NextPageCursor)
	if err != nil {
		t.Fatal(err)
	}
	exact, _ := f.page(s, r, 0, pc, 2)
	if len(exact.Items) == 0 || exact.Items[0].PacketHash != tied[2] {
		t.Fatalf("pageCursor page 2 starts with %v, want %s", exact.Items, tied[2])
	}

	// Q heard at AAA at T and at BBB 5 ms earlier.
	q := f.sitePacket(2100, psT0)
	f.sited(psObs(q, 1, "AAA", T.Add(time.Second)), psObs(q, 4, "BBB", T.Add(time.Second-5*time.Millisecond)))
	qHex := hex.EncodeToString(q)
	tq := T.Add(time.Second)
	two := siteReq{sites: []string{"AAA", "BBB"}}
	has := func(p api.PacketPage) int {
		n := 0
		for _, it := range p.Items {
			if it.PacketHash == qHex {
				n++
			}
		}
		return n
	}
	for _, c := range []*api.PacketCursor{{At: tq, Hash: q}, {At: tq.Add(-5 * time.Millisecond), Hash: q}} {
		if p, _ := f.page(s, two, 0, c, 50); has(p) != 0 {
			t.Fatalf("cursor %v: Q repeated", c)
		}
	}
	above := append(bytes.Clone(q), 0)
	if p, _ := f.page(s, two, 0, &api.PacketCursor{At: tq, Hash: above}, 50); len(p.Items) == 0 || p.Items[0].PacketHash != qHex || has(p) != 1 {
		t.Fatalf("cursor just above Q: %v", p.Items)
	}
	for _, sites := range [][]string{{"AAA", "BBB"}, {"AAA", "BBB", "CCC", "DDD"}} {
		f.walk(s, f.pool, siteReq{sites: sites}, 1)
	}
}

// bogusSites returns n three-letter codes that are neither known codes nor sites.
func psBogusSites(n int) []string {
	out := make([]string, 0, n)
	for a := 'Q'; a <= 'Y' && len(out) < n; a++ {
		for b := 'A'; b <= 'Z' && len(out) < n; b++ {
			for c := 'A'; c <= 'Z' && len(out) < n; c++ {
				out = append(out, string([]rune{a, b, c}))
			}
		}
	}
	return out
}

// argRecorder records the sqlc name and the arguments of every statement.
type argRecorder struct {
	sqlc.DBTX
	names []string
	args  [][]any
}

func (r *argRecorder) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.names, r.args = append(r.names, sqlcName(sql)), append(r.args, args)
	return r.DBTX.Exec(ctx, sql, args...)
}

func (r *argRecorder) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.names, r.args = append(r.names, sqlcName(sql)), append(r.args, args)
	return r.DBTX.Query(ctx, sql, args...)
}

func (r *argRecorder) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	r.names, r.args = append(r.names, sqlcName(sql)), append(r.args, args)
	return r.DBTX.QueryRow(ctx, sql, args...)
}

func TestPacketsBySitesManyBogusSitesPostgres(t *testing.T) {
	f := newSiteFixture(t)
	bogus := psBogusSites(5000)
	shapes := []siteReq{{}, {pt: []int16{4}}, {since: psT0.Add(5 * time.Hour)}, {scopes: []string{"#a"}}}
	for i, shape := range shapes {
		rec := &argRecorder{DBTX: f.pool}
		s := f.trusted(rec)
		r := shape
		r.sites = bogus
		page, _ := f.page(s, r, 0, nil, 50)
		js, _ := json.Marshal(page)
		if string(js) != `{"items":[],"hasMore":false}` {
			t.Fatalf("shape %d bogus only: %s", i, js)
		}
		if i > 0 && fmt.Sprint(rec.names) != "[FilterKnownSites]" {
			t.Fatalf("shape %d bogus only ran %v", i, rec.names)
		}
		rec.names, rec.args = nil, nil
		r.sites = append(slices.Clone(bogus), "AAA")
		f.walk(s, f.pool, r, 50)
		if i > 0 {
			for k, name := range rec.names {
				if !strings.HasPrefix(name, "ListPacketsBySites") {
					continue
				}
				if sites, _ := rec.args[k][0].([]string); !slices.Equal(sites, []string{"AAA"}) {
					t.Fatalf("shape %d: %s got sites %v", i, name, rec.args[k][0])
				}
			}
		}
		r.sites = append(slices.Clone(bogus), siteFixtureSites...)
		f.walk(s, f.pool, r, 50)
	}

	// The pre-filter probes each code once through an index.
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // read-only
	if _, err := tx.Exec(f.ctx, "SET LOCAL enable_seqscan = off; SET LOCAL jit = off"); err != nil {
		t.Fatal(err)
	}
	var plan []byte
	if err := tx.QueryRow(f.ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+filterKnownSitesSQL(), bogus).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plan), "Seq Scan") {
		t.Fatalf("FilterKnownSites seq scans: %s", plan)
	}
	var parsed []struct {
		Plan struct {
			Hit  int64 `json:"Shared Hit Blocks"`
			Read int64 `json:"Shared Read Blocks"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal(plan, &parsed); err != nil || parsed[0].Plan.Hit+parsed[0].Plan.Read > 25_000 {
		t.Fatalf("FilterKnownSites buffers %+v, %v", parsed, err)
	}

	s := f.trusted(f.pool)
	for _, r := range []siteReq{{sites: []string{"ZZZ"}}, {sites: []string{""}}, {sites: []string{"AAA"}, scopes: []string{"#none"}}, {sites: []string{"AAA"}, scopes: []string{"#unknown"}}} {
		if page, _ := f.page(s, r, 0, nil, 50); len(page.Items) != 0 || page.HasMore {
			t.Fatalf("%v: %+v", r, page)
		}
	}
}

// filterKnownSitesSQL is the generated text of FilterKnownSites.
func filterKnownSitesSQL() string {
	var text string
	rec := &textRecorder{text: &text}
	_, _ = sqlc.New(rec).FilterKnownSites(context.Background(), nil)
	return text
}

type textRecorder struct {
	sqlc.DBTX
	text *string
}

func (r *textRecorder) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	*r.text = sql
	return errRow{}
}

type errRow struct{}

func (errRow) Scan(...any) error { return pgx.ErrNoRows }

// A site whose packets are each heard by many observers: today's page collapses short at its scan depth; the site
// read model fills the page, and today's rows are its prefix.
func TestPacketsBySitesSaturatedPagesFullPostgres(t *testing.T) {
	f := newPsFixture(t)
	for i := range 12 {
		h := f.psPacket(i)
		for o := 1; o <= 20; o++ {
			f.sited(psObs(h, o, "EEE", psT0.Add(time.Duration(i)*time.Minute+time.Duration(o)*time.Millisecond)))
		}
	}
	legacy := New(f.pool, nil, 0, 0)
	trusted := New(f.pool, nil, 0, 0, WithReadModels(earnedPacketSites()))
	lp, err := legacy.ListPackets(f.ctx, nil, nil, []string{"EEE"}, nil, time.Time{}, time.Time{}, 0, nil, 3)
	if err != nil || len(lp.Items) != 2 || !lp.HasMore {
		t.Fatalf("legacy page %+v, %v", lp, err)
	}
	tp, err := trusted.ListPackets(f.ctx, nil, nil, []string{"EEE"}, nil, time.Time{}, time.Time{}, 0, nil, 3)
	if err != nil || len(tp.Items) != 3 || !tp.HasMore {
		t.Fatalf("trusted page %+v, %v", tp, err)
	}
	for i, it := range lp.Items {
		if it.PacketHash != tp.Items[i].PacketHash {
			t.Fatalf("legacy row %d %s is not a prefix of %v", i, it.PacketHash, tp.Items)
		}
	}
	third := psT0.Add(9*time.Minute + 20*time.Millisecond)
	if *tp.NextCursor != third.UnixMilli() {
		t.Fatalf("NextCursor %d, want %d", *tp.NextCursor, third.UnixMilli())
	}
	last, err := trusted.ListPackets(f.ctx, nil, nil, []string{"EEE"}, nil, time.Time{}, time.Time{}, 0, &api.PacketCursor{At: psT0.Add(2*time.Minute + 20*time.Millisecond), Hash: psHash(2)}, 3)
	if err != nil || len(last.Items) != 2 || last.HasMore {
		t.Fatalf("last page %+v, %v", last, err)
	}
}

// Unsaturated inputs: the read model's page equals today's except tie order and the latest observer on tied newest
// hearings.
func TestPacketsBySitesMatchesLegacyPostgres(t *testing.T) {
	f := newSiteFixture(t)
	legacy := New(f.pool, nil, 0, 0)
	trusted := f.trusted(f.pool)
	since := psT0.Add(2 * time.Hour)
	compared := 0
	for _, r := range []siteReq{
		{sites: []string{"AAA"}}, {sites: []string{"AAA", "BBB", "CCC", "DDD"}}, {sites: []string{"AAA", "NEW"}},
		{sites: []string{"AAA", "BBB"}, pt: []int16{4}}, {sites: []string{"BBB"}, since: since},
		{sites: []string{"AAA", "CCC"}, scopes: []string{"#a"}}, {sites: []string{"AAA", "BBB"}, rt: []int16{1}, since: since},
	} {
		for _, cursor := range []int64{0, psT0.Add(3 * time.Hour).UnixMilli()} {
			// The limit covers every observation, so today's scan never saturates.
			lp, err := legacy.ListPackets(f.ctx, r.pt, r.rt, r.sites, r.scopes, r.since, r.until, cursor, nil, 5000)
			if err != nil {
				t.Fatal(err)
			}
			tp, err := trusted.ListPackets(f.ctx, r.pt, r.rt, r.sites, r.scopes, r.since, r.until, cursor, nil, 5000)
			if err != nil {
				t.Fatal(err)
			}
			if len(lp.Items) != len(tp.Items) || lp.HasMore || tp.HasMore {
				t.Fatalf("%v: legacy %d rows (more %v), read model %d (more %v)", r, len(lp.Items), lp.HasMore, len(tp.Items), tp.HasMore)
			}
			keys := func(p api.PacketPage) map[string]api.PacketSummary {
				m := map[string]api.PacketSummary{}
				for _, it := range p.Items {
					m[it.PacketHash] = it
				}
				return m
			}
			lk := keys(lp)
			for _, it := range tp.Items {
				l, ok := lk[it.PacketHash]
				if !ok {
					t.Fatalf("%v: %s not on today's page", r, it.PacketHash)
				}
				if f.usTie[it.PacketHash] {
					if it.LatestObserver.ID.String() != f.latestObserverTruth(it.PacketHash) {
						t.Fatalf("%v: %s latest observer %s, want the highest observation id's", r, it.PacketHash, it.LatestObserver.ID)
					}
					l.LatestObserver, it.LatestObserver = nil, nil
				}
				lj, _ := json.Marshal(l)
				tj, _ := json.Marshal(it)
				if string(lj) != string(tj) {
					t.Fatalf("%v: %s differs:\n today %s\n model %s", r, it.PacketHash, lj, tj)
				}
				compared++
			}
		}
	}
	if compared < 1000 {
		t.Fatalf("compared %d rows", compared)
	}
}

// Trust decides the statement at every step; a walk that crosses a trust drop goes on from the same place.
func TestPacketSitesTrustLifecyclePostgres(t *testing.T) {
	f := newSiteFixture(t)
	rec := &recordingDBTX{DBTX: f.pool}
	tr := NewTrust("packet_sites_lifecycle_test", true, PacketSitesTrustPolicy)
	s := New(rec, nil, 0, 0, WithReadModels(&ReadModels{PacketSites: tr}))
	step := func(label, want string) {
		t.Helper()
		rec.names = nil
		if _, err := s.ListPackets(f.ctx, nil, nil, []string{"AAA"}, nil, time.Time{}, time.Time{}, 0, nil, 10); err != nil {
			t.Fatal(err)
		}
		if len(rec.names) == 0 || rec.names[0] != want {
			t.Fatalf("%s: ran %v, want %s", label, rec.names, want)
		}
	}
	run := func(n int64) {
		gen := tr.Generation()
		tr.ObserveRun(n)
		tr.Repaired(gen)
	}
	step("not earned", "ListPacketsByIATAs")
	tr.Earn()
	step("earned", "ListPacketsBySite")
	tr.Invalidate("test")
	step("invalidated", "ListPacketsByIATAs")
	run(0)
	step("a clean run after it", "ListPacketsBySite")
	run(1)
	step("a repairing run", "ListPacketsByIATAs")
	run(0)
	step("one clean run", "ListPacketsByIATAs")
	run(0)
	step("two clean runs", "ListPacketsBySite")
	disabled := NewTrust("packet_sites_lifecycle_disabled", false, PacketSitesTrustPolicy)
	disabled.Earn()
	if p, err := New(rec, nil, 0, 0, WithReadModels(&ReadModels{PacketSites: disabled})).ListPackets(f.ctx, nil, nil, []string{"AAA"}, nil, time.Time{}, time.Time{}, 0, nil, 10); err != nil || p.NextPageCursor != nil {
		t.Fatalf("disabled: %+v, %v", p, err)
	}

	// A walk: page 1 trusted, then trust drops; page 2 by both cursors or the page cursor alone is today's page.
	r := siteReq{sites: []string{"AAA", "BBB"}}
	p1, _ := f.page(s, r, 0, nil, 20)
	if p1.NextCursor == nil || p1.NextPageCursor == nil {
		t.Fatalf("page 1 cursors %+v", p1)
	}
	tr.Invalidate("test")
	pc, _ := api.ParsePacketCursor(*p1.NextPageCursor)
	want, err := New(f.pool, nil, 0, 0).ListPackets(f.ctx, nil, nil, r.sites, nil, time.Time{}, time.Time{}, *p1.NextCursor, nil, 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []int64{*p1.NextCursor, 0} {
		got, _ := f.page(s, r, c, pc, 20)
		gj, _ := json.Marshal(got)
		wj, _ := json.Marshal(want)
		if string(gj) != string(wj) || got.NextPageCursor != nil {
			t.Fatalf("cursor %d: page 2 after the drop differs from today's:\n%s\n%s", c, gj, wj)
		}
	}
}

// A window, or many type pairs, reads each site range at most scan depth entries deep, however rare the match:
// a window in the future returns empty pages with more, from a floor that moves down by the scan depth.
func TestPacketsBySitesBoundedPostgres(t *testing.T) {
	f := newSiteFixture(t)
	s := f.trusted(f.pool)
	var aaa int
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM packet_sites WHERE iata = 'AAA'").Scan(&aaa); err != nil {
		t.Fatal(err)
	}
	future := siteReq{sites: []string{"AAA", "BBB"}, since: time.Now().Add(24 * time.Hour)}
	const limit = 5
	depth := (limit + 1) * siteScanDepthPerRow
	pages := 0
	for page, pc := (api.PacketPage{}), (*api.PacketCursor)(nil); ; pages++ {
		page, _ = f.page(s, future, 0, pc, limit)
		if len(page.Items) != 0 {
			t.Fatalf("future window returned rows")
		}
		if !page.HasMore {
			break
		}
		pc, _ = api.ParsePacketCursor(*page.NextPageCursor)
		if pages > aaa {
			t.Fatal("walk does not end")
		}
	}
	if pages < aaa/depth || pages > aaa/depth+2 {
		t.Fatalf("%d pages for %d rows at AAA, depth %d", pages, aaa, depth)
	}
	// Each statement reads at most depth entries per site range.
	fl, _ := newPacketFilter(nil, []int16{3}, nil, future.since, time.Time{}, 0, nil, limit)
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{listPacketsBySitesSinceSQL(), []any{future.sites, fl.cursorTS, fl.cursorHash, int32(depth), fl.since, fl.until, fl.payloadTypes, fl.routeTypes, false, fl.pageLimit}},
	} {
		var plan []byte
		if err := f.pool.QueryRow(f.ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+stmt.sql, stmt.args...).Scan(&plan); err != nil {
			t.Fatal(err)
		}
		var scanned func(any) int64
		scanned = func(n any) int64 {
			var total int64
			switch v := n.(type) {
			case map[string]any:
				if v["Relation Name"] == "packet_sites" {
					rows, _ := v["Actual Rows"].(float64)
					loops, _ := v["Actual Loops"].(float64)
					total += int64(rows * loops)
				}
				for _, c := range v {
					total += scanned(c)
				}
			case []any:
				for _, c := range v {
					total += scanned(c)
				}
			}
			return total
		}
		var doc any
		_ = json.Unmarshal(plan, &doc)
		if n := scanned(doc); n > int64(depth*len(future.sites)) {
			t.Fatalf("packet_sites rows read %d > %d", n, depth*len(future.sites))
		}
	}
}

func listPacketsBySitesSinceSQL() string {
	var text string
	_, _ = sqlc.New(&queryRecorder{text: &text}).ListPacketsBySitesSince(context.Background(), sqlc.ListPacketsBySitesSinceParams{})
	return text
}

type queryRecorder struct {
	sqlc.DBTX
	text *string
}

func (r *queryRecorder) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	*r.text = sql
	return nil, pgx.ErrNoRows
}
