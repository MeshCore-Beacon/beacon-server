// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var cmiSites = []string{"YUL", "YOW", "YYZ", "YVR", "YQB", "YHZ"}

// cmiMessages is the dispatch fixture: messages on channels 7, 8 and 9, written through the real writers (the
// seeded insert after the first hearings, an Add for each later hearing).
type cmiMessages struct {
	ids     []int64   // every stored message, newest first
	tiedAt  time.Time // a sent_at several messages share
	channel map[int64]int32
}

// buildCmiMessages fills f with 120 messages: multi-site packets, repeat hearings at one site, untyped hearings,
// a #fr share, equal sent_at on every tenth message, three whose sender clocks put them in 2099 or 1970, and one
// message whose packet has no observation.
func buildCmiMessages(f *cmiFixture) cmiMessages {
	f.t.Helper()
	f.exec("INSERT INTO iata_codes (iata) VALUES ('YHZ') ON CONFLICT DO NOTHING")
	observers := map[string][]uuid.UUID{}
	for _, site := range cmiSites {
		for n := range 2 {
			id := uuid.New()
			f.exec("INSERT INTO observers (id, public_key) VALUES ($1, $2)", id, []byte(fmt.Sprintf("%s-%d", site, n)))
			observers[site] = append(observers[site], id)
		}
	}
	r := rand.New(rand.NewPCG(4, 2))
	base := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	out := cmiMessages{tiedAt: base.Add(-30 * time.Minute), channel: map[int64]int32{}}
	scope := int32(3)
	for i := range 120 {
		hash := []byte(fmt.Sprintf("msg-%03d", i))
		params := ingest.UpsertPacketParams{PacketHash: hash, PayloadType: 5, RouteType: 1, RawHeader: []byte{0x15}, RawPayload: []byte{byte(i)}}
		if i%4 == 0 {
			params.ScopeID = &scope
			params.RouteType = 0
			params.TransportCodes = []byte{1, 2, 3, 4}
		}
		if _, err := f.store.UpsertPacket(f.ctx, params); err != nil {
			f.t.Fatal(err)
		}
		channel := []int32{7, 7, 7, 8, 8, 9}[r.IntN(6)]
		sites := slices.Clone(cmiSites)
		r.Shuffle(len(sites), func(a, b int) { sites[a], sites[b] = sites[b], sites[a] })
		sites = sites[:1+r.IntN(3)]
		if i == 77 {
			sites = nil // a message whose packet was never stored as heard
		}
		hear := func(site string, n int) {
			pt := int16(5)
			if r.IntN(10) == 0 {
				pt = 0 // stored with no payload type
			}
			o := ingest.InsertObservationParams{PacketHash: hash, ObserverID: observers[site][n], IATA: site, HeardAt: base, HashSize: 1, PayloadType: pt}
			if _, _, err := f.store.InsertObservation(f.ctx, o); err != nil {
				f.t.Fatal(err)
			}
			if pt == 0 {
				f.exec("UPDATE packet_observations SET payload_type = NULL WHERE packet_hash = $1 AND observer_id = $2", hash, observers[site][n])
			}
		}
		first := 0
		if len(sites) > 0 {
			first = 1 + r.IntN(len(sites))
		}
		for _, site := range sites[:first] {
			hear(site, 0)
		}
		sent := base.Add(-time.Duration(i) * time.Minute)
		switch {
		case i%10 == 0:
			sent = out.tiedAt
		case i == 33 || i == 97:
			sent = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC) // an old id sent "after" every since
		case i == 61:
			sent = time.Date(1970, 1, 1, 0, 0, 3, 0, time.UTC)
		}
		m, err := f.store.InsertChannelMessage(f.ctx, ingest.InsertChannelMessageParams{ChannelID: int(channel), PacketHash: hash, SenderName: "s", Content: fmt.Sprint(i), SentAt: sent})
		if err != nil || m == nil {
			f.t.Fatalf("message %d: %+v, %v", i, m, err)
		}
		out.ids = append(out.ids, m.ID)
		out.channel[m.ID] = channel
		later := sites[first:]
		if len(sites) > 0 && r.IntN(3) == 0 {
			later = append(later, sites[0]) // a second observer at a site that already heard it
		}
		for _, site := range later {
			n := 0
			if slices.Contains(sites[:first], site) {
				n = 1
			}
			hear(site, n)
			if _, err := f.store.AddChannelMessageIATA(f.ctx, hash, site); err != nil {
				f.t.Fatal(err)
			}
		}
	}
	slices.Reverse(out.ids)
	f.exec("ANALYZE")
	return out
}

// bogusSites returns n three-letter codes that name no fixture site.
func bogusSites(n int) []string {
	out := make([]string, 0, n)
	for a := 'A'; a <= 'Z'; a++ {
		for b := 'A'; b <= 'Z'; b++ {
			for c := 'A'; c <= 'Z'; c++ {
				if code := string([]rune{a, b, c}); len(out) < n && !slices.Contains(cmiSites, code) && code != "ZZZ" {
					out = append(out, code)
				}
			}
		}
	}
	return out
}

// legacyPage is today's statement, through the legacy path, for the same request.
func legacyPage(ctx context.Context, q sqlc.Querier, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return listChannelMessagesLegacy(ctx, q, channelID, since, limit, iatas, scope, cursor)
}

func TestChannelMessagesByIATAsMatchesLegacyPostgres(t *testing.T) {
	f := newCmiFixture(t)
	fx := buildCmiMessages(f)
	if missing, extra := f.definition(); missing != 0 || extra != 0 {
		t.Fatalf("fixture: missing %d, extra %d", missing, extra)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	trusted := New(f.pool, nil, 0, 0, WithReadModels(earnedCmi()))
	q := sqlc.New(f.pool)

	bogus := bogusSites(5000)
	ch7, ch8, ch999 := int32(7), int32(8), int32(999)
	channels := []*int32{nil, &ch7, &ch8, &ch999}
	siteLists := [][]string{nil, {}, {"YUL"}, {"YUL", "YUL", "YOW"}, {"YUL "}, {"yul"}, {"YULX"}, {""}, cmiSites}
	scopes := []string{"", "#fr", "#none", "#unknown", " "}
	sinces := []time.Time{{}, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), fx.tiedAt}
	cursors := []int64{0, fx.ids[len(fx.ids)/2], fx.ids[len(fx.ids)-1], math.MaxInt32, math.MaxInt32 + 1, 3_000_000_000, math.MaxInt64, -1}
	limits := []int32{1, 2, 3, 50, 200}

	cases, nonEmpty := 0, 0
	compare := func(ch *int32, iatas []string, scope string, since time.Time, cursor int64, limit int32) {
		t.Helper()
		got, err := trusted.ListChannelMessages(ctx, ch, since, limit, iatas, scope, cursor)
		if err != nil {
			t.Fatal(err)
		}
		want, err := legacyPage(ctx, q, ch, since, limit, iatas, scope, cursor)
		if err != nil {
			t.Fatal(err)
		}
		cases++
		if len(want.Items) > 0 {
			nonEmpty++
		}
		if !reflect.DeepEqual(got, want) {
			chs := "all"
			if ch != nil {
				chs = fmt.Sprint(*ch)
			}
			t.Fatalf("channel %s iatas %d %q scope %q since %v cursor %d limit %d:\n got %+v\nwant %+v", chs, len(iatas), iatas[:min(len(iatas), 3)], scope, since, cursor, limit, got, want)
		}
	}
	// Sites with since across every channel, again with every page taking the walk instead of the messages sent since.
	defer func(n int64) { messagesSinceWindowRows = n }(messagesSinceWindowRows)
	messagesSinceWindowRows = 0
	for _, sites := range siteLists[2:] {
		for _, scope := range scopes {
			for _, since := range sinces[1:] {
				for _, cursor := range cursors {
					for _, limit := range limits {
						compare(nil, sites, scope, since, cursor, limit)
					}
				}
			}
		}
	}
	messagesSinceWindowRows = 5000
	for _, ch := range channels {
		for _, sites := range siteLists {
			for _, scope := range scopes {
				for _, since := range sinces {
					for _, cursor := range cursors {
						for _, limit := range limits {
							compare(ch, sites, scope, since, cursor, limit)
						}
					}
				}
			}
		}
		for _, scope := range scopes {
			for _, since := range sinces {
				for _, cursor := range cursors[:2] {
					for _, limit := range []int32{1, 50} {
						compare(ch, bogus, scope, since, cursor, limit)
					}
				}
			}
		}
	}
	t.Logf("%d cases identical, %d with rows", cases, nonEmpty)
	if nonEmpty < 1000 {
		t.Fatalf("only %d of %d cases returned rows: the fixture does not exercise the statements", nonEmpty, cases)
	}

	// Cursor chains: both paths walk the same ids, each once, newest first.
	chains := []struct {
		ch    *int32
		iatas []string
		scope string
		since time.Time
	}{
		{&ch7, []string{"YUL"}, "", time.Time{}}, {&ch7, []string{"YUL", "YOW"}, "#fr", time.Time{}}, {&ch7, nil, "#fr", time.Time{}},
		{nil, []string{"YUL"}, "", time.Time{}}, {nil, nil, "#fr", time.Time{}}, {nil, nil, "", fx.tiedAt},
		{nil, []string{"YUL", "YOW"}, "#fr", time.Time{}}, {nil, []string{"YUL", "YOW", "YVR"}, "", fx.tiedAt},
		{nil, cmiSites, "#fr", fx.tiedAt}, {nil, []string{"YOW"}, "", time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	walk := func(page func(cursor int64) (api.Page[api.ChannelMessage], error)) []int64 {
		t.Helper()
		var ids []int64
		cursor := int64(0)
		for range 1000 {
			p, err := page(cursor)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range p.Items {
				ids = append(ids, m.ID)
			}
			if !p.HasMore {
				return ids
			}
			cursor = *p.NextCursor
		}
		t.Fatal("chain did not end")
		return nil
	}
	for i, c := range chains {
		for _, limit := range []int32{1, 2, 3} {
			got := walk(func(cur int64) (api.Page[api.ChannelMessage], error) {
				return trusted.ListChannelMessages(ctx, c.ch, c.since, limit, c.iatas, c.scope, cur)
			})
			want := walk(func(cur int64) (api.Page[api.ChannelMessage], error) {
				return legacyPage(ctx, q, c.ch, c.since, limit, c.iatas, c.scope, cur)
			})
			if !slices.Equal(got, want) || len(got) == 0 {
				t.Fatalf("chain %d limit %d: %v, want %v", i, limit, got, want)
			}
			for j := 1; j < len(got); j++ {
				if got[j] >= got[j-1] {
					t.Fatalf("chain %d limit %d not strictly descending at %d: %v", i, limit, j, got)
				}
			}
		}
	}
}

// recordingDBTX records the sqlc name of every statement, and the text and args of the last one.
type recordingDBTX struct {
	sqlc.DBTX
	names []string
	sql   string
	args  []any
}

func (r *recordingDBTX) record(sql string, args []any) {
	r.names = append(r.names, sqlcName(sql))
	r.sql, r.args = sql, args
}

func (r *recordingDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.record(sql, args)
	return r.DBTX.Exec(ctx, sql, args...)
}

func (r *recordingDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.record(sql, args)
	return r.DBTX.Query(ctx, sql, args...)
}

func (r *recordingDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	r.record(sql, args)
	return r.DBTX.QueryRow(ctx, sql, args...)
}

func TestChannelMessagesZeroHitRegionPostgres(t *testing.T) {
	f := newCmiFixture(t)
	buildCmiMessages(f)
	trusted := New(f.pool, nil, 0, 0, WithReadModels(earnedCmi()))
	q := sqlc.New(f.pool)
	ch := int32(7)
	bogus := bogusSites(5000)
	cases := []struct {
		ch    *int32
		iatas []string
		scope string
	}{
		{&ch, []string{"ZZZ"}, ""}, {nil, []string{"ZZZ"}, ""}, {&ch, []string{"YOW"}, "#none"},
		{&ch, nil, "#none"}, {nil, nil, "#none"}, {&ch, bogus, ""}, {nil, bogus, ""},
	}
	for i, c := range cases {
		got, err := trusted.ListChannelMessages(f.ctx, c.ch, time.Time{}, 50, c.iatas, c.scope, 0)
		if err != nil {
			t.Fatal(err)
		}
		want, err := legacyPage(f.ctx, q, c.ch, time.Time{}, 50, c.iatas, c.scope, 0)
		if err != nil {
			t.Fatal(err)
		}
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(want)
		if string(g) != `{"items":[],"hasMore":false}` || string(g) != string(w) {
			t.Fatalf("case %d: %s, today %s", i, g, w)
		}
	}

	// The zero-hit site reads a few index pages: no scan, no observations.
	f.exec("VACUUM ANALYZE channel_message_iatas")
	rec := &recordingDBTX{DBTX: f.pool}
	if _, err := New(rec, nil, 0, 0, WithReadModels(earnedCmi())).ListChannelMessages(f.ctx, &ch, time.Time{}, 50, []string{"ZZZ"}, "", 0); err != nil {
		t.Fatal(err)
	}
	if rec.names[len(rec.names)-1] != "ListChannelMessagesByIATAs" {
		t.Fatalf("statements %v", rec.names)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if _, err := tx.Exec(f.ctx, "SET LOCAL enable_seqscan = off; SET LOCAL jit = off"); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err := tx.QueryRow(f.ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+rec.sql, rec.args...).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	var cmiNode map[string]any
	var nodes []map[string]any
	var visit func(n map[string]any)
	visit = func(n map[string]any) {
		nodes = append(nodes, n)
		if n["Relation Name"] == "channel_message_iatas" {
			cmiNode = n
		}
		if kids, ok := n["Plans"].([]any); ok {
			for _, k := range kids {
				visit(k.(map[string]any))
			}
		}
	}
	var parsed []map[string]any
	if err := json.Unmarshal([]byte(plan), &parsed); err != nil {
		t.Fatal(err)
	}
	visit(parsed[0]["Plan"].(map[string]any))
	if cmiNode == nil || cmiNode["Node Type"] != "Index Only Scan" || cmiNode["Index Name"] != "idx_cmi_channel_site" || cmiNode["Actual Rows"].(float64) != 0 {
		t.Fatalf("cmi node %v\n%s", cmiNode, plan)
	}
	for _, n := range nodes {
		if n["Node Type"] == "Seq Scan" || n["Relation Name"] == "packet_observations" {
			t.Fatalf("node %v in\n%s", n, plan)
		}
	}
	root := parsed[0]["Plan"].(map[string]any)
	if b := root["Shared Hit Blocks"].(float64) + root["Shared Read Blocks"].(float64); b > 10 {
		t.Fatalf("zero-hit page read %v buffers", b)
	}

	// An unknown scope runs the lookup and nothing else.
	rec = &recordingDBTX{DBTX: f.pool}
	if _, err := New(rec, nil, 0, 0, WithReadModels(earnedCmi())).ListChannelMessages(f.ctx, nil, time.Time{}, 50, nil, "#unknown", 0); err != nil {
		t.Fatal(err)
	}
	if strings.Join(rec.names, ",") != "GetTransportScopeByName" {
		t.Fatalf("an unknown scope ran %v", rec.names)
	}
}

func TestCmiTrustLifecyclePostgres(t *testing.T) {
	f := newCmiFixture(t)
	buildCmiMessages(f)
	rec := &recordingDBTX{DBTX: f.pool}
	tr := NewTrust("cmi_lifecycle_test", true, CmiTrustPolicy)
	s := New(rec, nil, 0, 0, WithReadModels(&ReadModels{Cmi: tr}))
	ch := int32(7)
	// The original statement's page is the oracle; while trusted every page equals it.
	want, err := listChannelMessagesLegacy(f.ctx, sqlc.New(f.pool), &ch, time.Time{}, 50, []string{"YUL"}, "", 0)
	if err != nil || len(want.Items) == 0 {
		t.Fatalf("oracle page %+v, %v", want, err)
	}
	// step checks one page: a statement name means served by it with the oracle's rows; "" means not ready, with no
	// statement run.
	step := func(label, stmt string) {
		t.Helper()
		rec.names = nil
		page, err := s.ListChannelMessages(f.ctx, &ch, time.Time{}, 50, []string{"YUL"}, "", 0)
		if got := strings.Join(rec.names, ","); got != stmt {
			t.Fatalf("%s: ran %q, want %q", label, got, stmt)
		}
		if stmt == "" {
			if !errors.Is(err, api.ErrPagesNotReady) {
				t.Fatalf("%s: err = %v, want ErrPagesNotReady", label, err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(page, want) {
			t.Fatalf("%s: page differs from the original statement's", label)
		}
	}
	step("not earned", "")
	tr.Earn()
	step("earned", "ListChannelMessagesByIATAs")
	for range 3 {
		tr.ObserveRun(1)
	}
	step("three repairing runs", "")
	tr.ObserveRun(0)
	tr.ObserveRun(0)
	step("two clean runs", "ListChannelMessagesByIATAs")
	tr.Invalidate("failed add")
	step("a writer failure", "")
	tr.Repaired(tr.Generation())
	step("the next reconcile", "ListChannelMessagesByIATAs")
	disabled := NewTrust("cmi_lifecycle_disabled", false, CmiTrustPolicy)
	disabled.Earn()
	s.readModels = &ReadModels{Cmi: disabled}
	step("disabled", "")
}
