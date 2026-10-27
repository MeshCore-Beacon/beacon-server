// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestRoutePathKey_MatchesPostgresDigest(t *testing.T) {
	// Golden vector: Postgres computes
	//   decode(md5(array_to_string(node_ids, ',')), 'hex')
	// over lowercase-hyphenated UUIDs. Migration 024 backfilled with that
	// expression; this pins the Go side to the identical bytes.
	a := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	got := hex.EncodeToString(routePathKey([]uuid.UUID{a, b}))
	want := "f097439148601d9f3291c474f82fa64c"
	if got != want {
		t.Errorf("routePathKey = %s, want %s", got, want)
	}
}

func TestUpsertKnownRoute_ComputesPathKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}

	a := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	wantKey, _ := hex.DecodeString("f097439148601d9f3291c474f82fa64c")

	mock.EXPECT().UpsertKnownRoute(gomock.Any(), gomock.Cond(func(p sqlc.UpsertKnownRouteParams) bool {
		return bytes.Equal(p.PathKey, wantKey)
	})).Return(nil)

	if err := store.UpsertKnownRoute(context.Background(), []uuid.UUID{a, b}, [][]byte{{0x37}, {0xd8}}, "PRG", 2); err != nil {
		t.Fatal(err)
	}
}

// A value ending in a blank matches no character(3) code, nor does an empty one in a list; no statement runs, and
// the page encodes as [].
func TestListKnownRoutesTrailingBlankIATA(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl) // no EXPECT: any call fails the test
	store := &Store{q: mock}
	for _, iatas := range [][]string{{"SEA "}, {"SEA  "}, {"   "}, {" "}, {"SE\x00 "}, {""}, {"SEA ", "", " "}} {
		for _, hop := range []int32{0, 3} {
			routes, err := store.ListKnownRoutes(context.Background(), iatas, hop, time.Time{}, 0, 50)
			b, _ := json.Marshal(routes)
			if err != nil || routes == nil || string(b) != "[]" {
				t.Fatalf("%q hop %d: %v %s", iatas, hop, err, b)
			}
		}
	}
}

// Each shape runs its own statement; the router decides how the hop pair is planned.
func TestListKnownRoutes_Dispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: q}
	cursor := time.UnixMilli(1_760_000_000_000)
	q.EXPECT().ListKnownRoutesGlobal(gomock.Any(), sqlc.ListKnownRoutesGlobalParams{CursorTs: pgtype.Timestamptz{Time: cursor, Valid: true}, CursorID: 7, PageLimit: 50}).Return(nil, nil)
	// 3000 analyzed routes, hop 3 a 10 % group: under the 1000-row floor, so the page reads the group.
	q.EXPECT().GetKnownRoutesHopStats(gomock.Any()).Return(sqlc.GetKnownRoutesHopStatsRow{Reltuples: 3000, HasStats: true,
		HopValues: []int32{2, 3}, HopFreqs: []float64{0.9, 0.1}, NDistinct: 2}, nil)
	q.EXPECT().ListKnownRoutesGlobalHop(gomock.Any(), sqlc.ListKnownRoutesGlobalHopParams{HopCount: 3, ReadGroup: true, PageLimit: 50}).Return(nil, nil)
	q.EXPECT().ListKnownRoutesByIATA(gomock.Any(), sqlc.ListKnownRoutesByIATAParams{Iata: "SEA", PageLimit: 50}).Return(nil, nil)
	q.EXPECT().ListKnownRoutesByIATAHop(gomock.Any(), sqlc.ListKnownRoutesByIATAHopParams{Iata: "SEA", HopCount: 3, PageLimit: 50}).Return(nil, nil)
	// A list that comes down to one code runs that IATA's statement; duplicates and blank-ended codes drop out.
	q.EXPECT().ListKnownRoutesByIATA(gomock.Any(), sqlc.ListKnownRoutesByIATAParams{Iata: "KLS", PageLimit: 50}).Return(nil, nil)
	q.EXPECT().ListKnownRoutesByIATAHop(gomock.Any(), sqlc.ListKnownRoutesByIATAHopParams{Iata: "KLS", HopCount: 3, PageLimit: 50}).Return(nil, nil)
	q.EXPECT().ListKnownRoutesByIATAs(gomock.Any(), sqlc.ListKnownRoutesByIATAsParams{Iatas: []string{"SEA", "KLS"},
		CursorTs: pgtype.Timestamptz{Time: cursor, Valid: true}, CursorID: 7, PageLimit: 50}).Return(nil, nil)
	q.EXPECT().ListKnownRoutesByIATAsHop(gomock.Any(), sqlc.ListKnownRoutesByIATAsHopParams{Iatas: []string{"SEA", "KLS"}, HopCount: 3, PageLimit: 50}).Return(nil, nil)
	q.EXPECT().GetNodesByIDs(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	for _, c := range []struct {
		iatas  []string
		hop    int32
		cursor time.Time
		id     int64
	}{
		{nil, 0, cursor, 7}, {[]string{}, 3, time.Time{}, 0}, {[]string{"SEA"}, 0, time.Time{}, 0}, {[]string{"SEA"}, 3, time.Time{}, 0},
		{[]string{"KLS", "KLS", "SEA "}, 0, time.Time{}, 0}, {[]string{"KLS", ""}, 3, time.Time{}, 0},
		{[]string{"SEA", "KLS", "SEA"}, 0, cursor, 7}, {[]string{"SEA", "KLS"}, 3, time.Time{}, 0},
	} {
		routes, err := store.ListKnownRoutes(context.Background(), c.iatas, c.hop, c.cursor, c.id, 50)
		if err != nil || routes == nil || len(routes) != 0 {
			t.Fatalf("%+v: %v %v", c, routes, err)
		}
	}
}

func TestRouteCutoffCeil(t *testing.T) {
	base := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct{ in, want time.Time }{
		{base.Add(586 * time.Microsecond), base.Add(time.Millisecond)},
		{base.Add(time.Millisecond), base.Add(time.Millisecond)},
		{base, base},
		{base.Add(586_400 * time.Nanosecond), base.Add(time.Millisecond)},
	} {
		if got := ceilMs(c.in.Truncate(time.Microsecond)); !got.Equal(c.want) {
			t.Errorf("ceilMs(%v) = %v, want %v", c.in, got, c.want)
		}
	}

	// The store sends the exact cutoff (pgx floors it to µs) and the ms ceiling of its µs floor;
	// 1 ms + 400 ns floors to 1 ms, so its ceiling stays 1 ms.
	for _, c := range []struct{ cutoff, ceil time.Time }{
		{base.Add(586_400 * time.Nanosecond), base.Add(time.Millisecond)},
		{base.Add(time.Millisecond + 400*time.Nanosecond), base.Add(time.Millisecond)},
	} {
		ctrl := gomock.NewController(t)
		mock := mockdb.NewMockQuerier(ctrl)
		store := &Store{q: mock}
		windowEnd := base.Add(time.Millisecond)
		start := base.Add(-time.Hour)
		mock.EXPECT().DeleteGraceRoutesSlice(gomock.Any(), gomock.Cond(func(p sqlc.DeleteGraceRoutesSliceParams) bool {
			return p.GraceCeil.Valid && p.GraceCeil.Time.Equal(c.ceil) &&
				p.GraceCutoff.Valid && p.GraceCutoff.Time.Equal(c.cutoff) &&
				p.AfterTs.Valid && p.AfterTs.InfinityModifier == pgtype.Finite && p.AfterTs.Time.Equal(start) && p.AfterID == 0 &&
				p.MinObservations == 3 && p.BatchSize == 7
		})).Return(sqlc.DeleteGraceRoutesSliceRow{WindowEnd: pgtype.Timestamptz{Time: windowEnd, Valid: true}}, nil)
		res, err := store.DeleteGraceRoutesSlice(context.Background(), RouteKey{TS: start}, c.cutoff, 3, 7)
		if err != nil {
			t.Fatal(err)
		}
		if res.Last != (RouteKey{}) || res.Unresolved != nil || !res.WindowEnd.Equal(windowEnd) {
			t.Errorf("empty slice mapped to %+v", res)
		}
		ctrl.Finish()
	}

	// A grace walk from -infinity would never advance: refused before any statement runs.
	{
		ctrl := gomock.NewController(t)
		store := &Store{q: mockdb.NewMockQuerier(ctrl)} // no EXPECT: any call fails the test
		if _, err := store.DeleteGraceRoutesSlice(context.Background(), RouteKey{}, base, 3, 7); err == nil {
			t.Error("DeleteGraceRoutesSlice from RouteKey{} returned no error")
		}
		ctrl.Finish()
	}

	// DeleteExpiredRoutes sends the same pair, and maps the batch's last key.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}
	cutoff := base.Add(586 * time.Microsecond)
	after := RouteKey{TS: base, ID: 9}
	mock.EXPECT().DeleteExpiredRoutes(gomock.Any(), gomock.Cond(func(p sqlc.DeleteExpiredRoutesParams) bool {
		return p.RetentionCeil.Time.Equal(base.Add(time.Millisecond)) && p.RetentionCutoff.Time.Equal(cutoff) &&
			p.AfterTs.InfinityModifier == pgtype.Finite && p.AfterTs.Time.Equal(base) && p.AfterID == 9 && p.BatchSize == 7
	})).Return(sqlc.DeleteExpiredRoutesRow{Selected: 7, LastTs: pgtype.Timestamptz{Time: base, Valid: true}, LastID: 20}, nil)
	selected, last, err := store.DeleteExpiredRoutes(context.Background(), after, cutoff, 7)
	if err != nil || selected != 7 || last != (RouteKey{TS: base, ID: 20}) {
		t.Errorf("DeleteExpiredRoutes = %d, %+v, %v", selected, last, err)
	}
}

func TestStateRoundTrip(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}
	type pos struct {
		TS time.Time `json:"ts"`
		ID int64     `json:"id"`
	}
	in := pos{TS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ID: 42}
	var saved json.RawMessage
	mock.EXPECT().UpsertBackgroundState(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p sqlc.UpsertBackgroundStateParams) error {
			if p.Key != "routes.grace" {
				t.Errorf("key %q", p.Key)
			}
			saved = p.Value
			return nil
		})
	if err := store.SaveState(context.Background(), "routes.grace", in); err != nil {
		t.Fatal(err)
	}

	mock.EXPECT().GetBackgroundState(gomock.Any(), "routes.grace").DoAndReturn(
		func(context.Context, string) (json.RawMessage, error) { return saved, nil })
	var out pos
	if ok, err := store.LoadState(context.Background(), "routes.grace", &out); err != nil || !ok || out != in {
		t.Errorf("LoadState = %v, %v, %+v; want true, nil, %+v", ok, err, out, in)
	}

	mock.EXPECT().GetBackgroundState(gomock.Any(), "missing").Return(nil, pgx.ErrNoRows)
	if ok, err := store.LoadState(context.Background(), "missing", &out); err != nil || ok {
		t.Errorf("missing key: %v, %v; want false, nil", ok, err)
	}
}

func TestReconfirmCursorDecode(t *testing.T) {
	cases := []struct {
		raw      string
		wantIATA string
		wantKey  []byte
	}{
		{`{}`, "", []byte{}},
		{`{"iata":"LRU"}`, "", []byte{}},
		{`{"iata":"LRU","path_key_hex":null}`, "", []byte{}},
		{`{"iata":"LRU","path_key_hex":"zz"}`, "", []byte{}},
		{`{"iata":"LRU","path_key_hex":"0a0b"}`, "LRU", []byte{0x0a, 0x0b}},
		{`{"iata":"LRU","path_key_hex":""}`, "LRU", []byte{}},
		{`{"iata":"","path_key_hex":"0a"}`, "", []byte{}},
	}
	for _, c := range cases {
		var cur reconfirmCursor
		if err := json.Unmarshal([]byte(c.raw), &cur); err != nil {
			t.Fatalf("%s: %v", c.raw, err)
		}
		iata, key := cur.position()
		if key == nil {
			t.Errorf("%s: nil key", c.raw)
		}
		if iata != c.wantIATA || !bytes.Equal(key, c.wantKey) {
			t.Errorf("%s: position = (%q, %x), want (%q, %x)", c.raw, iata, key, c.wantIATA, c.wantKey)
		}
	}
}

func TestHopGroupIsSmall(t *testing.T) {
	// 10.5M routes, limit 200: the bound is 0.7 * sqrt(200 * 10.5M), about 32K routes.
	st := sqlc.GetKnownRoutesHopStatsRow{Reltuples: 10_500_000, HasStats: true,
		HopValues: []int32{6, 21, 22, 25}, HopFreqs: []float64{0.14, 0.0038, 0.002, 0.001}, NullFrac: 0, NDistinct: 40}
	for _, c := range []struct {
		name  string
		st    sqlc.GetKnownRoutesHopStatsRow
		hop   int32
		limit int32
		want  bool
	}{
		{"dense hop", st, 6, 200, false},
		{"over the bound", st, 21, 200, false},
		{"under the bound", st, 22, 200, true},
		{"rare hop", st, 25, 200, true},
		{"limit 50 halves the bound", st, 22, 50, false},
		// 1 - 0.1458 spread over the 36 values outside the list, capped at the least common listed value (0.001):
		// 10.5K routes, as the planner estimates it.
		{"value outside the list", st, 9, 200, true},
		{"value outside a list of common values", sqlc.GetKnownRoutesHopStatsRow{Reltuples: 10_500_000, HasStats: true,
			HopValues: []int32{6, 7}, HopFreqs: []float64{0.3, 0.2}, NDistinct: 4}, 9, 200, false},
		{"absent from a complete list", sqlc.GetKnownRoutesHopStatsRow{Reltuples: 10_500_000, HasStats: true,
			HopValues: []int32{6, 7}, HopFreqs: []float64{0.5, 0.5}, NDistinct: 2}, 1, 200, true},
		{"negative n_distinct scales with the rows", sqlc.GetKnownRoutesHopStatsRow{Reltuples: 10_500_000, HasStats: true,
			HopValues: []int32{6}, HopFreqs: []float64{0.5}, NDistinct: -0.00001}, 9, 200, false},
		{"no statistics", sqlc.GetKnownRoutesHopStatsRow{Reltuples: 10_500_000}, 25, 200, false},
		{"never counted", sqlc.GetKnownRoutesHopStatsRow{Reltuples: -1, HasStats: true}, 25, 200, false},
	} {
		if got := hopGroupIsSmall(c.st, c.hop, c.limit); got != c.want {
			t.Errorf("%s: hop %d limit %d = %v, want %v", c.name, c.hop, c.limit, got, c.want)
		}
	}
}

// The statistics are read once per TTL, and a failed read is retried soon while the last statistics stay in use.
func TestHopGroupSmallCaches(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: q}
	st := sqlc.GetKnownRoutesHopStatsRow{Reltuples: 3000, HasStats: true, HopValues: []int32{3}, HopFreqs: []float64{0.1}, NDistinct: 1}
	q.EXPECT().GetKnownRoutesHopStats(gomock.Any()).Return(st, nil).Times(1)
	for range 3 {
		if !store.hopGroupSmall(context.Background(), 3, 50) {
			t.Fatal("hop 3 not small")
		}
	}
	// Expired: the read fails, the old statistics still answer, and the next read waits for hopStatsRetry.
	store.hopStats.at = time.Now().Add(-hopStatsTTL)
	q.EXPECT().GetKnownRoutesHopStats(gomock.Any()).Return(sqlc.GetKnownRoutesHopStatsRow{}, errors.New("down")).Times(1)
	for range 3 {
		if !store.hopGroupSmall(context.Background(), 3, 50) {
			t.Fatal("hop 3 not small after a failed refresh")
		}
	}
	if wait := time.Until(store.hopStats.at.Add(hopStatsTTL)); wait > hopStatsRetry || wait < hopStatsRetry-time.Minute {
		t.Errorf("next read in %s, want about %s", wait, hopStatsRetry)
	}
}

// A request that goes away while it refreshes the statistics does not fail the read.
func TestHopGroupSmallReadOutlivesRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: q}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q.EXPECT().GetKnownRoutesHopStats(gomock.Any()).DoAndReturn(func(rctx context.Context) (sqlc.GetKnownRoutesHopStatsRow, error) {
		if rctx.Err() != nil {
			return sqlc.GetKnownRoutesHopStatsRow{}, rctx.Err()
		}
		if _, ok := rctx.Deadline(); !ok {
			t.Error("statistics read has no deadline")
		}
		return sqlc.GetKnownRoutesHopStatsRow{Reltuples: 3000, HasStats: true, HopValues: []int32{3}, HopFreqs: []float64{0.1}, NDistinct: 1}, nil
	}).Times(1)
	if !store.hopGroupSmall(ctx, 3, 50) {
		t.Fatal("hop 3 not small: the read failed with the request's context")
	}
}
