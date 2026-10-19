// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
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

// RF3: a value ending in a blank matches no character(3) code; no statement runs, and it encodes as [].
func TestListKnownRoutesTrailingBlankIATA(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl) // no EXPECT: any call fails the test
	store := &Store{q: mock, qc: mock}
	for _, iata := range []string{"SEA ", "SEA  ", "   ", " ", "SE\x00 "} {
		routes, err := store.ListKnownRoutes(context.Background(), iata, 0, time.Time{}, 0, 50)
		b, _ := json.Marshal(routes)
		if err != nil || routes == nil || string(b) != "[]" {
			t.Fatalf("%q: %v %s", iata, err, b)
		}
	}
}

// Each shape runs its own statement; the hop pair goes through qc.
func TestListKnownRoutes_Dispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	q, qc := mockdb.NewMockQuerier(ctrl), mockdb.NewMockQuerier(ctrl)
	store := &Store{q: q, qc: qc}
	cursor := time.UnixMilli(1_760_000_000_000)
	q.EXPECT().ListKnownRoutesGlobal(gomock.Any(), sqlc.ListKnownRoutesGlobalParams{CursorTs: pgtype.Timestamptz{Time: cursor, Valid: true}, CursorID: 7, PageLimit: 50}).Return(nil, nil)
	qc.EXPECT().ListKnownRoutesGlobalHop(gomock.Any(), sqlc.ListKnownRoutesGlobalHopParams{HopCount: 3, PageLimit: 50}).Return(nil, nil)
	q.EXPECT().ListKnownRoutesByIATA(gomock.Any(), sqlc.ListKnownRoutesByIATAParams{Iata: "SEA", PageLimit: 50}).Return(nil, nil)
	qc.EXPECT().ListKnownRoutesByIATAHop(gomock.Any(), sqlc.ListKnownRoutesByIATAHopParams{Iata: "SEA", HopCount: 3, PageLimit: 50}).Return(nil, nil)
	q.EXPECT().GetNodesByIDs(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	for _, c := range []struct {
		iata   string
		hop    int32
		cursor time.Time
		id     int64
	}{{"", 0, cursor, 7}, {"", 3, time.Time{}, 0}, {"SEA", 0, time.Time{}, 0}, {"SEA", 3, time.Time{}, 0}} {
		routes, err := store.ListKnownRoutes(context.Background(), c.iata, c.hop, c.cursor, c.id, 50)
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
		store := &Store{q: mock, qc: mock}
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
	store := &Store{q: mock, qc: mock}
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
