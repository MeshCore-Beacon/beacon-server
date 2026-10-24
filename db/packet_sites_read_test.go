// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func earnedPacketSites() *ReadModels {
	tr := NewTrust("packet_sites_dispatch_test", true, PacketSitesTrustPolicy)
	tr.Earn()
	return &ReadModels{PacketSites: tr}
}

func TestSiteStatement(t *testing.T) {
	for _, tc := range []struct {
		fam       siteFamily
		sites     int
		hasCursor bool
		want      string
	}{
		{sitePlain, 1, false, "ListPacketsBySite"},
		{sitePlain, 1, true, "ListPacketsBySite"},
		{sitePlain, 2, false, "ListPacketsBySitesFirst"},
		{sitePlain, 2, true, "ListPacketsBySites"},
		{siteTyped, 1, true, "ListPacketsBySitesTypedFirst"},
		{siteTyped, 3, false, "ListPacketsBySitesTypedFirst"},
		{siteTyped, 3, true, "ListPacketsBySitesTyped"},
		{siteMany, 1, false, "ListPacketsBySitesSince"},
		{siteMany, 3, true, "ListPacketsBySitesSince"},
		{siteWindow, 1, false, "ListPacketsBySitesWindow"},
		{siteWindow, 3, true, "ListPacketsBySitesWindow"},
		{siteScoped, 3, true, "ListPacketsBySitesScopedWindow"},
	} {
		if got := siteStatement(tc.fam, tc.sites, tc.hasCursor); got != tc.want {
			t.Errorf("siteStatement(%d, %d, %v) = %s, want %s", tc.fam, tc.sites, tc.hasCursor, got, tc.want)
		}
	}
	since := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		pt, rt []int16
		scopes []string
		since  time.Time
		want   siteFamily
	}{
		{"none", nil, nil, nil, time.Time{}, sitePlain},
		{"types only", []int16{4}, nil, nil, time.Time{}, siteTyped},
		{"four type pairs", []int16{4, 5}, []int16{0, 1}, nil, time.Time{}, siteTyped},
		{"eight type pairs", []int16{4, 5}, nil, nil, time.Time{}, siteTyped},
		{"many type pairs", []int16{4, 5, 6}, []int16{0, 1, 2}, nil, time.Time{}, siteMany},
		{"route types only", nil, []int16{3}, nil, time.Time{}, siteMany}, // 16 payload types x 1
		{"since and types", []int16{4}, nil, nil, since, siteWindow},
		{"since", nil, nil, nil, since, siteWindow},
		{"scopes, since and types", []int16{4}, nil, []string{"#a"}, since, siteScoped},
	} {
		f, ok := newPacketFilter(tc.pt, tc.rt, tc.scopes, tc.since, time.Time{}, 0, nil, 50)
		if !ok || siteFamilyOf(f) != tc.want {
			t.Errorf("%s: family %d (ok %v), want %d", tc.name, siteFamilyOf(f), ok, tc.want)
		}
	}
}

var (
	allPayloadTypes = []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	allRouteTypes   = []int16{0, 1, 2, 3}
	infinityTS      = pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}
)

// Each region input on a trusted store runs exactly the expected statements (gomock fails on any other).
func TestListPacketsRegionDispatch(t *testing.T) {
	since := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	const cursor = int64(1792442625019)
	pc := &api.PacketCursor{At: time.Date(2026, 1, 9, 0, 0, 0, 123000, time.UTC), Hash: []byte{0xab}}
	known := func(m *mockdb.MockQuerier, in, out []string) {
		m.EXPECT().FilterKnownSites(gomock.Any(), in).Return(out, nil)
	}
	cases := []struct {
		name         string
		pt, rt       []int16
		iatas        []string
		scopes       []string
		since, until time.Time
		cursor       int64
		pageCursor   *api.PacketCursor
		expect       func(m *mockdb.MockQuerier)
	}{
		{name: "one site", iatas: []string{"AAA"}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsBySite(gomock.Any(), sqlc.ListPacketsBySiteParams{Iata: "AAA", CursorTs: infinityTS, CursorHash: []byte{}, PageLimit: 51}).Return(nil, nil)
		}},
		{name: "duplicates and a cursor", iatas: []string{"AAA", "BBB", "AAA"}, cursor: cursor, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsBySites(gomock.Any(), sqlc.ListPacketsBySitesParams{
				Iatas: []string{"AAA", "BBB"}, CursorTs: pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}, CursorHash: []byte{}, PageLimit: 51,
			}).Return(nil, nil)
		}},
		{name: "pageCursor wins over cursor", iatas: []string{"AAA", "BBB"}, cursor: cursor, pageCursor: pc, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsBySites(gomock.Any(), sqlc.ListPacketsBySitesParams{
				Iatas: []string{"AAA", "BBB"}, CursorTs: pgtype.Timestamptz{Time: pc.At, Valid: true}, CursorHash: pc.Hash, PageLimit: 51,
			}).Return(nil, nil)
		}},
		{name: "two sites, page 1", iatas: []string{"AAA", "BBB"}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsBySitesFirst(gomock.Any(), sqlc.ListPacketsBySitesFirstParams{Iatas: []string{"AAA", "BBB"}, CursorTs: infinityTS, CursorHash: []byte{}, PageLimit: 51}).Return(nil, nil)
		}},
		{name: "typed", iatas: []string{"AAA", "BBB"}, pt: []int16{4}, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA", "BBB"}, []string{"AAA", "BBB"})
			m.EXPECT().ListPacketsBySitesTypedFirst(gomock.Any(), sqlc.ListPacketsBySitesTypedFirstParams{
				Iatas: []string{"AAA", "BBB"}, PayloadTypes: []int16{4}, RouteTypes: allRouteTypes, CursorTs: infinityTS, CursorHash: []byte{}, PageLimit: 51,
			}).Return(nil, nil)
		}},
		{name: "typed with a cursor, one site survives", iatas: []string{"AAA", "BBB"}, pt: []int16{4}, cursor: cursor, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA", "BBB"}, []string{"AAA"})
			m.EXPECT().ListPacketsBySitesTypedFirst(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySitesTypedFirstParams) bool {
				return slices.Equal(p.Iatas, []string{"AAA"}) && p.CursorHash != nil
			})).Return(nil, nil)
		}},
		{name: "since", iatas: []string{"AAA", "BBB"}, since: since, cursor: cursor, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA", "BBB"}, []string{"AAA", "BBB"})
			cur := pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
			m.EXPECT().ListPacketsBySitesWindow(gomock.Any(), sqlc.ListPacketsBySitesWindowParams{
				Iatas: []string{"AAA", "BBB"}, MainTs: cur, MainHash: []byte{}, LoTs: pgtype.Timestamptz{Time: since.Add(-30 * time.Minute), Valid: true},
				HiTs: infinityTS, SinceTs: pgtype.Timestamptz{Time: since, Valid: true}, UntilTs: infinityTS,
				PayloadTypes: allPayloadTypes, RouteTypes: allRouteTypes, Recheck: true, CursorTs: cur, CursorHash: []byte{}, PageLimit: 51,
			}).Return(nil, nil)
		}},
		{name: "until", iatas: []string{"AAA"}, until: since, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA"}, []string{"AAA"})
			hi := pgtype.Timestamptz{Time: since.Add(30 * time.Minute), Valid: true}
			m.EXPECT().ListPacketsBySitesWindow(gomock.Any(), sqlc.ListPacketsBySitesWindowParams{
				Iatas: []string{"AAA"}, MainTs: hi, MainHash: bytes.Repeat([]byte{0xff}, 64), LoTs: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true},
				HiTs: hi, SinceTs: pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, UntilTs: pgtype.Timestamptz{Time: since, Valid: true},
				PayloadTypes: allPayloadTypes, RouteTypes: allRouteTypes, CursorTs: infinityTS, CursorHash: []byte{}, PageLimit: 51,
			}).Return(nil, nil)
		}},
		{name: "scopes and since", iatas: []string{"AAA"}, scopes: []string{"#a"}, since: since, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA"}, []string{"AAA"})
			m.EXPECT().ListPacketsBySitesScopedWindow(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySitesScopedWindowParams) bool {
				return slices.Equal(p.ScopeNames, []string{"#a"}) && !p.Recheck && p.CursorHash != nil && p.LoTs.Time.Equal(since.Add(-30*time.Minute))
			})).Return(nil, nil)
		}},
		{name: "scopes alone", iatas: []string{"AAA", "BBB"}, scopes: []string{"#a"}, cursor: cursor, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA", "BBB"}, []string{"AAA", "BBB"})
			m.EXPECT().ListPacketsBySitesScopedWindow(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySitesScopedWindowParams) bool {
				return p.Recheck && p.LoTs.InfinityModifier == pgtype.NegativeInfinity && p.HiTs.InfinityModifier == pgtype.Infinity &&
					p.MainTs.Time.Equal(time.UnixMilli(cursor))
			})).Return(nil, nil)
		}},
		{name: "pre-filter leaves nothing", iatas: []string{"QQQ"}, pt: []int16{4}, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"QQQ"}, []string{})
		}},
		{name: "unknown payload type", iatas: []string{"AAA"}, pt: []int16{99}, since: since, until: since.Add(-time.Hour), expect: func(*mockdb.MockQuerier) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mock := mockdb.NewMockQuerier(ctrl)
			tc.expect(mock)
			s := &Store{q: mock}
			WithReadModels(earnedPacketSites())(s)
			page, err := s.ListPackets(context.Background(), tc.pt, tc.rt, tc.iatas, tc.scopes, tc.since, tc.until, tc.cursor, tc.pageCursor, 50)
			if err != nil {
				t.Fatal(err)
			}
			if page.Items == nil || len(page.Items) != 0 || page.HasMore || page.NextCursor != nil || page.NextPageCursor != nil {
				t.Fatalf("page = %+v, want an empty page with non-nil items", page)
			}
		})
	}
}

func siteRowAt(at time.Time, hash byte) siteRow {
	return siteRow{PacketHash: []byte{hash}, SiteHeardAt: pgtype.Timestamptz{Time: at, Valid: true}}
}

func TestListPacketsRegionNextPageCursor(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 456789000, time.UTC)
	rows := []siteRow{siteRowAt(t0, 3), siteRowAt(t0.Add(-time.Second), 2), siteRowAt(t0.Add(-2*time.Second), 1)}
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	mock.EXPECT().ListPacketsBySite(gomock.Any(), gomock.Any()).Return(rows, nil)
	mock.EXPECT().ListPacketsBySite(gomock.Any(), gomock.Any()).Return(rows[:2], nil)
	s := &Store{q: mock}
	WithReadModels(earnedPacketSites())(s)
	page, err := s.ListPackets(context.Background(), nil, nil, []string{"AAA"}, nil, time.Time{}, time.Time{}, 0, nil, 2)
	if err != nil || len(page.Items) != 2 || !page.HasMore {
		t.Fatalf("page %+v, %v", page, err)
	}
	wantPC := api.PacketCursor{At: rows[1].SiteHeardAt.Time, Hash: []byte{2}}.String()
	if page.NextCursor == nil || *page.NextCursor != rows[1].SiteHeardAt.Time.UnixMilli() || page.NextPageCursor == nil || *page.NextPageCursor != wantPC {
		t.Fatalf("cursors %v %v, want %d %s", page.NextCursor, page.NextPageCursor, rows[1].SiteHeardAt.Time.UnixMilli(), wantPC)
	}
	page, err = s.ListPackets(context.Background(), nil, nil, []string{"AAA"}, nil, time.Time{}, time.Time{}, 0, nil, 2)
	if err != nil || len(page.Items) != 2 || page.HasMore || page.NextCursor != nil || page.NextPageCursor != nil {
		t.Fatalf("last page %+v, %v", page, err)
	}
}

func sinceKey(at time.Time, hash byte) sqlc.ListPacketsBySitesSinceRow {
	return sqlc.ListPacketsBySitesSinceRow{PacketHash: []byte{hash}, SiteHeardAt: pgtype.Timestamptz{Time: at, Valid: true}}
}

func sinceFloor(at time.Time, hash byte) sqlc.ListPacketsBySitesSinceRow {
	r := sinceKey(at, hash)
	r.IsFloor = true
	return r
}

// manyPT x manyRT is nine type pairs: the many-pair family.
var manyPT, manyRT = []int16{4, 5, 6}, []int16{0, 1, 2}

// A bounded read that stopped above the end of a range before the page was full reads on from its floor in the
// same request, with the re-check, until the page is full: no short page with more.
func TestListPacketsRegionBoundedContinues(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 456789000, time.UTC)
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	mock.EXPECT().FilterKnownSites(gomock.Any(), gomock.Any()).Return([]string{"AAA", "BBB"}, nil)
	floor := t0.Add(-time.Minute)
	first := mock.EXPECT().ListPacketsBySitesSince(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySitesSinceParams) bool {
		return p.CursorTs.InfinityModifier == pgtype.Infinity && !p.Recheck && p.PageLimit == 3 && p.ScanDepth == 24
	})).Return([]sqlc.ListPacketsBySitesSinceRow{sinceKey(t0, 9), sinceFloor(floor, 7)}, nil)
	mock.EXPECT().ListPacketsBySitesSince(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySitesSinceParams) bool {
		return p.CursorTs.Time.Equal(floor) && bytes.Equal(p.CursorHash, []byte{7}) && p.Recheck && p.PageLimit == 2 && p.ScanDepth == 24
	})).After(first).Return([]sqlc.ListPacketsBySitesSinceRow{sinceKey(floor.Add(-time.Second), 5), sinceKey(floor.Add(-2*time.Second), 4)}, nil)
	mock.EXPECT().ListPacketsBySiteKeys(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySiteKeysParams) bool {
		return len(p.PacketHashes) == 3 && p.PacketHashes[0][0] == 9 && p.PacketHashes[2][0] == 4
	})).Return([]sqlc.ListPacketsBySiteKeysRow{
		sqlc.ListPacketsBySiteKeysRow(siteRowAt(t0, 9)), sqlc.ListPacketsBySiteKeysRow(siteRowAt(floor.Add(-time.Second), 5)),
		sqlc.ListPacketsBySiteKeysRow(siteRowAt(floor.Add(-2*time.Second), 4)),
	}, nil)
	s := &Store{q: mock}
	WithReadModels(earnedPacketSites())(s)
	page, err := s.ListPackets(context.Background(), manyPT, manyRT, []string{"AAA", "BBB"}, nil, time.Time{}, time.Time{}, 0, nil, 2)
	if err != nil || len(page.Items) != 2 || !page.HasMore || page.NextPageCursor == nil {
		t.Fatalf("page %+v, %v", page, err)
	}
	if want := (api.PacketCursor{At: floor.Add(-time.Second), Hash: []byte{5}}).String(); *page.NextPageCursor != want {
		t.Fatalf("next %s, want %s", *page.NextPageCursor, want)
	}
}

// When the bounded reads of a many-pair request run out of their budget before the page is full, every pair's range
// answers it from the same cursor, with the re-check only with a cursor and two or more sites.
func TestListPacketsRegionBoundedFallsBackToPairs(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	pc := &api.PacketCursor{At: t0.Add(-time.Hour + 123*time.Microsecond), Hash: []byte{1}}
	exhausted := func(mock *mockdb.MockQuerier, sites []string) {
		mock.EXPECT().FilterKnownSites(gomock.Any(), gomock.Any()).Return(sites, nil)
		calls := 0
		mock.EXPECT().ListPacketsBySitesSince(gomock.Any(), gomock.Any()).Times(siteBoundedRounds).DoAndReturn(
			func(_ context.Context, p sqlc.ListPacketsBySitesSinceParams) ([]sqlc.ListPacketsBySitesSinceRow, error) {
				calls++
				return []sqlc.ListPacketsBySitesSinceRow{sinceFloor(t0.Add(-time.Duration(calls)*24*time.Hour), byte(calls))}, nil
			})
	}
	rows := make([]siteRow, 6)
	for i := range rows {
		rows[i] = siteRowAt(t0.Add(-time.Duration(i+2)*time.Hour), byte(6-i))
	}
	typed := make([]sqlc.ListPacketsBySitesTypedRow, len(rows))
	first := make([]sqlc.ListPacketsBySitesTypedFirstRow, len(rows))
	for i, r := range rows {
		typed[i], first[i] = sqlc.ListPacketsBySitesTypedRow(r), sqlc.ListPacketsBySitesTypedFirstRow(r)
	}
	want := api.PacketCursor{At: t0.Add(-6 * time.Hour), Hash: []byte{2}}.String()
	check := func(t *testing.T, page api.PacketPage, err error) {
		t.Helper()
		if err != nil || len(page.Items) != 5 || !page.HasMore || page.NextPageCursor == nil || *page.NextPageCursor != want {
			t.Fatalf("page %+v, %v", page, err)
		}
	}
	t.Run("page cursor, two sites", func(t *testing.T) {
		mock := mockdb.NewMockQuerier(gomock.NewController(t))
		exhausted(mock, []string{"AAA", "BBB"})
		mock.EXPECT().ListPacketsBySitesTyped(gomock.Any(), sqlc.ListPacketsBySitesTypedParams{
			Iatas: []string{"AAA", "BBB"}, PayloadTypes: manyPT, RouteTypes: manyRT, CursorTs: pgtype.Timestamptz{Time: pc.At, Valid: true}, CursorHash: pc.Hash, PageLimit: 6,
		}).Return(typed, nil)
		s := &Store{q: mock}
		WithReadModels(earnedPacketSites())(s)
		page, err := s.ListPackets(context.Background(), manyPT, manyRT, []string{"AAA", "BBB"}, nil, time.Time{}, time.Time{}, 0, pc, 5)
		check(t, page, err)
	})
	t.Run("page 1", func(t *testing.T) {
		mock := mockdb.NewMockQuerier(gomock.NewController(t))
		exhausted(mock, []string{"AAA", "BBB"})
		mock.EXPECT().ListPacketsBySitesTypedFirst(gomock.Any(), sqlc.ListPacketsBySitesTypedFirstParams{
			Iatas: []string{"AAA", "BBB"}, PayloadTypes: manyPT, RouteTypes: manyRT, CursorTs: infinityTS, CursorHash: []byte{}, PageLimit: 6,
		}).Return(first, nil)
		s := &Store{q: mock}
		WithReadModels(earnedPacketSites())(s)
		page, err := s.ListPackets(context.Background(), manyPT, manyRT, []string{"AAA", "BBB"}, nil, time.Time{}, time.Time{}, 0, nil, 5)
		check(t, page, err)
	})
}

// hasMore counts the keys: a packet removed between the keys and their rows does not end the walk.
func TestListPacketsRegionHasMoreFromKeys(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	mock.EXPECT().FilterKnownSites(gomock.Any(), gomock.Any()).Return([]string{"AAA"}, nil)
	key := func(at time.Time, h byte) sqlc.ListPacketsBySitesWindowRow {
		return sqlc.ListPacketsBySitesWindowRow{PacketHash: []byte{h}, SiteHeardAt: pgtype.Timestamptz{Time: at, Valid: true}}
	}
	mock.EXPECT().ListPacketsBySitesWindow(gomock.Any(), gomock.Any()).Return([]sqlc.ListPacketsBySitesWindowRow{
		key(t0, 3), key(t0.Add(-time.Second), 2), key(t0.Add(-2*time.Second), 1),
	}, nil)
	mock.EXPECT().ListPacketsBySiteKeys(gomock.Any(), gomock.Any()).Return([]sqlc.ListPacketsBySiteKeysRow{
		sqlc.ListPacketsBySiteKeysRow(siteRowAt(t0, 3)), sqlc.ListPacketsBySiteKeysRow(siteRowAt(t0.Add(-2*time.Second), 1)),
	}, nil)
	s := &Store{q: mock}
	WithReadModels(earnedPacketSites())(s)
	page, err := s.ListPackets(context.Background(), nil, nil, []string{"AAA"}, nil, t0.Add(-time.Hour), time.Time{}, 0, nil, 2)
	if err != nil || len(page.Items) != 2 || !page.HasMore {
		t.Fatalf("page %+v, %v", page, err)
	}
}

// Untrusted (no read models, disabled, invalidated, never earned): today's statement with today's params; a
// pageCursor sent alone pages by its millisecond; no nextPageCursor.
// Until this process trusts packet_sites (never earned, turned off, invalidated, or no read models at all), a
// region page answers ErrPagesNotReady and runs no statement, whatever its cursor.
func TestListPacketsRegionUntrustedNotReady(t *testing.T) {
	pc := &api.PacketCursor{At: time.Date(2026, 1, 9, 0, 0, 0, 123456000, time.UTC), Hash: []byte{0xab}}
	disabled := NewTrust("packet_sites_untrusted_disabled", false, PacketSitesTrustPolicy)
	disabled.Earn()
	invalidated := NewTrust("packet_sites_untrusted_invalidated", true, PacketSitesTrustPolicy)
	invalidated.Earn()
	invalidated.Invalidate("test")
	for name, rm := range map[string]*ReadModels{
		"nil read models": nil,
		"not earned":      {PacketSites: NewTrust("packet_sites_untrusted_new", true, PacketSitesTrustPolicy)},
		"disabled":        {PacketSites: disabled},
		"invalidated":     {PacketSites: invalidated},
	} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				cursor int64
				pc     *api.PacketCursor
				pt     []int16
				since  time.Time
			}{
				{5, pc, nil, time.Time{}},
				{0, pc, nil, time.Time{}},
				{0, nil, nil, time.Time{}},
				{0, nil, []int16{4, 5, 6}, time.Time{}},
				{0, nil, nil, pc.At},
			} {
				ctrl := gomock.NewController(t)
				s := &Store{q: mockdb.NewMockQuerier(ctrl)} // strict: any statement fails the test
				WithReadModels(rm)(s)
				page, err := s.ListPackets(context.Background(), tc.pt, nil, []string{"AAA", "BBB"}, nil, tc.since, time.Time{}, tc.cursor, tc.pc, 5)
				if !errors.Is(err, api.ErrPagesNotReady) || page.Items != nil {
					t.Fatalf("cursor %d: page %+v, err %v; want ErrPagesNotReady", tc.cursor, page, err)
				}
				ctrl.Finish()
			}
		})
	}
}

// windowBounds lowers the page start to hi only for an infinite cursor or one above hi, and keeps every unbounded
// value for times outside years 1-9999.
func TestWindowBounds(t *testing.T) {
	until := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	hi := until.Add(siteSkew)
	for _, tc := range []struct {
		name         string
		since, until time.Time
		pc           *api.PacketCursor
		wantLo       string
		wantHi       string
		wantMain     string
		lowered      bool
	}{
		{name: "no window", wantLo: "-infinity", wantHi: "infinity", wantMain: "infinity"},
		{name: "since", since: until, wantLo: until.Add(-siteSkew).Format(time.RFC3339Nano), wantHi: "infinity", wantMain: "infinity"},
		{name: "until, page 1", until: until, wantLo: "-infinity", wantHi: hi.Format(time.RFC3339Nano), wantMain: hi.Format(time.RFC3339Nano), lowered: true},
		{name: "until, cursor above hi", until: until, pc: &api.PacketCursor{At: hi.Add(time.Microsecond), Hash: []byte{1}},
			wantLo: "-infinity", wantHi: hi.Format(time.RFC3339Nano), wantMain: hi.Format(time.RFC3339Nano), lowered: true},
		{name: "until, cursor at hi", until: until, pc: &api.PacketCursor{At: hi, Hash: []byte{1}},
			wantLo: "-infinity", wantHi: hi.Format(time.RFC3339Nano), wantMain: hi.Format(time.RFC3339Nano)},
		{name: "until, cursor below", until: until, pc: &api.PacketCursor{At: until, Hash: []byte{1}},
			wantLo: "-infinity", wantHi: hi.Format(time.RFC3339Nano), wantMain: until.Format(time.RFC3339Nano)},
		{name: "until past year 9999", until: time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC), wantLo: "-infinity", wantHi: "infinity", wantMain: "infinity"},
		{name: "since before year 1", since: time.Date(0, 6, 1, 0, 0, 0, 0, time.UTC), wantLo: "-infinity", wantHi: "infinity", wantMain: "infinity"},
		{name: "cursor past year 9999", until: until, pc: &api.PacketCursor{At: time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC), Hash: []byte{1}},
			wantLo: "-infinity", wantHi: hi.Format(time.RFC3339Nano), wantMain: "12000-01-01T00:00:00Z"},
	} {
		f, _ := newPacketFilter(nil, nil, nil, tc.since, tc.until, 0, tc.pc, 50)
		lo, h, main, mainHash := windowBounds(f)
		str := func(ts pgtype.Timestamptz) string {
			switch ts.InfinityModifier {
			case pgtype.Infinity:
				return "infinity"
			case pgtype.NegativeInfinity:
				return "-infinity"
			}
			return ts.Time.UTC().Format(time.RFC3339Nano)
		}
		got := []string{str(lo), str(h), str(main)}
		want := []string{tc.wantLo, tc.wantHi, tc.wantMain}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%s: bound %d = %s, want %s", tc.name, i, got[i], want[i])
			}
		}
		if lowered := len(mainHash) == maxCursorHashBytes; lowered != tc.lowered {
			t.Errorf("%s: start lowered to hi = %v, want %v", tc.name, lowered, tc.lowered)
		}
	}
}
