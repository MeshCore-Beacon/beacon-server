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
		{siteSince, 1, false, "ListPacketsBySitesSince"},
		{siteSince, 3, true, "ListPacketsBySitesSince"},
		{siteScoped, 3, true, "ListPacketsBySitesScoped"},
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
		{"many type pairs", []int16{4, 5, 6}, []int16{0, 1, 2}, nil, time.Time{}, siteSince},
		{"route types only", nil, []int16{3}, nil, time.Time{}, siteSince}, // 16 payload types x 1
		{"since and types", []int16{4}, nil, nil, since, siteSince},
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
			m.EXPECT().ListPacketsBySitesSince(gomock.Any(), sqlc.ListPacketsBySitesSinceParams{
				Iatas: []string{"AAA", "BBB"}, CursorTs: pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}, CursorHash: []byte{},
				ScanDepth: 51 * 8, SinceTs: pgtype.Timestamptz{Time: since, Valid: true}, UntilTs: infinityTS,
				PayloadTypes: allPayloadTypes, RouteTypes: allRouteTypes, Recheck: true, PageLimit: 51,
			}).Return(nil, nil)
		}},
		{name: "scopes and since", iatas: []string{"AAA"}, scopes: []string{"#a"}, since: since, expect: func(m *mockdb.MockQuerier) {
			known(m, []string{"AAA"}, []string{"AAA"})
			m.EXPECT().ListPacketsBySitesScoped(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsBySitesScopedParams) bool {
				return slices.Equal(p.ScopeNames, []string{"#a"}) && !p.Recheck && p.ScanDepth == 408 && p.CursorHash != nil
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
	page, err := s.ListPackets(context.Background(), nil, nil, []string{"AAA", "BBB"}, nil, t0.Add(-time.Hour), time.Time{}, 0, nil, 2)
	if err != nil || len(page.Items) != 2 || !page.HasMore || page.NextPageCursor == nil {
		t.Fatalf("page %+v, %v", page, err)
	}
	if want := (api.PacketCursor{At: floor.Add(-time.Second), Hash: []byte{5}}).String(); *page.NextPageCursor != want {
		t.Fatalf("next %s, want %s", *page.NextPageCursor, want)
	}
}

// When the bounded reads of one request run out of their budget before the page is full, today's statement answers:
// from the page cursor's exact position when the request carried one (or from the top on page 1), with the exact
// next cursor; with a millisecond cursor alone, exactly as today.
func TestListPacketsRegionBoundedFallsBack(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	pc := &api.PacketCursor{At: t0.Add(-time.Hour + 123*time.Microsecond), Hash: []byte{1}}
	exhausted := func(mock *mockdb.MockQuerier) {
		mock.EXPECT().FilterKnownSites(gomock.Any(), gomock.Any()).Return([]string{"AAA"}, nil)
		calls := 0
		mock.EXPECT().ListPacketsBySitesSince(gomock.Any(), gomock.Any()).Times(siteBoundedRounds).DoAndReturn(
			func(_ context.Context, p sqlc.ListPacketsBySitesSinceParams) ([]sqlc.ListPacketsBySitesSinceRow, error) {
				calls++
				return []sqlc.ListPacketsBySitesSinceRow{sinceFloor(t0.Add(-time.Duration(calls)*24*time.Hour), byte(calls))}, nil
			})
	}
	row := func(at time.Time, h byte) sqlc.ListPacketsByIATAsAfterRow {
		return sqlc.ListPacketsByIATAsAfterRow{PacketHash: []byte{h}, SiteHeardAt: pgtype.Timestamptz{Time: at, Valid: true}}
	}
	for _, tc := range []struct {
		name   string
		pc     *api.PacketCursor
		wantTS pgtype.Timestamptz
		wantH  []byte
	}{
		{"page cursor", pc, pgtype.Timestamptz{Time: pc.At, Valid: true}, pc.Hash},
		{"page 1", nil, infinityTS, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mock := mockdb.NewMockQuerier(ctrl)
			exhausted(mock)
			mock.EXPECT().ListPacketsByIATAsAfter(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsByIATAsAfterParams) bool {
				return p.CursorTs == tc.wantTS && bytes.Equal(p.CursorHash, tc.wantH) && p.ScanDepth == 6*8 && p.PageLimit == 6 &&
					slices.Equal(p.PayloadTypes, []int16{4, 5}) && p.SinceTs.Valid
			})).Return([]sqlc.ListPacketsByIATAsAfterRow{
				row(t0.Add(-2*time.Hour), 6), row(t0.Add(-3*time.Hour), 5), row(t0.Add(-4*time.Hour), 4),
				row(t0.Add(-5*time.Hour), 3), row(t0.Add(-6*time.Hour), 2), row(t0.Add(-7*time.Hour), 1),
			}, nil)
			s := &Store{q: mock}
			WithReadModels(earnedPacketSites())(s)
			page, err := s.ListPackets(context.Background(), []int16{4, 5}, nil, []string{"AAA"}, nil, t0.Add(-48*time.Hour), time.Time{}, 0, tc.pc, 5)
			want := api.PacketCursor{At: t0.Add(-6 * time.Hour), Hash: []byte{2}}.String()
			if err != nil || len(page.Items) != 5 || !page.HasMore || page.NextPageCursor == nil || *page.NextPageCursor != want {
				t.Fatalf("fallback page %+v, %v", page, err)
			}
		})
	}
	t.Run("millisecond cursor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mock := mockdb.NewMockQuerier(ctrl)
		exhausted(mock)
		const cursor = int64(1792442625019)
		mock.EXPECT().ListPacketsByIATAs(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsByIATAsParams) bool {
			return p.CursorTs.Time.Equal(time.UnixMilli(cursor)) && p.ScanDepth == 6*8 && p.PageLimit == 6
		})).Return([]sqlc.ListPacketsByIATAsRow{{PacketHash: []byte{2}, SiteHeardAt: pgtype.Timestamptz{Time: t0.Add(-2 * time.Hour), Valid: true}}}, nil)
		s := &Store{q: mock}
		WithReadModels(earnedPacketSites())(s)
		page, err := s.ListPackets(context.Background(), []int16{4, 5}, nil, []string{"AAA"}, nil, t0.Add(-48*time.Hour), time.Time{}, cursor, nil, 5)
		if err != nil || len(page.Items) != 1 || page.HasMore || page.NextPageCursor != nil {
			t.Fatalf("fallback page %+v, %v", page, err)
		}
	})
}

// hasMore counts the keys: a packet removed between the keys and their rows does not end the walk.
func TestListPacketsRegionHasMoreFromKeys(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	mock.EXPECT().FilterKnownSites(gomock.Any(), gomock.Any()).Return([]string{"AAA"}, nil)
	mock.EXPECT().ListPacketsBySitesSince(gomock.Any(), gomock.Any()).Return([]sqlc.ListPacketsBySitesSinceRow{
		sinceKey(t0, 3), sinceKey(t0.Add(-time.Second), 2), sinceKey(t0.Add(-2*time.Second), 1),
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
