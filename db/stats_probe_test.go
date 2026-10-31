// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

// probeCodes returns n distinct upper-case three-letter codes: AAA, AAB, ...
func probeCodes(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%c%c%c", 'A'+i/676, 'A'+i/26%26, 'A'+i%26)
	}
	return out
}

func TestDistinctIATAs(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want int
	}{
		{nil, 0},
		{[]string{}, 0},
		{[]string{"LIS", "LIS ", "LIS"}, 1},
		{[]string{"LIS", "lis"}, 2},
		{[]string{"", " "}, 1},
		{probeCodes(33), 33},
	} {
		if got := distinctIATAs(tc.in); got != tc.want {
			t.Errorf("distinctIATAs(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestStatsWideIATAListUsesTodayStatement(t *testing.T) {
	c32 := probeCodes(32)
	dup := append(append(append([]string{}, c32...), c32[:8]...), "AAA ") // 41 entries, 32 distinct as bpchar
	lower := append(append([]string{}, c32...), "aaa")                    // 33 distinct as bpchar
	cases := []struct {
		name  string
		iatas []string
		probe bool
	}{
		{"nil", nil, true},
		{"empty", []string{}, true},
		{"empty region", []string{""}, true},
		{"one", probeCodes(1), true},
		{"32", c32, true},
		{"33", probeCodes(33), false},
		{"32 with duplicates and a trailing blank", dup, true},
		{"32 plus a lowercase member", lower, false},
	}
	since := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC) // the store snaps it to 10:00 for the rollup reads
	hour := pgtype.Timestamptz{Time: since.Truncate(time.Hour), Valid: true}
	heard := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	nodeID := pgtype.UUID{Bytes: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Valid: true}

	t.Run("top nodes", func(t *testing.T) {
		rows := []sqlc.GetTopNodesRow{
			{NodeID: nodeID, PublicKey: "aa", Name: "node", NodeType: 2, Iata: "AAA", ObservationCount: 9, LastHeard: heard},
			{PublicKey: "bb", Iata: "AAB", ObservationCount: 3, LastHeard: heard},
		}
		var want []api.TopNode
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				mock := mockdb.NewMockQuerier(gomock.NewController(t))
				if len(tc.iatas) == 0 {
					allRows := make([]sqlc.GetTopNodesAllRow, len(rows))
					for i, r := range rows {
						allRows[i] = sqlc.GetTopNodesAllRow(r)
					}
					mock.EXPECT().GetTopNodesAll(gomock.Any(), sqlc.GetTopNodesAllParams{Since: hour, RowLimit: 5}).Return(allRows, nil)
				} else if tc.probe {
					probeRows := make([]sqlc.GetTopNodesProbeRow, len(rows))
					for i, r := range rows {
						probeRows[i] = sqlc.GetTopNodesProbeRow(r)
					}
					mock.EXPECT().GetTopNodesProbe(gomock.Any(), sqlc.GetTopNodesProbeParams{Since: hour, Iatas: tc.iatas, RowLimit: 5}).Return(probeRows, nil)
				} else {
					mock.EXPECT().GetTopNodes(gomock.Any(), sqlc.GetTopNodesParams{Since: hour, Iatas: tc.iatas, RowLimit: 5}).Return(rows, nil)
				}
				got, err := (&Store{q: mock}).GetStatsTopNodes(context.Background(), tc.iatas, since, 5)
				if err != nil {
					t.Fatal(err)
				}
				if want == nil {
					want = got
				}
				if len(got) != 2 || !reflect.DeepEqual(got, want) {
					t.Errorf("got %+v, want %+v", got, want)
				}
			})
		}
	})

	t.Run("top advertisers", func(t *testing.T) {
		rows := []sqlc.GetStatsTopAdvertisersRow{
			{NodeID: nodeID, PublicKey: "cc", Name: "node", NodeType: 2, AdvertCount: 10, FloodAdvertCount: 7, DirectAdvertCount: 3, LastHeard: heard, Iata: "AAA"},
			{PublicKey: "dd", AdvertCount: 1, FloodAdvertCount: 1, LastHeard: heard, Iata: "AAB"},
		}
		var want []api.TopAdvertiser
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				mock := mockdb.NewMockQuerier(gomock.NewController(t))
				if tc.probe {
					probeRows := make([]sqlc.GetStatsTopAdvertisersProbeRow, len(rows))
					for i, r := range rows {
						probeRows[i] = sqlc.GetStatsTopAdvertisersProbeRow(r)
					}
					mock.EXPECT().GetStatsTopAdvertisersProbe(gomock.Any(), sqlc.GetStatsTopAdvertisersProbeParams{Sort: "flood", Since: hour, Iatas: tc.iatas, RowLimit: 5}).Return(probeRows, nil)
				} else {
					mock.EXPECT().GetStatsTopAdvertisers(gomock.Any(), sqlc.GetStatsTopAdvertisersParams{Sort: "flood", Since: hour, Iatas: tc.iatas, RowLimit: 5}).Return(rows, nil)
				}
				got, err := (&Store{q: mock}).GetStatsTopAdvertisers(context.Background(), tc.iatas, since, 5, api.AdvertiserSortFlood)
				if err != nil {
					t.Fatal(err)
				}
				if want == nil {
					want = got
				}
				if len(got) != 2 || !reflect.DeepEqual(got, want) {
					t.Errorf("got %+v, want %+v", got, want)
				}
			})
		}
	})

	t.Run("path stats", func(t *testing.T) {
		rows := []sqlc.GetPathStatsRow{
			{IsHourly: false, Category: 0, HashBytes: 1, Entries: 2, Receptions: 5, MaxEntries: 2},
			{IsHourly: false, Category: 1, Receptions: 1},
			{IsHourly: true, Hour: hour, Category: 0, HashBytes: 1, Receptions: 5, MaxEntries: 2},
			{IsHourly: true, Hour: hour, Category: 1, Receptions: 1},
		}
		from, until := since.Truncate(time.Hour), since.Truncate(time.Hour).Add(3*time.Hour)
		run := func(t *testing.T, iatas []string, from time.Time, probe bool) *api.PathStats {
			mock := mockdb.NewMockQuerier(gomock.NewController(t))
			s, u := pgtype.Timestamptz{Time: from, Valid: true}, pgtype.Timestamptz{Time: until, Valid: true}
			if probe {
				probeRows := make([]sqlc.GetPathStatsProbeRow, len(rows))
				for i, r := range rows {
					probeRows[i] = sqlc.GetPathStatsProbeRow(r)
				}
				mock.EXPECT().GetPathStatsProbe(gomock.Any(), sqlc.GetPathStatsProbeParams{Since: s, Until: u, Iatas: iatas}).Return(probeRows, nil)
			} else {
				mock.EXPECT().GetPathStats(gomock.Any(), sqlc.GetPathStatsParams{Since: s, Until: u, Iatas: iatas}).Return(rows, nil)
			}
			got, err := (&Store{q: mock}).GetPathStats(context.Background(), from, until, iatas)
			if err != nil {
				t.Fatal(err)
			}
			return got
		}
		var want *api.PathStats
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := run(t, tc.iatas, from, tc.probe)
				if want == nil {
					want = got
				}
				if got.Receptions != 6 || len(got.Hourly) != 1 || !reflect.DeepEqual(got, want) {
					t.Errorf("got %+v, want %+v", got, want)
				}
			})
		}
		t.Run("since off the hour", func(t *testing.T) {
			got := run(t, probeCodes(1), since, false)
			if got.Receptions != 6 || len(got.Hourly) != 1 {
				t.Errorf("got %+v", got)
			}
		})
		t.Run("until off the hour", func(t *testing.T) {
			mock := mockdb.NewMockQuerier(gomock.NewController(t))
			mock.EXPECT().GetPathStats(gomock.Any(), sqlc.GetPathStatsParams{
				Since: pgtype.Timestamptz{Time: from, Valid: true}, Until: pgtype.Timestamptz{Time: until.Add(time.Minute), Valid: true},
			}).Return(rows, nil)
			if _, err := (&Store{q: mock}).GetPathStats(context.Background(), from, until.Add(time.Minute), nil); err != nil {
				t.Fatal(err)
			}
		})
	})
}
