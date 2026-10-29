// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/obscount"
	"github.com/google/uuid"
)

// The ingest process's live counts must give the directory exactly what re-reading the raw rows gives.
func TestObserverDirectoryLiveCountsMatchRawPostgres(t *testing.T) {
	ctx, tx, raw := directoryFixture(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := start
	counts := obscount.New(func() time.Time { return clock }) // covers hours from 01:00
	live := &Store{q: sqlc.New(tx), obsCounts: counts}
	if _, err := tx.Exec(ctx, `
 INSERT INTO analytics_rollup_hours(hour,status) VALUES
 ('2026-01-01 00:00:00+00','complete'), ('2026-01-01 01:00:00+00','complete'),
 ('2026-01-01 02:00:00+00','missing'), ('2026-01-01 03:00:00+00','partial'), ('2026-01-01 04:00:00+00','missing');
 INSERT INTO analytics_hourly_observer_identity(hour,iata,observer_id,observation_count) VALUES
 ('2026-01-01 00:00:00+00','YVR','00000000-0000-0000-0000-000000000001',40),
 ('2026-01-01 01:00:00+00','YVR','00000000-0000-0000-0000-000000000002',7),
 ('2026-01-01 01:00:00+00','YYJ','00000000-0000-0000-0000-000000000004',3);`); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(7, 11))
	iatas := []string{"YVR", "YYJ", "YYZ"}
	for range 700 {
		at := start.Add(time.Duration(rng.Int64N(int64(5*time.Hour + 10*time.Minute))))
		observer := 1 + rng.IntN(4)
		iata := iatas[rng.IntN(len(iatas))]
		directoryObservation(t, ctx, tx, observer, iata, at)
		counts.Add(at, uuid.MustParse("00000000-0000-0000-0000-00000000000"+string(rune('0'+observer))), iata)
	}
	clock = time.Date(2026, 1, 1, 5, 9, 0, 0, time.UTC)
	until := time.Date(2026, 1, 1, 5, 7, 31, 250000000, time.UTC)
	if s := counts.Snapshot(until); !s.Live() || !s.From.Equal(start.Add(time.Hour)) {
		t.Fatalf("counts not live: %+v", s.From)
	}
	page := func(store *Store, q api.ObserverDirectoryQuery) []string {
		t.Helper()
		var pages []string
		for {
			p, err := store.ListObserverDirectory(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			cursor := p.NextCursor
			p.GeneratedAt = 0
			b, _ := json.Marshal(p)
			pages = append(pages, string(b))
			if cursor == nil {
				return pages
			}
			q.Cursor = *cursor
		}
	}
	for _, since := range []time.Time{
		time.Date(2026, 1, 1, 0, 30, 0, 123000000, time.UTC),
		time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 2, 45, 10, 0, time.UTC),
		time.Date(2026, 1, 1, 4, 0, 0, 0, time.UTC),
	} {
		for _, region := range [][]string{nil, {"YVR"}, {"YVR", "YYJ"}} {
			for _, sort := range []string{"traffic", "name"} {
				q := api.ObserverDirectoryQuery{IATAs: region, Sort: sort, Since: since.UnixMilli(), Until: until.UnixMilli(), Limit: 2}
				want, got := page(raw, q), page(live, q)
				if len(want) != len(got) {
					t.Fatalf("%v %v %s: %d pages, want %d", since, region, sort, len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%v %v %s page %d:\n live %s\n raw  %s", since, region, sort, i, got[i], want[i])
					}
				}
			}
		}
	}
	// The live path is really taken: thin the raw rows of a counted hour and the counts still hold.
	if _, err := tx.Exec(ctx, `DELETE FROM packet_observations WHERE heard_at >= '2026-01-01 02:00:00+00' AND heard_at < '2026-01-01 03:00:00+00'`); err != nil {
		t.Fatal(err)
	}
	q := api.ObserverDirectoryQuery{Sort: "traffic", Since: start.Add(2 * time.Hour).UnixMilli(), Until: start.Add(3 * time.Hour).UnixMilli(), Limit: 10}
	if a, b := page(raw, q), page(live, q); a[0] == b[0] {
		t.Fatal("the live store re-read the raw rows")
	}
}
