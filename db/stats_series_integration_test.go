// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// seriesOracle computes a summary straight from raw rows over [since, until).
const seriesOracle = `
WITH po AS (
  SELECT po.*, date_trunc('hour', po.heard_at, 'UTC') AS hour, p.scope_id
  FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.heard_at >= $1 AND po.heard_at < $2
    AND (cardinality($3::bpchar[]) = 0 OR po.iata = ANY($3::bpchar[]))
    AND date_trunc('hour', po.heard_at, 'UTC') <> ALL($4::timestamptz[])
), sig AS (
  SELECT * FROM po WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
)
SELECT (SELECT count(*) FROM po),
       (SELECT count(*) FROM (SELECT DISTINCT hour, packet_hash FROM po) d),
       (SELECT count(DISTINCT observer_id) FROM po),
       (SELECT count(DISTINCT iata) FROM po),
       (SELECT count(*) FROM (SELECT DISTINCT hour, packet_hash FROM po WHERE scope_id IS NOT NULL) d),
       (SELECT count(DISTINCT scope_id) FROM po),
       (SELECT COALESCE(sum(snr), 0)::double precision FROM sig), (SELECT count(snr) FROM sig),
       (SELECT COALESCE(sum(rssi), 0)::double precision FROM sig), (SELECT count(rssi) FROM sig)`

func TestStatsSeriesPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	if err := r.RegisterHours(ctx, h0); err != nil {
		t.Fatal(err)
	}
	rollAll(t, ctx, r)
	r.Close(ctx)
	since, until := h0, h0.Add(3*time.Hour)

	for _, tc := range []struct {
		name     string
		iatas    []string
		excluded []time.Time // hours not complete
		maxPath  int32
	}{
		{"all", nil, nil, 2},
		{"one IATA", []string{"YYZ"}, nil, 1},
		{"overlapping IATAs", []string{"YVR", "YYZ", "YVR"}, nil, 2},
		{"empty region", []string{""}, nil, 0},
		{"partial hour excluded", nil, []time.Time{h0.Add(time.Hour)}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			for _, h := range tc.excluded {
				if _, err := tx.Exec(ctx, "UPDATE analytics_rollup_hours SET status = 'partial' WHERE hour = $1", h); err != nil {
					t.Fatal(err)
				}
			}
			got, err := (&Store{q: sqlc.New(tx)}).GetStatsSeries(ctx, since, until, tc.iatas)
			if err != nil {
				t.Fatal(err)
			}
			var want api.StatsSeriesValues
			excluded := tc.excluded
			if excluded == nil {
				excluded = []time.Time{}
			}
			iatas := tc.iatas
			if iatas == nil {
				iatas = []string{}
			}
			if err := tx.QueryRow(ctx, seriesOracle, since, until, iatas, excluded).Scan(&want.Observations, &want.UniquePackets,
				&want.ActiveObservers, &want.ActiveIATAs, &want.ScopedPackets, &want.ActiveScopes,
				&want.SNRSum, &want.SNRSamples, &want.RSSISum, &want.RSSISamples); err != nil {
				t.Fatal(err)
			}
			want.MaxPathEntries = tc.maxPath
			if got.Summary != want {
				t.Errorf("summary\n got %+v\nwant %+v", got.Summary, want)
			}
			if len(got.Hours) != 3 || got.Since != since.UnixMilli() || got.Until != until.UnixMilli() || got.EarliestComplete == nil {
				t.Fatalf("series shape: %+v", got)
			}
			var obs, packets int64
			for _, h := range got.Hours {
				partial := false
				for _, e := range tc.excluded {
					partial = partial || e.UnixMilli() == h.Hour
				}
				if partial != (h.Values == nil) || (partial && h.Status != "partial") {
					t.Errorf("hour %d status %s values %v", h.Hour, h.Status, h.Values)
				}
				if h.Values != nil {
					obs += h.Values.Observations
					packets += h.Values.UniquePackets
				}
			}
			// Cards and sparklines must agree for additive metrics.
			if obs != got.Summary.Observations || packets != got.Summary.UniquePackets {
				t.Errorf("hours sum to %d observations / %d packets, summary %d / %d", obs, packets, got.Summary.Observations, got.Summary.UniquePackets)
			}
			if got.CompleteHours != 3-len(tc.excluded) {
				t.Errorf("complete hours %d", got.CompleteHours)
			}
		})
	}

	// Hours never registered (or not yet eligible) are missing.
	got, err := store.GetStatsSeries(ctx, h0.Add(-2*time.Hour), h0, nil)
	if err != nil || len(got.Hours) != 2 || got.Hours[0].Status != "missing" || got.Hours[0].Values != nil || got.CompleteHours != 0 {
		t.Errorf("unregistered hours: %+v %v", got, err)
	}
}
