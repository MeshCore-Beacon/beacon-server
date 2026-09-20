// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/jackc/pgx/v5"
)

// Real view DDL and fixture rows are isolated in a rolled-back private schema.
func TestSignalPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	isolateStatsSchema(t, ctx, tx)
	_, err = tx.Exec(ctx, `
SET LOCAL TIME ZONE 'America/Vancouver';
CREATE TABLE packet_observations (heard_at timestamptz NOT NULL, iata char(3) NOT NULL, snr real, rssi smallint);
INSERT INTO packet_observations
SELECT ('2026-01-01 '||at||'+00')::timestamptz,iata,snr::real,rssi::smallint FROM (VALUES
 ('00:30:00','YVR','0',-100), ('00:45:00','YVR','0',0), ('00:59:00','YVR',NULL,NULL),
 ('01:00:00','YVR','-30',-140), ('01:10:00','YVR','-30.25',-141), ('01:20:00','YVR','30',0),
 ('01:30:00','YVR','5',NULL), ('01:40:00','YVR',NULL,-90), ('01:50:00','YVR','NaN',-80),
 ('02:00:00','YVR','Infinity',-70), ('02:10:00','YVR','-Infinity',-60),
 ('02:20:00','YYJ','10',-50), ('03:00:00','YYZ','-5',-110),
 ('04:00:00','YVR','20',-40), ('04:30:00','YVR','20',-40),
 ('03:10:00','YVR','0',NULL), ('03:20:00','YVR',NULL,0)
) v(at,iata,snr,rssi);`)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Now().UTC().Truncate(24 * time.Hour).Add(-48 * time.Hour)
	if _, err := tx.Exec(ctx, "UPDATE packet_observations SET heard_at=heard_at+($1::timestamptz-'2026-01-01 00:00+00'::timestamptz)", since); err != nil {
		t.Fatal(err)
	}
	applyStatsMigration(t, ctx, tx, "035_mv_signal_stats.sql")
	applyStatsMigration(t, ctx, tx, "035_mv_signal_stats.sql") // interrupted journal retry
	store := &Store{q: sqlc.New(tx)}
	until := since.Add(4 * time.Hour)
	for _, tc := range []struct {
		name                  string
		iatas                 []string
		receptions, snr, rssi int64
	}{
		{"global", nil, 15, 7, 10}, {"empty filter", []string{}, 15, 7, 10}, {"YVR", []string{"YVR"}, 13, 5, 8},
		{"duplicate/overlapping IATAs", []string{"YVR", "YYJ", "YVR"}, 14, 6, 9},
		{"unknown IATA", []string{"ZZZ"}, 0, 0, 0}, {"empty region", []string{""}, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.GetSignalStats(ctx, since, until, tc.iatas)
			if err != nil {
				t.Fatal(err)
			}
			if got.Receptions != tc.receptions || got.SNR.Samples != tc.snr || got.RSSI.Samples != tc.rssi {
				t.Fatalf("counts: %+v", got)
			}
			for _, metric := range []api.SignalMetric{got.SNR, got.RSSI} {
				var total int64
				for _, bin := range metric.Histogram {
					total += bin.Count
				}
				if total != metric.Samples || (metric.Average == nil) != (metric.Samples == 0) {
					t.Fatalf("histogram/average: %+v", metric)
				}
			}
			var receptions, snr, rssi int64
			for i, hour := range got.Hourly {
				if hour.Hour%3600000 != 0 || hour.Hour < since.Truncate(time.Hour).UnixMilli() || hour.Hour >= until.UnixMilli() || (i > 0 && hour.Hour <= got.Hourly[i-1].Hour) {
					t.Fatalf("UTC order/window: %+v", got.Hourly)
				}
				receptions += hour.Receptions
				snr += hour.SNRSamples
				rssi += hour.RSSISamples
			}
			if receptions != got.Receptions || snr != got.SNR.Samples || rssi != got.RSSI.Samples {
				t.Fatal("hourly totals differ")
			}
			if _, err := json.Marshal(got); err != nil {
				t.Fatal(err)
			}
			if tc.name == "YVR" {
				if math.Abs(*got.SNR.Average-(-5.05)) > 1e-9 || *got.RSSI.Average != -85.125 {
					t.Fatalf("unweighted/missing readings: %+v", got)
				}
				if got.SNR.Histogram[0].Count != 1 || got.SNR.Histogram[1].Count != 1 || got.SNR.Histogram[7].Count != 1 || got.SNR.Histogram[8].Count != 1 || got.SNR.Histogram[13].Count != 1 {
					t.Fatalf("SNR boundaries/zero: %+v", got.SNR.Histogram)
				}
				if got.RSSI.Histogram[0].Count != 1 || got.RSSI.Histogram[1].Count != 1 || got.RSSI.Histogram[15].Count != 1 {
					t.Fatalf("RSSI boundaries: %+v", got.RSSI.Histogram)
				}
				last := got.Hourly[len(got.Hourly)-1]
				if last.Receptions != 2 || last.SNRAverage != nil || last.RSSIAverage != nil {
					t.Fatalf("missing-only hour: %+v", last)
				}
			}
		})
	}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		if _, err = tx.Exec(ctx, "SET LOCAL plan_cache_mode="+mode); err != nil {
			t.Fatal(err)
		}
		got, err := store.GetSignalStats(ctx, since, until, []string{"YVR"})
		if err != nil || got.Receptions != 13 || got.SNR.Samples != 5 || got.RSSI.Samples != 8 {
			t.Fatalf("%s: %+v %v", mode, got, err)
		}
	}
	w := httptest.NewRecorder()
	request := httptest.NewRequest("GET", fmt.Sprintf("/signal?since=%d&until=%d&iatas=YVR", since.UnixMilli()+123, until.UnixMilli()+123), nil).WithContext(ctx)
	handlers.StatsRouter(store).ServeHTTP(w, request)
	var response api.SignalStats
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || response.Receptions != 13 || response.SNR.Samples != 5 {
		t.Fatalf("PostgreSQL HTTP response: status=%d body=%s error=%v", w.Code, w.Body.String(), err)
	}
	if _, err = tx.Exec(ctx, "TRUNCATE packet_observations"); err != nil {
		t.Fatal(err)
	}
	stale, err := store.GetSignalStats(ctx, since, until, nil)
	if err != nil || stale.Receptions != 15 {
		t.Fatalf("snapshot lost: %+v %v", stale, err)
	}
	if err := store.RefreshSignalStats(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSignalStats(ctx, since, until, nil)
	if err != nil || got.Receptions != 0 || got.Hourly == nil || len(got.Hourly) != 0 || got.SNR.Average != nil {
		t.Fatalf("empty: %+v %v", got, err)
	}
}
