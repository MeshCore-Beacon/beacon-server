// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/jackc/pgx/v5"
	meshcore "github.com/meshcore-go/meshcore-go"
)

func TestPathsPostgres(t *testing.T) {
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
CREATE TABLE packet_observations(heard_at timestamptz NOT NULL,iata char(3) NOT NULL,payload_type smallint,path_length_byte smallint NOT NULL,hash_size smallint NOT NULL,hop_count smallint NOT NULL,path_bytes bytea);
INSERT INTO packet_observations
SELECT CASE id WHEN 1 THEN '2026-01-01 00:30+00'::timestamptz WHEN 17 THEN '2026-01-01 04:00+00'::timestamptz WHEN 18 THEN '2026-01-01 04:30+00'::timestamptz WHEN 20 THEN '2026-01-01 03:00+00'::timestamptz
 ELSE '2026-01-01 00:30+00'::timestamptz+(id/10)*interval '2 hour'+(id%10)*interval '1 minute' END,
 iata,payload,raw,width,hops,CASE WHEN size IS NULL THEN NULL ELSE decode(repeat('ab',size),'hex') END
FROM (VALUES
 (1,'YVR',4,2,1,2,2),(2,'YVR',4,67,2,3,6),(3,'YYJ',5,130,3,2,6),
 (4,'YVR',4,64,2,0,NULL),(5,'YVR',9,3,1,3,3),(6,'YYJ',NULL,2,1,2,2),
 (7,'YVR',4,3,1,3,2),(8,'YVR',4,194,4,2,8),(9,'YVR',4,96,2,32,64),
 (10,'YVR',4,97,2,33,66),(11,'YVR',4,64,1,0,0),(12,'YVR',4,0,1,1,1),
 (13,'YYZ',5,0,1,0,0),(14,'YVR',16,1,1,1,1),(15,'YVR',4,63,1,63,63),
 (16,'YVR',4,149,3,21,63),(17,'YVR',4,1,1,1,1),(18,'YVR',4,1,1,1,1),
 (19,'YYJ',9,3,1,3,3),(20,'YVR',4,64,2,0,0)
) v(id,iata,payload,raw,width,hops,size);`)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Now().UTC().Truncate(24 * time.Hour).Add(-48 * time.Hour)
	if _, err := tx.Exec(ctx, "UPDATE packet_observations SET heard_at=to_timestamp(extract(epoch FROM $1::timestamptz)+extract(epoch FROM heard_at)-extract(epoch FROM '2026-01-01 00:00+00'::timestamptz))", since); err != nil {
		t.Fatal(err)
	}
	applyStatsMigration(t, ctx, tx, "036_mv_path_stats.sql")
	applyStatsMigration(t, ctx, tx, "036_mv_path_stats.sql") // interrupted journal retry
	store := &Store{q: sqlc.New(tx)}
	until := since.Add(4 * time.Hour)
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode="+mode); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			iatas  []string
			counts [5]int64
		}{
			{"global", nil, [5]int64{18, 6, 3, 2, 7}}, {"empty filter", []string{}, [5]int64{18, 6, 3, 2, 7}},
			{"YVR", []string{"YVR"}, [5]int64{14, 5, 2, 1, 6}}, {"overlap", []string{"YVR", "YYJ", "YVR"}, [5]int64{17, 6, 2, 2, 7}},
			{"unknown", []string{"ZZZ"}, [5]int64{}}, {"empty region", []string{""}, [5]int64{}},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				got, err := store.GetPathStats(ctx, since, until, tc.iatas)
				if err != nil {
					t.Fatal(err)
				}
				if [5]int64{got.Receptions, got.Hashed, got.Empty, got.Trace, got.Unclassified} != tc.counts {
					t.Fatalf("counts: %+v", got)
				}
				var widths, lengths, hours int64
				for _, b := range got.HashWidths {
					widths += b.Receptions
				}
				for _, b := range got.PathLengths {
					lengths += b.Receptions
				}
				for i, h := range got.Hourly {
					hours += h.Receptions
					if h.Receptions != h.OneByte+h.TwoByte+h.ThreeByte+h.Empty+h.Trace+h.Unclassified || h.Hour%3600000 != 0 || (i > 0 && h.Hour <= got.Hourly[i-1].Hour) {
						t.Fatalf("hour: %+v", h)
					}
				}
				if widths != got.Hashed || lengths != got.Hashed+got.Empty || hours != got.Receptions {
					t.Fatal("partitions do not reconcile")
				}
				if tc.name == "global" && (got.HashWidths[0].Receptions != 2 || got.HashWidths[1].Receptions != 2 || got.HashWidths[2].Receptions != 2 || got.PathLengths[0].Entries != 0 || got.PathLengths[0].Receptions != 3 || len(got.Hourly) != 3) {
					t.Fatalf("empty/trace widths or missing hour: %+v", got)
				}
			})
		}
	}
	w := httptest.NewRecorder()
	handlers.StatsRouter(store).ServeHTTP(w, httptest.NewRequest("GET", fmt.Sprintf("/paths?since=%d&until=%d&iatas=YVR", since.UnixMilli()+123, until.UnixMilli()+123), nil).WithContext(ctx))
	var response api.PathStats
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || response.Receptions != 14 || response.Hashed != 5 {
		t.Fatalf("HTTP %d: %s (%v)", w.Code, w.Body.String(), err)
	}
	if _, err = tx.Exec(ctx, "TRUNCATE packet_observations"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO packet_observations SELECT $1::timestamptz+interval '1 hour','YVR',4,i,(i>>6)+1,i&63,decode(repeat('ab',((i>>6)+1)*(i&63)),'hex') FROM generate_series(0,255)i;`, since); err != nil {
		t.Fatal(err)
	}
	stale, err := store.GetPathStats(ctx, since, until, nil)
	if err != nil || stale.Receptions != 18 {
		t.Fatalf("snapshot lost: %+v %v", stale, err)
	}
	if err := store.RefreshPathStats(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetPathStats(ctx, since, until, nil)
	if err != nil {
		t.Fatal(err)
	}
	var hashed, empty, unknown int64
	var widths [3]int64
	for raw := 0; raw < 256; raw++ {
		if !meshcore.IsValidPathLen(uint8(raw)) {
			unknown++
		} else if raw&63 == 0 {
			empty++
		} else {
			hashed++
			widths[raw>>6]++
		}
	}
	if got.Hashed != hashed || got.Empty != empty || got.Unclassified != unknown || got.Receptions != 256 {
		t.Fatalf("decoder agreement: %+v", got)
	}
	for i, count := range widths {
		if got.HashWidths[i].Receptions != count {
			t.Fatal("decoder width disagreement")
		}
	}
	if _, err = tx.Exec(ctx, "TRUNCATE packet_observations"); err != nil {
		t.Fatal(err)
	}
	if err := store.RefreshPathStats(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetPathStats(ctx, since, until, nil)
	if err != nil || got.Receptions != 0 || got.Hourly == nil || got.PathLengths == nil || len(got.HashWidths) != 3 {
		t.Fatalf("empty: %+v %v", got, err)
	}
}
