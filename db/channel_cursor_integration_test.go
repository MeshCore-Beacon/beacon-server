// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/jackc/pgx/v5"
)

type channelCursorQueries struct {
	sqlc.DBTX
	query string
	args  []any
}

func (q *channelCursorQueries) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	q.query, q.args = query, args
	return q.DBTX.Query(ctx, query, args...)
}

func TestChannelCursorPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	exec := func(sql string) {
		t.Helper()
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TEMP TABLE channels (LIKE public.channels INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE channel_iatas (LIKE public.channel_iatas INCLUDING ALL) ON COMMIT DROP;
CREATE INDEX ON channels(last_seen DESC,id DESC);
INSERT INTO channels(id,channel_hash,key_fingerprint,last_seen) VALUES
 (1,'\xaa','\x01','2026-09-08 12:00:00+00'),(2,'\xaa','\x02','2026-09-08 12:00:00+00'),
 (3,'\xaa','\x03','2026-09-08 12:00:00+00'),(4,'\xbb','\x04','2026-09-08 11:59:59+00'),
 (5,'\xcc','\x05','2026-09-08 12:00:00.000321+00'),
 (6,'\xcc','\x06','2026-09-08 12:00:00.000111+00'),(7,'\xcc','\x07','2026-09-08 12:00:00.000111+00');
INSERT INTO channel_iatas(channel_hash,iata,last_heard) VALUES
 ('\xaa','YOW',now()),('\xbb','YOW',now()),('\xcc','YYZ',now());`)
	queries := &channelCursorQueries{DBTX: tx}
	router := handlers.ChannelsRouter(&Store{q: sqlc.New(queries)})
	// Decode the wire contract so the same regression runs against the old server.
	type wirePage struct {
		Items          []api.ChannelSummary `json:"items"`
		NextCursor     *int64               `json:"nextCursor"`
		NextPageCursor *string              `json:"nextPageCursor"`
		HasMore        bool                 `json:"hasMore"`
	}
	request := func(t *testing.T, params url.Values) wirePage {
		t.Helper()
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/?"+params.Encode(), nil).WithContext(ctx))
		if w.Code != http.StatusOK {
			t.Fatalf("channel page HTTP %d", w.Code)
		}
		var page wirePage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	for _, tc := range []struct {
		name, query string
		want        []int
	}{
		{"all ties and precision", "", []int{5, 7, 6, 3, 2, 1, 4}},
		{"hash", "hash=AA", []int{3, 2, 1}},
		{"region", "iata=yow", []int{3, 2, 1, 4}},
		{"multi region", "iatas=yow,yyz", []int{5, 7, 6, 3, 2, 1, 4}},
		{"hash and region", "hash=cc&iata=yyz", []int{5, 7, 6}},
		{"no regional match", "hash=aa&iata=yyz", nil},
		{"missing region", "iata=ZZZ", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, _ := url.ParseQuery(tc.query)
			params.Set("limit", "2")
			params.Set("cursor", "0") // Clients may retain the legacy first-page value.
			var got []int
			for n := 0; n < 10; n++ {
				page := request(t, params)
				for _, item := range page.Items {
					got = append(got, item.ID)
				}
				if !page.HasMore {
					if page.NextCursor != nil || page.NextPageCursor != nil {
						t.Error("final page retains a cursor")
					}
					break
				}
				if page.NextCursor == nil {
					t.Fatal("legacy numeric cursor was removed")
				}
				if page.NextPageCursor != nil {
					params.Set("pageCursor", *page.NextPageCursor)
				} else {
					params.Set("cursor", strconv.FormatInt(*page.NextCursor, 10))
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("paged IDs %v; want %v", got, tc.want)
			}
		})
	}
	// The integer cursor still has its original strict millisecond semantics.
	legacy := request(t, url.Values{"limit": {"50"}, "cursor": {"1788868800000"}})
	if len(legacy.Items) != 1 || legacy.Items[0].ID != 4 {
		t.Error("legacy cursor semantics changed")
	}
	t.Run("newer activity stays ahead of the boundary", func(t *testing.T) {
		first := request(t, url.Values{"limit": {"2"}})
		if first.NextPageCursor == nil {
			t.Fatal("precise cursor missing")
		}
		exec(`INSERT INTO channels(id,channel_hash,key_fingerprint,last_seen)
VALUES (8,'\xee','\x08','2026-09-08 12:00:02+00')`)
		rest := request(t, url.Values{"limit": {"50"}, "pageCursor": {*first.NextPageCursor}})
		var ids []int
		for _, item := range rest.Items {
			ids = append(ids, item.ID)
		}
		if !reflect.DeepEqual(ids, []int{6, 3, 2, 1, 4}) {
			t.Errorf("new activity changed older traversal: %v", ids)
		}
		fresh := request(t, url.Values{"limit": {"2"}})
		if fresh.Items[0].ID != 8 {
			t.Error("fresh first page missed new activity")
		}
		exec("DELETE FROM channels WHERE id=8")
	})

	exec(`INSERT INTO channels(id,channel_hash,key_fingerprint,last_seen)
SELECT i,'\xdd',decode(lpad(to_hex(i),16,'0'),'hex'),
 '2026-09-09 12:00:00+00'::timestamptz-i*interval '1 millisecond'
FROM generate_series(1001,21000) i;
ANALYZE channels;`)
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		t.Run(mode, func(t *testing.T) {
			exec("SET LOCAL plan_cache_mode = '" + mode + "'")
			at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC).Add(-20 * time.Second)
			page := request(t, url.Values{"limit": {"50"}, "pageCursor": {fmt.Sprintf("v1:%d:20000", at.UnixMicro())}})
			if len(page.Items) != 50 || page.Items[0].ID != 20001 {
				t.Fatal("deep cursor returned the wrong page")
			}
			var raw []byte
			if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+queries.query, queries.args...).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var plans []map[string]any
			if err := json.Unmarshal(raw, &plans); err != nil {
				t.Fatal(err)
			}
			rows := 0.0
			indexed := false
			var visit func(map[string]any)
			visit = func(plan map[string]any) {
				if plan["Relation Name"] == "channels" {
					n, _ := plan["Actual Rows"].(float64)
					removed, _ := plan["Rows Removed by Filter"].(float64)
					loops, _ := plan["Actual Loops"].(float64)
					rows += (n + removed) * loops
					_, indexed = plan["Index Cond"]
				}
				children, _ := plan["Plans"].([]any)
				for _, child := range children {
					visit(child.(map[string]any))
				}
			}
			visit(plans[0]["Plan"].(map[string]any))
			if rows > 60 || !indexed {
				t.Errorf("deep page examines %.0f rows, indexed=%v", rows, indexed)
			}
			t.Logf("%s: examined %.0f rows; execution %v ms", mode, rows, plans[0]["Execution Time"])
		})
	}
}
