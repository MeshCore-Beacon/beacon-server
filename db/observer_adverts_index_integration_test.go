// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	advertsObserverA = uuid.MustParse("00000000-0000-0000-0000-0000000000a1") // adverts among many other observations
	advertsObserverB = uuid.MustParse("00000000-0000-0000-0000-0000000000b1") // adverts only
	advertsObserverC = uuid.MustParse("00000000-0000-0000-0000-0000000000c1") // no adverts
	advertsUnknown   = uuid.MustParse("00000000-0000-0000-0000-0000000000ff") // no observer row
)

// Packets of types 2, 4, 5 and 9; every 11th has no origin, and half the origins have a node.
// Below 2^31, A hears every packet below 5301 (ids 3i, about 150 adverts among 5,000 others), B only
// the adverts (3i+1), C a few non-adverts (3i+2). The sequence then crosses 2^31 for 30 more, split
// between A and B, adverts and others alike.
// Observations carry their packet's payload type.
const advertsFixture = `
INSERT INTO iata_codes(iata) VALUES ('YOW');
INSERT INTO observers(id,public_key) VALUES
 ('00000000-0000-0000-0000-0000000000a1','\xa1'),('00000000-0000-0000-0000-0000000000b1','\xb1'),('00000000-0000-0000-0000-0000000000c1','\xc1');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT int4send(i),
 CASE WHEN i % 35 = 0 OR (i > 5300 AND i % 2 = 0) THEN 4 ELSE (ARRAY[2,5,9])[1 + i % 3] END,
 0,1,CASE WHEN i % 11 = 0 THEN NULL ELSE int4send(i % 40) END,'\x00','\x00',now(),now()
FROM generate_series(1,5330) i;
INSERT INTO nodes(public_key,node_type,name) SELECT int4send(k),1,'Node '||k FROM generate_series(0,38,2) k;
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,rssi,snr,payload_type)
SELECT 3*i,p.packet_hash,'00000000-0000-0000-0000-0000000000a1','YOW','2026-01-01'::timestamptz+i*interval '1 second',0,1,i%4,-(i%100),(i%20)::real/2,p.payload_type
FROM generate_series(1,5300) i JOIN packets p ON p.packet_hash = int4send(i);
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type)
SELECT 3*i+1,p.packet_hash,'00000000-0000-0000-0000-0000000000b1','YOW','2026-01-01'::timestamptz+i*interval '1 second',0,1,1,p.payload_type
FROM generate_series(1,5300) i JOIN packets p ON p.packet_hash = int4send(i) WHERE p.payload_type = 4;
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type)
SELECT 3*i+2,p.packet_hash,'00000000-0000-0000-0000-0000000000c1','YOW','2026-01-01'::timestamptz+i*interval '1 second',0,1,0,p.payload_type
FROM generate_series(1,5300) i JOIN packets p ON p.packet_hash = int4send(i) WHERE p.payload_type <> 4 AND i % 10 = 1;
SELECT setval('packet_observations_id_seq', 2147483640);
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type)
SELECT p.packet_hash,CASE WHEN i % 3 = 0 THEN '00000000-0000-0000-0000-0000000000b1'::uuid ELSE '00000000-0000-0000-0000-0000000000a1'::uuid END,
 'YOW','2026-01-02'::timestamptz+i*interval '1 second',0,1,2,p.payload_type
FROM generate_series(5301,5330) i JOIN packets p ON p.packet_hash = int4send(i) ORDER BY i;
ANALYZE packet_observations;`

// advertsPool migrates a fresh schema and loads advertsFixture.
func advertsPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, advertsFixture); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

// joinFilterAdvertsSQL is the adverts statement that filtered on the packet's payload type, served until every
// observation carried its own (frozen as db/planguard/sql/planted/ListObserverAdvertsJoinFilter_16fb706.sql). It is
// the oracle: the observation-typed statement must return its rows.
const joinFilterAdvertsSQL = `-- name: ListObserverAdvertsJoinFilter :many
SELECT 
  po.id,
  encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.hop_count,
  n.name AS node_name,
  COALESCE(encode(p.origin_pubkey, 'hex'), '')::text AS node_public_key
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.observer_id = $1
  AND p.payload_type = 4
  AND ($2::bigint = 0 OR po.id > $2::bigint)
ORDER BY po.id ASC
LIMIT $3`

// joinFilterAdverts is the packet-typed statement's rows, in the observation-typed row type.
func joinFilterAdverts(t *testing.T, ctx context.Context, q sqlc.DBTX, obs uuid.UUID, cursor int64, limit int32) []sqlc.ListObserverAdvertsRow {
	t.Helper()
	rows, err := q.Query(ctx, joinFilterAdvertsSQL, obs, cursor, limit)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (sqlc.ListObserverAdvertsRow, error) {
		var i sqlc.ListObserverAdvertsRow
		err := r.Scan(&i.ID, &i.PacketHashHex, &i.PayloadType, &i.Iata, &i.HeardAt, &i.Rssi, &i.Snr, &i.HopCount, &i.NodeName, &i.NodePublicKey)
		return i, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func advertIDs(rows []sqlc.ListObserverAdvertsRow) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

// advertPages walks /{obs}/adverts at limit 50 and returns every page.
func advertPages(t *testing.T, ctx context.Context, h http.Handler, obs uuid.UUID) []api.Page[api.AdvertObservation] {
	t.Helper()
	var pages []api.Page[api.AdvertObservation]
	path := fmt.Sprintf("/%s/adverts?limit=50", obs)
	for range 100 {
		var page api.Page[api.AdvertObservation]
		getJSON(t, ctx, h, path, &page)
		pages = append(pages, page)
		if !page.HasMore {
			return pages
		}
		path = fmt.Sprintf("/%s/adverts?limit=50&cursor=%d", obs, *page.NextCursor)
	}
	t.Fatal("still paging after 100 pages")
	return nil
}

func pageIDs(pages []api.Page[api.AdvertObservation]) []int64 {
	var out []int64
	for _, p := range pages {
		for _, item := range p.Items {
			out = append(out, item.ID)
		}
	}
	return out
}

func saveDone(t *testing.T, ctx context.Context, s *Store, pool *pgxpool.Pool, done bool) {
	t.Helper()
	var maxID int64
	if err := pool.QueryRow(ctx, "SELECT max(id) FROM packet_observations").Scan(&maxID); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(ctx, ObservationPayloadTypeKey, ObservationPayloadTypeState{NextID: maxID + 1, Done: done}); err != nil {
		t.Fatal(err)
	}
}

// scanIndexes lists the index each scan of relation in an EXPLAIN (FORMAT JSON) plan uses ("" for none).
func scanIndexes(t *testing.T, plan, relation string) []string {
	t.Helper()
	var plans []struct{ Plan map[string]any }
	if err := json.Unmarshal([]byte(plan), &plans); err != nil || len(plans) == 0 {
		t.Fatalf("EXPLAIN JSON did not parse (%v): %s", err, plan)
	}
	var out []string
	var walk func(map[string]any)
	walk = func(p map[string]any) {
		if p["Relation Name"] == relation {
			name, _ := p["Index Name"].(string)
			out = append(out, name)
		}
		children, _ := p["Plans"].([]any)
		for _, c := range children {
			walk(c.(map[string]any))
		}
	}
	walk(plans[0].Plan)
	return out
}

func TestObserverAdvertsPartialIndexPostgres(t *testing.T) {
	ctx, pool := advertsPool(t)
	var def string
	var valid bool
	err := pool.QueryRow(ctx, `SELECT pg_get_indexdef(i.indexrelid), i.indisvalid AND i.indisready FROM pg_index i
 WHERE i.indexrelid = 'idx_observations_adverts'::regclass`).Scan(&def, &valid)
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if want := "CREATE INDEX idx_observations_adverts ON " + schema + ".packet_observations USING btree (observer_id, id) WHERE (payload_type = 4)"; def != want || !valid {
		t.Fatalf("index %q valid=%v, want %q", def, valid, want)
	}

	q := sqlc.New(pool)
	all := joinFilterAdverts(t, ctx, pool, advertsObserverA, 0, math.MaxInt32)
	if len(all) < 150 || all[len(all)-1].ID <= math.MaxInt32+1 {
		t.Fatalf("fixture: A has %d adverts, last %d", len(all), all[len(all)-1].ID)
	}
	var lastAdvert int64
	if err := pool.QueryRow(ctx, "SELECT max(id) FROM packet_observations WHERE payload_type = 4").Scan(&lastAdvert); err != nil {
		t.Fatal(err)
	}
	mid := all[len(all)/2].ID - 1 // not an observation id of A's

	t.Run("rows", func(t *testing.T) {
		for _, obs := range []uuid.UUID{advertsObserverA, advertsObserverB, advertsObserverC, advertsUnknown} {
			for _, cursor := range []int64{-5, 0, 1, mid, lastAdvert, math.MaxInt32, math.MaxInt32 + 1, 3_000_000_000, math.MaxInt64} {
				for _, limit := range []int32{1, 2, 51, 201, 100000} {
					got, err := q.ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{ObserverID: obs, CursorID: cursor, RowLimit: limit})
					if err != nil {
						t.Fatal(err)
					}
					if want := joinFilterAdverts(t, ctx, pool, obs, cursor, limit); !sameRows(got, want) {
						t.Fatalf("observer %s cursor %d limit %d:\ngot  %v\nwant %v", obs, cursor, limit, advertIDs(got), advertIDs(want))
					}
				}
			}
		}
	})

	t.Run("walk", func(t *testing.T) {
		store := New(pool, nil, 0, 0, WithPayloadTypeGate())
		saveDone(t, ctx, store, pool, true)
		h := handlers.ObserversRouter(store)
		if got, want := pageIDs(advertPages(t, ctx, h, advertsObserverA)), advertIDs(all); !slices.Equal(got, want) {
			t.Fatalf("walk got %v, want %v", got, want)
		}
		for _, cursor := range []int64{3_000_000_000, math.MaxInt64} {
			var page api.Page[api.AdvertObservation]
			getJSON(t, ctx, h, fmt.Sprintf("/%s/adverts?cursor=%d", advertsObserverA, cursor), &page)
			if page.Items == nil || len(page.Items) != 0 || page.HasMore || page.NextCursor != nil {
				t.Fatalf("cursor %d: %+v", cursor, page)
			}
		}
	})

	t.Run("plan", func(t *testing.T) {
		// The unfiltered index on the same key must exist, or choosing the partial one proves nothing.
		var other *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass('idx_observations_observer_id')::text").Scan(&other); err != nil || other == nil {
			t.Fatalf("idx_observations_observer_id missing: %v", err)
		}
		for _, c := range []struct {
			cursor int64
			want   []string
		}{
			{0, []string{"idx_observations_adverts"}},
			{mid, []string{"idx_observations_adverts"}},
			{3_000_000_000, []string{"idx_observations_adverts", "packet_observations_pkey"}},
		} {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off; SET LOCAL jit = off"); err != nil {
				t.Fatal(err)
			}
			capture := &statementCapture{Tx: tx, name: "ListObserverAdverts :many"}
			if _, err := sqlc.New(capture).ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{ObserverID: advertsObserverA, CursorID: c.cursor, RowLimit: 51}); err != nil {
				t.Fatal(err)
			}
			plan := explainJSON(t, ctx, tx, capture.statement, capture.args...)
			_ = tx.Rollback(ctx)
			scans := scanIndexes(t, plan, "packet_observations")
			if len(scans) != 1 || !slices.Contains(c.want, scans[0]) {
				t.Fatalf("cursor %d scans packet_observations by %q, want one of %v:\n%s", c.cursor, scans, c.want, plan)
			}
		}
	})
}

// statementRecorder records the sqlc name of every statement run through it.
type statementRecorder struct {
	sqlc.DBTX
	names []string
}

func (r *statementRecorder) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.names = append(r.names, sqlcName(sql))
	return r.DBTX.Exec(ctx, sql, args...)
}

func (r *statementRecorder) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.names = append(r.names, sqlcName(sql))
	return r.DBTX.Query(ctx, sql, args...)
}

func (r *statementRecorder) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	r.names = append(r.names, sqlcName(sql))
	return r.DBTX.QueryRow(ctx, sql, args...)
}

// advertStatements returns the distinct adverts statements recorded since the last call, and resets.
func (r *statementRecorder) advertStatements() []string {
	var out []string
	for _, n := range r.names {
		if strings.HasPrefix(n, "ListObserverAdverts") && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	r.names = nil
	return out
}

func TestAdvertsWaitForPayloadTypeBackfillPostgres(t *testing.T) {
	ctx, pool := advertsPool(t)
	tag, err := pool.Exec(ctx, `WITH a AS (
 SELECT id, row_number() OVER (ORDER BY id) AS rn, count(*) OVER () AS n
 FROM packet_observations WHERE observer_id = '00000000-0000-0000-0000-0000000000a1' AND payload_type = 4)
UPDATE packet_observations po SET payload_type = NULL FROM a
WHERE po.id = a.id AND ((a.rn % 10 = 3 AND a.rn <= 150) OR a.rn > a.n - 5)`)
	if err != nil {
		t.Fatal(err)
	}
	var high int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM packet_observations WHERE payload_type IS NULL AND id > 2147483648").Scan(&high); err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 20 || high == 0 {
		t.Fatalf("untyped %d adverts, %d above 2^31", tag.RowsAffected(), high)
	}

	want := advertIDs(joinFilterAdverts(t, ctx, pool, advertsObserverA, 0, math.MaxInt32))
	rec := &statementRecorder{DBTX: pool}
	store := New(rec, nil, 0, 0, WithPayloadTypeGate())
	h := handlers.ObserversRouter(store)
	walk := func(t *testing.T, statement string) []api.Page[api.AdvertObservation] {
		t.Helper()
		pages := advertPages(t, ctx, h, advertsObserverA)
		if got := pageIDs(pages); !slices.Equal(got, want) {
			t.Fatalf("walk got %v, want %v", got, want)
		}
		if got := rec.advertStatements(); !slices.Equal(got, []string{statement}) {
			t.Fatalf("statements %v, want only %s", got, statement)
		}
		return pages
	}

	// Until the backfill state says done (no state, then a state not done), the page answers 503 with Retry-After
	// and runs no adverts statement.
	notReady := func(t *testing.T) {
		t.Helper()
		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, httptest.NewRequest("GET", fmt.Sprintf("/%s/adverts?limit=50", advertsObserverA), nil).WithContext(ctx))
		if rec2.Code != http.StatusServiceUnavailable || rec2.Header().Get("Retry-After") == "" {
			t.Fatalf("before the backfill: status %d, Retry-After %q", rec2.Code, rec2.Header().Get("Retry-After"))
		}
		if got := rec.advertStatements(); len(got) != 0 {
			t.Fatalf("statements %v ran before the backfill", got)
		}
	}
	notReady(t)
	if err := store.SaveState(ctx, ObservationPayloadTypeKey, ObservationPayloadTypeState{NextID: 1}); err != nil {
		t.Fatal(err)
	}
	notReady(t)

	if _, err := pool.Exec(ctx, `UPDATE packet_observations po SET payload_type = p.payload_type
FROM packets p WHERE p.packet_hash = po.packet_hash AND po.payload_type IS NULL`); err != nil {
		t.Fatal(err)
	}
	saveDone(t, ctx, store, pool, true)
	walk(t, "ListObserverAdverts")

	saveDone(t, ctx, store, pool, false)
	walk(t, "ListObserverAdverts")
}
