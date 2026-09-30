// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type evidenceCapture struct {
	pgx.Tx
	statement string
	args      []any
}

func (c *evidenceCapture) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "-- name: ListRouteEvidence") {
		c.statement = sql
		c.args = args
	}
	return c.Tx.Query(ctx, sql, args...)
}

func TestRouteEvidencePostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	analyticsTables(t, ctx, tx)
	ddl, err := migrationFiles.ReadFile("migrations/024_known_routes_pathkey.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(ddl)
	start := strings.Index(text, "CREATE TABLE known_routes_new (")
	end := start + strings.Index(text[start:], "\n);") + 3
	table := strings.ReplaceAll(text[start:end], "known_routes_new", "known_routes")
	_, err = tx.Exec(ctx, `CREATE TABLE iata_codes (iata char(3) PRIMARY KEY); INSERT INTO iata_codes VALUES ('YOW'),('YVR');
 ALTER TABLE nodes ADD COLUMN latitude double precision, ADD COLUMN longitude double precision;`+table+`
 INSERT INTO observers(id,display_name) VALUES ('00000000-0000-0000-0000-000000000001','One'),('00000000-0000-0000-0000-000000000002','Two');
 INSERT INTO nodes(id,public_key,name) VALUES ('00000000-0000-0000-0000-000000000001','\xaa0102','A'),('00000000-0000-0000-0000-000000000002','\xbb0304','B');
 INSERT INTO packets(packet_hash) SELECT int4send(i) FROM generate_series(1,10) i;`)
	if err != nil {
		t.Fatal(err)
	}
	index, err := migrationFiles.ReadFile("migrations/041_route_evidence_index.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, strings.Replace(string(index), "CREATE INDEX CONCURRENTLY", "CREATE INDEX", 1)); err != nil {
		t.Fatal(err)
	}
	capture := &evidenceCapture{Tx: tx}
	store := &Store{q: sqlc.New(capture)}
	ids := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")}
	if err := store.UpsertKnownRoute(ctx, ids, [][]byte{{0xaa}, {0xbb}}, "YOW", 2); err != nil {
		t.Fatal(err)
	}
	key := hex.EncodeToString(routePathKey(ids))
	since := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Hour)
	until := since.Add(time.Hour)
	at := since.Add(time.Minute + 123456*time.Microsecond)
	_, err = tx.Exec(ctx, `INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type,snr)
 SELECT i,int4send(i), '00000000-0000-0000-0000-000000000001',CASE WHEN i=5 THEN 'YVR' ELSE 'YOW' END,
 CASE WHEN i=9 THEN $2::timestamptz WHEN i=1 THEN $1::timestamptz-interval '2 microseconds' ELSE $1::timestamptz END,
 CASE WHEN i=4 THEN 2 ELSE 1 END,CASE WHEN i=4 THEN 1 WHEN i=10 THEN 3 ELSE 2 END,
 CASE WHEN i=6 THEN '\xabbb'::bytea WHEN i=10 THEN '\xaabbcc'::bytea ELSE '\xaabb'::bytea END,
 CASE WHEN i=7 THEN 9 WHEN i=8 THEN NULL ELSE 4 END,CASE WHEN i=1 THEN 'NaN'::real ELSE 0 END FROM generate_series(1,10) i;
 `, at, until)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type) VALUES (11,int4send(1),'00000000-0000-0000-0000-000000000002','YOW',$1,1,2,'\xaabb',4)`, at.Add(-time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	query := api.RouteEvidenceQuery{Since: since, Until: until, Limit: 2}
	first, err := store.GetRouteEvidence(ctx, "YOW", key, query)
	if err != nil {
		t.Fatal(err)
	}
	if !first.HasMore || first.NextPageCursor == nil || len(first.Items) != 2 || first.Items[0].ID != 3 || first.Items[1].ID != 2 || first.Route.PathKey != key || first.HashSize != 1 || first.PathBytes != "aabb" {
		t.Fatalf("first page: %+v", first)
	}
	query.Cursor, err = api.ParseRouteEvidenceCursor(*first.NextPageCursor)
	if err != nil {
		t.Fatal(err)
	}
	if query.Cursor.HashSize != 1 || query.Cursor.PathBytes != "aabb" {
		t.Fatalf("cursor did not pin the original bytes: %+v", query.Cursor)
	}
	// A new representation arrives while page one is open. New reads follow it,
	// but pagination and copied links retain the exact original representation.
	_, err = tx.Exec(ctx, `INSERT INTO packets(packet_hash) VALUES (int4send(12)),(int4send(13)),(int4send(14));
 INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type)
 SELECT i,int4send(i),'00000000-0000-0000-0000-000000000001','YOW',$1,2,2,'\xaa01bb03',4 FROM generate_series(12,14) i`, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertKnownRoute(ctx, ids, [][]byte{{0xaa, 1}, {0xbb, 3}}, "YOW", 2); err != nil {
		t.Fatal(err)
	}
	freshQuery := api.RouteEvidenceQuery{Since: since, Until: until, Limit: 2}
	fresh, err := store.GetRouteEvidence(ctx, "YOW", key, freshQuery)
	if err != nil || fresh.HashSize != 2 || fresh.PathBytes != "aa01bb03" || len(fresh.Items) != 2 || fresh.Items[0].ID != 14 || fresh.Route.Hops[0].HashBytes != "aa01" {
		t.Fatalf("new representation lost: %+v %v", fresh, err)
	}
	if fresh.Route.ID != first.Route.ID || fresh.Route.PathKey != first.Route.PathKey || fresh.Route.FirstSeen != first.Route.FirstSeen || fresh.Route.ObservationCount != first.Route.ObservationCount+1 {
		t.Fatalf("route identity/counters changed: %+v", fresh.Route)
	}
	shared := freshQuery
	shared.HashSize, shared.PathBytes = first.HashSize, first.PathBytes
	copyPage, err := store.GetRouteEvidence(ctx, "YOW", key, shared)
	if err != nil || len(copyPage.Items) != 2 || copyPage.Items[0].ID != 3 || copyPage.Route.Hops[0].HashBytes != "aa" {
		t.Fatalf("shared representation changed: %+v %v", copyPage, err)
	}
	second, err := store.GetRouteEvidence(ctx, "YOW", key, query)
	if err != nil {
		t.Fatal(err)
	}
	if second.HasMore || len(second.Items) != 2 || second.Items[0].ID != 11 || second.Items[1].ID != 1 || second.Items[1].SNR != nil {
		t.Fatalf("precise second page: %+v", second)
	}
	if _, err = json.Marshal(second); err != nil {
		t.Fatalf("non-finite JSON: %v", err)
	}
	for _, prefixes := range [][][]byte{{{0xaa, 1, 2}, {0xbb, 3, 4}}, {{0xaa, 1, 2}, {0xbb, 3, 4}}} {
		if err := store.UpsertKnownRoute(ctx, ids, prefixes, "YOW", 2); err != nil {
			t.Fatal(err)
		}
	}
	thirdWidth, err := store.GetRouteEvidence(ctx, "YOW", key, freshQuery)
	if err != nil || thirdWidth.HashSize != 3 || thirdWidth.PathBytes != "aa0102bb0304" || len(thirdWidth.Items) != 0 || thirdWidth.Route.ID != first.Route.ID || thirdWidth.Route.ObservationCount != first.Route.ObservationCount+3 {
		t.Fatalf("third width or repeated upsert: %+v %v", thirdWidth, err)
	}
	// Missing identities or mismatched bytes cannot make a pinned query search
	// a different chain. Prefix collisions likewise never widen the SQL match.
	wrong := shared
	wrong.PathBytes = "aacc"
	if _, err := store.GetRouteEvidence(ctx, "YOW", key, wrong); !errors.Is(err, api.ErrRouteEvidenceInput) {
		t.Fatalf("unrelated path accepted: %v", err)
	}
	wrong.PathBytes = "aabbcc"
	if _, err := store.GetRouteEvidence(ctx, "YOW", key, wrong); !errors.Is(err, api.ErrRouteEvidenceInput) {
		t.Fatalf("wrong hop count accepted: %v", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO nodes(id,public_key,name) VALUES ('00000000-0000-0000-0000-000000000003','\xaa9988','Collision');
 DELETE FROM nodes WHERE id='00000000-0000-0000-0000-000000000001'`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRouteEvidence(ctx, "YOW", key, shared); !errors.Is(err, api.ErrRouteEvidenceInput) {
		t.Fatalf("missing node substituted by colliding node: %v", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO nodes(id,public_key,name) VALUES ('00000000-0000-0000-0000-000000000001','\xaa0102','A')`); err != nil {
		t.Fatal(err)
	}
	copyPage, err = store.GetRouteEvidence(ctx, "YOW", key, shared)
	if err != nil || len(copyPage.Items) != 2 || copyPage.Items[0].ID != 3 {
		t.Fatalf("collision broadened evidence: %+v %v", copyPage, err)
	}
	if err := store.UpsertKnownRoute(ctx, ids, [][]byte{{0xaa}, {0xbb}}, "YOW", 2); err != nil {
		t.Fatal(err)
	}
	query.Cursor.IATA = "YVR"
	if _, err = store.GetRouteEvidence(ctx, "YOW", key, query); !errors.Is(err, api.ErrRouteEvidenceInput) {
		t.Fatal("cross-route cursor accepted")
	}
	query.Cursor = nil
	if _, err = store.GetRouteEvidence(ctx, "YVR", key, query); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("unknown route did not return not-found")
	}
	if _, err = tx.Exec(ctx, "DELETE FROM packets"); err != nil {
		t.Fatal(err)
	}
	empty, err := store.GetRouteEvidence(ctx, "YOW", key, query)
	if err != nil || !empty.MatchAvailable || len(empty.Items) != 0 || empty.HasMore {
		t.Fatalf("expired evidence: %+v %v", empty, err)
	}

	// Many unrelated paths must not turn this into a raw-table or route-ID scan.
	started := time.Now()
	_, err = tx.Exec(ctx, `INSERT INTO packets(packet_hash) SELECT int4send(i) FROM generate_series(100,200099) i;`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type)
 SELECT packet_hash,'00000000-0000-0000-0000-000000000001','YOW',$1,1,2,
 CASE WHEN get_byte(packet_hash,3)=0 THEN '\xaabb'::bytea ELSE packet_hash END,4 FROM packets;`, at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "ANALYZE packet_observations; ANALYZE observers;"); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		if _, err = tx.Exec(ctx, "SET LOCAL plan_cache_mode = "+mode); err != nil {
			t.Fatal(err)
		}
		result, err := store.GetRouteEvidence(ctx, "YOW", key, query)
		if err != nil || len(result.Items) != 2 {
			t.Fatalf("indexed read: %v", err)
		}
		var plan string
		if err = tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+capture.statement, capture.args...).Scan(&plan); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan, "idx_observations_route_evidence") {
			t.Fatalf("missing evidence index: %s", plan)
		}
		t.Logf("%s plan: %s", mode, plan)
	}
	t.Logf("200000-row fixture insert/analyze/read: %s", time.Since(started))
	if _, err = tx.Exec(ctx, `UPDATE known_routes SET hash_prefix=ARRAY['\xaa'::bytea]`); err != nil {
		t.Fatal(err)
	}
	unavailable, err := store.GetRouteEvidence(ctx, "YOW", key, query)
	if err != nil || unavailable.MatchAvailable || len(unavailable.Items) != 0 {
		t.Fatalf("malformed stored path: %+v %v", unavailable, err)
	}
}

func TestRouteEvidenceIndexPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	schema := fmt.Sprintf("route_evidence_%d", time.Now().UnixNano())
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err = conn.Exec(ctx, "CREATE SCHEMA "+ident+"; SET search_path TO "+ident); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE") }()
	if _, err = conn.Exec(ctx, `CREATE TABLE packet_observations (id bigint,iata char(3),hash_size smallint,path_bytes bytea,heard_at timestamptz,payload_type smallint,hop_count smallint)`); err != nil {
		t.Fatal(err)
	}
	migration, err := migrationFiles.ReadFile("migrations/041_route_evidence_index.sql")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = applyMigration(ctx, conn, string(migration)); err != nil {
			t.Fatalf("migration/retry: %v", err)
		}
	}
	if _, err = conn.Exec(ctx, `UPDATE pg_index SET indisvalid=false WHERE indexrelid='idx_observations_route_evidence'::regclass`); err != nil {
		t.Fatal(err)
	}
	if err = applyMigration(ctx, conn, string(migration)); err != nil {
		t.Fatal(err)
	}
	var valid bool
	if err = conn.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid='idx_observations_route_evidence'::regclass`).Scan(&valid); err != nil || !valid {
		t.Fatalf("invalid index after recovery: %v", err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO packet_observations(id,iata,hash_size,path_bytes,heard_at,payload_type,hop_count)
SELECT n,'YOW',1,'\xaabb',NOW(),4,n FROM generate_series(0,2) n;
ANALYZE packet_observations`); err != nil {
		t.Fatal(err)
	}
	var indexed float32
	if err = conn.QueryRow(ctx, `SELECT reltuples FROM pg_class WHERE oid='idx_observations_route_evidence'::regclass`).Scan(&indexed); err != nil || indexed != 1 {
		t.Fatalf("0/1-hop rows entered the route index: %v %v", indexed, err)
	}
}
