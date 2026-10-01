// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	applyBaseline(t, ctx, tx)
	_, err := tx.Exec(ctx, `
 INSERT INTO observers(id,public_key,display_name) VALUES ('00000000-0000-0000-0000-000000000001','\x01','One'),('00000000-0000-0000-0000-000000000002','\x02','Two');
 INSERT INTO nodes(id,public_key,node_type,name) VALUES ('00000000-0000-0000-0000-000000000001','\xaa',2,'A'),('00000000-0000-0000-0000-000000000002','\xbb',2,'B');
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
 SELECT int4send(i),4,0,1,'\x00','\x00',NOW(),NOW() FROM generate_series(1,10) i;`)
	if err != nil {
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
	_, err = tx.Exec(ctx, `INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type,snr,path_length_byte)
 SELECT i,int4send(i), '00000000-0000-0000-0000-000000000001',CASE WHEN i=5 THEN 'YVR' ELSE 'YOW' END,
 CASE WHEN i=9 THEN $2::timestamptz WHEN i=1 THEN $1::timestamptz-interval '2 microseconds' ELSE $1::timestamptz END,
 CASE WHEN i=4 THEN 2 ELSE 1 END,CASE WHEN i=4 THEN 1 WHEN i=10 THEN 3 ELSE 2 END,
 CASE WHEN i=6 THEN '\xabbb'::bytea WHEN i=10 THEN '\xaabbcc'::bytea ELSE '\xaabb'::bytea END,
 CASE WHEN i=7 THEN 9 WHEN i=8 THEN NULL ELSE 4 END,CASE WHEN i=1 THEN 'NaN'::real ELSE 0 END,0 FROM generate_series(1,10) i;
 `, at, until)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type,path_length_byte) VALUES (11,int4send(1),'00000000-0000-0000-0000-000000000002','YOW',$1,1,2,'\xaabb',4,0)`, at.Add(-time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE packet_observations SET path_length_byte=((hash_size-1)<<6)|hop_count`); err != nil {
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
	_, err = tx.Exec(ctx, `INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
 SELECT int4send(i),4,0,1,'\x00','\x00',NOW(),NOW() FROM generate_series(100,200099) i;`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,path_bytes,payload_type)
 SELECT packet_hash,'00000000-0000-0000-0000-000000000001','YOW',$1,2,1,2,
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
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `INSERT INTO observers(id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT int4send(n),4,0,1,'\x00','\x00',NOW(),NOW() FROM generate_series(0,2) n;
INSERT INTO packet_observations(packet_hash,observer_id,iata,path_length_byte,hash_size,path_bytes,heard_at,payload_type,hop_count)
SELECT int4send(n),'00000000-0000-0000-0000-000000000001','YOW',n,1,'\xaabb',NOW(),4,n FROM generate_series(0,2) n;
ANALYZE packet_observations`); err != nil {
		t.Fatal(err)
	}
	var indexed float32
	if err := tx.QueryRow(ctx, `SELECT reltuples FROM pg_class WHERE oid='idx_observations_route_evidence'::regclass`).Scan(&indexed); err != nil || indexed != 1 {
		t.Fatalf("0/1-hop rows entered the route index: %v %v", indexed, err)
	}
}

func TestRouteEvidenceFollowsPrefixWidthPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `INSERT INTO observers(id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (int4send(1),4,0,1,'\x00','\x00',NOW(),NOW());
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,hash_size,hop_count,path_bytes,payload_type,path_length_byte)
VALUES (1,int4send(1),'00000000-0000-0000-0000-000000000001','YOW',NOW(),2,2,'\xaa11bb22',4,(1<<6)|2)`); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	if err := store.UpsertKnownRoute(ctx, ids, [][]byte{{0xaa}, {0xbb}}, "YOW", 2); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertKnownRoute(ctx, ids, [][]byte{{0xaa, 0x11}, {0xbb, 0x22}}, "YOW", 2); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	got, err := store.GetRouteEvidence(ctx, "YOW", hex.EncodeToString(routePathKey(ids)), api.RouteEvidenceQuery{Since: now.Add(-time.Hour), Until: now.Add(time.Minute), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got.HashSize != 2 || got.PathBytes != "aa11bb22" || len(got.Items) != 1 || got.Route.ObservationCount != 2 {
		t.Fatalf("evidence kept the first prefix width: %+v", got)
	}
}
