// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

type traceQueryCounter struct {
	sqlc.DBTX
	queries, observations int
}

func (c *traceQueryCounter) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	c.queries++
	if strings.Contains(sql, "-- name: ListObservationsForPacket ") {
		c.observations++
	}
	return c.DBTX.Query(ctx, sql, args...)
}

// Real SQL and Store mapping are exercised against temporary tables only.
func TestTraceDetailPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for the PostgreSQL regression test")
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
	for _, table := range []string{"packets", "packet_observations", "observers", "nodes", "node_short_ids", "transport_scopes"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO nodes (id,public_key,node_type,name) VALUES
 ('00000000-0000-0000-0000-000000000001',decode('aa'||repeat('00',31),'hex'),2,'YVR relay'),
 ('00000000-0000-0000-0000-000000000002',decode('aa01'||repeat('00',30),'hex'),2,'YYJ relay');
INSERT INTO node_short_ids (node_id,iata,prefix_4) VALUES
 ('00000000-0000-0000-0000-000000000001','YVR','\xaa000000'),
 ('00000000-0000-0000-0000-000000000002','YYJ','\xaa010000');
INSERT INTO packets (packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,trace_tag,parsed_payload,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),64,'0'),'hex'),9,0,1,'\x00','\x00','\x01020304',
 CASE WHEN i=100 THEN '{}'::jsonb ELSE '{"flags":0,"pathHashes":["aa"],"snrValues":[12.5]}'::jsonb END,
 '2026-01-01'::timestamptz+i*interval '1 second', '2026-01-01'::timestamptz+i*interval '1 second'+interval '1 second'
FROM generate_series(1,100) i;
INSERT INTO packet_observations (id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT i,decode(lpad(to_hex(i),64,'0'),'hex'),md5(i::text)::uuid,'YVR','2026-01-01'::timestamptz+interval '2 seconds',1,1,1
FROM generate_series(1,100) i WHERE i<>99;
-- The first packet was heard in YYJ before YVR. A repeat YYJ observation must
-- not trigger another region lookup or change equal-confidence precedence.
INSERT INTO packet_observations (id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count) VALUES
 (101,decode(lpad('1',64,'0'),'hex'),md5('101')::uuid,'YYJ','2026-01-01'::timestamptz,1,1,1),
 (102,decode(lpad('1',64,'0'),'hex'),md5('102')::uuid,'YYJ','2026-01-01'::timestamptz+interval '3 seconds',1,1,1);
ANALYZE packets; ANALYZE packet_observations; ANALYZE nodes; ANALYZE node_short_ids;`)
	if err != nil {
		t.Fatal(err)
	}
	counter := &traceQueryCounter{DBTX: tx}
	store := &Store{q: sqlc.New(counter)}
	start := time.Now()
	detail, err := store.GetTraceByTag(ctx, "01020304")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if detail == nil || detail.TraceTag != "01020304" || len(detail.Packets) != 100 {
		t.Fatal("trace packets missing")
	}
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, p := range detail.Packets {
		number := i + 1
		if p.PacketHash != fmt.Sprintf("%064x", number) || p.RouteType != 1 || p.FirstHeardAt != anchor.Add(time.Duration(number)*time.Second).UnixMilli() || p.LastHeardAt != p.FirstHeardAt+1000 {
			t.Fatalf("wrong packet fields or order at %d", number)
		}
		if number == 100 {
			if len(p.RawPath) != 0 || len(p.ResolvedRoute) != 0 {
				t.Fatal("invented path for empty payload")
			}
			continue
		}
		if len(p.RawPath) != 1 || p.RawPath[0].Hash != "aa" || p.RawPath[0].SNR == nil || *p.RawPath[0].SNR != 12.5 {
			t.Fatalf("wrong raw path/SNR at %d", number)
		}
		if number == 99 {
			if len(p.ResolvedRoute) != 0 {
				t.Fatal("unobserved packet must not borrow another packet's IATAs")
			}
			continue
		}
		want := "YVR relay"
		if number == 1 {
			want = "YYJ relay"
		}
		if len(p.ResolvedRoute) != 1 || p.ResolvedRoute[0].Confidence != "high" || len(p.ResolvedRoute[0].Nodes) != 1 || p.ResolvedRoute[0].Nodes[0].Name == nil || *p.ResolvedRoute[0].Nodes[0].Name != want {
			t.Fatalf("wrong region precedence/resolution at %d", number)
		}
		if p.ResolvedRoute[0].SNR == nil || *p.ResolvedRoute[0].SNR != 12.5 {
			t.Fatal("resolved SNR changed")
		}
	}
	t.Logf("100-packet trace: %.3f ms, %d database queries, %d separate observation queries", float64(elapsed.Microseconds())/1000, counter.queries, counter.observations)
	if counter.observations != 0 {
		t.Errorf("extra observation queries: %d; want 0", counter.observations)
	}
	if counter.queries != 100 {
		t.Errorf("got %d queries; want one packet query and 99 unchanged path-resolution queries", counter.queries)
	}
	counter.queries, counter.observations = 0, 0
	missing, err := store.GetTraceByTag(ctx, "00000000")
	if err != nil || missing != nil || counter.queries != 1 {
		t.Fatal("missing tag behavior changed")
	}
}
