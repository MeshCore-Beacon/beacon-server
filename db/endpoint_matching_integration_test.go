// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
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

type endpointMatchQueries struct {
	sqlc.DBTX
	calls int
	query string
}

func (q *endpointMatchQueries) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	q.calls++
	q.query = query
	return q.DBTX.Query(ctx, query, args...)
}

func TestPacketEndpointRolesPostgres(t *testing.T) {
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
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"packets", "packet_observations", "observers", "transport_scopes", "channel_messages", "nodes", "node_short_ids"} {
		exec("CREATE TEMP TABLE " + table + " (LIKE public." + table + " INCLUDING ALL) ON COMMIT DROP")
	}
	id := func(i int) uuid.UUID { return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)) }
	exec(`INSERT INTO nodes(id,public_key,name,node_type)
SELECT ('00000000-0000-0000-0000-'||lpad(i::text,12,'0'))::uuid,
 decode(prefix||repeat('00',28),'hex'),name,role
FROM (VALUES (1,'aa000001','Companion',1),(2,'aa000002','Repeater',2),
 (3,'ee000003','Room',3),(4,'aa000004','Sensor',4),(5,'aa000005','Unknown role',0),
 (6,'bb000006','Companion only',1),(7,'aa000007','Other region',1),
 (8,'cc000008','Relay',2),(9,'dd000009',NULL,1),(10,'aa000010','No membership',1)) v(i,prefix,name,role);
INSERT INTO node_short_ids(node_id,iata,prefix_4)
SELECT id,CASE WHEN name='Other region' THEN 'YYZ' ELSE 'YVR' END,substring(public_key from 1 for 4)
FROM nodes WHERE name IS DISTINCT FROM 'No membership';`)
	exec("INSERT INTO observers(id,public_key) VALUES ($1,'\\x01'),($2,'\\x02')", id(101), id(102))
	queries := &endpointMatchQueries{DBTX: tx}
	store := &Store{q: sqlc.New(queries)}
	check := func(hop *api.ResolvedHop, confidence string, members ...int) {
		t.Helper()
		if hop == nil || hop.Confidence != confidence || len(hop.Nodes) != len(members) {
			t.Errorf("endpoint candidate roles incorrect: confidence=%s, want %s with %d nodes", func() string {
				if hop == nil {
					return "nil"
				}
				return hop.Confidence
			}(), confidence, len(members))
			return
		}
		want := make(map[uuid.UUID]bool)
		for _, member := range members {
			want[id(member)] = true
		}
		for _, node := range hop.Nodes {
			if !want[node.ID] {
				t.Errorf("unexpected endpoint node %s", node.ID)
			}
			delete(want, node.ID)
		}
		if len(want) != 0 {
			t.Error("endpoint candidate missing")
		}
	}
	for i, kind := range []int{0, 1, 2, 8} {
		// These encrypted envelopes carry one-byte destination/source prefixes.
		hash := []byte{byte(i + 1)}
		payload := append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)
		exec(`INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES ($1,$2,0,1,$3,$4,NOW(),NOW())`, hash, kind, payload, []byte{byte(kind<<2 | 1)})
		exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,path_bytes,source_broker)
VALUES ($1,$2,$3,'YVR',NOW(),1,1,1,'\xcc','fixture')`, i+1, hash, id(101))
		packet, err := store.GetPacket(ctx, hash)
		if err != nil || len(packet.Observations) != 1 {
			t.Fatalf("packet detail: %v", err)
		}
		observation := packet.Observations[0]
		check(observation.ResolvedSource, "ambiguous", 1, 2, 4, 5)
		check(observation.ResolvedDestination, "high", 6)
		if len(observation.ResolvedPath) != 1 {
			t.Fatal("relay path missing")
		}
		check(&observation.ResolvedPath[0], "high", 8)
	}
	exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,source_broker)
VALUES (10,'\x01',$1,'YYZ',NOW()+interval '1 second',0,1,0,'fixture')`, id(102))
	packet, err := store.GetPacket(ctx, []byte{1})
	if err != nil || len(packet.Observations) != 2 {
		t.Fatalf("regional observations: %v", err)
	}
	check(packet.Observations[1].ResolvedSource, "high", 7)
	check(packet.Observations[1].ResolvedDestination, "none")
	// Intermediate relay matching keeps its existing infrastructure-only boundary.
	for width := 1; width <= 4; width++ {
		hash := []byte{0xaa, 0, 0, 2}[:width]
		resolved, err := store.ResolvePathHashes(ctx, "YVR", [][]byte{hash})
		if err != nil {
			t.Fatal(err)
		}
		hops := api.BuildResolvedPath([][]byte{hash}, resolved)
		check(&hops[0], "high", 2)
	}
	t.Log("endpoint roles and unchanged relay matching checked across four payload types")
	for _, hashes := range [][][]byte{nil, {}, {{}}, {{0xaa, 0xbb}}, {{0xaa}, {0xbb, 0xcc}}} {
		queries.calls = 0
		got, err := store.ResolveEndpointHashes(ctx, "YVR", hashes)
		if err != nil || len(got) != 0 || queries.calls != 0 {
			t.Fatal("invalid endpoint width reached the database")
		}
	}
	queries.calls = 0
	resolved, err := store.ResolveEndpointHashes(ctx, "YVR", [][]byte{{0xee}, {0xdd}, {0xff}, {0xbb}, {0xbb}})
	if err != nil || queries.calls != 1 {
		t.Fatalf("batched endpoint query: %v, calls=%d", err, queries.calls)
	}
	hops := api.BuildResolvedPath([][]byte{{0xee}, {0xdd}, {0xff}, {0xbb}}, resolved)
	check(&hops[0], "high", 3)
	check(&hops[1], "high", 9)
	if hops[1].Nodes[0].Name != nil {
		t.Error("unnamed node acquired a name")
	}
	check(&hops[2], "none")
	check(&hops[3], "high", 6)
	missing, err := store.ResolveEndpointHashes(ctx, "ZZZ", [][]byte{{0xaa}})
	if err != nil || len(missing) != 0 {
		t.Fatal("endpoint escaped its observation region")
	}

	// A large nonmatching population must not turn a short-hash lookup into a full scan.
	exec(`INSERT INTO nodes(id,public_key,node_type)
SELECT ('00000000-0000-0000-0000-'||lpad(i::text,12,'0'))::uuid,
 decode(lpad(to_hex(i),8,'0')||repeat('00',28),'hex'),1 FROM generate_series(10000,29999) i;
INSERT INTO node_short_ids(node_id,iata,prefix_4)
SELECT id,'YVR',substring(public_key from 1 for 4) FROM nodes WHERE substring(public_key from 1 for 1)='\x00';
ANALYZE nodes;
ANALYZE node_short_ids;`)
	if _, err := store.ResolveEndpointHashes(ctx, "YVR", [][]byte{{0xaa}}); err != nil {
		t.Fatal(err)
	}
	exec("PREPARE endpoint_role_plan AS " + queries.query)
	defer tx.Exec(context.Background(), "DEALLOCATE endpoint_role_plan")
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		exec("SET LOCAL plan_cache_mode=" + mode)
		var raw []byte
		if err := tx.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE endpoint_role_plan('YVR',ARRAY['\xaa'::bytea])`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var report []struct {
			Plan         map[string]any
			Milliseconds float64 `json:"Execution Time"`
		}
		if err := json.Unmarshal(raw, &report); err != nil {
			t.Fatal(err)
		}
		var examined, nodeRows float64
		indexed := false
		var walk func(map[string]any)
		walk = func(plan map[string]any) {
			if cond, ok := plan["Index Cond"].(string); ok && strings.Contains(cond, "prefix_1") && strings.Contains(cond, "iata") {
				indexed = true
			}
			if plan["Relation Name"] == "node_short_ids" {
				rows, _ := plan["Actual Rows"].(float64)
				removed, _ := plan["Rows Removed by Filter"].(float64)
				loops, _ := plan["Actual Loops"].(float64)
				examined += (rows + removed) * loops
			}
			if plan["Relation Name"] == "nodes" {
				rows, _ := plan["Actual Rows"].(float64)
				removed, _ := plan["Rows Removed by Filter"].(float64)
				loops, _ := plan["Actual Loops"].(float64)
				nodeRows += (rows + removed) * loops
			}
			if children, ok := plan["Plans"].([]any); ok {
				for _, child := range children {
					walk(child.(map[string]any))
				}
			}
		}
		walk(report[0].Plan)
		if !indexed || examined > 10 || nodeRows > 10 {
			t.Errorf("%s endpoint lookup scanned %.0f short IDs and %.0f nodes, indexed=%v", mode, examined, nodeRows, indexed)
		}
		t.Logf("%s: %.0f short-ID rows and %.0f node rows examined in %.3f ms", mode, examined, nodeRows, report[0].Milliseconds)
	}
}
