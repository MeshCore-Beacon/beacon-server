// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

// Trace tags, not individual packets, are the entities being paginated. All
// fixture writes use temporary tables and are rolled back on exit.
func TestTraceTagPaginationPostgres(t *testing.T) {
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
	for _, table := range []string{"packets", "trace_iatas", "transport_scopes"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO transport_scopes (id,name,transport_key,key_fingerprint) VALUES
 (1,'scope-one',decode(repeat('01',16),'hex'),decode(repeat('01',8),'hex')),
 (2,'scope-two',decode(repeat('02',16),'hex'),decode(repeat('02',8),'hex'));
INSERT INTO packets (packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,trace_tag,scope_id,first_heard_at,last_heard_at,parsed_payload)
SELECT decode(lpad(to_hex(i),64,'0'),'hex'),9,0,1,'\x00','\x00',decode(tag,'hex'),scope,
 '2026-01-01'::timestamptz+first*interval '1 second','2026-01-01'::timestamptz+last*interval '1 second',payload::jsonb
FROM (VALUES
 (1,'aaaaaaaa',1,1,40,'{"type":"TRACE","pathHashes":["aa"],"snrValues":[1]}'),
 (2,'aaaaaaaa',1,2,10,'{"type":"TRACE","pathHashes":["aa","bb"],"snrValues":[1,2]}'),
 (3,'aaaaaaaa',2,3,25,'{"type":"PING","pathHashes":["aa","bb","cc"],"snrValues":[1,2,3]}'),
 (4,'bbbbbbbb',1,4,30,'{"type":"TRACE","pathHashes":["bb","11","22","33"],"snrValues":[2,3,4,5]}'),
 (5,'bbbbbbbb',2,5,5,'{"type":"PING","pathHashes":["bb"],"snrValues":[2]}'),
 (6,'cccccccc',1,6,20,'{"type":"TRACE","pathHashes":["cc"],"snrValues":[3]}'),
 (7,'dddddddd',2,7,15,'{"type":"PING","pathHashes":["dd"],"snrValues":[4]}'),
 (8,NULL,1,8,50,'{"type":"TRACE","pathHashes":[],"snrValues":[]}')
) v(i,tag,scope,first,last,payload);
INSERT INTO trace_iatas (trace_tag,iata) VALUES
 ('\xaaaaaaaa','YYZ'),('\xaaaaaaaa','YVR'),('\xbbbbbbbb','YYZ'),('\xcccccccc','YVR');`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time {
		if seconds == 0 {
			return time.Time{}
		}
		return anchor.Add(time.Duration(seconds) * time.Second)
	}
	paths := map[string][]string{"a": {"aa", "bb", "cc"}, "b": {"bb", "11", "22", "33"}, "c": {"cc"}, "d": {"dd"}}
	snrs := map[string][]float32{"a": {1, 2, 3}, "b": {2, 3, 4, 5}, "c": {3}, "d": {4}}
	summary := func(tag string, first, last int, count, iatas int64, kind string) api.TraceTagSummary {
		return api.TraceTagSummary{TraceTag: strings.Repeat(tag, 8), FirstHeardAt: at(first).UnixMilli(), LastHeardAt: at(last).UnixMilli(), PacketCount: count, IATACount: iatas, TraceType: kind, PathHashes: paths[tag], SNRValues: snrs[tag]}
	}
	a, b, c, d := summary("a", 1, 40, 3, 2, "TRACE"), summary("b", 4, 30, 2, 1, "TRACE"), summary("c", 6, 20, 1, 1, "TRACE"), summary("d", 7, 15, 1, 0, "PING")
	for _, tc := range []struct {
		name                 string
		iatas                []string
		scope, kind          string
		since, until, cursor int
		want                 []api.TraceTagSummary
	}{
		{"first page", nil, "", "", 0, 0, 0, []api.TraceTagSummary{a, b, c, d}},
		{"cursor at newest group", nil, "", "", 0, 0, 40, []api.TraceTagSummary{b, c, d}},
		{"cursor between packet times", nil, "", "", 0, 0, 30, []api.TraceTagSummary{c, d}},
		{"end of groups", nil, "", "", 0, 0, 15, []api.TraceTagSummary{}},
		{"region", []string{"YYZ"}, "", "", 0, 0, 35, []api.TraceTagSummary{b}},
		{"multiple regions", []string{"YYZ", "YVR", "YYZ"}, "", "", 0, 0, 35, []api.TraceTagSummary{b, c}},
		{"unknown region", []string{"ZZZ"}, "", "", 0, 0, 0, []api.TraceTagSummary{}},
		{"scope", nil, "scope-one", "", 0, 0, 30, []api.TraceTagSummary{c}},
		{"scope defines group", nil, "scope-two", "", 0, 0, 20, []api.TraceTagSummary{d, summary("b", 5, 5, 1, 1, "PING")}},
		{"type", nil, "", "TRACE", 0, 0, 35, []api.TraceTagSummary{summary("b", 4, 30, 1, 1, "TRACE"), c}},
		{"type defines group", nil, "", "PING", 0, 0, 30, []api.TraceTagSummary{summary("a", 3, 25, 1, 2, "PING"), d, summary("b", 5, 5, 1, 1, "PING")}},
		{"time window defines group", nil, "", "", 3, 6, 26, []api.TraceTagSummary{summary("a", 3, 25, 1, 2, "PING"), c}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ListTraceTags(ctx, tc.iatas, tc.scope, tc.kind, at(tc.since), at(tc.until), at(tc.cursor), 20)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("trace summaries differ: got %+v, want %+v", got, tc.want)
			}
		})
	}
	// Reassemble pages using the same epoch-millisecond cursor exposed by the API.
	reference := map[string]api.TraceTagSummary{a.TraceTag: a, b.TraceTag: b, c.TraceTag: c, d.TraceTag: d}
	seen := map[string]bool{}
	var cursor time.Time
	total := 0
	for page := 0; page < 10; page++ {
		rows, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			total++
			if seen[row.TraceTag] {
				t.Errorf("pagination repeated trace tag %s", row.TraceTag)
			}
			seen[row.TraceTag] = true
			if !reflect.DeepEqual(row, reference[row.TraceTag]) {
				t.Errorf("pagination changed summary for %s", row.TraceTag)
			}
		}
		cursor = time.UnixMilli(rows[len(rows)-1].LastHeardAt)
	}
	t.Logf("two-row pages returned %d summaries for %d distinct trace tags", total, len(seen))
	if total != 4 || len(seen) != 4 {
		t.Errorf("expected exactly four complete trace summaries")
	}
}
