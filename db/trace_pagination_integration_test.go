// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5"
)

// Trace tags, not individual packets, are the entities being paginated.
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
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `INSERT INTO transport_scopes (id,name,transport_key,key_fingerprint) VALUES
 (1,'scope-one',decode(repeat('01',16),'hex'),decode(repeat('01',8),'hex')),
 (2,'scope-two',decode(repeat('02',16),'hex'),decode(repeat('02',8),'hex'))`); err != nil {
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
	// Each packet is heard once when new and again (duplicate) at its last time.
	for _, p := range []struct {
		tag         string
		iata        string
		scope       int32
		first, last int
		kind        string
		hashes      []string
	}{
		{"aaaaaaaa", "YYZ", 1, 1, 40, "TRACE", []string{"aa"}},
		{"aaaaaaaa", "YVR", 1, 2, 10, "TRACE", []string{"aa", "bb"}},
		{"aaaaaaaa", "YVR", 2, 3, 25, "PING", []string{"aa", "bb", "cc"}},
		{"bbbbbbbb", "YYZ", 1, 4, 30, "TRACE", []string{"bb", "11", "22", "33"}},
		{"bbbbbbbb", "YYZ", 2, 5, 5, "PING", []string{"bb"}},
		{"cccccccc", "YVR", 1, 6, 20, "TRACE", []string{"cc"}},
		{"dddddddd", "YOW", 2, 7, 15, "PING", []string{"dd"}},
	} {
		tag, _ := hex.DecodeString(p.tag)
		snr := make([]float32, len(p.hashes))
		for i := range snr {
			snr[i] = float32(i + int(p.tag[0]-'a') + 1)
		}
		payload, _ := json.Marshal(map[string]any{"type": p.kind, "pathHashes": p.hashes, "snrValues": snr})
		scope := p.scope
		hash := append(append([]byte{}, tag...), byte(len(p.hashes)), byte(p.first))
		if _, err := store.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: hash, PayloadType: 9, RouteType: 1,
			RawPayload: []byte{0}, RawHeader: []byte{0}, ParsedPayload: payload, ScopeID: &scope, TraceTag: tag}); err != nil {
			t.Fatal(err)
		}
		for _, heard := range []int{p.first, p.last} {
			if err := store.RecordTrace(ctx, ingest.TraceHearing{TraceTag: tag, IATA: p.iata, HeardAt: at(heard)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	paths := map[string][]string{"a": {"aa", "bb", "cc"}, "b": {"bb", "11", "22", "33"}, "c": {"cc"}, "d": {"dd"}}
	snrs := map[string][]float32{"a": {1, 2, 3}, "b": {2, 3, 4, 5}, "c": {3}, "d": {4}}
	summary := func(tag string, first, last int, count, iatas int64, kind string) api.TraceTagSummary {
		return api.TraceTagSummary{TraceTag: strings.Repeat(tag, 8), FirstHeardAt: at(first).UnixMilli(), LastHeardAt: at(last).UnixMilli(), PacketCount: count, IATACount: iatas, TraceType: kind, PathHashes: paths[tag], SNRValues: snrs[tag]}
	}
	a, b, c, d := summary("a", 1, 40, 3, 2, "TRACE"), summary("b", 4, 30, 2, 1, "TRACE"), summary("c", 6, 20, 1, 1, "TRACE"), summary("d", 7, 15, 1, 1, "PING")
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
		// Filters apply to the tag summary: its first scope, any-TRACE type, and first_heard_at.
		{"scope", nil, "scope-one", "", 0, 0, 30, []api.TraceTagSummary{c}},
		{"first scope wins", nil, "scope-two", "", 0, 0, 0, []api.TraceTagSummary{d}},
		{"type", nil, "", "TRACE", 0, 0, 35, []api.TraceTagSummary{b, c}},
		{"mixed tag is TRACE", nil, "", "PING", 0, 0, 0, []api.TraceTagSummary{d}},
		{"time window on first heard", nil, "", "", 3, 6, 26, []api.TraceTagSummary{c}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ListTraceTags(ctx, tc.iatas, tc.scope, tc.kind, at(tc.since), at(tc.until), at(tc.cursor), "", 20)
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
		rows, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, cursor, "", 2)
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

	if err := store.DeleteOldTraceTags(ctx, at(20)); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, time.Time{}, "", 20); err != nil || !reflect.DeepEqual(got, []api.TraceTagSummary{a, b, c}) {
		t.Errorf("after cleanup got %+v, %v; want a, b, c", got, err)
	}
}

// Storing a TRACE packet records its contribution even if the hearing write that follows
// is lost; the next hearing replaces the packet's provisional times.
func TestTraceSummarySurvivesMissedHearingPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO iata_codes (iata) VALUES ('YVR')"); err != nil {
		t.Fatal(err)
	}
	store := New(pool, 0, 0)
	tag := []byte{0xee, 0xee, 0xee, 0xee}
	payload := json.RawMessage(`{"type":"TRACE","pathHashes":["aa","bb"],"snrValues":[1,2]}`)
	upsert := func(hash byte) {
		t.Helper()
		if _, err := store.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: []byte{hash}, PayloadType: 9, RouteType: 1,
			RawPayload: []byte{0}, RawHeader: []byte{0}, ParsedPayload: payload, TraceTag: tag}); err != nil {
			t.Fatal(err)
		}
	}
	upsert(1) // the RecordTrace that would follow never runs
	upsert(1) // a broker duplicate must not count twice
	rows, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, time.Time{}, "", 10)
	if err != nil || len(rows) != 1 || rows[0].PacketCount != 1 || len(rows[0].PathHashes) != 2 || rows[0].TraceType != "TRACE" {
		t.Fatalf("summary after a missed hearing: %+v %v", rows, err)
	}
	heard := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RecordTrace(ctx, ingest.TraceHearing{TraceTag: tag, IATA: "YVR", HeardAt: heard}); err != nil {
		t.Fatal(err)
	}
	rows, err = store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, time.Time{}, "", 10)
	if err != nil || len(rows) != 1 || rows[0].FirstHeardAt != heard.UnixMilli() || rows[0].LastHeardAt != heard.UnixMilli() || rows[0].IATACount != 1 {
		t.Errorf("first hearing should replace provisional times: %+v %v", rows, err)
	}
}

// Tags sharing a millisecond (second-resolution observer clocks, sub-ms times) must
// all survive paging with the (cursor, cursorTag) keyset.
func TestTraceTagPaginationTiesPostgres(t *testing.T) {
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
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `INSERT INTO trace_tags (trace_tag, first_heard_at, last_heard_at, heard) VALUES
 ('\x00000001', '2026-01-01 00:00:00+00', '2026-01-01 00:00:10+00', true),
 ('\x00000002', '2026-01-01 00:00:00+00', '2026-01-01 00:00:10+00', true),
 ('\x00000003', '2026-01-01 00:00:00+00', '2026-01-01 00:00:10+00', true),
 ('\x00000004', '2026-01-01 00:00:00+00', '2026-01-01 00:00:10+00', true),
 ('\x00000005', '2026-01-01 00:00:00+00', '2026-01-01 00:00:05.000300+00', true),
 ('\x00000006', '2026-01-01 00:00:00+00', '2026-01-01 00:00:05.000700+00', true),
 ('\x00000007', '2026-01-01 00:00:00+00', '2026-01-01 00:00:04+00', true)`); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	want := []string{"00000004", "00000003", "00000002", "00000001", "00000006", "00000005", "00000007"}
	var got []string
	var cursor time.Time
	var cursorTag string
	for page := 0; page < 10; page++ {
		rows, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, cursor, cursorTag, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			got = append(got, r.TraceTag)
		}
		last := rows[len(rows)-1]
		cursor, cursorTag = time.UnixMilli(last.LastHeardAt), last.TraceTag
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("keyset pages = %v, want %v", got, want)
	}

	// Without cursorTag the cursor stays exclusive on the millisecond.
	rows, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC), "", 10)
	if err != nil || len(rows) != 1 || rows[0].TraceTag != "00000007" {
		t.Errorf("ms-only cursor: %+v %v", rows, err)
	}
}
