// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// Keep the upstream pagination oracle's original seven-column contract. Dev1's
// extra quality metadata is covered separately by the TRACE quality regressions.
type legacyTraceTagRow struct {
	TraceTag     string
	FirstHeardAt pgtype.Timestamptz
	LastHeardAt  pgtype.Timestamptz
	PacketCount  int64
	IataCount    int64
	TraceType    string
	BestPayload  []byte
}

func traceLegacyProjection(rows []sqlc.ListTraceTagsRow) []legacyTraceTagRow {
	result := make([]legacyTraceTagRow, len(rows))
	for i, row := range rows {
		result[i] = legacyTraceTagRow{
			TraceTag: row.TraceTag, FirstHeardAt: row.FirstHeardAt, LastHeardAt: row.LastHeardAt,
			PacketCount: row.PacketCount, IataCount: row.IataCount, TraceType: row.TraceType, BestPayload: row.BestPayload,
		}
	}
	return result
}

// legacyListTraceTags is the trace list before the row-comparison keyset, verbatim from the generated
// db/sqlc/queries.sql.go. The statement changed in place, so no sqlc statement keeps this text.
const legacyListTraceTags = `-- name: ListTraceTags :many

SELECT
    encode(t.trace_tag, 'hex') AS trace_tag,
    t.first_heard_at,
    t.last_heard_at,
    t.packet_count,
    (SELECT COUNT(*)
     FROM trace_iatas ti
     WHERE ti.trace_tag = t.trace_tag
       AND (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR ti.iata = ANY($1::bpchar[]))) AS iata_count,
    COALESCE(t.trace_type, '')::text AS trace_type,
    t.best_payload
FROM trace_tags t
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR EXISTS (
        SELECT 1 FROM trace_iatas ti WHERE ti.trace_tag = t.trace_tag AND ti.iata = ANY($1::bpchar[])))
  AND ($2::text = '' OR t.scope_id = (SELECT id FROM transport_scopes WHERE name = $2))
  AND ($3::timestamptz IS NULL OR t.first_heard_at >= $3)
  AND ($4::timestamptz IS NULL OR t.first_heard_at <= $4)
  -- Keyset on the millisecond clients see, tie-broken by tag; matches idx_trace_tags_keyset.
  AND ($5::timestamptz IS NULL
       OR date_trunc('milliseconds', t.last_heard_at, 'UTC') < $5
       OR ($8::bytea IS NOT NULL AND date_trunc('milliseconds', t.last_heard_at, 'UTC') = $5 AND t.trace_tag < $8))
  AND ($7::text = '' OR t.trace_type = $7)
ORDER BY date_trunc('milliseconds', t.last_heard_at, 'UTC') DESC, t.trace_tag DESC
LIMIT $6
`

// traceKeysetBase is the newest instant in the trace fixture. Row i is heard i minutes and i microseconds before it,
// except two three-way ties inside one millisecond (rows 10-12 and 20-22, distinct microseconds) and one pair tied to
// the microsecond (rows 30-31).
var traceKeysetBase = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

const traceKeysetTags = `
INSERT INTO trace_tags (trace_tag, first_heard_at, last_heard_at, packet_count, trace_type, scope_id, best_payload, heard)
SELECT substring(decode(md5('trace-' || i), 'hex') FROM 1 FOR 4),
       h - (i % 7) * interval '1 hour',
       h,
       i,
       (ARRAY['PING', 'TRACE', NULL])[i % 3 + 1],
       (ARRAY[1, 2, NULL])[i % 3 + 1],
       jsonb_build_object('pathHashes', jsonb_build_array(to_hex(i)), 'flags', 0, 'snrValues', jsonb_build_array(i)),
       true
FROM (SELECT i, CASE
        WHEN i IN (10, 11, 12) THEN $1::timestamptz - interval '10 minutes' + (i - 9) * interval '300 microseconds'
        WHEN i IN (20, 21, 22) THEN $1::timestamptz - interval '20 minutes' + interval '7 milliseconds' + (i - 19) * interval '200 microseconds'
        WHEN i IN (30, 31) THEN $1::timestamptz - interval '30 minutes' + interval '450 microseconds'
        ELSE $1::timestamptz - i * interval '1 minute' - i * interval '1 microsecond'
      END AS h
      FROM generate_series(0, 59) i) s`

const traceKeysetIATAs = `
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (1, 'scope-a', '\x01', '\x01'), (2, 'scope-b', '\x02', '\x02');
INSERT INTO trace_iatas (trace_tag, iata)
SELECT substring(decode(md5('trace-' || i), 'hex') FROM 1 FOR 4), iata
FROM generate_series(0, 59) i
CROSS JOIN LATERAL (VALUES ('YVR', i % 2 = 0), ('YYJ', i % 3 = 0), ('YYZ', i % 5 = 0), ('YOW', i % 4 = 1)) v(iata, member)
WHERE member;`

func TestTraceTagsKeysetMatchesTodayPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, traceKeysetIATAs)
	if _, err := tx.Exec(ctx, traceKeysetTags, traceKeysetBase); err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, tx, "ANALYZE trace_tags; ANALYZE trace_iatas")
	// Every tag with its last_heard_at, to find the ties and build the expected walk order.
	type tagTime struct {
		tag string
		at  time.Time
	}
	rows, err := tx.Query(ctx, "SELECT encode(trace_tag, 'hex'), last_heard_at FROM trace_tags")
	if err != nil {
		t.Fatal(err)
	}
	var all []tagTime
	for rows.Next() {
		var r tagTime
		if err := rows.Scan(&r.tag, &r.at); err != nil {
			t.Fatal(err)
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(all) != 60 {
		t.Fatalf("fixture holds %d tags, want 60", len(all))
	}
	byMilli := map[int64][]string{}
	for _, r := range all {
		byMilli[r.at.UnixMilli()] = append(byMilli[r.at.UnixMilli()], r.tag)
	}
	var tiedMillis []int64
	var tiedTags []string
	for ms, tags := range byMilli {
		if len(tags) > 1 {
			tiedMillis = append(tiedMillis, ms)
			tiedTags = append(tiedTags, tags...)
		}
	}
	sort.Slice(tiedMillis, func(i, j int) bool { return tiedMillis[i] < tiedMillis[j] })
	sort.Strings(tiedTags)
	if len(tiedMillis) != 3 || len(tiedTags) != 8 {
		t.Fatalf("fixture ties: %d tied milliseconds holding %d tags, want 3 and 8", len(tiedMillis), len(tiedTags))
	}

	ts := func(at time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: at, Valid: true} }
	nonTied := traceKeysetBase.Add(-5*time.Minute - 5*time.Microsecond).UnixMilli() // row 5, alone in its millisecond
	if len(byMilli[nonTied]) != 1 {
		t.Fatalf("millisecond %d holds %v, want one tag", nonTied, byMilli[nonTied])
	}
	cursors := []pgtype.Timestamptz{
		{},
		ts(time.Unix(0, 0)),
		ts(time.UnixMilli(nonTied)),
		ts(time.UnixMilli(tiedMillis[0]).Add(500 * time.Microsecond)), // a cursor off the millisecond grid
		ts(traceKeysetBase.Add(24 * time.Hour)),
	}
	for _, ms := range tiedMillis {
		cursors = append(cursors, ts(time.UnixMilli(ms)))
	}
	cursorTags := [][]byte{nil, {}, {0x00}, {0xff, 0xff, 0xff, 0xff, 0xff}} // {} is an empty, non-NULL bytea
	for _, tag := range tiedTags {
		b, err := hex.DecodeString(tag)
		if err != nil {
			t.Fatal(err)
		}
		cursorTags = append(cursorTags, b)
	}
	windows := []struct{ since, until pgtype.Timestamptz }{
		{},
		{since: ts(traceKeysetBase.Add(-3 * time.Hour))},
		{until: ts(traceKeysetBase.Add(-time.Hour))},
		{since: ts(traceKeysetBase.Add(-3 * time.Hour)), until: ts(traceKeysetBase.Add(-time.Hour))},
	}

	q := sqlc.New(tx)
	compared, nonEmpty := 0, 0
	for _, iatas := range [][]string{nil, {}, {"YVR"}, {"YVR", "YVR", "yvr"}} {
		for _, scope := range []string{"", "scope-a", "missing"} {
			for _, w := range windows {
				for _, traceType := range []string{"", "PING", "TRACE", "BOGUS"} {
					for _, limit := range []int32{1, 2, 50, 201} {
						for _, cursor := range cursors {
							for _, tag := range cursorTags {
								p := sqlc.ListTraceTagsParams{
									Column1: iatas, Column2: scope, Column3: w.since, Column4: w.until,
									Column5: cursor, Limit: limit, Column7: traceType, Column8: tag,
								}
								got, err := q.ListTraceTags(ctx, p)
								if err != nil {
									t.Fatal(err)
								}
								want := legacyRows[legacyTraceTagRow](t, ctx, tx, legacyListTraceTags,
									iatas, scope, w.since, w.until, cursor, limit, traceType, tag)
								if !sameRows(traceLegacyProjection(got), want) {
									t.Fatalf("ListTraceTags(%+v) = %d rows %v, legacy projection %d rows %v",
										p, len(got), traceTags(got), len(want), want)
								}
								compared++
								if len(got) > 0 {
									nonEmpty++
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d parameter sets compared, %d non-empty", compared, nonEmpty)
	if nonEmpty < compared/4 {
		t.Fatalf("only %d of %d parameter sets returned rows; the grid is too sparse to compare", nonEmpty, compared)
	}

	// Walking pages by (millisecond, last tag) returns every tag once, newest millisecond first, tag descending inside it.
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i].at.UnixMilli(), all[j].at.UnixMilli()
		if a != b {
			return a > b
		}
		return all[i].tag > all[j].tag
	})
	wantOrder := make([]string, len(all))
	for i, r := range all {
		wantOrder[i] = r.tag
	}
	store := &Store{q: q}
	for _, limit := range []int32{1, 2, 50} {
		var gotOrder []string
		var cursor time.Time
		var cursorTag string
		for page := 0; page <= len(all); page++ {
			items, err := store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, cursor, cursorTag, limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) == 0 {
				break
			}
			for _, it := range items {
				gotOrder = append(gotOrder, it.TraceTag)
			}
			last := items[len(items)-1]
			cursor, cursorTag = time.UnixMilli(last.LastHeardAt), last.TraceTag
		}
		if !reflect.DeepEqual(gotOrder, wantOrder) {
			t.Errorf("limit %d walk = %v, want %v", limit, gotOrder, wantOrder)
		}
	}

	// The cursor is an index condition on idx_trace_tags_keyset, also in the generic plan a prepared statement reuses.
	capture := &statementCapture{Tx: tx, name: "ListTraceTags"}
	if _, err := sqlc.New(capture).ListTraceTags(ctx, sqlc.ListTraceTagsParams{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if capture.statement == "" {
		t.Fatal("ListTraceTags statement not captured")
	}
	mustExec(t, ctx, tx, "SET LOCAL jit = off")
	// Sixty rows favour a bitmap scan and a sort; with both off the planner shows the ordered keyset scan it picks at size.
	mustExec(t, ctx, tx, "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off")
	mustExec(t, ctx, tx, "PREPARE trace_keyset AS "+capture.statement)
	execute := fmt.Sprintf("EXPLAIN (FORMAT JSON) EXECUTE trace_keyset(NULL, '', NULL, NULL, '%s', 50, '', '\\x%s')",
		time.UnixMilli(tiedMillis[0]).UTC().Format(time.RFC3339Nano), tiedTags[0])
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
		var raw []byte
		if err := tx.QueryRow(ctx, execute).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var plans []struct{ Plan map[string]any }
		if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
			t.Fatalf("EXPLAIN JSON did not parse (%v): %s", err, raw)
		}
		var scans []map[string]any
		var walk func(map[string]any)
		walk = func(n map[string]any) {
			if n["Relation Name"] == "trace_tags" {
				scans = append(scans, n)
			}
			if children, ok := n["Plans"].([]any); ok {
				for _, c := range children {
					walk(c.(map[string]any))
				}
			}
		}
		walk(plans[0].Plan)
		var cond string
		if len(scans) == 1 {
			cond, _ = scans[0]["Index Cond"].(string)
		}
		if len(scans) != 1 || scans[0]["Node Type"] != "Index Scan" || scans[0]["Index Name"] != "idx_trace_tags_keyset" ||
			!strings.Contains(cond, "ROW(date_trunc('milliseconds'::text, last_heard_at, 'UTC'::text), trace_tag) < ROW(") {
			t.Errorf("%s: trace_tags scan is not an index-conditioned scan of idx_trace_tags_keyset: %s", mode, raw)
		}
	}
	mustExec(t, ctx, tx, "DEALLOCATE trace_keyset")
}

func traceTags(rows []sqlc.ListTraceTagsRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.TraceTag
	}
	return out
}

func TestTelemetrySinceMatchesCatchAllPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	obsA := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	obsB := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	obsC := uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	unknown := uuid.MustParse("00000000-0000-0000-0000-0000000000ff")
	now := time.Now().UTC().Truncate(time.Microsecond)
	// A: 300 points over 40 days, inserted in an order unrelated to reported_at so ids and times disagree. B: one point.
	// Then the sequence jumps and A gets 20 more points, older than some earlier ones, whose ids cross 2^31.
	for _, st := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO observers (id, public_key) VALUES ($1, '\x0a'), ($2, '\x0b'), ($3, '\x0c')`, []any{obsA, obsB, obsC}},
		{`INSERT INTO observer_telemetry (observer_id, reported_at, battery_voltage_mv, airtime_tx_secs, uptime_seconds, receive_errors)
SELECT $1, $2::timestamptz - i * interval '192 minutes', 3600 + i, i * 1.5, i * 60, CASE WHEN i % 4 = 0 THEN NULL ELSE i END
FROM generate_series(0, 299) i ORDER BY md5(i::text)`, []any{obsA, now}},
		{`INSERT INTO observer_telemetry (observer_id, reported_at, battery_voltage_mv) VALUES ($1, $2::timestamptz - interval '2 hours', 3900)`, []any{obsB, now}},
		{`SELECT setval('observer_telemetry_id_seq', 2147483640)`, nil},
		{`INSERT INTO observer_telemetry (observer_id, reported_at, noise_floor_db, queue_length, debug_flags)
SELECT $1, $2::timestamptz - i * interval '37 hours' - interval '1 minute', -90 - i, i, i
FROM generate_series(0, 19) i`, []any{obsA, now}},
	} {
		if _, err := tx.Exec(ctx, st.sql, st.args...); err != nil {
			t.Fatal(err)
		}
	}
	var median int64
	if err := tx.QueryRow(ctx, "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY id) FROM observer_telemetry WHERE observer_id = $1", obsA).Scan(&median); err != nil {
		t.Fatal(err)
	}

	q := sqlc.New(tx)
	afterIDs := []int64{-5, 0, median, math.MaxInt32, math.MaxInt32 + 1, math.MaxInt32 + 3, math.MaxInt64}
	compared, nonEmpty := 0, 0
	for _, obs := range []uuid.UUID{obsA, obsB, obsC, unknown} {
		for _, since := range []time.Time{now.Add(-24 * time.Hour), now.Add(-168 * time.Hour), now.Add(-720 * time.Hour), time.Unix(0, 0)} {
			for _, afterID := range afterIDs {
				sinceTS := pgtype.Timestamptz{Time: since, Valid: true}
				got, err := q.GetObserverTelemetrySince(ctx, sqlc.GetObserverTelemetrySinceParams{ObserverID: obs, Since: sinceTS, AfterID: afterID})
				if err != nil {
					t.Fatal(err)
				}
				want, err := q.GetObserverTelemetry(ctx, sqlc.GetObserverTelemetryParams{ObserverID: obs, Column2: sinceTS, Column4: afterID})
				if err != nil {
					t.Fatal(err)
				}
				converted := make([]sqlc.GetObserverTelemetryRow, len(got))
				for i, r := range got {
					converted[i] = sqlc.GetObserverTelemetryRow(r)
				}
				if !sameRows(converted, want) {
					t.Fatalf("observer %s since %s after %d: since-form %d rows, catch-all %d rows", obs, since, afterID, len(got), len(want))
				}
				compared++
				if len(got) > 0 {
					nonEmpty++
				}
			}
		}
	}
	t.Logf("%d parameter sets compared, %d non-empty", compared, nonEmpty)

	// Past 2^31 the cursor still pages: ids 2^31 .. 2^31+12 follow 2^31-1, and 2^31+2 leaves 10 of them.
	for _, tc := range []struct {
		after int64
		want  int
	}{{math.MaxInt32, 13}, {math.MaxInt32 + 1, 12}, {math.MaxInt32 + 3, 10}, {math.MaxInt64, 0}, {0, 320}, {-5, 320}} {
		got, err := q.GetObserverTelemetrySince(ctx, sqlc.GetObserverTelemetrySinceParams{
			ObserverID: obsA, Since: pgtype.Timestamptz{Time: time.Unix(0, 0), Valid: true}, AfterID: tc.after,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != tc.want {
			t.Errorf("after %d: %d rows, want %d", tc.after, len(got), tc.want)
		}
	}
}
