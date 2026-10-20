// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// legacyGetObserverComparison is the comparison before each observer's receptions were read on their own,
// verbatim from the generated db/sqlc/observer_comparison.sql.go. The statement changed in place.
const legacyGetObserverComparison = `-- name: GetObserverComparison :one
WITH heard AS (
    SELECT po.packet_hash,
           bool_or(po.observer_id = $1::uuid) AS seen_a,
           bool_or(po.observer_id = $2::uuid) AS seen_b
    FROM packet_observations po
    JOIN packets p ON p.packet_hash = po.packet_hash
    WHERE po.observer_id IN ($1::uuid, $2::uuid)
      AND po.heard_at >= $3::timestamptz
      AND po.heard_at < $4::timestamptz
      AND p.route_type IN (0, 1)
      AND (COALESCE(cardinality($5::text[]), 0) = 0 OR po.iata = ANY($5::text[]))
    GROUP BY po.packet_hash
)
SELECT count(*) AS total_packets,
       count(*) FILTER (WHERE seen_a AND NOT seen_b) AS only_a,
       count(*) FILTER (WHERE seen_b AND NOT seen_a) AS only_b,
       count(*) FILTER (WHERE seen_a AND seen_b) AS both,
       EXISTS (SELECT 1 FROM observers WHERE id = $1::uuid) AS known_a,
       EXISTS (SELECT 1 FROM observers WHERE id = $2::uuid) AS known_b
FROM heard
`

var comparisonBase = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// comparisonFixture: 2,000 packets of every route type, one every five minutes from comparisonBase (about seven
// days), heard by eight observers with skewed rates (90 % down to 2 %) at a per-observer offset of whole seconds, so
// many receptions share an instant. Observer 1 hears at YVR and sometimes YYJ, observer 4 alternates YVR and YYZ,
// observers 5-8 hear at YOW. Observer 9 is known and never hears anything.
var comparisonFixture = []string{`
INSERT INTO observers (id, public_key)
SELECT ('00000000-0000-0000-0000-' || lpad(i::text, 12, '0'))::uuid, decode(lpad(to_hex(i), 64, '0'), 'hex')
FROM generate_series(1, 9) i WHERE $1::timestamptz IS NOT NULL`, `
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
SELECT decode(md5('cmp-' || i), 'hex'), i % 12, 0, (ARRAY[0, 1, 1, 0, 2, 1, 3, 0])[i % 8 + 1], '\x00', '\x00',
       $1::timestamptz + i * interval '5 minutes', $1::timestamptz + i * interval '5 minutes'
FROM generate_series(1, 2000) i`, `
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, source_broker)
SELECT decode(md5('cmp-' || i), 'hex'),
       ('00000000-0000-0000-0000-' || lpad(o::text, 12, '0'))::uuid,
       CASE WHEN o = 1 AND i % 7 = 0 THEN 'YYJ' WHEN o IN (1, 2) THEN 'YVR' WHEN o = 3 THEN 'YYJ'
            WHEN o = 4 THEN (ARRAY['YVR', 'YYZ'])[i % 2 + 1] ELSE 'YOW' END,
       $1::timestamptz + i * interval '5 minutes' + (o % 3) * interval '1 second',
       0, 1, 0, 'a'
FROM generate_series(1, 2000) i
CROSS JOIN generate_series(1, 8) o
WHERE ('x' || substr(md5(i || '-' || o), 1, 8))::bit(32)::bigint % 100 < (ARRAY[90, 70, 40, 10, 2, 2, 2, 2])[o]`}

// The comparison reads each observer's receptions from idx_observations_observer and returns exactly what the
// statement it replaced returned, in custom and generic plans, also with sequential scans off.
func TestObserverComparisonMatchesTodayPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	// Migration 046 drops it; nothing may lean on it.
	mustExec(t, ctx, tx, "DROP INDEX idx_observations_observer_id")
	for _, sql := range comparisonFixture {
		if _, err := tx.Exec(ctx, sql, comparisonBase); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, ctx, tx, "ANALYZE observers; ANALYZE packets; ANALYZE packet_observations")
	mustExec(t, ctx, tx, "SET LOCAL jit = off")

	obs := func(i int) uuid.UUID { return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)) }
	unknown := uuid.MustParse("00000000-0000-0000-0000-0000000000ff")
	pairs := [][2]uuid.UUID{
		{obs(1), obs(2)}, {obs(2), obs(1)}, {obs(1), obs(4)}, {obs(3), obs(4)}, {obs(4), obs(5)},
		{obs(5), obs(6)}, {obs(1), obs(9)}, {obs(9), obs(5)}, {obs(1), obs(1)}, {obs(2), unknown},
		{unknown, obs(1)},
	}
	// One reception instant, to put window edges exactly on receptions.
	edge := comparisonBase.Add(500*5*time.Minute + time.Second)
	ts := func(at time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: at, Valid: true} }
	windows := [][2]time.Time{
		{comparisonBase.Add(-time.Hour), comparisonBase.Add(8 * 24 * time.Hour)},
		{comparisonBase.Add(24 * time.Hour), comparisonBase.Add(48 * time.Hour)},
		{edge, edge.Add(time.Hour)},
		{edge.Add(-time.Hour), edge},
		{edge, edge.Add(time.Nanosecond * 1000)},
		{comparisonBase.Add(30 * 24 * time.Hour), comparisonBase.Add(31 * 24 * time.Hour)},
	}
	iataSets := [][]string{nil, {}, {"YVR"}, {"YYJ", "YVR", "YYJ"}, {"YOW"}, {"ZZZ"}, {""}, {"yvr"}}

	q := sqlc.New(tx)
	settings := []string{"RESET enable_seqscan", "SET LOCAL enable_seqscan = off"}
	compared, nonEmpty := 0, 0
	for _, setting := range settings {
		for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
			mustExec(t, ctx, tx, setting)
			mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
			// Cached plans keep the settings they were made under.
			if err := tx.Conn().DeallocateAll(ctx); err != nil {
				t.Fatal(err)
			}
			for _, pair := range pairs {
				for _, w := range windows {
					for _, iatas := range iataSets {
						p := sqlc.GetObserverComparisonParams{
							ObserverA: pair[0], ObserverB: pair[1], Since: ts(w[0]), Until: ts(w[1]), Iatas: iatas,
						}
						got, err := q.GetObserverComparison(ctx, p)
						if err != nil {
							t.Fatal(err)
						}
						want := legacyRows[sqlc.GetObserverComparisonRow](t, ctx, tx, legacyGetObserverComparison,
							p.ObserverA, p.ObserverB, p.Since, p.Until, p.Iatas)
						if len(want) != 1 || got != want[0] {
							t.Fatalf("%s, %s: GetObserverComparison(%+v) = %+v, today %+v", setting, mode, p, got, want)
						}
						compared++
						if got.TotalPackets > 0 {
							nonEmpty++
						}
					}
				}
			}
		}
	}
	t.Logf("%d parameter sets compared, %d non-empty", compared, nonEmpty)
	if nonEmpty < compared/4 {
		t.Fatalf("only %d of %d parameter sets counted packets; the grid is too sparse to compare", nonEmpty, compared)
	}

	// Plans: every observation scan is a range of idx_observations_observer, never a walk of the window.
	capture := &statementCapture{Tx: tx, name: "GetObserverComparison"}
	if _, err := sqlc.New(capture).GetObserverComparison(ctx, sqlc.GetObserverComparisonParams{ObserverA: obs(1), ObserverB: obs(2)}); err != nil {
		t.Fatal(err)
	}
	if capture.statement == "" {
		t.Fatal("GetObserverComparison statement not captured")
	}
	mustExec(t, ctx, tx, "SET LOCAL enable_seqscan = off")
	mustExec(t, ctx, tx, "PREPARE observer_comparison AS "+capture.statement)
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		mustExec(t, ctx, tx, "SET LOCAL plan_cache_mode = "+mode)
		for _, pair := range [][2]int{{1, 2}, {5, 6}, {1, 9}} {
			execute := fmt.Sprintf("EXPLAIN (FORMAT JSON) EXECUTE observer_comparison('%s', '%s', '%s', '%s', NULL)",
				obs(pair[0]), obs(pair[1]), comparisonBase.Format(time.RFC3339), comparisonBase.Add(7*24*time.Hour).Format(time.RFC3339))
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
				index, _ := n["Index Name"].(string)
				if n["Relation Name"] == "packet_observations" ||
					(n["Node Type"] == "Bitmap Index Scan" && (strings.HasPrefix(index, "idx_observations") || strings.HasPrefix(index, "packet_observations"))) {
					scans = append(scans, n)
				}
				if children, ok := n["Plans"].([]any); ok {
					for _, c := range children {
						walk(c.(map[string]any))
					}
				}
			}
			walk(plans[0].Plan)
			indexScans := 0
			for _, s := range scans {
				switch s["Node Type"] {
				case "Bitmap Heap Scan":
					continue // its Bitmap Index Scan child names the index
				case "Index Scan", "Index Only Scan", "Bitmap Index Scan":
					if s["Index Name"] != "idx_observations_observer" {
						t.Errorf("%s %v: observations read through %v, not idx_observations_observer: %s", mode, pair, s["Index Name"], raw)
					}
					indexScans++
				default:
					t.Errorf("%s %v: observations read by %v: %s", mode, pair, s["Node Type"], raw)
				}
			}
			if indexScans != 2 {
				t.Errorf("%s %v: %d observation index scans, want one per observer: %s", mode, pair, indexScans, raw)
			}
		}
	}
	mustExec(t, ctx, tx, "DEALLOCATE observer_comparison")
}
