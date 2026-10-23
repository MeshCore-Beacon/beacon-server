// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"cmp"
	"reflect"
	"slices"
	"strings"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

type nodeTypeRow struct {
	NodeType int16
	Count    int64
}

// Today's statement, verbatim (origin/dev queries.sql:1233-1240), before the split.
const legacyStatsNodeTypes = `SELECT
  n.node_type,
  COUNT(DISTINCT n.id)::bigint AS count
FROM nodes n
LEFT JOIN node_iatas ni ON ni.node_id = n.id
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR ni.iata = ANY($1::bpchar[]))
GROUP BY n.node_type
ORDER BY count DESC`

// The unfiltered page must not read node_iatas (plan cliff); every input returns today's rows.
func TestStatsNodeTypesSplitPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `
INSERT INTO nodes(public_key,node_type) SELECT int4send(i), 1 + i % 4 FROM generate_series(1,40) i;
INSERT INTO node_iatas(node_id,iata) SELECT id,'YVR' FROM nodes WHERE get_byte(public_key,3) % 3 = 0;
INSERT INTO node_iatas(node_id,iata) SELECT id,'YYJ' FROM nodes WHERE get_byte(public_key,3) % 5 < 2;`); err != nil {
		t.Fatal(err)
	}
	capture := &statementCapture{Tx: tx, name: "GetStatsNodeTypes"}
	store := &Store{q: sqlc.New(capture)}
	if _, err := store.GetStatsNodeTypes(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+capture.statement, capture.args...).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan, "node_iatas") {
		t.Fatalf("unfiltered node types read node_iatas: %s", plan)
	}
	many := []string{"YVR"}
	for i := range 49 {
		many = append(many, string(rune('A'+i%26))+"Z"+string(rune('A'+i/26)))
	}
	norm := func(r []nodeTypeRow) []nodeTypeRow {
		return slices.SortedFunc(slices.Values(r), func(a, b nodeTypeRow) int {
			return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.NodeType, b.NodeType))
		})
	}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode = "+mode); err != nil {
			t.Fatal(err)
		}
		for _, iatas := range [][]string{nil, {}, {"YVR"}, {"YYJ"}, {"YVR", "YYJ"}, {"YVR", "YVR"}, {"yvr"}, {"YV"}, {"YVR "}, {""}, {"ZZZ"}, many} {
			want := legacyRows[nodeTypeRow](t, ctx, tx, legacyStatsNodeTypes, iatas)
			got, err := store.GetStatsNodeTypes(ctx, iatas)
			if err != nil {
				t.Fatal(err)
			}
			gotRows := make([]nodeTypeRow, 0, len(got))
			for i, g := range got {
				if i > 0 && g.Count > got[i-1].Count {
					t.Fatalf("%s %q: not ordered by count: %+v", mode, iatas, got)
				}
				gotRows = append(gotRows, nodeTypeRow{NodeType: g.NodeType, Count: g.Count})
			}
			if !reflect.DeepEqual(norm(gotRows), norm(want)) {
				t.Fatalf("%s %q: got %+v, today %+v", mode, iatas, gotRows, want)
			}
		}
	}
}
