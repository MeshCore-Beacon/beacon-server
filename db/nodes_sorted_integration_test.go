// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Every sorted walk, at any page size, must give the order a plain ORDER BY gives, and that order
// must be the web table's: natural names, unnamed last, types by name, ties newest first.
func TestListNodesSortedPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for the PostgreSQL regression test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	for _, table := range []string{"nodes", "node_iatas", "node_neighbors", "observers", "transport_scopes"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (1, '#test', decode(repeat('00', 16), 'hex'), decode(repeat('00', 8), 'hex'))`); err != nil {
		t.Fatal(err)
	}

	named := []string{"Node 10", "Node 9", "node 9", "Älpha", "alpha", "Alpha", "🏔️ Peak", "#hash", "Zulu", "zebra", "42", "Dup", "Dup", "Dup"}
	rng := rand.New(rand.NewPCG(3, 5))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := map[uuid.UUID]string{}
	add := func(name *string, nodeType int16, seen time.Time) uuid.UUID {
		id := uuid.New()
		key := make([]byte, 32)
		copy(key, id[:])
		if _, err := tx.Exec(ctx, `INSERT INTO nodes (id, public_key, node_type, name, last_seen, default_scope_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			id, key, nodeType, name, seen, map[bool]any{true: 1, false: nil}[rng.IntN(3) == 0]); err != nil {
			t.Fatal(err)
		}
		if rng.IntN(2) == 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO node_iatas (node_id, iata, last_heard) VALUES ($1, 'YVR', $2)`, id, seen); err != nil {
				t.Fatal(err)
			}
		}
		ids[id] = "<nil>"
		if name != nil {
			ids[id] = *name
		}
		return id
	}
	types := []int16{1, 2, 3, 4, 0, 5, 13}
	tie := base.Add(-time.Hour) // the three Dups share a last_seen with two others, so id breaks ties
	for i, name := range named {
		seen := base.Add(-time.Duration(rng.IntN(100000)) * time.Second)
		if name == "Dup" || i < 2 {
			seen = tie
		}
		add(&name, types[rng.IntN(len(types))], seen)
	}
	empty := ""
	add(nil, 2, base.Add(-3*time.Second))
	add(&empty, 1, base.Add(-2*time.Second))
	for i := range 160 {
		name := fmt.Sprintf("n%d", rng.IntN(60)) // repeats, so names tie too
		add(&name, types[rng.IntN(len(types))], base.Add(-time.Duration(i%40)*time.Minute))
	}
	all := make([]uuid.UUID, 0, len(ids))
	for id := range ids {
		all = append(all, id)
	}
	for range 400 {
		a, b := all[rng.IntN(len(all))], all[rng.IntN(len(all))]
		if a == b {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO node_neighbors (node_id, neighbor_id, iata) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			a, b, []string{"YVR", "YYJ"}[rng.IntN(2)]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, `ANALYZE nodes; ANALYZE node_iatas; ANALYZE node_neighbors`); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx), staleThreshold: 24 * time.Hour}

	// The same orders written plainly, over the same filters.
	want := func(sort api.NodeSort, nodeType int16, iatas []string, scope string) []uuid.UUID {
		t.Helper()
		dir := map[bool]string{false: "ASC", true: "DESC"}[sort.Descending]
		key := map[string]string{
			api.NodeSortName:      "(coalesce(n.name, '') = ''), coalesce(n.name, '') COLLATE natural_text " + dir,
			api.NodeSortType:      "CASE WHEN n.node_type BETWEEN 1 AND 4 THEN n.node_type ELSE 5 END " + dir,
			api.NodeSortNeighbors: "(SELECT count(DISTINCT neighbor_id) FROM node_neighbors WHERE node_id = n.id) " + dir,
		}[sort.By]
		rows, err := tx.Query(ctx, `SELECT n.id FROM nodes n LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE ($1::smallint = 0 OR n.node_type = $1) AND (coalesce(cardinality($2::text[]), 0) = 0 OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY($2)))
  AND ($3 = '' OR ts.name = $3)
ORDER BY `+key+`, n.last_seen DESC, n.id`, nodeType, iatas, scope)
		if err != nil {
			t.Fatal(err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	walk := func(sort api.NodeSort, nodeType int16, iatas []string, scope string, limit int32) []uuid.UUID {
		t.Helper()
		var got []uuid.UUID
		var cursor *api.NodeSortCursor
		for {
			page, err := store.ListNodesSorted(ctx, nodeType, iatas, nil, nil, nil, "", "", scope, sort, cursor, limit, false)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range page.Items {
				got = append(got, n.ID)
			}
			if !page.HasMore {
				return got
			}
			if cursor, err = api.ParseNodeSortCursor(*page.NextPageCursor); err != nil {
				t.Fatal(err)
			}
			if len(got) > len(ids) {
				t.Fatal("the walk repeats rows")
			}
		}
	}
	for _, by := range []string{api.NodeSortName, api.NodeSortType, api.NodeSortNeighbors} {
		for _, desc := range []bool{false, true} {
			sort := api.NodeSort{By: by, Descending: desc}
			for _, f := range []struct {
				nodeType int16
				iatas    []string
				scope    string
			}{{0, nil, ""}, {2, nil, ""}, {0, []string{"YVR"}, ""}, {0, nil, "#test"}} {
				expect := want(sort, f.nodeType, f.iatas, f.scope)
				if len(expect) == 0 {
					t.Fatalf("%+v %+v: empty fixture", sort, f)
				}
				for _, limit := range []int32{1, 2, 3, 7, 500} {
					if got := walk(sort, f.nodeType, f.iatas, f.scope, limit); !slices.Equal(got, expect) {
						t.Fatalf("%+v %+v limit %d: walk differs from ORDER BY\n got %v\nwant %v", sort, f, limit, names(got, ids), names(expect, ids))
					}
				}
			}
		}
	}

	// The plain order is the web table's.
	asc := names(want(api.NodeSort{By: api.NodeSortName}, 0, nil, ""), ids)
	desc := names(want(api.NodeSort{By: api.NodeSortName, Descending: true}, 0, nil, ""), ids)
	for _, order := range [][]string{
		{"#hash", "🏔️ Peak", "42", "alpha", "Alpha", "Älpha", "node 9", "Node 9", "Node 10", "zebra", "Zulu"},
		{"Node 10", "Node 9"}, // desc reverses names only
	} {
		list := asc
		if len(order) == 2 {
			list = desc
		}
		last := -1
		for _, name := range order {
			i := slices.Index(list, name)
			if i <= last {
				t.Fatalf("%q out of order in %v", name, list)
			}
			last = i
		}
	}
	for _, list := range [][]string{asc, desc} {
		if tail := list[len(list)-2:]; !slices.Contains(tail, "<nil>") || !slices.Contains(tail, "") {
			t.Fatalf("unnamed nodes are not last: %v", tail)
		}
	}
}

func names(order []uuid.UUID, ids map[uuid.UUID]string) []string {
	out := make([]string, len(order))
	for i, id := range order {
		out[i] = ids[id]
	}
	return out
}
