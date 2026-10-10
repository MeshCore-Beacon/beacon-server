// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestSearchCrossIATARoutesPostgres(t *testing.T) {
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

	// YVR route P→S→A→B, B heard by X in YYJ, YYJ route X→C→T→Z. S is aa, T is bb.
	// P also links to X (before the source, must be ignored); YYJ route T→X has X after T.
	ids := map[string]uuid.UUID{}
	for _, n := range []string{"P", "S", "A", "B", "Q", "X", "C", "T", "Z", "D1", "D2"} {
		ids[n] = uuid.New()
	}
	order := []string{"P", "S", "A", "B", "Q", "X", "C", "T", "Z", "D1", "D2"}
	nodeIDs, lats := make([]uuid.UUID, len(order)), make([]float64, len(order))
	for i, n := range order {
		nodeIDs[i], lats[i] = ids[n], float64(i+1)
	}
	id := func(n string) uuid.UUID { return ids[n] }
	for _, st := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO nodes (id, public_key, node_type, name, latitude, longitude)
SELECT id, decode(md5(id::text), 'hex'), 2, name, lat, 0
FROM unnest($1::uuid[], $2::text[], $3::float8[]) AS t(id, name, lat)`, []any{nodeIDs, order, lats}},
		{`INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT id, iata, prefix FROM unnest($1::uuid[], $2::bpchar[], $3::bytea[]) AS t(id, iata, prefix)`, []any{nodeIDs,
			[]string{"YVR", "YVR", "YVR", "YVR", "YVR", "YYJ", "YYJ", "YYJ", "YYJ", "YYZ", "YYZ"},
			[][]byte{{1, 0, 0, 1}, {0xaa, 0, 0, 1}, {2, 0, 0, 1}, {3, 0, 0, 1}, {4, 0, 0, 1}, {5, 0, 0, 1}, {6, 0, 0, 1}, {0xbb, 0, 0, 1}, {7, 0, 0, 1}, {0xaa, 0, 0, 2}, {0xaa, 0, 0, 3}}}},
		{`INSERT INTO node_neighbors (node_id, neighbor_id, iata, last_seen) VALUES
  ($1, $2, 'YYJ', '2026-01-01 00:00:00+00'), ($3, $2, 'YYJ', '2026-01-01 00:00:00+00')`, []any{id("B"), id("X"), id("P")}},
		{`INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count) VALUES
  ('\x01', ARRAY[$1, $2, $3, $4]::uuid[], ARRAY['\x01','\xaa','\x02','\x03']::bytea[], 'YVR', 4),
  ('\x02', ARRAY[$2, $3, $4, $5]::uuid[], ARRAY['\xaa','\x02','\x03','\x04']::bytea[], 'YVR', 4),
  ('\x03', ARRAY[$6, $7, $8, $9]::uuid[], ARRAY['\x05','\x06','\xbb','\x07']::bytea[], 'YYJ', 4),
  ('\x04', ARRAY[$8, $6]::uuid[], ARRAY['\xbb','\x05']::bytea[], 'YYJ', 2)`,
			[]any{id("P"), id("S"), id("A"), id("B"), id("Q"), id("X"), id("C"), id("T"), id("Z")}},
	} {
		if _, err := tx.Exec(ctx, st.sql, st.args...); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}

	names := func(hops []api.RouteHop) []string {
		out := make([]string, len(hops))
		for i, h := range hops {
			for n, id := range ids {
				if id == h.NodeID {
					out[i] = n
				}
			}
		}
		return out
	}
	want := func(t *testing.T, got []api.CrossIATARoute) {
		t.Helper()
		if len(got) != 1 {
			t.Fatalf("got %d routes, want 1: %+v", len(got), got)
		}
		r := got[0]
		if s, d := names(r.SourceSegment), names(r.TargetSegment); len(s) != 3 || s[0] != "S" || s[2] != "B" || len(d) != 3 || d[0] != "X" || d[2] != "T" {
			t.Errorf("segments = %v → %v, want [S A B] → [X C T]", s, d)
		}
		h := r.CrossHop
		if h.FromNode.ID != ids["B"] || h.FromNode.Latitude == nil || *h.FromNode.Latitude != 4 || h.ToNode.ID != ids["X"] ||
			h.FromIATA != "YVR" || h.ToIATA != "YYJ" || r.TotalHops != 6 {
			t.Errorf("cross hop = %+v total %d", h, r.TotalHops)
		}
	}

	for _, tc := range []struct {
		name string
		q    api.CrossRouteSearch
	}{
		{"legacy pair", api.CrossRouteSearch{FromHash: "aa", ToHash: "bb", FromIATAs: []string{"YVR"}, ToIATAs: []string{"YYJ"}}},
		{"iatas", api.CrossRouteSearch{FromHash: "aa", ToHash: "bb", FromIATAs: []string{"YVR", "YYJ", "YYZ"}, ToIATAs: []string{"YVR", "YYJ", "YYZ"}}},
		{"all iatas", api.CrossRouteSearch{FromHash: "aa", ToHash: "bb"}},
		{"wider hash", api.CrossRouteSearch{FromHash: "aa000001", ToHash: "bb00"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.SearchCrossIATARoutes(ctx, tc.q)
			if err != nil {
				t.Fatal(err)
			}
			want(t, got)
		})
	}

	t.Run("within-IATA search", func(t *testing.T) {
		for _, tc := range []struct {
			name, from, to string
			iatas          []string
			want           int
			iata           string
		}{
			{"two iatas", "aa", "03", []string{"YVR", "YYJ"}, 2, "YVR"},
			{"all iatas", "aa", "03", nil, 2, "YVR"},
			{"mixed widths", "aa00", "0300", nil, 2, "YVR"},
			{"destination before source", "05", "bb", nil, 1, "YYJ"},
			{"wrong order", "03", "aa", nil, 0, ""},
			{"other iata only", "aa", "03", []string{"YYJ"}, 0, ""},
		} {
			got, err := store.SearchKnownRoutes(ctx, tc.iatas, tc.from, tc.to)
			if err != nil || len(got) != tc.want {
				t.Fatalf("%s: %d routes, err %v; want %d", tc.name, len(got), err, tc.want)
			}
			for _, r := range got {
				if h := names(r.Hops); r.IATA != tc.iata || len(h) != 3 || int(r.HopCount) != len(h) {
					t.Errorf("%s: route %s %v hopCount %d", tc.name, r.IATA, h, r.HopCount)
				}
			}
		}
	})

	t.Run("neighbor row written receiver-first", func(t *testing.T) {
		if _, err := tx.Exec(ctx, `UPDATE node_neighbors SET node_id = neighbor_id, neighbor_id = node_id, iata = 'YVR' WHERE node_id = $1`, ids["B"]); err != nil {
			t.Fatal(err)
		}
		got, err := store.SearchCrossIATARoutes(ctx, api.CrossRouteSearch{FromHash: "aa", ToHash: "bb"})
		if err != nil {
			t.Fatal(err)
		}
		want(t, got)
	})

	t.Run("same path in two target iatas", func(t *testing.T) {
		if _, err := tx.Exec(ctx, `INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT node_id, 'YOW', prefix_4 FROM node_short_ids WHERE iata = 'YYJ'`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count)
SELECT path_key, node_ids, hash_prefix, 'YOW', hop_count FROM known_routes WHERE iata = 'YYJ'`); err != nil {
			t.Fatal(err)
		}
		got, err := store.SearchCrossIATARoutes(ctx, api.CrossRouteSearch{FromHash: "aa", ToHash: "bb"})
		if err != nil || len(got) != 2 || got[0].CrossHop.ToIATA == got[1].CrossHop.ToIATA {
			t.Fatalf("got %d routes, err %v; want one each to YYJ and YOW", len(got), err)
		}
	})

	t.Run("reverse direction", func(t *testing.T) {
		got, err := store.SearchCrossIATARoutes(ctx, api.CrossRouteSearch{FromHash: "aa", ToHash: "bb", FromIATAs: []string{"YYJ"}, ToIATAs: []string{"YVR"}})
		if err != nil || len(got) != 0 {
			t.Fatalf("got %d routes, err %v; want none", len(got), err)
		}
	})

	t.Run("too broad", func(t *testing.T) {
		if _, err := tx.Exec(ctx, `INSERT INTO iata_codes (iata) SELECT 'Q' || lpad(i::text, 2, '0') FROM generate_series(1, 60) i`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT $1, 'Q' || lpad(i::text, 2, '0'), '\xaa000001'::bytea FROM generate_series(1, 60) i`, ids["S"]); err != nil {
			t.Fatal(err)
		}
		if _, err := store.SearchCrossIATARoutes(ctx, api.CrossRouteSearch{FromHash: "aa", ToHash: "bb"}); !errors.Is(err, api.ErrRouteSearchTooBroad) {
			t.Fatalf("err = %v, want ErrRouteSearchTooBroad", err)
		}
	})
}
