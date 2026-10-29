// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// hygieneMigrations drop the lookup-table indexes that no plan reads.
var hygieneMigrations = []string{
	"047_drop_channel_iatas_iata.sql",
	"048_drop_node_neighbors_node.sql",
	"049_drop_transport_scopes_fingerprint.sql",
	"050_drop_meshmapper_channel_members_fingerprint.sql",
	"051_drop_region_iatas_iata.sql",
	"052_drop_trace_tags_first_heard.sql",
}

// hygieneIndexSets is each lookup table's full index set after the migrations.
var hygieneIndexSets = []struct {
	table   string
	indexes []string
}{
	{"channel_iatas", []string{"channel_iatas_pkey"}},
	{"node_neighbors", []string{"idx_node_neighbors_neighbor", "node_neighbors_pkey"}},
	{"transport_scopes", []string{"transport_scopes_name_key", "transport_scopes_pkey"}},
	{"meshmapper_channel_members", []string{"meshmapper_channel_members_pkey"}},
	{"region_iatas", []string{"region_iatas_pkey"}},
	{"trace_tags", []string{"idx_trace_tags_keyset", "idx_trace_tags_last_heard", "trace_tags_pkey"}},
	// Kept: GetScopeByName reads it for a rare scope.
	{"analytics_hourly_scope_sets", []string{"analytics_hourly_scope_sets_pkey", "idx_analytics_scope_sets_iatas", "idx_analytics_scope_sets_scope"}},
}

// checkIndexSets compares each table's valid, ready indexes with the expected set.
func checkIndexSets(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, want := range hygieneIndexSets {
		var got, bad []string
		if err := pool.QueryRow(ctx, `SELECT coalesce(array_agg(indexrelid::regclass::text ORDER BY indexrelid::regclass::text), '{}'),
       coalesce(array_agg(indexrelid::regclass::text) FILTER (WHERE NOT (indisvalid AND indisready)), '{}')
FROM pg_index WHERE indrelid = $1::regclass`, want.table).Scan(&got, &bad); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want.indexes) || len(bad) != 0 {
			t.Errorf("%s indexes = %v, want %v; not valid and ready: %v", want.table, got, want.indexes, bad)
		}
	}
}

// The migrations leave the six lookup tables with the indexes a statement or a constraint reads, and keep the scope
// sets index.
func TestHygieneIndexSetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	checkIndexSets(t, ctx, pool)

	// An operator already dropped them by hand, or a crash lost the ledger rows: the drops re-run as no-ops.
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename IN ($1, $2)", hygieneMigrations[0], hygieneMigrations[5]); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("re-running dropped-index migrations: %v", err)
	}
	checkRecorded(t, ctx, pool, hygieneMigrations[0], hygieneMigrations[5])
	checkIndexSets(t, ctx, pool)
}

// Node n1..n600; node n1 has 200 neighbors at YVR (a 20-row last_seen tie run) and 40 more at YYJ. 60 member IATAs
// (M00..M59), each listing a different slice of the 300 channels' fingerprints. Five regions over the four fixture IATAs.
// 400 trace tags whose first hearings come in runs of four.
const hygieneReaderFixture = `
INSERT INTO iata_codes (iata) SELECT 'M' || lpad(g::text, 2, '0') FROM generate_series(0, 59) g;
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint)
SELECT g, '#scope-' || g, int4send(g), sha256(int4send(g)) FROM generate_series(1, 30) g;
INSERT INTO nodes (id, public_key, node_type, name, first_seen, last_seen)
SELECT md5('hn' || i)::uuid, sha256(int4send(i)), 1 + i % 4, 'hn-' || i, '2026-05-01', '2026-06-01'::timestamptz - i * interval '1 minute'
FROM generate_series(1, 600) i;
INSERT INTO node_neighbors (node_id, neighbor_id, iata, first_seen, last_seen, observation_count, snr)
SELECT md5('hn1')::uuid, md5('hn' || (i + 1))::uuid, 'YVR', '2026-05-01',
       '2026-06-01'::timestamptz - (CASE WHEN i BETWEEN 50 AND 69 THEN 50 ELSE i END) * interval '1 second', i, i / 4.0
FROM generate_series(1, 200) i;
INSERT INTO node_neighbors (node_id, neighbor_id, iata, first_seen, last_seen, observation_count)
SELECT md5('hn1')::uuid, md5('hn' || (i + 1))::uuid, 'YYJ', '2026-04-01', '2026-06-02'::timestamptz - i * interval '1 second', 1
FROM generate_series(1, 40) i;
INSERT INTO node_neighbors (node_id, neighbor_id, iata, first_seen, last_seen)
SELECT md5('hn' || i)::uuid, md5('hn' || (i + d))::uuid, 'YYJ', '2026-05-01', '2026-06-01'::timestamptz - (i * 3 + d) * interval '1 second'
FROM generate_series(2, 590) i, generate_series(1, 3) d;
INSERT INTO channels (id, channel_hash, key_fingerprint, name, first_seen, last_seen, message_count)
SELECT i, set_byte('\x00'::bytea, 0, i % 200), sha256(int4send(i)), 'hc-' || i, '2026-05-01',
       '2026-06-01'::timestamptz - (CASE WHEN i BETWEEN 100 AND 109 THEN 100 ELSE i END) * interval '1 second', i
FROM generate_series(1, 300) i;
INSERT INTO meshmapper_channel_members (iata, key_fingerprint)
SELECT 'M' || lpad(m::text, 2, '0'), sha256(int4send(i)) FROM generate_series(0, 59) m, generate_series(1, 300) i WHERE i % 60 = m OR i % 7 = m % 7;
INSERT INTO channel_iatas (channel_hash, iata, last_heard)
SELECT set_byte('\x00'::bytea, 0, i), 'M' || lpad((i % 60)::text, 2, '0'), '2026-06-01'::timestamptz - i * interval '1 minute'
FROM generate_series(0, 199) i;
INSERT INTO regions (id, slug, name) SELECT g, 'hr-' || g, 'Region ' || g FROM generate_series(1, 5) g;
INSERT INTO region_iatas (region_id, iata) VALUES (1, 'YVR'), (1, 'YYJ'), (2, 'YYZ'), (3, 'YOW'), (3, 'YVR'), (4, 'M00'), (5, 'M01'), (5, 'M02');
INSERT INTO channel_config_scopes (key_fingerprint, region_slug) VALUES (sha256(int4send(5)), 'hr-1'), (sha256(int4send(6)), NULL);
INSERT INTO trace_tags (trace_tag, first_heard_at, last_heard_at, packet_count, trace_type, heard)
SELECT int4send(i), '2026-05-20'::timestamptz + (i / 4) * interval '1 minute', '2026-05-20'::timestamptz + i * interval '90 seconds',
       i, CASE WHEN i % 2 = 0 THEN 'TRACE' ELSE 'PING' END, true
FROM generate_series(1, 400) i;
INSERT INTO analytics_hourly_scope_sets (hour, iatas, scope_id, packets)
SELECT '2026-05-01'::timestamptz + h * interval '1 hour', ARRAY['YVR']::bpchar[], 1 + h % 30, h FROM generate_series(0, 500) h;
ANALYZE nodes, node_neighbors, channels, meshmapper_channel_members, channel_iatas, regions, region_iatas, trace_tags, transport_scopes, channel_config_scopes, analytics_hourly_scope_sets;`

// Rows from every reader of the dropped lookup indexes are the same before and after the drops, in four plan modes.
func TestHygieneReadersAfterIndexDropsPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, hygieneReaderFixture)
	// The plan modes below include enable_seqscan off; JIT would only add compile time to those forced plans.
	mustExec(t, ctx, tx, "SET LOCAL jit = off")
	q := sqlc.New(tx)
	store := &Store{q: q}

	var hub uuid.UUID
	if err := tx.QueryRow(ctx, "SELECT md5('hn1')::uuid").Scan(&hub); err != nil {
		t.Fatal(err)
	}
	var page2 api.ChannelCursor
	if err := tx.QueryRow(ctx, "SELECT last_seen, id FROM channels ORDER BY last_seen DESC, id DESC OFFSET 49 LIMIT 1").Scan(&page2.LastSeen, &page2.ID); err != nil {
		t.Fatal(err)
	}
	one := []string{"M07"}
	var sixty, bogus []string
	for i := range 60 {
		sixty = append(sixty, fmt.Sprintf("M%02d", i))
	}
	for i := range 5000 {
		bogus = append(bogus, fmt.Sprintf("Z%04d", i))
	}
	exact := func(v any) any { return v }
	normNeighbors := func(v any) any {
		return sortTieRuns(v.([]api.NodeNeighbor), func(n api.NodeNeighbor) string { return strconv.FormatInt(n.LastSeen, 10) },
			func(n api.NodeNeighbor) string { return n.ID.String() })
	}
	normLinks := func(v any) any {
		rows := slices.Clone(v.([]sqlc.GetNeighborLinksRow))
		slices.SortFunc(rows, func(a, b sqlc.GetNeighborLinksRow) int {
			if c := slices.Compare(a.NodeID[:], b.NodeID[:]); c != 0 {
				return c
			}
			return slices.Compare(a.NeighborID[:], b.NeighborID[:])
		})
		return rows
	}
	channels := func(name string, iatas []string, pc *api.ChannelCursor) readerShape {
		return readerShape{name: name, norm: exact, run: func() (any, error) { return store.ListChannels(ctx, 50, nil, iatas, nil, 0, pc) }}
	}
	var hubs, spokes []uuid.UUID
	for i := 1; i <= 30; i++ {
		var id uuid.UUID
		if err := tx.QueryRow(ctx, "SELECT md5('hn' || $1::int)::uuid", i).Scan(&id); err != nil {
			t.Fatal(err)
		}
		hubs = append(hubs, id)
		if err := tx.QueryRow(ctx, "SELECT md5('hn' || $1::int)::uuid", i+2).Scan(&id); err != nil {
			t.Fatal(err)
		}
		spokes = append(spokes, id)
	}
	since := time.Date(2026, 5, 20, 0, 30, 0, 0, time.UTC)
	until := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)
	shapes := []readerShape{
		channels("channels 1 iata", one, nil),
		channels("channels 60 iatas", sixty, nil),
		channels("channels 5000 bogus iatas", bogus, nil),
		channels("channels region iata", []string{"YVR"}, nil),
		channels("channels 60 iatas page 2", sixty, &page2),
		channels("channels 1 iata page 2", one, &page2),
		{name: "neighbors of the 240-neighbor node", norm: normNeighbors, run: func() (any, error) { return store.GetNodeNeighbors(ctx, hub) }},
		{name: "neighbors of a small node", norm: normNeighbors, run: func() (any, error) { return store.GetNodeNeighbors(ctx, spokes[5]) }},
		{name: "neighbor links", norm: normLinks, run: func() (any, error) {
			return q.GetNeighborLinks(ctx, sqlc.GetNeighborLinksParams{ANodes: hubs, BNodes: spokes})
		}},
		{name: "trace tags", norm: exact, run: func() (any, error) {
			return store.ListTraceTags(ctx, nil, "", "", time.Time{}, time.Time{}, time.Time{}, "", 50)
		}},
		{name: "trace tags first-heard window", norm: exact, run: func() (any, error) {
			return store.ListTraceTags(ctx, nil, "", "", since, until, time.Time{}, "", 50)
		}},
		{name: "trace tags first-heard window PING", norm: exact, run: func() (any, error) {
			return store.ListTraceTags(ctx, nil, "", "PING", since, until, time.Time{}, "", 5)
		}},
		{name: "region iatas", norm: exact, run: func() (any, error) { return q.GetRegionIATAs(ctx, 1) }},
		{name: "region iatas empty", norm: exact, run: func() (any, error) { return q.GetRegionIATAs(ctx, 99) }},
		{name: "delete old nodes cascades", norm: exact, run: func() (any, error) {
			mustExec(t, ctx, tx, "SAVEPOINT del")
			defer mustExec(t, ctx, tx, "ROLLBACK TO SAVEPOINT del")
			if err := store.DeleteOldNodes(ctx, time.Date(2026, 5, 31, 22, 0, 0, 0, time.UTC)); err != nil {
				return nil, err
			}
			var nodes, links int
			if err := tx.QueryRow(ctx, "SELECT (SELECT count(*) FROM nodes), (SELECT count(*) FROM node_neighbors)").Scan(&nodes, &links); err != nil {
				return nil, err
			}
			return [2]int{nodes, links}, nil
		}},
	}

	before := runShapes(t, ctx, tx, shapes)
	for name, want := range map[string]int{"channels 5000 bogus iatas": 1 /* the Beacon-wide channel */, "channels 60 iatas": 50} {
		if n := len(before[name][0].val.(api.ChannelPage).Items); n != want {
			t.Fatalf("%s: %d rows, want %d", name, n, want)
		}
	}
	if n := len(before["neighbors of the 240-neighbor node"][0].val.([]api.NodeNeighbor)); n != 200 {
		t.Fatalf("hub has %d neighbors, want 200", n)
	}
	if n := len(before["trace tags first-heard window"][0].val.([]api.TraceTagSummary)); n == 0 {
		t.Fatal("the first-heard window matched nothing")
	}
	if c := before["delete old nodes cascades"][0].val.([2]int); c[0] >= 600 {
		t.Fatalf("DeleteOldNodes deleted nothing: %v", c)
	}

	applyMigrationsTx(t, ctx, tx, hygieneMigrations...)
	mustExec(t, ctx, tx, "ANALYZE channel_iatas, node_neighbors, transport_scopes, meshmapper_channel_members, region_iatas, trace_tags")
	var left int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relkind = 'i' AND relnamespace = current_schema()::regnamespace AND relname = ANY($1)`,
		[]string{"idx_channel_iatas_iata", "idx_node_neighbors_node", "idx_transport_scopes_fingerprint",
			"idx_meshmapper_channel_members_fingerprint", "idx_region_iatas_iata", "idx_trace_tags_first_heard"}).Scan(&left); err != nil || left != 0 {
		t.Fatalf("%d of the six indexes left after the drops (err %v)", left, err)
	}
	after := runShapes(t, ctx, tx, shapes)
	compareShapes(t, shapes, before, after)
}
