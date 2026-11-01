// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// smallTableMigrations rebuild node_iatas' region index and drop the small-table indexes
// that kept ingest updates from being HOT.
var smallTableMigrations = []string{
	"011_node_iatas_iata_v2.sql",
	"012_drop_node_iatas_iata.sql",
	"013_rename_node_iatas_iata_v2.sql",
	"014_drop_nodes_type_last_seen.sql",
	"015_drop_nodes_pubkey.sql",
	"016_drop_nodes_location.sql",
	"017_drop_nodes_min_firmware.sql",
	"018_drop_nodes_multibyte_paths.sql",
	"019_drop_nodes_multibyte_traces.sql",
	"020_drop_observers_last_seen.sql",
	"021_drop_observers_pubkey.sql",
	"022_drop_observers_type.sql",
	"023_drop_channels_last_seen_id.sql",
	"024_observers_channels_fillfactor.sql",
	"058_nodes_fillfactor.sql",
}

// sortTieRuns returns rows with each maximal run of equal key sorted by id; the order
// across runs is left alone.
func sortTieRuns[T any](rows []T, key func(T) string, id func(T) string) []T {
	out := slices.Clone(rows)
	for start := 0; start < len(out); {
		end := start + 1
		for end < len(out) && key(out[end]) == key(out[start]) {
			end++
		}
		slices.SortStableFunc(out[start:end], func(a, b T) int { return strings.Compare(id(a), id(b)) })
		start = end
	}
	return out
}

func TestSortTieRuns(t *testing.T) {
	type row struct{ k, id string }
	in := []row{{"b", "2"}, {"b", "1"}, {"a", "9"}, {"c", "5"}, {"c", "3"}, {"b", "0"}}
	got := sortTieRuns(in, func(r row) string { return r.k }, func(r row) string { return r.id })
	want := []row{{"b", "1"}, {"b", "2"}, {"a", "9"}, {"c", "3"}, {"c", "5"}, {"b", "0"}}
	if !reflect.DeepEqual(got, want) || in[0].id != "2" {
		t.Fatalf("got %v, want %v; input changed: %v", got, want, in)
	}
}

var smallTableIndexSets = []struct {
	table      string
	indexes    []string
	reloptions []string
}{
	{"nodes", []string{"nodes_pkey", "nodes_public_key_key"}, []string{"autovacuum_vacuum_scale_factor=0.05", "fillfactor=50"}},
	{"node_iatas", []string{"idx_node_iatas_iata", "node_iatas_pkey"}, []string{"autovacuum_vacuum_scale_factor=0.05"}},
	{"observers", []string{"idx_observers_last_iata", "observers_pkey", "observers_public_key_key"}, []string{"fillfactor=50"}},
	{"channels", []string{"channels_channel_hash_key_fingerprint_key", "channels_hashtag_key", "channels_pkey",
		"idx_channels_hash", "idx_channels_hash_no_key", "idx_channels_hashtag"}, []string{"fillfactor=50"}},
}

func checkSmallTableIndexSets(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, want := range smallTableIndexSets {
		var got, bad, rel []string
		if err := pool.QueryRow(ctx, `SELECT coalesce(array_agg(indexrelid::regclass::text ORDER BY indexrelid::regclass::text), '{}'),
       coalesce(array_agg(indexrelid::regclass::text) FILTER (WHERE NOT (indisvalid AND indisready)), '{}'),
       (SELECT coalesce(reloptions, '{}') FROM pg_class WHERE oid = $1::regclass)
FROM pg_index WHERE indrelid = $1::regclass`, want.table).Scan(&got, &bad, &rel); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want.indexes) || len(bad) != 0 || !slices.Equal(rel, want.reloptions) {
			t.Errorf("%s indexes = %v, want %v; not valid and ready: %v; reloptions = %v, want %v",
				want.table, got, want.indexes, bad, rel, want.reloptions)
		}
	}
	var def string
	var v2Gone bool
	if err := pool.QueryRow(ctx, "SELECT pg_get_indexdef('idx_node_iatas_iata'::regclass), to_regclass('idx_node_iatas_iata_v2') IS NULL").Scan(&def, &v2Gone); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(def, "USING btree (iata)") || !v2Gone {
		t.Errorf("idx_node_iatas_iata = %q, idx_node_iatas_iata_v2 gone = %v", def, v2Gone)
	}
}

func checkRecorded(t *testing.T, ctx context.Context, pool *pgxpool.Pool, files ...string) {
	t.Helper()
	for _, f := range files {
		var recorded bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename = $1)", f).Scan(&recorded); err != nil || !recorded {
			t.Fatalf("%s not recorded: %v", f, err)
		}
	}
}

// The migrations leave the four ingest-churned tables with exactly the indexes a reader or a constraint needs.
func TestSmallTableIndexSetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	checkSmallTableIndexSets(t, ctx, pool)

	// An operator already dropped these by hand, or a crash lost the ledger rows: the drops re-run as no-ops.
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename IN ('014_drop_nodes_type_last_seen.sql', '023_drop_channels_last_seen_id.sql')"); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("re-running dropped-index migrations: %v", err)
	}
	checkRecorded(t, ctx, pool, "014_drop_nodes_type_last_seen.sql", "023_drop_channels_last_seen_id.sql")
	checkSmallTableIndexSets(t, ctx, pool)

	// A crash after the replacement index was built but before its ledger row: 011 records the valid index.
	if _, err := pool.Exec(ctx, "CREATE INDEX CONCURRENTLY idx_node_iatas_iata_v2 ON node_iatas USING btree (iata)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename = '011_node_iatas_iata_v2.sql'"); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("011 over a pre-built index: %v", err)
	}
	checkRecorded(t, ctx, pool, "011_node_iatas_iata_v2.sql")
	if _, err := pool.Exec(ctx, "DROP INDEX idx_node_iatas_iata_v2"); err != nil {
		t.Fatal(err)
	}
	checkSmallTableIndexSets(t, ctx, pool)
}

// applyMigrationsTx applies migration files inside tx, CONCURRENTLY removed so they can run there.
func applyMigrationsTx(t *testing.T, ctx context.Context, tx pgx.Tx, files ...string) {
	t.Helper()
	for _, f := range files {
		body, err := migrationFiles.ReadFile("migrations/" + f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, strings.ReplaceAll(string(body), "CONCURRENTLY ", "")); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
}

// planSettings are the four plan modes each reader runs under, before and after the drops.
// enable_seqscan off steers the old plans onto idx_nodes_type_last_seen and the node_iatas index.
var planSettings = []string{
	"SET LOCAL plan_cache_mode = force_custom_plan; SET LOCAL enable_seqscan = on",
	"SET LOCAL plan_cache_mode = force_custom_plan; SET LOCAL enable_seqscan = off",
	"SET LOCAL plan_cache_mode = force_generic_plan; SET LOCAL enable_seqscan = on",
	"SET LOCAL plan_cache_mode = force_generic_plan; SET LOCAL enable_seqscan = off",
}

// readerShape is one reader call; norm sorts its tie runs so equal results compare equal.
type readerShape struct {
	name string
	run  func() (any, error)
	norm func(any) any
}

type shapeResult struct {
	val any
	err error
}

func runShapes(t *testing.T, ctx context.Context, tx pgx.Tx, shapes []readerShape) map[string][]shapeResult {
	t.Helper()
	out := make(map[string][]shapeResult)
	for _, setting := range planSettings {
		mustExec(t, ctx, tx, setting)
		for _, s := range shapes {
			v, err := s.run()
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("%s under %q: %v", s.name, setting, err)
			}
			out[s.name] = append(out[s.name], shapeResult{v, err})
		}
	}
	mustExec(t, ctx, tx, "RESET plan_cache_mode; RESET enable_seqscan")
	return out
}

func compareShapes(t *testing.T, shapes []readerShape, before, after map[string][]shapeResult, skip ...string) {
	t.Helper()
	for _, s := range shapes {
		if slices.Contains(skip, s.name) {
			continue
		}
		for i := range planSettings {
			b, a := before[s.name][i], after[s.name][i]
			if (a.err == nil) != (b.err == nil) || (a.err != nil && a.err.Error() != b.err.Error()) {
				t.Errorf("%s under %q: error %v, before %v", s.name, planSettings[i], a.err, b.err)
				continue
			}
			if a.err != nil {
				continue
			}
			if !reflect.DeepEqual(s.norm(a.val), s.norm(b.val)) {
				t.Errorf("%s under %q: rows differ after the drops\nafter:  %+v\nbefore: %+v", s.name, planSettings[i], a.val, b.val)
			}
		}
	}
}

func timeKey(ts time.Time) string { return ts.UTC().Format(time.RFC3339Nano) }

func queryTimes(t *testing.T, ctx context.Context, tx pgx.Tx, sql string) map[uuid.UUID]string {
	t.Helper()
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[uuid.UUID]string)
	var id uuid.UUID
	var ts time.Time
	if _, err := pgx.ForEachRow(rows, []any{&id, &ts}, func() error { out[id] = timeKey(ts); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

// Nodes 1..3000: node i has last_seen base - i s, except nodes 140-164, which share base - 140 s
// (global ranks 140-164).
const smallTableReaderFixture = `
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (1, '#test', '\x01', '\x02');
INSERT INTO nodes (id, public_key, node_type, name, latitude, longitude, location_source, last_advert_at,
                   supports_multibyte_paths, supports_multibyte_traces, default_scope_id, first_seen, last_seen,
                   radio_freq_mhz, radio_sf, radio_bw_khz, device_clock_drift_seconds)
SELECT md5('node' || i)::uuid,
       CASE WHEN i % 100 = 0 THEN '\x0000'::bytea || int4send(i) ELSE sha256(int4send(i)) END,
       1 + i % 4, 'node-' || i,
       CASE WHEN i % 3 = 0 THEN 49 + i / 10000.0 END, CASE WHEN i % 3 = 0 THEN -123 - i / 10000.0 END,
       CASE WHEN i % 3 = 0 THEN 'advert' END,
       '2026-06-01'::timestamptz - (CASE WHEN i BETWEEN 140 AND 164 THEN 140 ELSE i END) * interval '1 second',
       i % 7 = 0, i % 5 = 0, CASE WHEN i % 4 = 0 THEN 1 END,
       '2026-05-01'::timestamptz + i * interval '1 second',
       '2026-06-01'::timestamptz - (CASE WHEN i BETWEEN 140 AND 164 THEN 140 ELSE i END) * interval '1 second',
       CASE WHEN i % 2 = 0 THEN 910.525 END, CASE WHEN i % 2 = 0 THEN 7 END, CASE WHEN i % 2 = 0 THEN 62.5 END,
       CASE WHEN 1 + i % 4 NOT IN (2, 3) THEN NULL
            WHEN i BETWEEN 1000 AND 1019 THEN CASE WHEN i % 2 = 0 THEN 99999 ELSE -99999 END
            ELSE CASE WHEN i % 2 = 0 THEN i ELSE -i END END
FROM generate_series(1, 3000) i;
INSERT INTO node_iatas (node_id, iata, first_heard, last_heard, observation_count)
SELECT md5('node' || i)::uuid, 'YVR', '2026-05-01', '2026-06-01'::timestamptz - i * interval '2 seconds', 1 + i % 97
FROM generate_series(1, 3000) i WHERE i % 3 <> 0;
INSERT INTO node_iatas (node_id, iata, first_heard, last_heard, observation_count)
SELECT md5('node' || i)::uuid, 'YYJ', '2026-05-01', '2026-06-01'::timestamptz - i * interval '2 seconds' - interval '1 second', 1 + i % 13
FROM generate_series(1, 3000) i WHERE i % 5 = 0;
INSERT INTO node_neighbors (node_id, neighbor_id, iata, first_seen, last_seen, snr)
SELECT md5('node' || i)::uuid, md5('node' || (i + d))::uuid, 'YVR', '2026-05-01', '2026-06-01', d
FROM generate_series(1, 50) i, generate_series(1, 2) d;
INSERT INTO observers (id, public_key, display_name, first_seen, last_seen)
SELECT md5('observer' || k)::uuid, n.public_key, 'obs-' || k, '2026-05-01', '2026-06-01'
FROM generate_series(1, 20) k JOIN nodes n ON n.id = md5('node' || (k * 13))::uuid;
ANALYZE nodes, node_iatas, node_neighbors, observers;`

// Rows from every reader of the dropped indexes are today's, before and after the drops, in both plan modes.
func TestNodeReadersAfterIndexDropsPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	applyMigrationsTx(t, ctx, tx, "003_meshmapper_repeaters.sql", "062_node_scope_evidence.sql", "063_scope_activity.sql") // GetNode reads imported metadata and native scope evidence
	mustExec(t, ctx, tx, smallTableReaderFixture)
	store := &Store{q: sqlc.New(tx)}

	lastSeen := queryTimes(t, ctx, tx, "SELECT id, last_seen FROM nodes")
	tieRun := queryTimes(t, ctx, tx, "SELECT id, last_seen FROM nodes WHERE last_seen = '2026-06-01'::timestamptz - interval '140 seconds'")
	if len(tieRun) != 25 {
		t.Fatalf("tie run has %d nodes, want 25", len(tieRun))
	}
	var type2Cursor, globalCursor time.Time
	var busiestYVR uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT (SELECT last_seen FROM nodes WHERE node_type = 2 ORDER BY last_seen DESC OFFSET 399 LIMIT 1),
       (SELECT last_seen FROM nodes ORDER BY last_seen DESC OFFSET 1999 LIMIT 1),
       (SELECT node_id FROM node_iatas WHERE iata = 'YVR' ORDER BY observation_count DESC, node_id LIMIT 1)`).Scan(&type2Cursor, &globalCursor, &busiestYVR); err != nil {
		t.Fatal(err)
	}
	yes := true
	nodeKey := func(n api.NodeSummary) string { return lastSeen[n.ID] }
	nodeID := func(n api.NodeSummary) string { return n.ID.String() }
	normNodes := func(v any) any {
		p := v.(api.NodePage)
		p.Items = sortTieRuns(p.Items, nodeKey, nodeID)
		return p
	}
	listNodes := func(name string, nodeType int16, iatas []string, mbPaths, mbTraces *bool, prefix, nameFilter string, cursor time.Time, limit int32, neighbors bool) readerShape {
		var ms int64
		if !cursor.IsZero() {
			ms = cursor.UnixMilli()
		}
		return readerShape{name: name, norm: normNodes, run: func() (any, error) {
			return store.ListNodes(ctx, nodeType, iatas, mbPaths, mbTraces, nil, prefix, nameFilter, "", ms, nil, limit, neighbors)
		}}
	}
	var none time.Time
	normDrift := func(v any) any {
		return sortTieRuns(v.([]api.ClockDriftEntry),
			func(e api.ClockDriftEntry) string {
				return strconv.Itoa(max(e.ClockDriftSeconds, -e.ClockDriftSeconds))
			},
			func(e api.ClockDriftEntry) string { return e.NodeID.String() })
	}
	normTypes := func(v any) any {
		return sortTieRuns(v.([]api.NodeTypeCount),
			func(c api.NodeTypeCount) string { return strconv.FormatInt(c.Count, 10) },
			func(c api.NodeTypeCount) string { return strconv.Itoa(int(c.NodeType)) })
	}
	same := func(v any) any { return v }
	shapes := []readerShape{
		listNodes("global l50", 0, nil, nil, nil, "", "", none, 50, true),
		listNodes("global l150", 0, nil, nil, nil, "", "", none, 150, true),
		listNodes("type1", 1, nil, nil, nil, "", "", none, 50, false),
		listNodes("type2", 2, nil, nil, nil, "", "", none, 50, false),
		listNodes("type3", 3, nil, nil, nil, "", "", none, 50, false),
		listNodes("type2 deep cursor", 2, nil, nil, nil, "", "", type2Cursor, 50, false),
		listNodes("global deep cursor l200", 0, nil, nil, nil, "", "", globalCursor, 200, false),
		listNodes("YVR", 0, []string{"YVR"}, nil, nil, "", "", none, 50, false),
		listNodes("YVR type2", 2, []string{"YVR"}, nil, nil, "", "", none, 50, false),
		listNodes("name node-1", 0, nil, nil, nil, "", "node-1", none, 50, false),
		listNodes("mbPaths", 0, nil, &yes, nil, "", "", none, 50, false),
		listNodes("mbTraces", 0, nil, nil, &yes, "", "", none, 50, false),
		listNodes("pubkey prefix 0000", 0, nil, nil, nil, "0000", "", none, 50, false),
		listNodes("type9", 9, nil, nil, nil, "", "", none, 50, false),
		{name: "clock drift", norm: normDrift, run: func() (any, error) { return store.GetStatsClockDrift(ctx, nil, 500) }},
		{name: "clock drift YVR", norm: normDrift, run: func() (any, error) { return store.GetStatsClockDrift(ctx, []string{"YVR"}, 500) }},
		{name: "node types", norm: normTypes, run: func() (any, error) { return store.GetStatsNodeTypes(ctx, nil) }},
		{name: "node types YVR", norm: normTypes, run: func() (any, error) { return store.GetStatsNodeTypes(ctx, []string{"YVR"}) }},
		{name: "node busiest YVR", norm: same, run: func() (any, error) { return store.GetNode(ctx, busiestYVR) }},
		{name: "node not found", norm: same, run: func() (any, error) { return store.GetNode(ctx, uuid.Nil) }},
	}

	before := runShapes(t, ctx, tx, shapes)
	for _, r := range before["type9"] {
		if items := r.val.(api.NodePage).Items; items == nil || len(items) != 0 {
			t.Fatalf("type9 items = %#v, want empty non-nil", items)
		}
	}
	for _, r := range before["node not found"] {
		if !errors.Is(r.err, pgx.ErrNoRows) {
			t.Fatalf("GetNode(uuid.Nil) error = %v, want no rows", r.err)
		}
	}
	if n := len(before["clock drift"][0].val.([]api.ClockDriftEntry)); n != 500 {
		t.Fatalf("clock drift returned %d rows, want 500", n)
	}

	applyMigrationsTx(t, ctx, tx, smallTableMigrations...)
	mustExec(t, ctx, tx, "ANALYZE nodes, node_iatas, node_neighbors, observers")
	after := runShapes(t, ctx, tx, shapes)

	compareShapes(t, shapes, before, after, "global l150")
	// LIMIT 150 splits the 25-node tie run at ranks 140-164: rows 1-139 are fixed, the last 11 are any 11 of the run.
	for i := range planSettings {
		for _, r := range []shapeResult{before["global l150"][i], after["global l150"][i]} {
			p := r.val.(api.NodePage)
			if len(p.Items) != 150 || !p.HasMore || p.NextCursor == nil {
				t.Fatalf("global l150 under %q: %d items, hasMore %v", planSettings[i], len(p.Items), p.HasMore)
			}
			seen := map[uuid.UUID]bool{}
			for _, n := range p.Items[139:] {
				if _, ok := tieRun[n.ID]; !ok || seen[n.ID] {
					t.Errorf("global l150 under %q: row %s is not a distinct member of the tie run", planSettings[i], n.ID)
				}
				seen[n.ID] = true
			}
		}
		b, a := before["global l150"][i].val.(api.NodePage), after["global l150"][i].val.(api.NodePage)
		if !reflect.DeepEqual(a.Items[:139], b.Items[:139]) || *a.NextCursor != *b.NextCursor {
			t.Errorf("global l150 under %q: first 139 rows or cursor differ after the drops", planSettings[i])
		}
	}
}

// Observers 1..400: observer i has last_seen base - i s, except 50-61, which share base - 50 s.
// Channels 1..120: channel i has last_seen base - i s, except 30-35, which share base - 30 s.
const observerChannelReaderFixture = `
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (1, '#test', '\x01', '\x02');
INSERT INTO observers (id, public_key, display_name, observer_type, radio_freq_mhz, radio_sf, radio_bw_khz,
                       last_status_at, first_seen, last_seen, last_iata, last_iata_at)
SELECT md5('observer' || i)::uuid, sha256(int4send(i)), 'obs-' || i,
       CASE i % 3 WHEN 0 THEN 'repeater' WHEN 1 THEN 'companion' END,
       CASE WHEN i % 4 = 0 THEN 910.525 END, CASE WHEN i % 4 = 0 THEN 7 END, CASE WHEN i % 4 = 0 THEN 62.5 END,
       CASE WHEN i % 2 = 0 THEN now() ELSE '2026-05-01'::timestamptz + i * interval '1 second' END,
       '2026-05-01',
       '2026-06-01'::timestamptz - (CASE WHEN i BETWEEN 50 AND 61 THEN 50 ELSE i END) * interval '1 second',
       CASE WHEN i % 5 < 3 THEN 'YVR' WHEN i % 5 = 3 THEN 'YYJ' END,
       CASE WHEN i % 5 < 4 THEN '2026-06-01'::timestamptz END
FROM generate_series(1, 400) i;
INSERT INTO observer_brokers (observer_id, broker_name, first_seen, last_seen)
SELECT md5('observer' || i)::uuid, 'mqtt1', '2026-05-01', '2026-06-01' FROM generate_series(1, 400) i WHERE i % 2 = 1;
INSERT INTO observer_scopes (observer_id, scope_id) VALUES (md5('observer7')::uuid, 1);
INSERT INTO channels (id, channel_hash, key_fingerprint, name, is_hashtag, key_known, first_seen, last_seen, message_count)
SELECT i, set_byte('\x00'::bytea, 0, i % 40), CASE WHEN i % 3 <> 0 THEN sha256(int4send(i)) END,
       CASE WHEN i % 3 <> 0 THEN 'chan-' || i END, i % 6 = 1, i % 3 = 1,
       '2026-05-01',
       '2026-06-01'::timestamptz - (CASE WHEN i BETWEEN 30 AND 35 THEN 30 ELSE i END) * interval '1 second', i
FROM generate_series(1, 120) i;
ANALYZE observers, observer_brokers, observer_scopes, channels;`

// ListObservers (idx_observers_last_seen, _type, _pubkey) and ListChannels (idx_channels_last_seen_id) keep their rows.
func TestObserverChannelReadersAfterIndexDropsPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	mustExec(t, ctx, tx, observerChannelReaderFixture)
	store := &Store{q: sqlc.New(tx)}

	lastSeen := queryTimes(t, ctx, tx, "SELECT id, last_seen FROM observers")
	var obsCursor, chanCursor time.Time
	var page60 api.ChannelCursor
	if err := tx.QueryRow(ctx, `SELECT (SELECT last_seen FROM observers ORDER BY last_seen DESC OFFSET 249 LIMIT 1),
       (SELECT last_seen FROM channels ORDER BY last_seen DESC, id DESC OFFSET 39 LIMIT 1),
       (SELECT last_seen FROM channels ORDER BY last_seen DESC, id DESC OFFSET 59 LIMIT 1),
       (SELECT id FROM channels ORDER BY last_seen DESC, id DESC OFFSET 59 LIMIT 1)`).Scan(&obsCursor, &chanCursor, &page60.LastSeen, &page60.ID); err != nil {
		t.Fatal(err)
	}
	normObservers := func(v any) any {
		p := v.(api.ObserverPage)
		p.Items = sortTieRuns(p.Items, func(o api.ObserverSummary) string { return lastSeen[o.ID] },
			func(o api.ObserverSummary) string { return o.ID.String() })
		return p
	}
	listObservers := func(name string, iatas []string, observerType, broker, status, nameFilter, scope string, cursor time.Time, limit int32) readerShape {
		var ms int64
		if !cursor.IsZero() {
			ms = cursor.UnixMilli()
		}
		return readerShape{name: name, norm: normObservers, run: func() (any, error) {
			return store.ListObservers(ctx, iatas, observerType, broker, status, nameFilter, scope, ms, nil, limit)
		}}
	}
	exact := func(v any) any { return v }
	listChannels := func(name string, hash []byte, keyKnown *bool, cursor time.Time, pageCursor *api.ChannelCursor) readerShape {
		var ms int64
		if !cursor.IsZero() {
			ms = cursor.UnixMilli()
		}
		return readerShape{name: name, norm: exact, run: func() (any, error) {
			return store.ListChannels(ctx, 50, hash, nil, keyKnown, ms, pageCursor)
		}}
	}
	var none time.Time
	yes := true
	shapes := []readerShape{
		listObservers("observers first l201", nil, "", "", "", "", "", none, 201),
		listObservers("observers legacy cursor", nil, "", "", "", "", "", obsCursor, 50),
		listObservers("observers repeater", nil, "repeater", "", "", "", "", none, 50),
		listObservers("observers YVR", []string{"YVR"}, "", "", "", "", "", none, 50),
		listObservers("observers online", nil, "", "", "online", "", "", none, 50),
		listObservers("observers mqtt1", nil, "", "mqtt1", "", "", "", none, 50),
		listObservers("observers name obs-1", nil, "", "", "", "obs-1", "", none, 50),
		listObservers("observers scope #test", nil, "", "", "", "", "#test", none, 50),
		listChannels("channels first l50", nil, nil, none, nil),
		listChannels("channels legacy cursor", nil, nil, chanCursor, nil),
		listChannels("channels page cursor", nil, nil, none, &page60),
		listChannels("channels key known", nil, &yes, none, nil),
		listChannels("channels hash 05", []byte{0x05}, nil, none, nil),
	}

	before := runShapes(t, ctx, tx, shapes)
	if n := len(before["observers first l201"][0].val.(api.ObserverPage).Items); n != 201 {
		t.Fatalf("observers first page has %d rows, want 201", n)
	}
	if n := len(before["observers scope #test"][0].val.(api.ObserverPage).Items); n != 1 {
		t.Fatalf("observers in #test = %d, want 1", n)
	}
	if n := len(before["channels hash 05"][0].val.(api.ChannelPage).Items); n != 3 {
		t.Fatalf("channels with hash 05 = %d, want 3", n)
	}

	applyMigrationsTx(t, ctx, tx, smallTableMigrations...)
	mustExec(t, ctx, tx, "ANALYZE observers, observer_brokers, observer_scopes, channels")
	after := runShapes(t, ctx, tx, shapes)
	compareShapes(t, shapes, before, after)
}
