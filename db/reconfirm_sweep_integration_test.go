// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// legacyAmbiguousPrefixes and legacyReconfirmRoutes are the pre-change statements (from
// db/queries/queries.sql), verbatim with sqlc's parameter numbering. legacyReconfirmRoutes takes
// before, batchSize, ambIata, ambLen, ambPrefix.
const legacyAmbiguousPrefixes = `SELECT iata::text AS iata, 1::int AS len, prefix_1 AS prefix FROM node_short_ids GROUP BY iata, prefix_1 HAVING COUNT(*) > 1
UNION ALL
SELECT iata::text, 2::int, prefix_2 FROM node_short_ids GROUP BY iata, prefix_2 HAVING COUNT(*) > 1
UNION ALL
SELECT iata::text, 3::int, prefix_3 FROM node_short_ids GROUP BY iata, prefix_3 HAVING COUNT(*) > 1
UNION ALL
SELECT iata::text, 4::int, prefix_4 FROM node_short_ids GROUP BY iata, prefix_4 HAVING COUNT(*) > 1`

const legacyReconfirmRoutes = `WITH batch AS MATERIALIZED (
    SELECT r.iata, r.path_key, r.node_ids, r.hash_prefix
    FROM known_routes r
    WHERE r.last_reconfirmed_at < $1
    ORDER BY r.last_reconfirmed_at
    LIMIT $2
    FOR UPDATE OF r SKIP LOCKED
),
amb AS (
    SELECT a.iata::char(3) AS iata, a.len, a.p
    FROM ROWS FROM (unnest($3::text[]), unnest($4::int[]), unnest($5::bytea[])) AS a(iata, len, p)
),
dead AS (
    SELECT b.iata, b.path_key
    FROM batch b
    WHERE EXISTS (
        SELECT 1
        FROM unnest(b.node_ids) AS hop_node_id
        WHERE NOT EXISTS (
            SELECT 1 FROM node_short_ids ns
            WHERE ns.node_id = hop_node_id
              AND ns.iata = b.iata
        )
    )
    UNION
    SELECT DISTINCT b.iata, b.path_key
    FROM batch b
    CROSS JOIN LATERAL unnest(b.hash_prefix) AS hp
    JOIN amb a ON a.iata = b.iata AND a.len = length(hp) AND a.p = hp
),
deleted AS (
    DELETE FROM known_routes kr
    USING dead d
    WHERE kr.iata = d.iata AND kr.path_key = d.path_key
),
updated AS (
    UPDATE known_routes kr
    SET last_reconfirmed_at = GREATEST(NOW(), $1::timestamptz)
    FROM batch b
    WHERE kr.iata = b.iata AND kr.path_key = b.path_key
      AND NOT EXISTS (
          SELECT 1 FROM dead d
          WHERE d.iata = b.iata AND d.path_key = b.path_key
      )
)
SELECT count(*) FROM batch`

// reconfirmLegacyFixture: 3 regions x 400 generated routes over random and deliberately colliding
// short ids (departed hops, hops known only in another region, 1-4-byte ambiguous prefixes), plus
// the hand-written edge rows x01-x15.
const reconfirmLegacyFixture = `
TRUNCATE known_routes, node_short_ids;
INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT md5(r || i)::uuid, r, decode(substr(md5('p' || r || i), 1, 8), 'hex')
FROM unnest(ARRAY['SEA','SFO','YYZ']) r, generate_series(1, 80) i;
-- Nodes 82-84 share the first 2, 3 and 4 bytes of nodes 2-4: ambiguity at every width.
INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT md5(r || (80 + w))::uuid, r,
       substr(decode(substr(md5('p' || r || w), 1, 8), 'hex'), 1, w) || decode(repeat('ee', 4 - w), 'hex')
FROM unnest(ARRAY['SEA','SFO','YYZ']) r, generate_series(2, 4) w;
INSERT INTO node_short_ids (node_id, iata, prefix_4) VALUES
 (md5('good')::uuid, 'SEA', '\x0f0f0f0f'),
 (md5('ambA')::uuid, 'SEA', '\xab000001'), (md5('ambB')::uuid, 'SEA', '\xab000002'),
 (md5('aklA')::uuid, 'AKL', '\xdeadbeef'), (md5('aklB')::uuid, 'AKL', '\xdeadbeef'),
 (md5('sfoOnly')::uuid, 'SFO', '\x0f0f0f0f');
-- Hop node n of route j: n in 81..95 is departed unless it is one of the colliders 82-84; every
-- 23rd hop names a node of another region.
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count, last_reconfirmed_at)
SELECT decode(md5(r || '/' || j), 'hex'), h.nodes, h.prefixes, r, cardinality(h.nodes),
       '2026-01-01', '2026-02-01', j, '2026-03-01'::timestamptz + j * interval '1 second'
FROM unnest(ARRAY['SEA','SFO','YYZ']) r, generate_series(1, 400) j,
LATERAL (
  SELECT array_agg(md5(x.reg || x.n)::uuid ORDER BY k) AS nodes,
         array_agg(substr(decode(substr(md5('p' || x.reg || x.n), 1, 8), 'hex'), 1, 1 + (j + k) % 4) ORDER BY k) AS prefixes
  FROM generate_series(1, 1 + j % 5) k,
  LATERAL (SELECT (j * 7 + k * 13) % 95 + 1 AS n,
                  CASE WHEN (j + k) % 23 = 0 THEN CASE WHEN r = 'SFO' THEN 'SEA' ELSE 'SFO' END ELSE r END AS reg) x
) h;
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count, last_reconfirmed_at)
SELECT key, nodes, prefixes, iata, hops, '2026-01-01', '2026-02-01', 1, '2026-03-02'
FROM (VALUES
 (decode(lpad('a1',32,'0'),'hex'), ARRAY[md5('good')::uuid, md5('sfoOnly')::uuid], ARRAY['\x0f0f'::bytea, '\x0f0f'::bytea], 'SEA', 2),
 (decode(lpad('a2',32,'0'),'hex'), ARRAY[md5('good')::uuid, md5('good')::uuid, md5('good')::uuid], ARRAY['\x0f0f'::bytea], 'SEA', 3),
 (decode(lpad('a3',32,'0'),'hex'), array_append(array_fill(md5('good')::uuid, ARRAY[63]), '00000000-0000-0000-0000-0000000000ff'::uuid), array_fill('\x0f0f'::bytea, ARRAY[64]), 'SEA', 64),
 (decode(lpad('a4',32,'0'),'hex'), array_fill(md5('good')::uuid, ARRAY[64]), array_append(array_fill('\x0f0f'::bytea, ARRAY[63]), '\xab'::bytea), 'SEA', 64),
 (decode(lpad('a5',32,'0'),'hex'), array_fill(md5('good')::uuid, ARRAY[64]), array_fill('\x0f0f'::bytea, ARRAY[64]), 'SEA', 64),
 (decode(lpad('a6',32,'0'),'hex'), '{}'::uuid[], ARRAY['\xdeadbeef'::bytea], 'AKL', 1),
 (decode(lpad('a7',32,'0'),'hex'), '{}'::uuid[], ARRAY['\xdeadbe'::bytea], 'AKL', 1),
 (decode(lpad('b7',32,'0'),'hex'), '{}'::uuid[], ARRAY['\x0102'::bytea], 'AKL', 1),
 (decode(lpad('a8',32,'0'),'hex'), ARRAY[md5('good')::uuid], ARRAY['\xab00'::bytea], 'SEA', 1),
 (decode(repeat('00',16),'hex'), ARRAY[md5('good')::uuid], ARRAY['\xab'::bytea], 'SEA', 1),
 (decode(repeat('ff',16),'hex'), ARRAY[md5('good')::uuid], ARRAY['\xab'::bytea], 'SEA', 1),
 (decode(repeat('00',16),'hex'), '{}'::uuid[], '{}'::bytea[], 'SFO', 0),
 ('\x00'::bytea, ARRAY['00000000-0000-0000-0000-0000000000ff'::uuid], '{}'::bytea[], 'SEA', 1),
 (''::bytea, ARRAY['00000000-0000-0000-0000-0000000000ff'::uuid], '{}'::bytea[], 'SEA', 1),
 (decode(lpad('ae',32,'0'),'hex'), ARRAY['00000000-0000-0000-0000-0000000000ff'::uuid, '00000000-0000-0000-0000-0000000000ff'::uuid], ARRAY['\xab'::bytea, '\xab'::bytea], 'SEA', 2),
 (decode(lpad('af',32,'0'),'hex'), '{}'::uuid[], '{}'::bytea[], 'AAA', 0)
) AS e(key, nodes, prefixes, iata, hops);`

func setReconfirmHooks(t *testing.T, batch int, timeout time.Duration) {
	t.Helper()
	oldBatch, oldTimeout := reconfirmBatchSize, reconfirmStatementTimeout
	reconfirmBatchSize, reconfirmStatementTimeout = batch, timeout
	t.Cleanup(func() { reconfirmBatchSize, reconfirmStatementTimeout = oldBatch, oldTimeout })
}

func loadReconfirmCursor(t *testing.T, ctx context.Context, store *Store) (reconfirmCursor, bool) {
	t.Helper()
	var cur reconfirmCursor
	ok, err := store.LoadState(ctx, ReconfirmCursorKey, &cur)
	if err != nil {
		t.Fatal(err)
	}
	return cur, ok
}

// The sweep deletes exactly what today's ReconfirmTask loop deletes, and stamps nothing.
func TestReconfirmSweepMatchesLegacyPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	if _, err := pool.Exec(ctx, reconfirmLegacyFixture); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cur := pgx.Identifier{schema}.Sanitize()
	oldName := schema + "_old"
	old := pgx.Identifier{oldName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE SCHEMA %[1]s;
CREATE TABLE %[1]s.known_routes (LIKE %[2]s.known_routes INCLUDING ALL);
CREATE TABLE %[1]s.node_short_ids (LIKE %[2]s.node_short_ids INCLUDING ALL);
INSERT INTO %[1]s.known_routes SELECT * FROM %[2]s.known_routes;
INSERT INTO %[1]s.node_short_ids (node_id, iata, prefix_4) SELECT node_id, iata, prefix_4 FROM %[2]s.node_short_ids;
CREATE TABLE %[1]s.orig AS SELECT * FROM %[2]s.known_routes;`, old, cur))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA "+old+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	var total int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes`).Scan(&total); err != nil {
		t.Fatal(err)
	}

	// Old: today's loop on the copy (amb computed once, before = now, batch 1000 until n < 1000).
	cfg, err := pgx.ParseConfig(os.Getenv("BEACON_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = old
	legacy, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close(context.Background())
	var ambIata []string
	var ambLen []int32
	var ambPrefix [][]byte
	rows, err := legacy.Query(ctx, legacyAmbiguousPrefixes)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var i string
		var l int32
		var p []byte
		if err := rows.Scan(&i, &l, &p); err != nil {
			t.Fatal(err)
		}
		ambIata, ambLen, ambPrefix = append(ambIata, i), append(ambLen, l), append(ambPrefix, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	for {
		var n int64
		if err := legacy.QueryRow(ctx, legacyReconfirmRoutes, before, int32(1000), ambIata, ambLen, ambPrefix).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n < 1000 {
			break
		}
	}
	var oldLeft int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+old+`.known_routes`).Scan(&oldLeft); err != nil {
		t.Fatal(err)
	}
	oldDeleted := total - oldLeft
	if oldDeleted == 0 || oldLeft == 0 {
		t.Fatalf("fixture is degenerate: %d of %d deleted", oldDeleted, total)
	}

	// New.
	setReconfirmHooks(t, 7, 5*time.Second)
	res, err := store.ReconfirmSweep(ctx, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Wrapped || res.Scanned != total || res.Deleted != oldDeleted || res.Skipped != 0 || res.TimedOut != 0 {
		t.Fatalf("sweep = %+v; want Wrapped, Scanned %d, Deleted %d", res, total, oldDeleted)
	}
	const cols = `id, path_key, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count`
	var onlyNew, onlyOld, stamped int64
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT
 (SELECT count(*) FROM (SELECT %[1]s FROM %[2]s.known_routes EXCEPT SELECT %[1]s FROM %[3]s.known_routes) x),
 (SELECT count(*) FROM (SELECT %[1]s FROM %[3]s.known_routes EXCEPT SELECT %[1]s FROM %[2]s.known_routes) x),
 (SELECT count(*) FROM %[2]s.known_routes n JOIN %[3]s.orig o USING (iata, path_key)
   WHERE n.last_reconfirmed_at IS DISTINCT FROM o.last_reconfirmed_at)`, cols, cur, old)).Scan(&onlyNew, &onlyOld, &stamped); err != nil {
		t.Fatal(err)
	}
	if onlyNew != 0 || onlyOld != 0 {
		t.Errorf("survivors differ: %d only in new, %d only in old", onlyNew, onlyOld)
	}
	if stamped != 0 {
		t.Errorf("%d survivors had last_reconfirmed_at changed", stamped)
	}
	t.Logf("%d routes, %d deleted by both", total, oldDeleted)
}

// A statement timeout rolls back the batch but keeps the connection and its
// reconfirm_amb; a twice-timed-out slice is skipped.
func TestReconfirmBatchTimeoutKeepsConnectionPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	_, err := pool.Exec(ctx, `
TRUNCATE known_routes, node_short_ids;
INSERT INTO node_short_ids (node_id, iata, prefix_4) VALUES
 (md5('n1')::uuid, 'SEA', '\x11223344'), (md5('n2')::uuid, 'SEA', '\x55667788'), (md5('n3')::uuid, 'SEA', '\x55667799');
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count)
SELECT int4send(i),
       ARRAY[md5(CASE WHEN i = 1 THEN 'gone' WHEN i IN (12, 15, 18, 21, 24) THEN 'n2' ELSE 'n1' END)::uuid],
       ARRAY[CASE WHEN i IN (12, 15, 18, 21, 24) THEN '\x55'::bytea ELSE '\x11'::bytea END], 'SEA', 1
FROM generate_series(1, 30) i;`)
	if err != nil {
		t.Fatal(err)
	}
	busy, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback(context.Background())
	if _, err := busy.Exec(ctx, `SELECT 1 FROM known_routes WHERE path_key=int4send(1) FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	setReconfirmHooks(t, 10, 200*time.Millisecond)
	res, err := store.ReconfirmSweep(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if res.TimedOut != 2 || res.Skipped != 1 || !res.Wrapped || res.Deleted != 5 || res.Scanned != 29 {
		t.Fatalf("sweep = %+v; want TimedOut 2, Skipped 1, Wrapped, Deleted 5, Scanned 29", res)
	}
	rows, err := pool.Query(ctx, `SELECT get_byte(path_key, 3) FROM known_routes ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	left, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 25 || left[0] != 1 {
		t.Fatalf("routes left = %v; want 25 including route 1", left)
	}
	if err := busy.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	res, err = store.ReconfirmSweep(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 || res.Scanned != 25 || !res.Wrapped || res.TimedOut != 0 {
		t.Fatalf("second sweep = %+v; want Deleted 1, Scanned 25, Wrapped", res)
	}
}

func TestReconfirmEndsAtWrapPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	_, err := pool.Exec(ctx, `
TRUNCATE known_routes;
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count)
SELECT int4send(i), ARRAY['00000000-0000-0000-0000-000000000001'::uuid], ARRAY['\x11'::bytea], 'SEA', 1
FROM generate_series(1, 25) i;`)
	if err != nil {
		t.Fatal(err)
	}
	setReconfirmHooks(t, 10, 5*time.Second)
	res, err := store.ReconfirmSweep(ctx, 1000)
	if err != nil || res.Scanned != 25 || !res.Wrapped || res.Deleted != 0 {
		t.Fatalf("first sweep = %+v, %v; want Scanned 25, Wrapped", res, err)
	}
	cur, ok := loadReconfirmCursor(t, ctx, store)
	if !ok || cur.IATA != "" || cur.PathKeyHex == nil || *cur.PathKeyHex != "" || cur.Cycles != 1 || cur.CycleStartedAt.IsZero() {
		t.Fatalf("cursor after wrap = %+v (found %v)", cur, ok)
	}
	res, err = store.ReconfirmSweep(ctx, 12)
	if err != nil || res.Scanned != 12 || res.Wrapped {
		t.Fatalf("budget sweep = %+v, %v; want Scanned 12, not Wrapped", res, err)
	}
	cur, _ = loadReconfirmCursor(t, ctx, store)
	if cur.IATA != "SEA" || cur.PathKeyHex == nil || *cur.PathKeyHex != hex.EncodeToString([]byte{0, 0, 0, 12}) || cur.Cycles != 1 {
		t.Fatalf("cursor after 12 = %+v", cur)
	}
	res, err = store.ReconfirmSweep(ctx, 1000)
	if err != nil || res.Scanned != 13 || !res.Wrapped {
		t.Fatalf("third sweep = %+v, %v; want Scanned 13, Wrapped", res, err)
	}
	if cur, _ = loadReconfirmCursor(t, ctx, store); cur.Cycles != 2 {
		t.Fatalf("cycles = %d, want 2", cur.Cycles)
	}
}

func TestReconfirmReleasesEarlierBatchPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	if _, err := pool.Exec(ctx, `UPDATE known_routes SET node_ids=ARRAY[md5('departed')::uuid] WHERE path_key=int4send(2)`); err != nil {
		t.Fatal(err)
	}
	setReconfirmHooks(t, 1, 5*time.Second)
	gate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(context.Background())
	key := time.Now().UnixNano()
	if _, err := gate.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`
CREATE FUNCTION pause_second_route() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF OLD.path_key=int4send(2) THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN OLD; END $$;
CREATE TRIGGER pause_second_route BEFORE DELETE ON known_routes FOR EACH ROW EXECUTE FUNCTION pause_second_route();`, key))
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		_, err := store.ReconfirmSweep(ctx, 1000)
		finished <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second batch did not reach the test gate")
		}
		time.Sleep(time.Millisecond)
	}
	upsertCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = sqlc.New(pool).UpsertKnownRoute(upsertCtx, sqlc.UpsertKnownRouteParams{
		PathKey: []byte{0, 0, 0, 1}, NodeIds: []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")},
		HashPrefix: [][]byte{{0x11}}, Iata: "SEA", HopCount: 1,
	})
	if err != nil {
		t.Fatalf("completed batch still blocks ingest: %v", err)
	}
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	var count, total int
	if err := pool.QueryRow(ctx, `SELECT observation_count, (SELECT count(*) FROM known_routes) FROM known_routes WHERE path_key=int4send(1)`).Scan(&count, &total); err != nil || count != 2 || total != 1 {
		t.Fatalf("observation count %d, routes %d, error %v; want 2 and 1", count, total, err)
	}
}

func TestReconfirmPreservesRouteDataPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	before := time.Now().UTC().Truncate(time.Microsecond)
	_, err := pool.Exec(ctx, `
TRUNCATE known_routes, node_short_ids;
INSERT INTO node_short_ids (node_id,iata,prefix_4) VALUES
 (md5('n1')::uuid,'SEA','\x11223344'), (md5('n2')::uuid,'SEA','\x11223355'),
 (md5('n3')::uuid,'SEA','\xaabbccdd'), (md5('n4')::uuid,'SEA','\xaabbccdd'),
 (md5('n5')::uuid,'SEA','\x55667788'), (md5('n6')::uuid,'SFO','\x55667799');
INSERT INTO known_routes (id,path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen,observation_count,last_reconfirmed_at)
SELECT id,int4send(id),ARRAY[md5(node)::uuid],ARRAY[decode(prefix,'hex')],iata,1,
       '2025-12-01','2026-01-01',42,'2026-01-01'
FROM (VALUES
 (1,'n1','11223344','SEA'), (2,'n2','11223355','SEA'),
 (3,'n5','55','SEA'), (4,'n5','5566','SEA'), (5,'n5','556677','SEA'), (6,'n5','55667788','SEA'),
 (7,'n1','11','SEA'), (8,'n1','1122','SEA'), (9,'n1','112233','SEA'), (10,'n3','aabbccdd','SEA'),
 (11,'missing','55','SEA'), (12,'n6','55','SEA'),
 (13,'n1','11223344','SEA'), (14,'n5','55667788','SEA'), (15,'n6','55','SFO'),
 (16,'n5','55','SEA'), (17,'n5','55','SEA')
) AS routes(id,node,prefix,iata);
UPDATE known_routes SET node_ids=node_ids || md5('n5')::uuid, hash_prefix=hash_prefix || '\x55667788'::bytea, hop_count=2 WHERE id=13;
UPDATE known_routes SET node_ids=node_ids || md5('missing')::uuid, hash_prefix=hash_prefix || '\x99'::bytea, hop_count=2 WHERE id=14;`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE known_routes SET last_reconfirmed_at=$1::timestamptz + CASE WHEN id=16 THEN interval '1 hour' ELSE interval '0' END WHERE id IN (16,17)`, before); err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[int][]byte {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT id, to_jsonb(r) FROM known_routes r`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		result := map[int][]byte{}
		for rows.Next() {
			var id int
			var data []byte
			if err := rows.Scan(&id, &data); err != nil {
				t.Fatal(err)
			}
			result[id] = data
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	original := snapshot()
	setReconfirmHooks(t, 3, 5*time.Second)
	res, err := store.ReconfirmSweep(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scanned != 17 || res.Deleted != 7 || !res.Wrapped {
		t.Fatalf("sweep = %+v; want Scanned 17, Deleted 7, Wrapped", res)
	}
	after := snapshot()
	want := []int{1, 2, 3, 4, 5, 6, 13, 15, 16, 17}
	if len(after) != len(want) {
		t.Fatalf("surviving routes: %v", after)
	}
	for _, id := range want {
		if !bytes.Equal(original[id], after[id]) {
			t.Errorf("route %d lost or changed fields: before %s, after %s", id, original[id], after[id])
		}
	}
}

func TestReconfirmCancellationDoesNotPartiallyDeletePostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	_, err := pool.Exec(ctx, `
UPDATE known_routes SET node_ids=ARRAY[md5('departed')::uuid] WHERE path_key=int4send(1);
CREATE FUNCTION pause_dead_route() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_sleep(1); RETURN OLD; END $$;
CREATE TRIGGER pause_dead_route BEFORE DELETE ON known_routes FOR EACH ROW EXECUTE FUNCTION pause_dead_route();`)
	if err != nil {
		t.Fatal(err)
	}
	sweepCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := store.ReconfirmSweep(sweepCtx, 1000); err == nil {
		t.Fatal("expected sweep cancellation")
	}
	// Waiting for this DDL also waits for cancellation to release the row lock.
	if _, err := pool.Exec(ctx, `DROP TRIGGER pause_dead_route ON known_routes`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("cancelled sweep partially deleted routes: %d, %v", count, err)
	}
	if _, ok := loadReconfirmCursor(t, ctx, store); ok {
		t.Fatal("cancelled sweep saved a cursor")
	}
	res, err := store.ReconfirmSweep(ctx, 1000)
	if err != nil || res.Deleted != 1 || res.Scanned != 2 {
		t.Fatalf("retry = %+v, %v; want Scanned 2, Deleted 1", res, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes WHERE path_key=int4send(2)`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retry lost the valid route: %d, %v", count, err)
	}
}

func TestReconfirmSkipsBusyRoutePostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	if _, err := pool.Exec(ctx, `UPDATE known_routes SET node_ids=ARRAY[md5('departed')::uuid] WHERE path_key=int4send(2)`); err != nil {
		t.Fatal(err)
	}
	busy, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback(context.Background())
	if _, err := busy.Exec(ctx, `SELECT 1 FROM known_routes WHERE path_key=int4send(1) FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	sweepCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	res, err := store.ReconfirmSweep(sweepCtx, 1000)
	if err != nil {
		t.Fatalf("sweep should not wait on a busy live route: %v", err)
	}
	if res.Scanned != 2 || res.Deleted != 1 {
		t.Fatalf("sweep = %+v; want Scanned 2, Deleted 1", res)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes WHERE path_key=int4send(1)`).Scan(&left); err != nil || left != 1 {
		t.Fatalf("busy live route: %d, %v", left, err)
	}
}

// A private schema lets independent connections exercise row locks.
func reconfirmPool(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	schema := pgx.Identifier{"reconfirm_" + uuid.NewString()}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `
CREATE TABLE known_routes (LIKE public.known_routes INCLUDING ALL);
CREATE TABLE node_short_ids (LIKE public.node_short_ids INCLUDING ALL);
CREATE TABLE background_state (LIKE public.background_state INCLUDING ALL);
INSERT INTO node_short_ids (node_id, iata, prefix_4) VALUES
 ('00000000-0000-0000-0000-000000000001', 'SEA', '\x11223344');
INSERT INTO known_routes (path_key,node_ids,hash_prefix,iata,hop_count,last_reconfirmed_at)
SELECT int4send(i), ARRAY['00000000-0000-0000-0000-000000000001'::uuid], ARRAY['\x11'::bytea], 'SEA', 1,
       '2026-01-01'::timestamptz + i * interval '1 second'
FROM generate_series(1,2) i;`)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, pool, New(pool, pool, 0, 0)
}
