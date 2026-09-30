// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ambiguity(t *testing.T, ctx context.Context, store *Store) AmbiguousPrefixes {
	t.Helper()
	amb, err := store.AmbiguousPrefixes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return amb
}

func TestReconfirmReleasesEarlierBatchPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	before := time.Now()
	if n, err := store.ReconfirmRoutes(ctx, 1, before, ambiguity(t, ctx, store)); err != nil || n != 1 {
		t.Fatalf("first batch: %d, %v", n, err)
	}
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
BEGIN IF NEW.path_key=int4send(2) THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$;
CREATE TRIGGER pause_second_route BEFORE UPDATE ON known_routes FOR EACH ROW EXECUTE FUNCTION pause_second_route();`, key))
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	amb := ambiguity(t, ctx, store)
	go func() {
		_, err := store.ReconfirmRoutes(ctx, 1, before, amb)
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
	var count int
	if err := pool.QueryRow(ctx, `SELECT observation_count FROM known_routes WHERE path_key=int4send(1)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("concurrent observation increment lost: count %d, error %v", count, err)
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
		rows, err := pool.Query(ctx, `SELECT id, to_jsonb(r)-'last_reconfirmed_at' FROM known_routes r`)
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
	var checked int64
	for range 10 {
		n, err := store.ReconfirmRoutes(ctx, 3, before, ambiguity(t, ctx, store))
		if err != nil {
			t.Fatal(err)
		}
		checked += n
		if n == 0 {
			break
		}
	}
	if checked != 15 {
		t.Fatalf("checked %d routes; want 15 once each, including deleted routes", checked)
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
	var stamped int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes WHERE id NOT IN (16,17) AND last_reconfirmed_at >= $1`, before).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped != 8 {
		t.Errorf("validated survivors = %d, want 8", stamped)
	}
}

func TestReconfirmCancelledBatchRollsBackPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	before := time.Now()
	if _, err := store.ReconfirmRoutes(ctx, 1, before, ambiguity(t, ctx, store)); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `
CREATE FUNCTION pause_reconfirm() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$;
CREATE TRIGGER pause_reconfirm BEFORE UPDATE ON known_routes FOR EACH ROW EXECUTE FUNCTION pause_reconfirm();`)
	if err != nil {
		t.Fatal(err)
	}
	batchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := store.ReconfirmRoutes(batchCtx, 1, before, ambiguity(t, ctx, store)); err == nil {
		t.Fatal("expected batch cancellation")
	}
	// Waiting for this DDL also waits for cancellation to release the row lock.
	if _, err := pool.Exec(ctx, `DROP TRIGGER pause_reconfirm ON known_routes`); err != nil {
		t.Fatal(err)
	}
	var total, stamped int
	if err := pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE last_reconfirmed_at >= $1) FROM known_routes`, before).Scan(&total, &stamped); err != nil {
		t.Fatal(err)
	}
	if total != 2 || stamped != 1 {
		t.Fatalf("cancellation changed committed work: total %d, checked %d", total, stamped)
	}
	if n, err := store.ReconfirmRoutes(ctx, 1, before, ambiguity(t, ctx, store)); err != nil || n != 1 {
		t.Fatalf("cancelled route was not available to retry: %d, %v", n, err)
	}
}

func TestReconfirmFutureCutoffDoesNotRepeatPostgres(t *testing.T) {
	ctx, _, store := reconfirmPool(t)
	before := time.Now().Add(time.Hour)
	if n, err := store.ReconfirmRoutes(ctx, 2, before, ambiguity(t, ctx, store)); err != nil || n != 2 {
		t.Fatalf("first batch: %d, %v", n, err)
	}
	if n, err := store.ReconfirmRoutes(ctx, 2, before, ambiguity(t, ctx, store)); err != nil || n != 0 {
		t.Fatalf("completed routes consumed the run budget twice: %d, %v", n, err)
	}
}

func TestReconfirmCancellationDoesNotPartiallyDeletePostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	_, err := pool.Exec(ctx, `
UPDATE known_routes SET node_ids=ARRAY[md5('departed')::uuid] WHERE path_key=int4send(1);
CREATE FUNCTION pause_valid_route() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$;
CREATE TRIGGER pause_valid_route BEFORE UPDATE ON known_routes FOR EACH ROW EXECUTE FUNCTION pause_valid_route();`)
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	batchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := store.ReconfirmRoutes(batchCtx, 2, before, ambiguity(t, ctx, store)); err == nil {
		t.Fatal("expected batch cancellation")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER pause_valid_route ON known_routes`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("cancelled batch partially deleted routes: %d, %v", count, err)
	}
	if n, err := store.ReconfirmRoutes(ctx, 2, before, ambiguity(t, ctx, store)); err != nil || n != 2 {
		t.Fatalf("retry did not process both routes: %d, %v", n, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes WHERE path_key=int4send(2)`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retry lost the valid route: %d, %v", count, err)
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
INSERT INTO node_short_ids (node_id, iata, prefix_4) VALUES
 ('00000000-0000-0000-0000-000000000001', 'SEA', '\x11223344');
INSERT INTO known_routes (path_key,node_ids,hash_prefix,iata,hop_count,last_reconfirmed_at)
SELECT int4send(i), ARRAY['00000000-0000-0000-0000-000000000001'::uuid], ARRAY['\x11'::bytea], 'SEA', 1,
       '2026-01-01'::timestamptz + i * interval '1 second'
FROM generate_series(1,2) i;`)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, pool, New(pool, 0, 0)
}

func TestReconfirmSkipsBusyRoutePostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	busy, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Rollback(context.Background())
	if _, err := busy.Exec(ctx, `SELECT 1 FROM known_routes WHERE path_key=int4send(1) FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	batchCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := store.ReconfirmRoutes(batchCtx, 1, time.Now(), ambiguity(t, ctx, store)); err != nil {
		t.Fatalf("maintenance should skip the busy route and validate the next one: %v", err)
	}
	var checked, preserved int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE last_reconfirmed_at > '2026-02-01'), count(*) FROM known_routes`).Scan(&checked, &preserved); err != nil {
		t.Fatal(err)
	}
	if checked != 1 || preserved != 2 {
		t.Fatalf("checked %d, preserved %d; want 1 and 2", checked, preserved)
	}
	if err := busy.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconfirmRoutes(ctx, 1, time.Now(), ambiguity(t, ctx, store)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM known_routes WHERE last_reconfirmed_at > '2026-02-01'`).Scan(&checked); err != nil {
		t.Fatal(err)
	}
	if checked != 2 {
		t.Fatalf("deferred route was not validated after release: checked %d", checked)
	}
}
