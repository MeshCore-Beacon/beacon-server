// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// indexDropMigrations drop the observation index that no served statement needs once advert pages read the partial
// adverts index (migration 034).
var indexDropMigrations = []string{
	"046_drop_observations_observer_id.sql",
}

// checkIndexSet fails unless table has exactly the indexes in want (sorted), each valid and ready.
func checkIndexSet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, want []string) {
	t.Helper()
	var got, bad []string
	if err := pool.QueryRow(ctx, `SELECT coalesce(array_agg(c.relname::text ORDER BY c.relname::text), '{}'),
       coalesce(array_agg(c.relname::text) FILTER (WHERE NOT (x.indisvalid AND x.indisready)), '{}')
FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
WHERE x.indrelid = $1::regclass`, table).Scan(&got, &bad); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) || len(bad) != 0 {
		t.Errorf("%s indexes = %v, want %v; not valid and ready: %v", table, got, want, bad)
	}
}

// rerunDrops deletes the ledger rows of files, as if an operator dropped their indexes by hand or a crash lost the
// rows, and checks that the migrations run them again as no-ops and record them.
func rerunDrops(t *testing.T, ctx context.Context, pool *pgxpool.Pool, files ...string) {
	t.Helper()
	if _, err := pool.Exec(ctx, "DELETE FROM schema_migrations WHERE filename = ANY($1)", files); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("re-running %v: %v", files, err)
	}
	checkRecorded(t, ctx, pool, files...)
}

// The migrations leave packet_observations with exactly the indexes a statement or a constraint reads.
func TestObservationsIndexSetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	want := []string{"idx_observations_adverts", "idx_observations_heard", "idx_observations_iata_heard",
		"idx_observations_observer", "idx_observations_packet", "idx_observations_route_evidence",
		"packet_observations_packet_hash_observer_id_key", "packet_observations_pkey"}
	checkIndexSet(t, ctx, pool, "packet_observations", want)
	// The observer foreign key keeps an index that leads with observer_id, and advert pages keep theirs.
	for ix, lead := range map[string]string{
		"idx_observations_observer": "USING btree (observer_id,",
		"idx_observations_adverts":  "USING btree (observer_id, id) WHERE (payload_type = 4)",
	} {
		var def string
		if err := pool.QueryRow(ctx, "SELECT pg_get_indexdef($1::regclass)", ix).Scan(&def); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(def, lead) {
			t.Errorf("%s = %q, want it to contain %q", ix, def, lead)
		}
	}

	rerunDrops(t, ctx, pool, "046_drop_observations_observer_id.sql")
	checkIndexSet(t, ctx, pool, "packet_observations", want)
}

// indexDropFixture adds to packetFixture: observations typed from their packets (advert pages read the observation's
// own type), and observer 5 hearing every fifth packet with ids above 2^31, at the packet's newest heard_at (a tie);
// observer 6, old and silent, which the observer cleanup removes.
const indexDropFixture = `
UPDATE packet_observations po SET payload_type = p.payload_type FROM packets p WHERE p.packet_hash = po.packet_hash;
INSERT INTO observers (id, public_key, display_name, last_seen)
VALUES ('00000000-0000-0000-0000-000000000305', int2send(5::int2), 'obs5', '2026-01-01'),
       ('00000000-0000-0000-0000-000000000306', int2send(6::int2), 'obs6, never heard anything', '2026-01-01');
INSERT INTO packet_observations (id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes, payload_type)
SELECT 3000000000 + i, p.packet_hash, '00000000-0000-0000-0000-000000000305', 'YYJ', p.last_heard_at, 65, 1, 1, '\x05', p.payload_type
FROM generate_series(5, 600, 5) i JOIN packets p ON p.packet_hash = decode(md5(i::text), 'hex');
ANALYZE packets, packet_observations, observers;`

// runIsolated runs fn inside a savepoint that is rolled back, so a writer leaves the fixture as it was.
func runIsolated(ctx context.Context, tx pgx.Tx, fn func() (any, error)) (any, error) {
	if _, err := tx.Exec(ctx, "SAVEPOINT drops_reader"); err != nil {
		return nil, err
	}
	v, err := fn()
	if _, rerr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT drops_reader"); rerr != nil {
		return nil, rerr
	}
	return v, err
}

// errCode keeps only the SQLSTATE of a PostgreSQL error, so before and after compare by class.
func errCode(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return errors.New(pe.Code)
	}
	return err
}

// Rows from every reader of the dropped observer index are the same before and after the drop, in four plan modes:
// the advert pages, the observer cleanup and the observer foreign-key check.
func TestReadersAfterIndexDropsPostgres(t *testing.T) {
	ctx, tx, store := packetsGlobalTx(t)
	// The partial adverts index (034) is in place before the drop, as in every migrated database.
	applyMigrationsTx(t, ctx, tx, "034_observations_adverts.sql")
	mustExec(t, ctx, tx, indexDropFixture)
	mustExec(t, ctx, tx, "SET LOCAL jit = off")
	q := sqlc.New(unnamedDBTX{tx})
	same := func(v any) any { return v }
	obs1 := uuid.MustParse("00000000-0000-0000-0000-000000000301")
	obs5 := uuid.MustParse("00000000-0000-0000-0000-000000000305")
	adverts := func(name string, obs uuid.UUID, cursor int64) readerShape {
		return readerShape{name: name, norm: same, run: func() (any, error) {
			return q.ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{ObserverID: obs, CursorID: cursor, RowLimit: 21})
		}}
	}
	var midAdvert int64
	if err := tx.QueryRow(ctx, "SELECT id FROM packet_observations WHERE observer_id = $1 AND payload_type = 4 ORDER BY id OFFSET 3 LIMIT 1", obs5).Scan(&midAdvert); err != nil {
		t.Fatal(err)
	}
	shapes := []readerShape{
		adverts("adverts obs1", obs1, 0),
		adverts("adverts obs5", obs5, 0),
		adverts("adverts obs5 mid", obs5, midAdvert),
		adverts("adverts obs5 2^31", obs5, math.MaxInt32+1),
		adverts("adverts obs5 past end", obs5, math.MaxInt64),
		{name: "delete old observers", norm: func(v any) any {
			ids := slices.Clone(v.([]uuid.UUID))
			slices.SortFunc(ids, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
			return ids
		}, run: func() (any, error) {
			return runIsolated(ctx, tx, func() (any, error) { return store.DeleteOldObservers(ctx, packetBase.Add(time.Hour)) })
		}},
		{name: "observer delete blocked by its observations", norm: same, run: func() (any, error) {
			_, err := runIsolated(ctx, tx, func() (any, error) {
				return tx.Exec(ctx, "DELETE FROM observers WHERE id = $1", obs5)
			})
			return errCode(err).Error(), nil
		}},
	}

	before := runShapes(t, ctx, tx, shapes)
	for name, check := range map[string]func(any) bool{
		"adverts obs5":                                func(v any) bool { return len(v.([]sqlc.ListObserverAdvertsRow)) > 0 },
		"adverts obs5 2^31":                           func(v any) bool { return len(v.([]sqlc.ListObserverAdvertsRow)) > 0 },
		"delete old observers":                        func(v any) bool { return len(v.([]uuid.UUID)) > 0 },
		"observer delete blocked by its observations": func(v any) bool { return v.(string) == "23503" },
	} {
		if r := before[name][0]; r.err != nil || !check(r.val) {
			t.Fatalf("%s: fixture does not exercise it: %+v, %v", name, r.val, r.err)
		}
	}

	applyMigrationsTx(t, ctx, tx, indexDropMigrations...)
	mustExec(t, ctx, tx, "ANALYZE packet_observations")
	after := runShapes(t, ctx, tx, shapes)
	compareShapes(t, shapes, before, after)
}
