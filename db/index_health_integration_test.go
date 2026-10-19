// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func mustPoolExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
}

func indexHealthOf(t *testing.T, ctx context.Context, store *Store, name string) IndexHealth {
	t.Helper()
	all, err := store.MeasureIndexHealth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range all {
		if h.Name == name {
			return h
		}
	}
	t.Fatalf("index %s not measured in %+v", name, all)
	return IndexHealth{}
}

func savedBaselines(t *testing.T, ctx context.Context, store *Store) map[string]indexBaseline {
	t.Helper()
	b := map[string]indexBaseline{}
	if ok, err := store.LoadState(ctx, indexBaselinesKey, &b); err != nil || !ok {
		t.Fatalf("index baselines: ok=%v err=%v", ok, err)
	}
	return b
}

func TestIndexHealthBaselineOnReindexPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := New(pool, pool, 0, 0)
	mustPoolExec(t, ctx, pool, `CREATE TABLE ih_probe (a int, b text); CREATE INDEX ih_probe_a ON ih_probe (a);
CREATE TABLE ih_unanalyzed (a int); CREATE INDEX ih_unanalyzed_a ON ih_unanalyzed (a);
INSERT INTO ih_probe SELECT i, 'x' FROM generate_series(1, 50000) i; ANALYZE ih_probe;`)

	first := indexHealthOf(t, ctx, store, "ih_probe_a")
	if !first.HasRatio || first.Ratio != 1 || first.Bytes <= 0 {
		t.Fatalf("first sight %+v, want ratio 1 and a size", first)
	}
	f1 := savedBaselines(t, ctx, store)["ih_probe_a"].Filenode
	if f1 == 0 {
		t.Fatal("no baseline filenode saved")
	}
	if h := indexHealthOf(t, ctx, store, "ih_unanalyzed_a"); h.HasRatio {
		t.Errorf("unanalyzed index %+v has a ratio", h)
	}
	if _, ok := savedBaselines(t, ctx, store)["ih_unanalyzed_a"]; ok {
		t.Error("unanalyzed index got a baseline")
	}

	mustPoolExec(t, ctx, pool, `UPDATE ih_probe SET a = a + 1`)
	mustPoolExec(t, ctx, pool, `UPDATE ih_probe SET a = a + 1`)
	mustPoolExec(t, ctx, pool, `VACUUM ih_probe`)
	bloated := indexHealthOf(t, ctx, store, "ih_probe_a")
	if !bloated.HasRatio || bloated.Ratio <= 2.5 {
		t.Errorf("after two full updates %+v, want ratio > 2.5", bloated)
	}
	if got := savedBaselines(t, ctx, store)["ih_probe_a"].Filenode; got != f1 {
		t.Errorf("baseline filenode %d after bloat, want %d", got, f1)
	}

	mustPoolExec(t, ctx, pool, `REINDEX INDEX ih_probe_a`)
	rebuilt := indexHealthOf(t, ctx, store, "ih_probe_a")
	if !rebuilt.HasRatio || math.Abs(rebuilt.Ratio-1) >= 0.01 {
		t.Errorf("after REINDEX %+v, want ratio 1", rebuilt)
	}
	if got := savedBaselines(t, ctx, store)["ih_probe_a"].Filenode; got == f1 {
		t.Errorf("baseline filenode still %d after REINDEX", got)
	}

	mustPoolExec(t, ctx, pool, `DROP INDEX ih_probe_a`)
	if _, err := store.MeasureIndexHealth(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := savedBaselines(t, ctx, store)["ih_probe_a"]; ok {
		t.Error("dropped index keeps its baseline")
	}
}

func TestUnusedIndexesPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	store := New(pool, pool, 0, 0)
	mustPoolExec(t, ctx, pool, `CREATE TABLE ui_probe (a int PRIMARY KEY, b int); CREATE INDEX ui_probe_b ON ui_probe (b);
INSERT INTO ui_probe SELECT i, i FROM generate_series(1, 1000) i;`)
	unused, err := store.UnusedIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(unused) != 1 || unused[0].Name != "ui_probe_b" || unused[0].Bytes <= 0 {
		t.Errorf("unused %+v, want only ui_probe_b (the primary key is unique)", unused)
	}
}

func TestPgActivityPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	store := New(pool, pool, 0, 0)
	for range 2 { // the second call uses the cached server version
		a, err := store.PgActivity(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if a.WALBytes <= 0 || a.XIDAge < 0 || a.CheckpointsTimed < 0 || a.CheckpointsRequested < 0 {
			t.Errorf("activity %+v", a)
		}
	}
}
