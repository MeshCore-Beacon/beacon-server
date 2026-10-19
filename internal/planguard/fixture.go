// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFixture is returned by BuildFixture for a database that holds data and was not built by it.
var ErrNotFixture = errors.New("not a planguard fixture database")

const (
	fixtureDir      = "fixture"
	fixtureData     = "fixture/testdata"
	fixtureSource   = "fixture/source.sql"
	fixtureSchema   = "planguard_src"
	fixtureStatsTgt = 200
	// Generator files from this number on run after ANALYZE: they change data the statistics must not see.
	fixtureAfterAnalyze = 90
)

var (
	fixtureStepRe = regexp.MustCompile(`^([0-9]{2})_[a-z0-9_]+\.sql$`)
	fixtureCSVRe  = regexp.MustCompile(`^[a-z][a-z0-9_]*\.csv$`)
)

// fixtureGuardTables are the public tables BuildFixture looks at before it writes: a database where one of them
// holds a row and that carries no fixture marker is somebody's data, and BuildFixture refuses it.
var fixtureGuardTables = []string{"packets", "packet_observations", "known_routes", "nodes", "channel_messages"}

type fixtureFiles struct {
	source string
	csvs   []string // base names, sorted
	steps  []string // NN_*.sql base names, sorted
	hash   string
}

// readFixtureFiles lists the generator in fsys and hashes every file of it.
func readFixtureFiles(fsys fs.FS) (fixtureFiles, error) {
	var ff fixtureFiles
	h := sha256.New()
	add := func(p string) error {
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", p, len(b))
		h.Write(b)
		return nil
	}
	if err := add(fixtureSource); err != nil {
		return ff, fmt.Errorf("fixture: %w", err)
	}
	ff.source = fixtureSource
	steps, err := fs.ReadDir(fsys, fixtureDir)
	if err != nil {
		return ff, fmt.Errorf("fixture: %w", err)
	}
	for _, d := range steps {
		if d.IsDir() || d.Name() == path.Base(fixtureSource) {
			continue
		}
		if !fixtureStepRe.MatchString(d.Name()) {
			return ff, fmt.Errorf("fixture: %s is not NN_name.sql", d.Name())
		}
		ff.steps = append(ff.steps, d.Name())
	}
	csvs, err := fs.ReadDir(fsys, fixtureData)
	if err != nil {
		return ff, fmt.Errorf("fixture: %w", err)
	}
	for _, d := range csvs {
		if d.IsDir() || !fixtureCSVRe.MatchString(d.Name()) {
			return ff, fmt.Errorf("fixture: %s/%s is not a lower-case name.csv", fixtureData, d.Name())
		}
		ff.csvs = append(ff.csvs, d.Name())
	}
	sort.Strings(ff.steps)
	sort.Strings(ff.csvs)
	if len(ff.steps) == 0 || len(ff.csvs) == 0 {
		return ff, errors.New("fixture: no generator steps or no testdata")
	}
	for _, s := range ff.steps {
		if err := add(path.Join(fixtureDir, s)); err != nil {
			return ff, err
		}
	}
	for _, c := range ff.csvs {
		if err := add(path.Join(fixtureData, c)); err != nil {
			return ff, err
		}
	}
	ff.hash = hex.EncodeToString(h.Sum(nil))
	return ff, nil
}

// FixtureHash is the hash BuildFixture records for the generator in fsys: a database whose finished marker holds
// it was built from exactly these files.
func FixtureHash(fsys fs.FS) (string, error) {
	ff, err := readFixtureFiles(fsys)
	return ff.hash, err
}

// fixtureState is what the database says about an earlier build.
type fixtureState struct {
	marker   bool   // planguard_src.fixture_marker exists
	hash     string // its hash; "" when the build did not finish
	hasData  bool   // a guard table holds a row
	ledger   bool   // schema_migrations exists
	migrated int    // rows in schema_migrations
}

func readFixtureState(ctx context.Context, conn *pgx.Conn) (fixtureState, error) {
	var st fixtureState
	if err := conn.QueryRow(ctx, "SELECT to_regclass('"+fixtureSchema+".fixture_marker') IS NOT NULL, "+
		"to_regclass('public.schema_migrations') IS NOT NULL").Scan(&st.marker, &st.ledger); err != nil {
		return st, err
	}
	if st.marker {
		err := conn.QueryRow(ctx, "SELECT coalesce(max(hash) FILTER (WHERE built_at IS NOT NULL), '') FROM "+
			fixtureSchema+".fixture_marker").Scan(&st.hash)
		if err != nil {
			return st, err
		}
	}
	if st.ledger {
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.schema_migrations").Scan(&st.migrated); err != nil {
			return st, err
		}
	}
	for _, t := range fixtureGuardTables {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", "public."+t).Scan(&exists); err != nil {
			return st, err
		}
		if !exists {
			continue
		}
		var has bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM public."+t+")").Scan(&has); err != nil {
			return st, err
		}
		st.hasData = st.hasData || has
	}
	return st, nil
}

// BuildFixture migrates the database through db.RunMigrations, loads fixture/testdata/*.csv into schema
// planguard_src with COPY FROM STDIN (pgconn CopyFrom), runs fixture/NN_*.sql in name order, ANALYZEs under
// default_statistics_target = 200, runs the stale-region step, and records planguard_src.fixture_marker(hash,
// built_at). It returns early when the marker's hash equals the hash of the embedded fixture files.
// It refuses (error "not a planguard fixture database") when public.packets has a row and no marker exists.
// (It checks packet_observations, known_routes, nodes and channel_messages the same way.)
//
// fixture/source.sql creates the planguard_src tables the CSVs load into. Steps numbered 90 and up run after
// ANALYZE. Autovacuum is switched off on every public table the build fills, so the statistics stay the ones
// it took. The pool's connections must resolve unqualified names to public. A build that stops part way leaves
// an unfinished marker, and the next call starts again from empty tables.
func BuildFixture(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	ff, err := readFixtureFiles(fsys)
	if err != nil {
		return err
	}
	pc, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer pc.Release()
	conn := pc.Conn()
	var sp string
	if err := conn.QueryRow(ctx, "SELECT current_schemas(false)::text").Scan(&sp); err != nil {
		return err
	}
	if !strings.HasPrefix(sp, "{public") {
		return fmt.Errorf("fixture: the connection's search_path resolves to %s; it must start with public", sp)
	}

	st, err := readFixtureState(ctx, conn)
	if err != nil {
		return fmt.Errorf("fixture: read state: %w", err)
	}
	if !st.marker && st.hasData {
		return fmt.Errorf("%w: it holds data and no %s.fixture_marker", ErrNotFixture, fixtureSchema)
	}
	if err := db.RunMigrations(ctx, pool); err != nil {
		return err
	}
	var migrated int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.schema_migrations").Scan(&migrated); err != nil {
		return err
	}
	if st.marker && st.hash == ff.hash && migrated == st.migrated {
		return nil
	}
	if st.marker {
		if err := resetFixture(ctx, conn); err != nil {
			return fmt.Errorf("fixture: reset: %w", err)
		}
	}
	if err := generateFixture(ctx, conn, fsys, ff); err != nil {
		return fmt.Errorf("fixture: %w", err)
	}
	return nil
}

// resetFixture empties a fixture database built from other files or migrations: every public table but the
// migration ledger, and the source schema.
func resetFixture(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `DO $$
DECLARE t text;
BEGIN
  SELECT string_agg(format('public.%I', c.relname), ', ') INTO t FROM pg_class c
  WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND c.relname <> 'schema_migrations';
  IF t IS NOT NULL THEN
    EXECUTE 'TRUNCATE ' || t || ' RESTART IDENTITY CASCADE';
  END IF;
END $$; DROP SCHEMA IF EXISTS `+fixtureSchema+` CASCADE`)
	return err
}

// buildSettings make the build fast and repeatable. GIN inserts collect in the pending list, which VACUUM merges.
// The generator draws from random() after setseed, so the data depends on the order rows reach each draw, which is
// the plan's: the planner's inputs are pinned to PostgreSQL's built-in values, whatever the server's
// configuration, and plans stay serial.
const buildSettings = `SET jit = off; SET max_parallel_workers_per_gather = 0; SET work_mem = '256MB';
SET hash_mem_multiplier = 2; SET maintenance_work_mem = '512MB'; SET synchronous_commit = off; SET timezone = 'UTC';
SET gin_pending_list_limit = '256MB';
SET seq_page_cost = 1; SET random_page_cost = 4; SET cpu_tuple_cost = 0.01; SET cpu_index_tuple_cost = 0.005;
SET cpu_operator_cost = 0.0025; SET effective_cache_size = '4GB'; SET default_statistics_target = 100;
SET enable_seqscan = on; SET enable_indexscan = on; SET enable_bitmapscan = on; SET enable_hashjoin = on;
SET enable_mergejoin = on; SET enable_nestloop = on; SET enable_sort = on; SET enable_hashagg = on;
SET enable_material = on; SET enable_memoize = on; SET enable_incremental_sort = on`

func generateFixture(ctx context.Context, conn *pgx.Conn, fsys fs.FS, ff fixtureFiles) error {
	if _, err := conn.Exec(ctx, buildSettings); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "RESET ALL")
	}()
	run := func(name string) error {
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	}
	var before, after []string
	for _, s := range ff.steps {
		n := int(s[0]-'0')*10 + int(s[1]-'0')
		if n >= fixtureAfterAnalyze {
			after = append(after, s)
		} else {
			before = append(before, s)
		}
	}

	// Load and generate in one transaction, marker included but unfinished.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, "CREATE SCHEMA "+fixtureSchema+"; CREATE TABLE "+fixtureSchema+
		".fixture_marker (hash text NOT NULL, built_at timestamptz); INSERT INTO "+fixtureSchema+
		".fixture_marker (hash) VALUES ('"+ff.hash+"')"); err != nil {
		return err
	}
	if err := run(ff.source); err != nil {
		return err
	}
	for _, c := range ff.csvs {
		b, err := fs.ReadFile(fsys, path.Join(fixtureData, c))
		if err != nil {
			return err
		}
		table := fixtureSchema + "." + strings.TrimSuffix(c, ".csv")
		if _, err := tx.Conn().PgConn().CopyFrom(ctx, bytes.NewReader(b),
			"COPY "+table+" FROM STDIN (FORMAT csv, HEADER true)"); err != nil {
			return fmt.Errorf("load %s: %w", c, err)
		}
	}
	for _, s := range before {
		if err := run(path.Join(fixtureDir, s)); err != nil {
			return err
		}
	}
	// Autovacuum must not re-sample what ANALYZE below records.
	if _, err := tx.Exec(ctx, `DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT c.relname FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' LOOP
    EXECUTE format('ALTER TABLE public.%I SET (autovacuum_enabled = false)', r.relname);
  END LOOP;
END $$`); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// VACUUM sets the visibility map, so index-only scans are costed as on a settled database.
	if _, err := conn.Exec(ctx, fmt.Sprintf("SET default_statistics_target = %d", fixtureStatsTgt)); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "VACUUM (ANALYZE)"); err != nil {
		return err
	}
	for _, s := range after {
		if err := run(path.Join(fixtureDir, s)); err != nil {
			return err
		}
	}
	_, err = conn.Exec(ctx, "UPDATE "+fixtureSchema+".fixture_marker SET built_at = now()")
	return err
}
