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

// FixtureCreateDatabase is how a fixture database must be created: with C collation, so text order (and with it the
// generated data, the statistics and the plans) is the same on every platform and libc.
const FixtureCreateDatabase = "CREATE DATABASE %s TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8'"

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
	// fixtureFormat versions the Go side of the generator (the build sequence and the marker); bump it when a change
	// here alters what a build produces.
	fixtureFormat = 3
)

var (
	fixtureStepRe = regexp.MustCompile(`^([0-9]{2})_[a-z0-9_]+\.sql$`)
	fixtureCSVRe  = regexp.MustCompile(`^[a-z][a-z0-9_]*\.csv$`)
)

// generatorInputs are the generator's inputs that live in Go rather than in fixture/. They are hashed with the
// files, and generateFixture builds from the copy it hashed.
type generatorInputs struct {
	format       int
	settings     string // the build session's SETs
	statsTarget  int    // default_statistics_target for ANALYZE
	afterAnalyze int    // the first step number that runs after ANALYZE
}

var currentInputs = generatorInputs{fixtureFormat, buildSettings, fixtureStatsTgt, fixtureAfterAnalyze}

type fixtureFiles struct {
	in     generatorInputs
	source string
	csvs   []string // base names, sorted
	steps  []string // NN_*.sql base names, sorted
	hash   string
}

// generatorFile reports whether a directory entry belongs to the generator. Names starting with "." or "_" are
// left out, as go:embed leaves them out, so editor and system files beside the generator do not break it.
func generatorFile(d fs.DirEntry) bool {
	return !d.IsDir() && !strings.HasPrefix(d.Name(), ".") && !strings.HasPrefix(d.Name(), "_")
}

// readFixtureFiles lists the generator in fsys and hashes its Go-side inputs and every file of it.
func readFixtureFiles(fsys fs.FS) (fixtureFiles, error) {
	return readFixtureFilesWith(fsys, currentInputs)
}

func readFixtureFilesWith(fsys fs.FS, in generatorInputs) (fixtureFiles, error) {
	ff := fixtureFiles{in: in}
	h := sha256.New()
	fmt.Fprintf(h, "format=%d\x00settings=%s\x00stats_target=%d\x00after_analyze=%d\x00", in.format, in.settings,
		in.statsTarget, in.afterAnalyze)
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
		if !generatorFile(d) || d.Name() == path.Base(fixtureSource) {
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
		if d.IsDir() && !strings.HasPrefix(d.Name(), ".") && !strings.HasPrefix(d.Name(), "_") {
			return ff, fmt.Errorf("fixture: %s/%s is a directory", fixtureData, d.Name())
		}
		if !generatorFile(d) {
			continue
		}
		if !fixtureCSVRe.MatchString(d.Name()) {
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

// fixtureHash is the hash BuildFixture records for the generator in fsys and this build of the code: a database
// whose finished marker holds it, on a server of the marker's server_version_num, was built from exactly these
// inputs.
func fixtureHash(fsys fs.FS) (string, error) {
	ff, err := readFixtureFiles(fsys)
	return ff.hash, err
}

// fixtureState is what the database says about an earlier build.
type fixtureState struct {
	marker   bool   // planguard_src.fixture_marker exists
	hash     string // the hash of a finished build; "" when the build did not finish
	version  int    // server_version_num the finished build ran on
	collate  string // datcollate the finished build ran under
	dataIn   string // a public table that holds a row (schema_migrations aside), or ""
	noLedger bool   // public holds tables but no schema_migrations
	migrated int    // rows in schema_migrations
}

func readFixtureState(ctx context.Context, conn *pgx.Conn) (fixtureState, error) {
	var st fixtureState
	if err := conn.QueryRow(ctx, "SELECT to_regclass('"+fixtureSchema+".fixture_marker') IS NOT NULL").Scan(&st.marker); err != nil {
		return st, err
	}
	if st.marker {
		// to_jsonb reads a marker of an older format too, which has no server_version_num.
		err := conn.QueryRow(ctx, "SELECT coalesce(max(m.hash) FILTER (WHERE m.built_at IS NOT NULL), ''), "+
			"coalesce(max((to_jsonb(m) ->> 'server_version_num')::int) FILTER (WHERE m.built_at IS NOT NULL), 0), "+
			"coalesce(max(to_jsonb(m) ->> 'datcollate') FILTER (WHERE m.built_at IS NOT NULL), '') FROM "+
			fixtureSchema+".fixture_marker m").Scan(&st.hash, &st.version, &st.collate)
		if err != nil {
			return st, err
		}
	}
	// packets first, so a Beacon database is named by its packets.
	rows, err := conn.Query(ctx, "SELECT c.relname FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace "+
		"AND c.relkind IN ('r', 'p') ORDER BY c.relname <> 'packets', c.relname")
	if err != nil {
		return st, err
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return st, err
	}
	ledger := false
	for _, t := range tables {
		if t == "schema_migrations" {
			ledger = true
			if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.schema_migrations").Scan(&st.migrated); err != nil {
				return st, err
			}
			continue
		}
		if st.dataIn != "" {
			continue
		}
		var has bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM public."+pgx.Identifier{t}.Sanitize()+")").Scan(&has); err != nil {
			return st, err
		}
		if has {
			st.dataIn = t
		}
	}
	st.noLedger = len(tables) > 0 && !ledger
	return st, nil
}

// BuildFixture migrates the database through db.RunMigrations, loads fixture/testdata/*.csv into schema
// planguard_src with COPY FROM STDIN (pgconn CopyFrom), runs fixture/NN_*.sql in name order, ANALYZEs under
// default_statistics_target = 200, runs the stale-region step, and records planguard_src.fixture_marker(hash,
// built_at). It returns early when the marker's hash equals the hash of the embedded fixture files.
// It refuses (error "not a planguard fixture database") when public.packets has a row and no marker exists.
//
// The guard is wider than packets: without a marker, it refuses a database where any public table other than
// schema_migrations holds a row (a migrated Beacon database already does), or where public holds tables but no
// schema_migrations. The hash covers the generator's Go-side inputs too (format, build settings, statistics
// target, the after-ANALYZE step number), and a build is reused only on the server version and collation it ran
// on. The database must have C collation and ctype (FixtureCreateDatabase); any other is refused before anything is
// written.
//
// fixture/source.sql creates the planguard_src tables the CSVs load into. Steps numbered 90 and up run after
// ANALYZE. Autovacuum is switched off on every public table the build fills, so the statistics stay the ones
// it took. The pool's connections must resolve unqualified names to public. The marker is written, unfinished,
// before the first migration, so a build that stops part way is recognised as a fixture and rebuilt from empty
// tables by the next call.
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
	var sp, dbname, collate, ctype string
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_schemas(false)::text, current_setting('server_version_num')::int, "+
		"d.datname, d.datcollate, d.datctype FROM pg_database d WHERE d.datname = current_database()").Scan(
		&sp, &version, &dbname, &collate, &ctype); err != nil {
		return err
	}
	if !strings.HasPrefix(sp, "{public") {
		return fmt.Errorf("fixture: the connection's search_path resolves to %s; it must start with public", sp)
	}
	if collate != "C" || ctype != "C" {
		return fmt.Errorf("fixture: database %s has LC_COLLATE %q and LC_CTYPE %q; a fixture needs C, so its text order "+
			"does not depend on the platform: "+FixtureCreateDatabase, dbname, collate, ctype, pgx.Identifier{dbname}.Sanitize())
	}

	st, err := readFixtureState(ctx, conn)
	if err != nil {
		return fmt.Errorf("fixture: read state: %w", err)
	}
	if !st.marker {
		switch {
		case st.dataIn != "":
			return fmt.Errorf("%w: public.%s holds data and there is no %s.fixture_marker", ErrNotFixture, st.dataIn, fixtureSchema)
		case st.noLedger:
			return fmt.Errorf("%w: public holds tables but no schema_migrations", ErrNotFixture)
		}
	}
	if st.marker && st.hash == ff.hash && st.version == version && st.collate == collate {
		if err := db.RunMigrations(ctx, pool); err != nil {
			return err
		}
		var migrated int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.schema_migrations").Scan(&migrated); err != nil {
			return err
		}
		if migrated == st.migrated {
			return nil
		}
	}
	if st.marker {
		if err := resetFixture(ctx, conn); err != nil {
			return fmt.Errorf("fixture: reset: %w", err)
		}
	}
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+fixtureSchema+"; CREATE TABLE "+fixtureSchema+
		".fixture_marker (hash text NOT NULL, server_version_num int NOT NULL, datcollate text NOT NULL, built_at timestamptz); "+
		"INSERT INTO "+fixtureSchema+".fixture_marker (hash, server_version_num, datcollate) VALUES ('"+ff.hash+"', "+
		fmt.Sprint(version)+", "+quoteLit(collate)+")"); err != nil {
		return fmt.Errorf("fixture: marker: %w", err)
	}
	if err := db.RunMigrations(ctx, pool); err != nil {
		return err
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
	if _, err := conn.Exec(ctx, ff.in.settings); err != nil {
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
		if n >= ff.in.afterAnalyze {
			after = append(after, s)
		} else {
			before = append(before, s)
		}
	}

	// Load and generate in one transaction; the marker stays unfinished until the end.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
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
	if _, err := conn.Exec(ctx, fmt.Sprintf("SET default_statistics_target = %d", ff.in.statsTarget)); err != nil {
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
