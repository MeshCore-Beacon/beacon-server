// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"context"
	"fmt"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tinyGenerator is a fixture generator small enough to build in seconds.
func tinyGenerator() fstest.MapFS {
	return fstest.MapFS{
		"fixture/source.sql":         {Data: []byte("CREATE TABLE planguard_src.codes (iata char(3) NOT NULL);")},
		"fixture/testdata/codes.csv": {Data: []byte("iata\nAAA\nBBB\n")},
		"fixture/10_codes.sql":       {Data: []byte("INSERT INTO iata_codes (iata) SELECT iata FROM planguard_src.codes;")},
	}
}

// Every Go-side generator input is part of the hash, so changing one makes a built fixture stale.
func TestFixtureHashCoversGoInputs(t *testing.T) {
	base, err := readFixtureFilesWith(tinyGenerator(), currentInputs)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*generatorInputs){
		"format":        func(in *generatorInputs) { in.format++ },
		"settings":      func(in *generatorInputs) { in.settings += "; SET work_mem = '128MB'" },
		"stats target":  func(in *generatorInputs) { in.statsTarget++ },
		"after analyze": func(in *generatorInputs) { in.afterAnalyze++ },
	} {
		in := currentInputs
		change(&in)
		ff, err := readFixtureFilesWith(tinyGenerator(), in)
		if err != nil {
			t.Fatal(err)
		}
		if ff.hash == base.hash {
			t.Errorf("changing the %s leaves the fixture hash at %s", name, base.hash)
		}
	}
	if currentInputs.settings != buildSettings || currentInputs.statsTarget != fixtureStatsTgt ||
		currentInputs.afterAnalyze != fixtureAfterAnalyze || currentInputs.format != fixtureFormat {
		t.Errorf("currentInputs %+v do not mirror the constants the build uses", currentInputs)
	}
}

// Files go:embed would leave out (names starting with "." or "_") are left out of the generator and its hash.
func TestFixtureFilesSkipHiddenNames(t *testing.T) {
	base, err := readFixtureFiles(tinyGenerator())
	if err != nil {
		t.Fatal(err)
	}
	fsys := tinyGenerator()
	for _, p := range []string{"fixture/.DS_Store", "fixture/._10_codes.sql", "fixture/_draft.sql", "fixture/.10_codes.sql.swp",
		"fixture/testdata/.DS_Store", "fixture/testdata/_old.csv", "fixture/testdata/.codes.csv.swp"} {
		fsys[p] = &fstest.MapFile{Data: []byte("junk")}
	}
	got, err := readFixtureFiles(fsys)
	if err != nil {
		t.Fatalf("hidden files break the generator: %v", err)
	}
	if got.hash != base.hash || len(got.steps) != 1 || len(got.csvs) != 1 {
		t.Errorf("hidden files changed the generator: steps %v csvs %v hash %s (want %s)", got.steps, got.csvs, got.hash, base.hash)
	}
	fsys["fixture/notes.txt"] = &fstest.MapFile{Data: []byte("x")}
	if _, err := readFixtureFiles(fsys); err == nil {
		t.Error("a visible non-step file in fixture/ was accepted")
	}
}

// A built fixture is reused only while every input matches: the files, the Go-side inputs and the server version.
func TestBuildFixtureRebuildsStaleInputsPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	const name = "planguard_fixture_inputs_test"
	drop := func() error {
		_, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		return err
	}
	if err := drop(); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(FixtureCreateDatabase, name)); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := drop(); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})

	builtAt := func() time.Time {
		var at time.Time
		if err := pool.QueryRow(ctx, "SELECT built_at FROM planguard_src.fixture_marker WHERE built_at IS NOT NULL").Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	build := func() {
		t.Helper()
		if err := BuildFixture(ctx, pool, tinyGenerator()); err != nil {
			t.Fatal(err)
		}
	}
	build()
	first := builtAt()
	build()
	if at := builtAt(); !at.Equal(first) {
		t.Fatalf("an unchanged fixture was rebuilt (%s -> %s)", first, at)
	}

	// The marker says another server version built it.
	if _, err := pool.Exec(ctx, "UPDATE planguard_src.fixture_marker SET server_version_num = 1"); err != nil {
		t.Fatal(err)
	}
	build()
	second := builtAt()
	if !second.After(first) {
		t.Errorf("a fixture built on another server version was reused")
	}

	// A Go-side input changed.
	saved := currentInputs
	t.Cleanup(func() { currentInputs = saved })
	currentInputs.settings += "; SET work_mem = '128MB'"
	build()
	if at := builtAt(); !at.After(second) {
		t.Errorf("a fixture built with other build settings was reused")
	}
	var codes int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM iata_codes").Scan(&codes); err != nil || codes != 2 {
		t.Errorf("iata_codes after the rebuilds: %d (%v), want 2 (the tables were emptied first)", codes, err)
	}
}
