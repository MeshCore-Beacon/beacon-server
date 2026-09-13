// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestExportPostgres runs only on an explicitly selected private test server with
// pg_dump/psql installed. It creates and drops two randomly named test databases.
// BEACON_BACKUP_TEST_BINARY points to the exact compiled command being verified.
func TestExportPostgres(t *testing.T) {
	if os.Getenv("BEACON_BACKUP_TEST_POSTGRES") != "1" {
		t.Skip("set BEACON_BACKUP_TEST_POSTGRES=1 and BEACON_BACKUP_TEST_BINARY on a private PostgreSQL test server")
	}
	binary := os.Getenv("BEACON_BACKUP_TEST_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("the exact backup command must be an absolute path")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatal("cannot connect to private test PostgreSQL")
	}
	defer admin.Close(context.Background())
	names := []string{"beacon_backup_test_" + strings.ToLower(rand.Text()), "beacon_backup_test_" + strings.ToLower(rand.Text())}
	for _, name := range names {
		ident := pgx.Identifier{name}.Sanitize()
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if _, err := admin.Exec(cleanup, "DROP DATABASE "+ident); err != nil {
				t.Errorf("cannot remove own test database: %v", err)
			}
		}()
	}
	poolFor := func(name string) *pgxpool.Pool {
		t.Helper()
		cfg, err := pgxpool.ParseConfig("")
		if err != nil {
			t.Fatal("invalid private test connection settings")
		}
		cfg.ConnConfig.Database = name
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal("cannot open own test database")
		}
		return pool
	}
	source, target := poolFor(names[0]), poolFor(names[1])
	defer source.Close()
	defer target.Close()
	if err := db.RunMigrations(ctx, source); err != nil {
		t.Fatal(err)
	}
	_, err = source.Exec(ctx, `
CREATE TABLE backup_fixture (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, message text, raw bytea, detail jsonb);
INSERT INTO backup_fixture (message, raw, detail) VALUES ('café 雪', decode('00ff10', 'hex'), '{"nested":[1,null,true]}'), ('line one
line two', NULL, '{}');
CREATE TABLE backup_child (id bigint PRIMARY KEY REFERENCES backup_fixture(id));
INSERT INTO backup_child VALUES (1);
CREATE VIEW backup_view AS SELECT id, message FROM backup_fixture;
CREATE MATERIALIZED VIEW backup_materialized AS SELECT count(*) AS count FROM backup_child;
`)
	if err != nil {
		t.Fatal(err)
	}
	const check = `SELECT json_build_object(
 'rows', (SELECT json_agg(x ORDER BY id) FROM backup_fixture x),
 'children', (SELECT json_agg(x ORDER BY id) FROM backup_child x),
 'view', (SELECT json_agg(x ORDER BY id) FROM backup_view x),
 'materialized', (SELECT json_agg(x) FROM backup_materialized x),
 'migrations', (SELECT json_agg(x ORDER BY filename) FROM schema_migrations x),
 'columns', (SELECT md5(string_agg(table_name||column_name||data_type, ',' ORDER BY table_name,ordinal_position)) FROM information_schema.columns WHERE table_schema='public')
)::text`
	var before, after string
	if err := source.QueryRow(ctx, check).Scan(&before); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	config := []byte("# saved configuration, including synthetic key\nchannel_keys:\n  keys: {}\n")
	configPath, output := filepath.Join(dir, "config.yaml"), filepath.Join(dir, "backup.tar.gz")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "-config", configPath, "-output", output, "-max-bytes", "16777216", "-timeout", "1m")
	command.Env = append(os.Environ(), "PGDATABASE="+names[0])
	if err := command.Run(); err != nil {
		t.Fatal("exact backup command failed")
	}
	original, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"existing", "size", "connection", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			failedOutput := filepath.Join(dir, failure+".tar.gz")
			limit, timeout, database := "16777216", "30s", names[0]
			if failure == "existing" {
				failedOutput = output
			} else if failure == "size" {
				limit = "64"
			} else if failure == "connection" {
				database = "beacon_backup_test_missing_" + strings.ToLower(rand.Text())
			} else {
				lock, err := source.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Rollback(context.Background())
				if _, err := lock.Exec(ctx, "LOCK TABLE backup_fixture IN ACCESS EXCLUSIVE MODE"); err != nil {
					t.Fatal(err)
				}
				timeout = "1s"
			}
			failed := exec.CommandContext(ctx, binary, "-config", configPath, "-output", failedOutput, "-max-bytes", limit, "-timeout", timeout)
			failed.Env = append(os.Environ(), "PGDATABASE="+database)
			message, err := failed.CombinedOutput()
			if err == nil || bytes.Contains(message, []byte(database)) || bytes.Contains(message, []byte("café")) {
				t.Fatal("expected a failure without database identity or payload diagnostics")
			}
			if failure == "existing" {
				retained, err := os.ReadFile(output)
				if err != nil || !bytes.Equal(retained, original) {
					t.Fatal("existing output changed")
				}
			} else if _, err := os.Lstat(failedOutput); !os.IsNotExist(err) {
				t.Fatal("failed export published an output")
			}
			staging, _ := filepath.Glob(filepath.Join(dir, ".beacon-backup-*"))
			if len(staging) != 0 {
				t.Fatal("failed export retained private staging")
			}
		})
	}
	f, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr, members := tar.NewReader(gz), map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		members[h.Name], err = io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := io.Copy(io.Discard, gz); err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(members["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 || manifest.FormatVersion != 1 || !bytes.Equal(members["config.yaml"], config) {
		t.Fatal("unexpected bundle members or saved config")
	}
	for _, member := range manifest.Files {
		hash := sha256.Sum256(members[member.Name])
		if member.SHA256 != hex.EncodeToString(hash[:]) || member.Size != int64(len(members[member.Name])) {
			t.Fatal("bundle checksum mismatch")
		}
	}
	restore := exec.CommandContext(ctx, "psql", "-X", "--set=ON_ERROR_STOP=on", "--single-transaction")
	restore.Env = append(os.Environ(), "PGDATABASE="+names[1])
	restore.Stdin = bytes.NewReader(members["database.sql"])
	if err := restore.Run(); err != nil {
		t.Fatal("restore into own empty database failed")
	}
	if err := target.QueryRow(ctx, check).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("schema, journal, data, relationships or materialized view changed after restore")
	}
	var nextID int
	if err := target.QueryRow(ctx, "INSERT INTO backup_fixture (message) VALUES ('next') RETURNING id").Scan(&nextID); err != nil || nextID != 3 {
		t.Fatal("identity sequence did not survive restore")
	}
	t.Log("exact command restored all migrations, columns, Unicode/bytea/JSON data, relationships, views and identity sequence")
}
