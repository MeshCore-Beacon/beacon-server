// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This opt-in test only creates/restores/drops its own randomly named databases.
func TestBackupDownloadPostgres(t *testing.T) {
	if os.Getenv("BEACON_BACKUP_TEST_POSTGRES") != "1" {
		t.Skip("requires pg_dump/psql and an explicitly selected private PostgreSQL test server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatal("cannot connect to private PostgreSQL")
	}
	defer admin.Close(context.Background())
	sourceName, targetName := "beacon_download_"+strings.ToLower(rand.Text()), "beacon_download_"+strings.ToLower(rand.Text())
	for _, name := range []string{sourceName, targetName} {
		ident := pgx.Identifier{name}.Sanitize()
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
			t.Fatal("cannot create private fixture")
		}
		defer func() {
			if _, err := admin.Exec(ctx, "DROP DATABASE "+ident+" WITH (FORCE)"); err != nil {
				t.Error("cannot remove private fixture")
			}
		}()
	}
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		t.Fatal("invalid fixture configuration")
	}
	cfg.ConnConfig.Database = sourceName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("cannot open source fixture")
	}
	defer pool.Close()
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE TABLE download_fixture (id bigint GENERATED ALWAYS AS IDENTITY, message text); INSERT INTO download_fixture(message) VALUES ('café 雪')"); err != nil {
		t.Fatal(err)
	}
	dsn := (&url.URL{Scheme: "postgres", Host: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))),
		User: url.UserPassword(cfg.ConnConfig.User, cfg.ConnConfig.Password), Path: "/" + sourceName,
		RawQuery: "sslmode=disable&pool_max_conns=2"}).String()
	service, err := backup.ConnectionService(dsn)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	saved := []byte("# verbatim saved file\nbackup:\n  enabled: true\n")
	if err := os.WriteFile(configPath, saved, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", dir)
	server := httptest.NewServer(New(nil, nil, nil, Options{
		Auth: config.AuthConfig{APIKey: "fixture-key"},
		AdminRoutes: map[string]http.Handler{"/backup": handlers.BackupRouter(backup.Options{
			ConnectionService: service, ConfigPath: configPath, MaxBytes: 16 << 20, Timeout: 30 * time.Second, Version: "http-fixture",
		})},
	}))
	defer server.Close()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/v1/admin/backup", nil)
	request.Header.Set("Authorization", "Bearer fixture-key")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("download status=%d", response.StatusCode)
	}
	gz, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tarReader, members := tar.NewReader(gz), map[string][]byte{}
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil || header.Typeflag != tar.TypeReg || header.Mode != 0600 {
			t.Fatal("invalid member")
		}
		members[header.Name], err = io.ReadAll(tarReader)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := io.Copy(io.Discard, gz); err != nil {
		t.Fatal(err)
	}
	var manifest backup.Manifest
	if json.Unmarshal(members["manifest.json"], &manifest) != nil || len(members) != 3 || manifest.FormatVersion != 1 || !bytes.Equal(members["config.yaml"], saved) {
		t.Fatal("invalid manifest/config")
	}
	for _, file := range manifest.Files {
		sum := sha256.Sum256(members[file.Name])
		if file.Size != int64(len(members[file.Name])) || file.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatal("checksum mismatch")
		}
	}
	if bytes.Contains(members["database.sql"], []byte("beacon_backup]")) {
		t.Fatal("connection settings leaked into bundle")
	}
	restore := exec.CommandContext(ctx, "psql", "-X", "--set=ON_ERROR_STOP=on", "--single-transaction")
	restore.Env = append(os.Environ(), "PGDATABASE="+targetName)
	restore.Stdin = bytes.NewReader(members["database.sql"])
	if err := restore.Run(); err != nil {
		t.Fatal("cannot restore downloaded SQL into empty fixture")
	}
	targetCfg := admin.Config().Copy()
	targetCfg.Database = targetName
	target, err := pgx.ConnectConfig(ctx, targetCfg)
	if err != nil {
		t.Fatal("cannot connect to restored fixture")
	}
	defer target.Close(context.Background())
	const fingerprint = `SELECT md5(string_agg(table_name||column_name||data_type, ',' ORDER BY table_name,ordinal_position)) FROM information_schema.columns WHERE table_schema='public'`
	var before, after, message string
	if pool.QueryRow(ctx, fingerprint).Scan(&before) != nil || target.QueryRow(ctx, fingerprint).Scan(&after) != nil || before != after {
		t.Fatal("schema differs after restore")
	}
	if target.QueryRow(ctx, "SELECT message FROM download_fixture").Scan(&message) != nil || message != "café 雪" {
		t.Fatal("wrong database or changed data")
	}
	var nextID int
	if target.QueryRow(ctx, "INSERT INTO download_fixture(message) VALUES ('next') RETURNING id").Scan(&nextID) != nil || nextID != 2 {
		t.Fatal("identity sequence changed")
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "beacon-download-*"))
	if len(paths) != 0 {
		t.Fatal("private HTTP staging retained")
	}
	t.Log("HTTP bundle restored current Beacon schema and Unicode data; native client ignored ambient PGDATABASE")
}
