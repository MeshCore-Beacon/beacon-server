// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
)

func TestVerifyCommandIsReadOnlyAndOffline(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir) // no pg_dump, psql or shell helper is available
	t.Setenv("PGDATABASE", "")
	archive := filepath.Join(dir, "private.tar.gz")
	m := backup.Manifest{FormatVersion: 1, CreatedAt: time.Now().UTC(), DatabaseFormat: "postgresql-plain-sql", ToolVersion: "PRIVATE_CANARY"}
	data := map[string][]byte{"database.sql": []byte("SELECT 'PRIVATE_CANARY';"), "config.yaml": {}}
	for _, name := range []string{"database.sql", "config.yaml"} {
		hash := sha256.Sum256(data[name])
		m.Files = append(m.Files, backup.File{Name: name, Size: int64(len(data[name])), SHA256: hex.EncodeToString(hash[:])})
	}
	data["manifest.json"], _ = json.Marshal(m)
	var raw bytes.Buffer
	gz := gzip.NewWriter(&raw)
	tw := tar.NewWriter(gz)
	for _, name := range []string{"manifest.json", "database.sql", "config.yaml"} {
		if err := tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(data[name])), Mode: 0600}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archive, raw.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"-verify", archive}, 0},
		{[]string{"-verify", archive, "-max-bytes", "1"}, 1},
		{[]string{"-verify", archive, "-timeout", "0"}, 2},
		{[]string{"-verify", archive, "-output", filepath.Join(dir, "must-not-exist")}, 2},
		{[]string{"-verify", archive, "-config", "missing.yaml"}, 2},
		{[]string{"-verify", "", "-output", filepath.Join(dir, "must-not-exist")}, 2},
		{[]string{"-verify", dir}, 1},
		{[]string{"-verify", filepath.Join(dir, "PRIVATE_CANARY-missing")}, 1},
		{[]string{"-verify", archive, "unexpected"}, 2},
	} {
		var stdout, stderr bytes.Buffer
		if got := run(context.Background(), tc.args, &stdout, &stderr); got != tc.code || strings.Contains(stdout.String()+stderr.String(), "PRIVATE_CANARY") {
			t.Fatalf("exit %d, want %d; diagnostic %s", got, tc.code, stderr.String())
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 || files[0].Name() != "private.tar.gz" {
		t.Fatal("verification extracted files or created output")
	}
	unchanged, err := os.ReadFile(archive)
	if err != nil || !bytes.Equal(unchanged, raw.Bytes()) {
		t.Fatal("input archive changed")
	}
}
