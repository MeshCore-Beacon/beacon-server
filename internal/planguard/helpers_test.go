// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func mapFS(files map[string]string) fs.FS {
	m := fstest.MapFS{}
	for name, body := range files {
		m[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return m
}

func dirFS(dir string) fs.FS { return os.DirFS(dir) }

func sqlcFiles(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob("../../db/sqlc/*.sql.go")
	if err != nil || len(matches) == 0 {
		t.Fatalf("glob db/sqlc: %v (%d files)", err, len(matches))
	}
	out := make([]string, len(matches))
	for i, m := range matches {
		out[i] = filepath.Base(m)
	}
	return out
}
