// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"os/exec"
	"path"
	"reflect"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

func TestLoadStatementsParsesSqlc(t *testing.T) {
	m, err := planguard.LoadStatements("../../db/sqlc")
	if err != nil {
		t.Fatalf("LoadStatements: %v", err)
	}
	if len(m) < 190 {
		t.Fatalf("got %d statements, want at least 190", len(m))
	}
	got := m["ListKnownRoutesByIATA"]
	if !strings.HasPrefix(got, "-- name: ListKnownRoutesByIATA :many\n") {
		t.Fatalf("ListKnownRoutesByIATA text does not start with its header: %q", firstLine(got))
	}
	if !strings.Contains(got, "LIMIT $4") {
		t.Fatalf("ListKnownRoutesByIATA text lacks LIMIT $4")
	}
	for name, text := range m {
		if name == "" {
			t.Fatalf("empty statement name for text %q", firstLine(text))
		}
		if !strings.HasPrefix(text, "-- name: "+name+" :") {
			t.Fatalf("%s: text header does not name it: %q", name, firstLine(text))
		}
	}

	fromFS, err := planguard.LoadStatementsFS(db.SQLCSources, "sqlc/*.sql.go")
	if err != nil {
		t.Fatalf("LoadStatementsFS: %v", err)
	}
	if !reflect.DeepEqual(fromFS, m) {
		t.Fatalf("embedded statements differ from the directory: %d vs %d", len(fromFS), len(m))
	}
}

func TestLoadStatementsRejectsDuplicatesAndEmpty(t *testing.T) {
	dup := map[string]string{
		"a.sql.go": "package db\n\nconst a = `-- name: Same :one\nSELECT 1`\n",
		"b.sql.go": "package db\n\nconst b = `-- name: Same :one\nSELECT 2`\n",
	}
	if _, err := planguard.LoadStatementsFS(mapFS(dup), "*.sql.go"); err == nil || !strings.Contains(err.Error(), "Same") {
		t.Fatalf("duplicate name: err = %v, want one naming Same", err)
	}
	none := map[string]string{"a.sql.go": "package db\n\nconst a = \"SELECT 1\"\n"}
	if _, err := planguard.LoadStatementsFS(mapFS(none), "*.sql.go"); err == nil {
		t.Fatal("no statement: want an error")
	}
	broken := map[string]string{"a.sql.go": "package db\n\nconst a = `-- name: A :one\n"}
	if _, err := planguard.LoadStatementsFS(mapFS(broken), "*.sql.go"); err == nil || !strings.Contains(err.Error(), "a.sql.go") {
		t.Fatalf("parse error: err = %v, want one naming a.sql.go", err)
	}
}

func TestLoadStatementsAtRef(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	if err := exec.Command("git", "-C", "../..", "rev-parse", "--verify", "HEAD").Run(); err != nil {
		t.Skip("not a git checkout")
	}
	atHead, err := planguard.LoadStatementsAtRef("../..", "HEAD")
	if err != nil {
		t.Fatalf("LoadStatementsAtRef: %v", err)
	}
	out, err := exec.Command("git", "-C", "../..", "diff", "--name-only", "HEAD", "--", "db/sqlc").Output()
	if err != nil {
		t.Fatalf("git diff: %v", err)
	}
	changed := map[string]bool{}
	for _, f := range strings.Fields(string(out)) {
		changed[path.Base(f)] = true
	}
	for file := range changed {
		t.Logf("skipping %s: uncommitted change", file)
	}
	compared := 0
	for _, file := range sqlcFiles(t) {
		if changed[file] {
			continue
		}
		one, err := planguard.LoadStatementsFS(dirFS("../../db/sqlc"), file)
		if err != nil {
			if strings.Contains(err.Error(), "no statement") {
				continue
			}
			t.Fatalf("%s: %v", file, err)
		}
		for name, text := range one {
			if atHead[name] != text {
				t.Fatalf("%s: text at HEAD differs from the working tree", name)
			}
			compared++
		}
	}
	if compared < 190 {
		t.Fatalf("compared %d statements, want at least 190", compared)
	}

	if _, err := planguard.LoadStatementsAtRef("../..", "no-such-ref-xyz"); err == nil {
		t.Fatal("bad ref: want an error")
	}
	if _, err := planguard.LoadStatementsAtRef("../..", "--output=/tmp/x"); err == nil {
		t.Fatal("option-like ref: want an error")
	}
}

func TestNormalizeRenumbers(t *testing.T) {
	got := planguard.Normalize("-- name: X :one\nSELECT $2::int,\n  $10 ;")
	if want := "SELECT $?::int, $?"; got != want {
		t.Fatalf("Normalize = %q, want %q", got, want)
	}
	if a, b := planguard.Normalize("SELECT $1 -- trailing\nFROM t;;"), planguard.Normalize("SELECT  $7\n\tFROM t"); a != b {
		t.Fatalf("renumbered texts differ: %q vs %q", a, b)
	}
	// A "--" inside a string literal is data, not a comment.
	if got := planguard.Normalize("SELECT '--x' AS y"); got != "SELECT '--x' AS y" {
		t.Fatalf("Normalize stripped a literal: %q", got)
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
