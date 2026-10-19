// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/jackc/pgx/v5/pgconn"
)

// Statement texts and resolvers go over the simple protocol and into psql scripts, where a second statement (a
// COMMIT) would run outside the READ ONLY transaction. Text with a ';' outside literals and comments is refused at
// load, and again by the runner and the emitter.
func TestCorpusRefusesMultiStatement(t *testing.T) {
	const base = `
statements:
  Q: {sqlc: Q, plan: custom}
  F: {file: f.sql, plan: harness}
resolvers:
  top: SELECT public_key FROM nodes LIMIT 1
entries:
  - {id: q, stmt: Q, params: [{r: top}]}
  - {id: f, stmt: F, params: [{sql: "SELECT 1"}]}
`
	load := func(yaml, file string, sqlc map[string]string) error {
		_, err := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": yaml, "f.sql": file}), "c.yaml", sqlc)
		return err
	}
	if err := load(base, "SELECT ';' AS a, $$;$$ AS b -- ;\n;\n", testSQLC); err != nil {
		t.Fatalf("';' in literals and comments, and a trailing ';': %v", err)
	}
	bad := map[string]error{
		"file statement": load(base, "SELECT 1; COMMIT", testSQLC),
		"sqlc statement": load(base, "SELECT 1", map[string]string{"Q": "-- name: Q :one\nSELECT 1; COMMIT; DELETE FROM nodes"}),
		"resolver":       load(strings.Replace(base, "LIMIT 1", "LIMIT 1; COMMIT", 1), "SELECT 1", testSQLC),
		"inline sql":     load(strings.Replace(base, `{sql: "SELECT 1"}`, `{sql: "SELECT 1; COMMIT"}`, 1), "SELECT 1", testSQLC),
	}
	for name, err := range bad {
		if err == nil || !strings.Contains(err.Error(), "more than one statement") {
			t.Errorf("%s: err %v, want more than one statement", name, err)
		}
	}
}

var commitRe = regexp.MustCompile(`\bCOMMIT\b`)

// recordSession answers every command with an empty result and records it.
type recordSession struct{ sent []string }

func (r *recordSession) Exec(_ context.Context, sql string) ([]*pgconn.Result, error) {
	r.sent = append(r.sent, sql)
	return []*pgconn.Result{{}}, nil
}

func TestRunnerRefusesMultiStatement(t *testing.T) {
	c := loadEmitCorpus(t)
	c.Statements["Q"].SQL = "-- name: Q :one\nSELECT 1; COMMIT; DELETE FROM nodes"
	c.Resolvers["top"] = "SELECT 1; COMMIT"
	s := &recordSession{}
	rep, err := planguard.Run(context.Background(), c, s, planguard.RunOptions{
		Tier: "local", Baseline: planguard.BaselineOff, Only: regexp.MustCompile(`^q\.(top|lit)$`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range s.sent {
		if commitRe.MatchString(sql) || strings.Contains(sql, "DELETE") {
			t.Errorf("sent %q", sql)
		}
	}
	n := 0
	for _, r := range rep.Results {
		if r.Stmt != "Q" {
			continue
		}
		n++
		if r.Verdict != planguard.VerdictError || len(r.Failures) == 0 || !strings.Contains(r.Failures[0].Msg, "more than one statement") {
			t.Errorf("%s|%s: verdict %s failures %v, want ERROR (more than one statement)", r.ID, r.Mode, r.Verdict, r.Failures)
		}
	}
	if n == 0 {
		t.Fatal("no run of Q")
	}
}

func TestEmitRefusesMultiStatement(t *testing.T) {
	const nonce = "0123456789abcdef"
	c := loadEmitCorpus(t)
	c.Resolvers["top"] = "SELECT 1; COMMIT"
	var b bytes.Buffer
	if err := planguard.EmitResolvers(&b, c, planguard.RunOptions{Tier: "prod"}, nonce); err == nil || !strings.Contains(err.Error(), "more than one statement") {
		t.Errorf("EmitResolvers: err %v, want more than one statement", err)
	}

	c = loadEmitCorpus(t)
	c.Statements["Q"].SQL = "-- name: Q :one\nSELECT 1; DELETE FROM nodes"
	b.Reset()
	params := map[string]*string{"top": nil, "gone": nil}
	v := "\\x01"
	params["top"] = &v
	if err := planguard.EmitPlans(&b, c, planguard.RunOptions{Tier: "prod"}, params, nonce); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "DELETE") {
		t.Errorf("EmitPlans wrote the second statement:\n%s", b.String())
	}
	if strings.Contains(b.String(), "PREPARE") {
		t.Errorf("EmitPlans prepared a refused statement:\n%s", b.String())
	}
}
