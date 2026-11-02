// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"bytes"
	"context"
	"os"
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
	// standard_conforming_strings is on: a backslash inside a plain literal is text.
	if err := load(base, "SELECT '\\x01'::bytea, 'a\\b'", testSQLC); err != nil {
		t.Fatalf("backslash inside a plain literal: %v", err)
	}
	const multi, esc, meta = "more than one statement", "escape string", "meta-command"
	bad := []struct {
		name string
		err  error
		want string
	}{
		{"file statement", load(base, "SELECT 1; COMMIT", testSQLC), multi},
		{"sqlc statement", load(base, "SELECT 1", map[string]string{"Q": "-- name: Q :one\nSELECT 1; COMMIT; DELETE FROM nodes"}), multi},
		{"resolver", load(strings.Replace(base, "LIMIT 1", "LIMIT 1; COMMIT", 1), "SELECT 1", testSQLC), multi},
		{"inline sql", load(strings.Replace(base, `{sql: "SELECT 1"}`, `{sql: "SELECT 1; COMMIT"}`, 1), "SELECT 1", testSQLC), multi},
		{"file escape string", load(base, "SELECT E'\\';'", testSQLC), esc},
		{"file psql meta-command", load(base, "SELECT 1\n\\! id", testSQLC), meta},
		{"sqlc escape string", load(base, "SELECT 1", map[string]string{"Q": "-- name: Q :one\nSELECT e'\\';' FROM nodes"}), esc},
		{"resolver meta-command", load(strings.Replace(base, "LIMIT 1", "LIMIT 1 \\! id", 1), "SELECT 1", testSQLC), meta},
		{"raw statement", load(strings.Replace(base, `{sql: "SELECT 1"}`, `{raw: "1; DROP TABLE nodes"}`, 1), "SELECT 1", testSQLC), "raw"},
		{"raw meta-command", load(strings.Replace(base, `{sql: "SELECT 1"}`, `{raw: "'x'\n\\! id"}`, 1), "SELECT 1", testSQLC), "raw"},
		{"raw literal meta-command", load(strings.Replace(base, `{sql: "SELECT 1"}`, `{raw: "'x\n\\! id'"}`, 1), "SELECT 1", testSQLC), "raw"},
		{"settings value", load("tiers: {local: {settings: {work_mem: \"1; COMMIT\"}}}\n"+base, "SELECT 1", testSQLC), "work_mem"},
		{"dollar quote backslash", load(base, "SELECT $q$ \\! id $q$", testSQLC), "dollar quote"},
		{"inline sql escape string", load(strings.Replace(base, `{sql: "SELECT 1"}`, `{sql: "SELECT E'x'"}`, 1), "SELECT 1", testSQLC), esc},
	}
	for _, b := range bad {
		if b.err == nil || !strings.Contains(b.err.Error(), b.want) {
			t.Errorf("%s: err %v, want %q", b.name, b.err, b.want)
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

// A settings value is checked again when a script is emitted, so a corpus changed after load cannot carry a ';'.
func TestEmitRefusesSettingsValue(t *testing.T) {
	c := loadEmitCorpus(t)
	c.Tiers["prod"] = planguard.TierSettings{Settings: map[string]string{"work_mem": "16MB; COMMIT"}}
	var b bytes.Buffer
	v := "\\x01"
	err := planguard.EmitPlans(&b, c, planguard.RunOptions{Tier: "prod"}, map[string]*string{"top": &v}, "0123456789abcdef")
	if err == nil || !strings.Contains(err.Error(), "work_mem") {
		t.Errorf("EmitPlans with a settings value holding ';': err %v", err)
	}
	if err := planguard.EmitResolvers(&b, c, planguard.RunOptions{Tier: "prod"}, "0123456789abcdef"); err == nil {
		t.Error("EmitResolvers with a settings value holding ';': no error")
	}
}

// The scanner agrees with PostgreSQL on high-bit bytes in identifiers and dollar-quote tags: what it passes runs as
// one statement, and what it refuses for ';' really is two.
func TestStatementScannerMatchesPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx := context.Background()
	pc, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close(ctx)
	for sql, statements := range map[string]int{
		"SELECT $é$ ; $é$ AS v":               1,
		"SELECT 1 AS aé$x$":                   1,
		"SELECT 1 AS aé$x$; SELECT 2 AS b$x$": 2,
		"SELECT ';' AS a, $q$;$q$ AS b":       1,
	} {
		res, err := pc.Exec(ctx, sql).ReadAll()
		if err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
		if len(res) != statements {
			t.Errorf("%q: PostgreSQL ran %d statements, want %d", sql, len(res), statements)
		}
		_, loadErr := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": "statements:\n  F: {file: f.sql, plan: harness}\nentries:\n  - {id: f, stmt: F}\n", "f.sql": sql}), "c.yaml", testSQLC)
		if refused := loadErr != nil; refused != (statements > 1) {
			t.Errorf("%q: %d statements in PostgreSQL, but the guard refused=%v (%v)", sql, statements, refused, loadErr)
		}
	}
}

func TestRunnerRefusesMetaCommands(t *testing.T) {
	for name, text := range map[string]string{
		"meta-command":  "-- name: Q :one\nSELECT 1\n\\! id",
		"escape string": "-- name: Q :one\nSELECT E'\\';' FROM nodes",
	} {
		c := loadEmitCorpus(t)
		c.Statements["Q"].SQL = text
		s := &recordSession{}
		rep, err := planguard.Run(context.Background(), c, s, planguard.RunOptions{
			Tier: "local", Baseline: planguard.BaselineOff, Only: regexp.MustCompile(`^q\.lit$`)})
		if err != nil {
			t.Fatal(err)
		}
		for _, sql := range s.sent {
			if strings.Contains(sql, "PREPARE") {
				t.Errorf("%s: sent %q", name, sql)
			}
		}
		if len(rep.Results) == 0 || rep.Results[0].Verdict != planguard.VerdictError {
			t.Errorf("%s: results %+v, want ERROR", name, rep.Results)
		}
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

	for _, r := range []string{"SELECT E'\\';'", "SELECT 1\n\\! id"} {
		c := loadEmitCorpus(t)
		c.Resolvers["top"] = r
		b.Reset()
		if err := planguard.EmitResolvers(&b, c, planguard.RunOptions{Tier: "prod"}, nonce); err == nil {
			t.Errorf("EmitResolvers with resolver %q: no error", r)
		}
	}
	// A line of an emitted body that starts with a backslash is refused even inside a literal.
	c = loadEmitCorpus(t)
	b.Reset()
	line := "x\n\\! id"
	if err := planguard.EmitPlans(&b, c, planguard.RunOptions{Tier: "prod"}, map[string]*string{"top": &line}, nonce); err == nil ||
		!strings.Contains(err.Error(), "backslash") {
		t.Errorf("EmitPlans with a param line starting with a backslash: err %v", err)
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

// Every statement is checked again once rendered: a raw param changed after load cannot carry a second statement
// into the runner's session or an emitted script.
func TestRenderedStatementsAreChecked(t *testing.T) {
	c := loadEmitCorpus(t)
	c.Entry("q.lit").Params = []any{map[string]any{"raw": "1); COMMIT; SELECT (1"}}
	s := &recordSession{}
	rep, err := planguard.Run(context.Background(), c, s, planguard.RunOptions{
		Tier: "local", Baseline: planguard.BaselineOff, Only: regexp.MustCompile(`^q\.lit$`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range s.sent {
		if commitRe.MatchString(sql) || strings.Contains(sql, "PREPARE") {
			t.Errorf("sent %q", sql)
		}
	}
	if len(rep.Results) == 0 || rep.Results[0].Verdict != planguard.VerdictError ||
		!strings.Contains(rep.Results[0].Failures[0].Msg, "rendered statement") {
		t.Errorf("results %+v, want ERROR (rendered statement)", rep.Results)
	}
	var b bytes.Buffer
	if err := planguard.EmitPlans(&b, c, planguard.RunOptions{Tier: "prod", Only: regexp.MustCompile(`^q\.lit$`)}, map[string]*string{}, "0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
	if commitRe.MatchString(b.String()) {
		t.Errorf("EmitPlans wrote the second statement:\n%s", b.String())
	}
}
