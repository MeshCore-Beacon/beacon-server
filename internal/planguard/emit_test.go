// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

const emitCorpus = `
defaults: {big_tables: [nodes]}
tiers: {prod: {}}
statements:
  Q: {sqlc: Q, plan: custom}
  P: {sqlc: NotMergedYet, plan: custom, pending: some_feature}
resolvers:
  top: SELECT public_key FROM nodes ORDER BY last_seen DESC LIMIT 1
  gone: SELECT NULL::text
  pend: SELECT iata FROM a_table_of_the_pending_feature LIMIT 1
entries:
  - {id: q.top, stmt: Q, params: [{r: top}], modes: [custom, generic]}
  - {id: q.gone, stmt: Q, params: [{r: gone}]}
  - {id: q.lit, stmt: Q, params: ["it's"]}
  - {id: p.one, stmt: P}
  - {id: p.res, stmt: P, params: [{r: pend}]}
`

func loadEmitCorpus(t *testing.T) *planguard.Corpus {
	t.Helper()
	c, err := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": emitCorpus}), "c.yaml", testSQLC)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewNonce(t *testing.T) {
	a, err := planguard.NewNonce()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := planguard.NewNonce()
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(a) || a == b {
		t.Errorf("nonces %q %q, want two different 16-hex strings", a, b)
	}
}

// A pending entry skips, so its resolvers are not emitted (or run): they may read tables that do not exist yet.
func TestEmitResolversLeavesOutPending(t *testing.T) {
	c := loadEmitCorpus(t)
	const nonce = "0123456789abcdef"
	for _, include := range []bool{false, true} {
		var rs bytes.Buffer
		if err := planguard.EmitResolvers(&rs, c, planguard.RunOptions{Tier: "prod", IncludePending: include}, nonce); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(rs.String(), `\echo @@`+nonce+` R pend`); got != include {
			t.Errorf("include pending %v: script has the pending resolver: %v", include, got)
		}
	}
}

func TestEmitScripts(t *testing.T) {
	c := loadEmitCorpus(t)
	const nonce = "0123456789abcdef"
	o := planguard.RunOptions{Tier: "prod"}
	var rs bytes.Buffer
	if err := planguard.EmitResolvers(&rs, c, o, nonce); err != nil {
		t.Fatal(err)
	}
	script := rs.String()
	const head = "SET default_transaction_read_only = on; SET lock_timeout = '1s'; SET statement_timeout = '20s';\n"
	if !strings.HasPrefix(script, head) {
		t.Errorf("resolver script opens with %.120q", script)
	}
	for _, want := range []string{`\echo @@` + nonce + ` R gone`, `\echo @@` + nonce + ` R top`, `\echo @@` + nonce + ` END`} {
		if !strings.Contains(script, want) {
			t.Errorf("resolver script lacks %q:\n%s", want, script)
		}
	}

	top := "abc"
	var ps bytes.Buffer
	if err := planguard.EmitPlans(&ps, c, o, map[string]*string{"top": &top, "gone": nil}, nonce); err != nil {
		t.Fatal(err)
	}
	script = ps.String()
	if !strings.HasPrefix(script, head) {
		t.Errorf("plan script opens with %.120q", script)
	}
	for _, want := range []string{
		`\echo @@` + nonce + ` P q.top|custom`, `\echo @@` + nonce + ` P q.top|generic`, `\echo @@` + nonce + ` P q.lit|custom`,
		"EXPLAIN (FORMAT JSON) EXECUTE planguard_q('abc');", "EXPLAIN (FORMAT JSON) EXECUTE planguard_q('it''s');",
		"BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY;", "SET standard_conforming_strings = on;", "SET LOCAL standard_conforming_strings = on", "SET LOCAL plan_cache_mode = force_generic_plan;",
		"ROLLBACK;\nDEALLOCATE planguard_q;", `\echo @@` + nonce + ` I indexes`, `\echo @@` + nonce + ` END`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("plan script lacks %q:\n%s", want, script)
		}
	}
	for _, not := range []string{"q.gone", "p.one", "ANALYZE"} {
		if strings.Contains(script, not) {
			t.Errorf("plan script holds %q", not)
		}
	}
}

func TestEmitFramingSurvivesHostileText(t *testing.T) {
	const nonce = "00ff00ff00ff00ff"
	const other = "1111111111111111"
	m := "@@" + nonce + " "
	resolverOut := "SET\nSET\nSET\n" + m + "planguard\n" +
		m + "R hostile\nVx\n@@CASE y\n" +
		m + "R missing\nN\n" +
		m + "R empty\nV\n" +
		m + "R trailing\nVa\n\n" +
		m + "END\n"
	res, err := planguard.ParseResolverOutput(strings.NewReader(resolverOut), nonce)
	if err != nil {
		t.Fatal(err)
	}
	if v := res["hostile"]; v == nil || *v != "x\n@@CASE y" {
		t.Errorf("hostile = %q", deref(v))
	}
	if v, ok := res["missing"]; !ok || v != nil {
		t.Errorf("missing = %q (present %v), want NULL", deref(v), ok)
	}
	if v := res["empty"]; v == nil || *v != "" {
		t.Errorf("empty = %q", deref(v))
	}
	if v := res["trailing"]; v == nil || *v != "a\n" {
		t.Errorf("trailing = %q", deref(v))
	}

	plan := `[
  {
    "Plan": {"Node Type": "Index Scan", "Relation Name": "nodes", "Index Name": "nodes_pkey",
             "Index Cond": "(public_key = '@@` + other + ` P a|b'::text)"}
  }
]`
	planOut := m + "planguard\n" + m + "P q.top|custom\n" + plan + "\n" + m + "I indexes\n[\"nodes_pkey\"]\n" + m + "END\n"
	plans, err := planguard.ParsePlanOutput(strings.NewReader(planOut), nonce)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(plans["q.top|custom"]); got != plan {
		t.Errorf("plan = %q, want %q", got, plan)
	}
	if got := string(plans[planguard.IndexesKey]); got != `["nodes_pkey"]` {
		t.Errorf("indexes = %q", got)
	}

	bad := map[string]struct{ out, want string }{
		"truncated json":   {m + "P a|custom\n[{\"Plan\": {\"Node Type\"\n" + m + "END\n", "a|custom"},
		"marker no body":   {m + "P a|custom\n" + m + "P b|custom\n" + plan + "\n" + m + "END\n", "a|custom"},
		"no end marker":    {m + "P b|custom\n" + plan + "\n", "end marker"},
		"unknown kind":     {m + "X b|custom\n" + plan + "\n" + m + "END\n", "X b|custom"},
		"duplicate block":  {m + "P b|custom\n" + plan + "\n" + m + "P b|custom\n" + plan + "\n" + m + "END\n", "b|custom"},
		"key without mode": {m + "P b\n" + plan + "\n" + m + "END\n", `"b"`},
	}
	for name, tc := range bad {
		if _, err := planguard.ParsePlanOutput(strings.NewReader(tc.out), nonce); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want one naming %q", name, err, tc.want)
		}
	}
	badRes := map[string]struct{ out, want string }{
		"marker no body": {m + "R a\n" + m + "R b\nVx\n" + m + "END\n", "resolver a"},
		"bad body":       {m + "R a\nxyz\n" + m + "END\n", "resolver a"},
		"no end marker":  {m + "R a\nVx\n", "end marker"},
	}
	for name, tc := range badRes {
		if _, err := planguard.ParseResolverOutput(strings.NewReader(tc.out), nonce); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("resolvers %s: err = %v, want one naming %q", name, err, tc.want)
		}
	}
	if n, err := planguard.ScriptNonce([]byte(planOut)); err != nil || n != nonce {
		t.Errorf("ScriptNonce = %q, %v", n, err)
	}
	if _, err := planguard.ScriptNonce([]byte("SET\n@@CASE x\n")); err == nil {
		t.Error("ScriptNonce found a nonce in output without a header")
	}
}

func TestCheckPlans(t *testing.T) {
	c := loadEmitCorpus(t)
	top := "abc"
	o := planguard.RunOptions{Tier: "prod", Params: map[string]*string{"top": &top, "gone": nil}}
	plan := []byte(`[{"Plan": {"Node Type": "Index Scan", "Relation Name": "nodes", "Index Name": "nodes_pkey"}}]`)
	rep, err := planguard.CheckPlans(c, o, map[string][]byte{"q.top|custom": plan, "q.top|generic": plan,
		planguard.IndexesKey: []byte(`["nodes_pkey"]`)})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]planguard.Result{}
	for _, r := range rep.Results {
		got[r.ID+"|"+r.Mode] = r
	}
	if r := got["q.top|custom"]; r.Verdict != planguard.VerdictOK || len(r.Signature) == 0 || r.Params[0] != "'abc'" {
		t.Errorf("q.top: %+v", r)
	}
	if r := got["q.gone|custom"]; r.Verdict != planguard.VerdictSkip || r.Skip != "resolver gone returned no value on this tier" {
		t.Errorf("q.gone: %+v", r)
	}
	if r := got["p.one|custom"]; r.Verdict != planguard.VerdictSkip || r.Skip != "pending: some_feature" {
		t.Errorf("p.one: %+v", r)
	}
	if r := got["q.lit|custom"]; r.Verdict != planguard.VerdictError {
		t.Errorf("q.lit with no plan in the output: %+v", r)
	}
	if r := got["p.res|custom"]; r.Verdict != planguard.VerdictSkip || r.Skip != "pending: some_feature" {
		t.Errorf("p.res: %+v", r)
	}
	if len(rep.Results) != 6 || !contains(rep.Indexes, "nodes_pkey") {
		t.Errorf("results %d, indexes %v", len(rep.Results), rep.Indexes)
	}
	if _, err := planguard.CheckPlans(c, o, map[string][]byte{"stranger|custom": plan}); err == nil {
		t.Error("a plan for a run outside the selection was accepted")
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func TestEmitRejectsHostileNames(t *testing.T) {
	hostile := []string{"a`touch /tmp/x`", "a\nb", "../../etc/x", "a/b", "..", "a:b", "a b", ""}
	for _, id := range hostile {
		body := strings.Replace(emitCorpus, "{id: q.lit,", "{id: "+yamlQuote(id)+",", 1)
		if _, err := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": body}), "c.yaml", testSQLC); err == nil {
			t.Errorf("entry id %q loaded", id)
		}
		res := strings.Replace(emitCorpus, "  gone: SELECT", "  "+yamlQuote(id)+": SELECT", 1)
		if _, err := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": res}), "c.yaml", testSQLC); err == nil {
			t.Errorf("resolver name %q loaded", id)
		}
	}

	// A corpus changed after loading is checked again before anything is emitted.
	for _, id := range hostile {
		c := loadEmitCorpus(t)
		c.Entries[2].ID = id
		var b bytes.Buffer
		if err := planguard.EmitPlans(&b, c, planguard.RunOptions{Tier: "prod"}, nil, "0123456789abcdef"); err == nil {
			t.Errorf("EmitPlans accepted entry id %q:\n%s", id, b.String())
		}
		c = loadEmitCorpus(t)
		c.Resolvers[id] = "SELECT 1"
		if err := planguard.EmitResolvers(&b, c, planguard.RunOptions{Tier: "prod"}, "0123456789abcdef"); err == nil {
			t.Errorf("EmitResolvers accepted resolver name %q", id)
		}
	}
}

func TestSearchPathElements(t *testing.T) {
	c := loadEmitCorpus(t)
	for sp, ok := range map[string]bool{
		"": true, "public": true, "t_s2b, public": true, `"My Schema", public`: true,
		`"$user", public`: false, "public; DROP TABLE x": false, "pub$lic": false, "a,,b": false, "'x'": false, `"a"b"`: false,
	} {
		var b bytes.Buffer
		err := planguard.EmitResolvers(&b, c, planguard.RunOptions{Tier: "prod", SearchPath: sp}, "0123456789abcdef")
		if (err == nil) != ok {
			t.Errorf("search_path %q: err = %v, want ok=%v", sp, err, ok)
		}
	}
}

func yamlQuote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}

// A mode's own known_issue turns that mode's failures into KNOWN_ISSUE; other modes still fail, and an error is
// always ERROR.
func TestModeKnownIssue(t *testing.T) {
	const body = `
defaults: {big_tables: [nodes], forbid: [{node: Seq Scan, relations: [big]}]}
tiers: {prod: {}}
statements:
  Q: {sqlc: Q, plan: custom}
entries:
  - {id: q.seq, stmt: Q, params: [x], modes: [custom, generic], mode: {generic: {known_issue: generic plan is known to scan}}}
  - {id: q.err, stmt: Q, params: [y], modes: [generic], mode: {generic: {known_issue: generic plan is known to scan}}}
`
	c, err := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": body}), "c.yaml", testSQLC)
	if err != nil {
		t.Fatal(err)
	}
	seq := []byte(`[{"Plan": {"Node Type": "Seq Scan", "Relation Name": "nodes"}}]`)
	rep, err := planguard.CheckPlans(c, planguard.RunOptions{Tier: "prod", Params: map[string]*string{}},
		map[string][]byte{"q.seq|custom": seq, "q.seq|generic": seq})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]planguard.Verdict{
		"q.seq|custom":  planguard.VerdictFalsePositive,
		"q.seq|generic": planguard.VerdictKnownIssue,
		"q.err|generic": planguard.VerdictError, // no plan in the output
	}
	if len(rep.Results) != 3 {
		t.Fatalf("%d results, want 3", len(rep.Results))
	}
	for _, r := range rep.Results {
		if v := want[r.ID+"|"+r.Mode]; r.Verdict != v {
			t.Errorf("%s|%s: verdict %s, want %s", r.ID, r.Mode, r.Verdict, v)
		}
	}

	bad := strings.Replace(body, "{generic: {known_issue: generic plan is known to scan}}}\n  - {id: q.err",
		"{generic: {skip: no, known_issue: generic plan is known to scan}}}\n  - {id: q.err", 1)
	if _, err := planguard.LoadCorpus(mapFS(map[string]string{"c.yaml": bad}), "c.yaml", testSQLC); err == nil ||
		!strings.Contains(err.Error(), "skip excludes") {
		t.Errorf("skip with known_issue: err = %v", err)
	}
}
