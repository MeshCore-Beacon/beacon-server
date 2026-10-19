// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

var testSQLC = map[string]string{
	"Q": "-- name: Q :one\nSELECT * FROM nodes WHERE public_key = $1",
	"C": "-- name: C :many\nSELECT 1",
	"M": "-- name: M :exec\nDELETE FROM packets WHERE id < $1",
	"I": "-- name: I :exec\nINSERT INTO nodes (public_key) VALUES ($1)",
	"L": "-- name: L :many\nSELECT 2",
}

func TestCorpusRejectsUnknownFields(t *testing.T) {
	cases := map[string]string{
		"unknown_top.yaml":            "unexpected_top",
		"unknown_entry.yaml":          "unexpected_entry_key",
		"allow_sort_above_limit.yaml": "allow_sort_above_limit",
	}
	for f, key := range cases {
		_, err := planguard.LoadCorpus(os.DirFS("testdata/corpus"), f, map[string]string{"Q": "-- name: Q :many\nSELECT 1"})
		if err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("%s: err = %v, want one naming %q", f, err, key)
		}
	}
}

func TestCorpusValidation(t *testing.T) {
	cases := []struct {
		file string
		want []string // every fragment must appear in the error
	}{
		{"bad_duplicate_id.yaml", []string{"duplicate entry id", `"a"`}},
		{"bad_unknown_stmt.yaml", []string{`entry "a"`, `unknown stmt "Nope"`}},
		{"bad_unknown_inherit.yaml", []string{`entry "a"`, `unknown entry "nope"`}},
		{"bad_inherit_cycle.yaml", []string{"inherit cycle"}},
		{"bad_sort_exempt.yaml", []string{`entry "a"`, `"no_such_set"`}},
		{"bad_default_sort_exempt.yaml", []string{"defaults", `"no_default_set"`}},
		{"bad_index_cond_quote.yaml", []string{`entry "a"`, "literal", "iata = 'SEA'"}},
		{"bad_index_cond_digit.yaml", []string{`entry "a"`, "literal", "hop_count = 3"}},
		{"bad_index_cond_param.yaml", []string{`entry "a"`, "parameter", "public_key = $1"}},
		{"bad_scan_pattern_literal.yaml", []string{`sort_exempt_sets "pinned"`, "literal"}},
		{"bad_resolver_schema.yaml", []string{`resolver "hop_case"`, "t_fix_routes_hop."}},
		{"bad_cached_no_admission.yaml", []string{`statement "G"`, "admission"}},
		{"bad_cached_unknown_profile.yaml", []string{`statement "G"`, `"region_page"`}},
		{"bad_unknown_mode.yaml", []string{`entry "a"`, `mode "forced"`}},
		{"bad_unknown_mode_override.yaml", []string{`entry "a"`, `mode "plain"`}},
		{"bad_missing_sqlc.yaml", []string{`statement "Gone"`, "sqlc"}},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			_, err := planguard.LoadCorpus(os.DirFS("testdata/corpus"), tc.file, testSQLC)
			if err == nil {
				t.Fatal("want an error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Fatalf("err = %v, want it to contain %q", err, w)
				}
			}
		})
	}

	c, err := planguard.LoadCorpus(os.DirFS("testdata/corpus"), "good.yaml", testSQLC)
	if err != nil {
		t.Fatalf("good.yaml: %v", err)
	}
	if got := c.Defaults.Forbid[0].Relations; !reflect.DeepEqual(got, []string{"packets", "packet_observations"}) {
		t.Errorf("defaults forbid relations = %v, want big expanded", got)
	}
	hit := c.Entry("q.hit")
	if hit == nil {
		t.Fatal("q.hit missing")
	}
	if got := hit.Expect.Forbid[0].Relations; !reflect.DeepEqual(got, []string{"packets", "packet_observations", "nodes"}) {
		t.Errorf("entry forbid relations = %v, want big expanded in place", got)
	}
	if c.Entry("nope") != nil {
		t.Error("Entry(nope) != nil")
	}

	miss := c.Entry("q.miss")
	if !reflect.DeepEqual(miss.Tiers, hit.Tiers) || !reflect.DeepEqual(miss.Expect, hit.Expect) ||
		miss.Mode["generic"] == nil || miss.Mode["generic"].DegradeTo.Budget.Buffers != 20 {
		t.Errorf("inherit did not copy tiers/expect/mode: %+v", miss)
	}
	if miss.Shape != "miss" || !reflect.DeepEqual(miss.Shapes, []string{"miss"}) || miss.Params != nil {
		t.Errorf("inherit overwrote the entry's own fields: %+v", miss)
	}
	own := c.Entry("q.miss_own")
	if !reflect.DeepEqual(own.Tiers, []string{"prod"}) || own.Expect.Budget.Buffers != 3 ||
		own.Expect.Budget.PlanningMs != 5 || len(own.Expect.Driver) != 1 {
		t.Errorf("inherit must keep set fields and fill only empty ones: %+v", own)
	}
	if got := c.ModesOf(own); !reflect.DeepEqual(got, []string{"custom"}) {
		t.Errorf("ModesOf(q.miss_own) = %v, want its own modes", got)
	}

	modes := map[string][]string{
		"q.hit":   {"custom", "generic", "auto"}, // cached
		"c.one":   {"custom"},                    // custom
		"c.modes": {"custom", "generic"},         // entry override
		"m.one":   {"custom"},                    // maint
		"i.one":   {"auto"},                      // ingest
		"l.one":   {"custom", "generic", "auto"}, // legacy
		"h.one":   {"custom", "generic", "auto"}, // harness
		"p.one":   {"custom"},                    // pending custom
	}
	for id, want := range modes {
		if got := c.ModesOf(c.Entry(id)); !reflect.DeepEqual(got, want) {
			t.Errorf("ModesOf(%s) = %v, want %v", id, got, want)
		}
	}

	if got := c.Statements["Q"].SQL; got != testSQLC["Q"] {
		t.Errorf("Q SQL = %q", got)
	}
	if got := c.Statements["H"].SQL; !strings.Contains(got, "SELECT pg_sleep($1)") {
		t.Errorf("H SQL = %q, want the file text", got)
	}
	if got := c.Statements["P"].SQL; got != "" {
		t.Errorf("pending statement without text got SQL %q", got)
	}
	if c.Statements["Q"].Kind != "select" || c.Statements["M"].Kind != "dml" {
		t.Errorf("kinds = %q, %q", c.Statements["Q"].Kind, c.Statements["M"].Kind)
	}
	if c.Tiers["fixture"].Settings["random_page_cost"] != "1.1" || c.Groups["routes"].SearchPath["local"] != "t_s2b, public" {
		t.Errorf("tiers/groups not decoded: %+v %+v", c.Tiers, c.Groups)
	}
}

func TestDerivedStatementsMatchSource(t *testing.T) {
	sqlcText, err := planguard.LoadStatements("../../db/sqlc")
	if err != nil {
		t.Fatal(err)
	}
	c, err := planguard.LoadCorpus(os.DirFS("testdata/corpus"), "derived.yaml", sqlcText)
	if err != nil {
		t.Fatalf("derived.yaml: %v", err)
	}
	n := 0
	for name, st := range c.Statements {
		if st.DerivedFrom == "" {
			continue
		}
		n++
		src := sqlcText[st.DerivedFrom]
		if !planguard.DerivedBodyMatches(src, st.SQL) {
			t.Errorf("%s: body is not part of %s", name, st.DerivedFrom)
		}
		// One edited token must break the match.
		edited := strings.Replace(st.SQL, "LIMIT", "OFFSET", 1)
		if edited == st.SQL || planguard.DerivedBodyMatches(src, edited) {
			t.Errorf("%s: an edited body still matches", name)
		}
	}
	if n == 0 {
		t.Fatal("no derived statement checked")
	}

	_, err = planguard.LoadCorpus(os.DirFS("testdata/corpus"), "bad_derived_edited.yaml", sqlcText)
	if err == nil || !strings.Contains(err.Error(), `statement "DeleteExpiredRoutesSelect"`) ||
		!strings.Contains(err.Error(), "DeleteExpiredRoutes") {
		t.Fatalf("edited derived file: err = %v, want one naming the statement and its source", err)
	}
}

func TestStripLockClause(t *testing.T) {
	for in, want := range map[string]string{
		"LIMIT $5\n  FOR UPDATE OF r SKIP LOCKED),": "LIMIT $5),",
		"LIMIT 1 FOR UPDATE SKIP LOCKED":            "LIMIT 1",
		"LIMIT 1 for update of a, b skip locked":    "LIMIT 1",
		"LIMIT 1 FOR UPDATE":                        "LIMIT 1 FOR UPDATE",
	} {
		if got := planguard.StripLockClause(in); got != want {
			t.Errorf("StripLockClause(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDerivedBodyNeedsTokenBoundaries(t *testing.T) {
	src := "-- name: X :one\nWITH c AS (SELECT id FROM t ORDER BY id LIMIT $5 OFFSET $6 FOR UPDATE SKIP LOCKED) SELECT count(*) FROM c"
	if planguard.DerivedBodyMatches(src, "SELECT id FROM t ORDER BY id LIMIT $1") {
		t.Error("a body that stops before OFFSET matched")
	}
	if !planguard.DerivedBodyMatches(src, "SELECT id FROM t ORDER BY id LIMIT $1 OFFSET $2") {
		t.Error("the whole CTE body did not match")
	}
	if planguard.DerivedBodyMatches(src, "ELECT id FROM t ORDER BY id LIMIT $1 OFFSET $2") {
		t.Error("a body starting mid-token matched")
	}
	if !planguard.DerivedBodyMatches("SELECT a FROM t;", "SELECT a FROM t") {
		t.Error("a whole statement did not match itself")
	}
}

func TestCorpusResolversReadBeaconTablesOnly(t *testing.T) {
	_, err := planguard.LoadCorpus(os.DirFS("testdata/corpus"), "bad_resolver_schema.yaml", testSQLC)
	if err == nil || !strings.Contains(err.Error(), "hop_case") {
		t.Fatalf("err = %v, want one naming resolver hop_case", err)
	}
}
