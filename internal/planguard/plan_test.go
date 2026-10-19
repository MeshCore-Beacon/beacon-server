// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

// loadPlan reads a trimmed EXPLAIN (FORMAT JSON) file from testdata/plans.
func loadPlan(t *testing.T, name string) *planguard.Explain {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "plans", name))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := planguard.ParseExplain(b)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return ex
}

func TestParseExplainRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "[]", "{", `[{"Plan": 1}]`, `[{}]`} {
		if _, err := planguard.ParseExplain([]byte(in)); err == nil {
			t.Errorf("ParseExplain(%q) = nil error, want one", in)
		}
	}
}

func TestParseExplainReadsTimesAndBuffers(t *testing.T) {
	ex := loadPlan(t, "globalhop_rare_retired.json")
	if !ex.Analyzed {
		t.Error("Analyzed = false for an ANALYZE plan")
	}
	if ex.PlanBuffers != 4 {
		t.Errorf("PlanBuffers = %d, want 4", ex.PlanBuffers)
	}
	if ex.PlanningMs <= 0 || ex.ExecMs <= 0 {
		t.Errorf("PlanningMs = %v, ExecMs = %v; want both > 0", ex.PlanningMs, ex.ExecMs)
	}
	if got := ex.Root["Node Type"]; got != "Limit" {
		t.Errorf("root = %v, want Limit", got)
	}
}

func TestSignatureIgnoresCostsAndValues(t *testing.T) {
	// The same shape at different costs, row counts and literals ('SEA' vs 'BWE', 51 vs 201 rows).
	a := loadPlan(t, "routes_byiata_custom.json")
	b := loadPlan(t, "routes_byiata_mid.json")
	if a.Root["Total Cost"] == b.Root["Total Cost"] {
		t.Fatal("fixture plans have equal costs; the test would prove nothing")
	}
	sa, sb := planguard.Signature(a.Root), planguard.Signature(b.Root)
	if !reflect.DeepEqual(sa, sb) {
		t.Fatalf("signatures differ:\n%s\n---\n%s", strings.Join(sa, "\n"), strings.Join(sb, "\n"))
	}
	want := []string{"Limit", "  Index Scan on known_routes using idx_known_routes_iata_keyset"}
	if !reflect.DeepEqual(sa, want) {
		t.Errorf("Signature = %q, want %q", sa, want)
	}

	c := loadPlan(t, "routes_byiata_mid.json")
	kid := c.Root["Plans"].([]any)[0].(map[string]any)
	kid["Index Name"] = "idx_known_routes_iata"
	if reflect.DeepEqual(planguard.Signature(c.Root), sa) {
		t.Error("a changed index left the signature unchanged")
	}
}

func TestSignatureKeepsAliasJoinAndSubplanShape(t *testing.T) {
	sig := strings.Join(planguard.Signature(loadPlan(t, "bysites_ok.json").Root), "\n")
	for _, want := range []string{
		"  Limit [InitPlan]",
		"Index Only Scan on packet_sites ps using idx_packet_sites_site",
		"Index Only Scan on packet_sites x using packet_sites_pkey [SubPlan]",
		"Aggregate {Sorted}",
	} {
		if !strings.Contains(sig, want) {
			t.Errorf("signature lacks %q:\n%s", want, sig)
		}
	}
	for _, banned := range []string{"Cost", "Rows", "2026-10-18", "iata ="} {
		if strings.Contains(sig, banned) {
			t.Errorf("signature holds %q:\n%s", banned, sig)
		}
	}
}
