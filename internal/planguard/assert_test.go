// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

var bigTables = []string{"known_routes", "packet_observations", "packets", "packet_sites"}

func kinds(fs []planguard.Failure) []planguard.FailureKind {
	out := []planguard.FailureKind{}
	for _, f := range fs {
		out = append(out, f.Kind)
	}
	return out
}

func wantKinds(t *testing.T, ch planguard.Check, want ...planguard.FailureKind) {
	t.Helper()
	if want == nil {
		want = []planguard.FailureKind{}
	}
	if got := kinds(ch.Failures); !reflect.DeepEqual(got, want) {
		t.Fatalf("failure kinds = %v, want %v; failures: %+v", got, want, ch.Failures)
	}
}

func TestRetiredIndexNeverHidesBudgetFailure(t *testing.T) {
	retired := []string{"idx_known_routes_hop_count"}
	c := &planguard.Corpus{Defaults: planguard.Defaults{BigTables: bigTables, RetiredIndexes: retired}}
	ex := loadPlan(t, "globalhop_rare_retired.json")
	exp := &planguard.Expect{
		Driver: []planguard.DriverSpec{{Relation: "known_routes", Index: "idx_known_routes_hop_iata"}},
		Budget: planguard.Budget{Buffers: 1500},
	}

	ch := c.Assess(ex, exp, true)
	wantKinds(t, ch, planguard.FailDriver, planguard.FailBudget)
	if ch.Buffers != 8902 {
		t.Errorf("Buffers = %d, want 8902", ch.Buffers)
	}
	if !reflect.DeepEqual(ch.Failures[0].Indexes, retired) {
		t.Errorf("driver failure indexes = %v, want %v", ch.Failures[0].Indexes, retired)
	}
	if len(ch.Failures[1].Indexes) != 0 {
		t.Errorf("budget failure names indexes %v", ch.Failures[1].Indexes)
	}
	if !reflect.DeepEqual(ch.Retired, retired) {
		t.Errorf("Retired = %v, want %v", ch.Retired, retired)
	}
	if v := planguard.Decide(false, "", ch.Failures, ch.Retired); v != planguard.VerdictFalsePositive {
		t.Errorf("driver + budget: verdict %s, want FALSE_POSITIVE", v)
	}

	exp.Budget.Buffers = 10000
	ch = c.Assess(ex, exp, true)
	wantKinds(t, ch, planguard.FailDriver)
	if v := planguard.Decide(false, "", ch.Failures, ch.Retired); v != planguard.VerdictRetiredIndex {
		t.Errorf("driver on a retired index only: verdict %s, want RETIRED_INDEX", v)
	}

	// A forbidden Seq Scan next to the retired driver: the real channel_messages Seq Scan from another plan,
	// grafted beside the driver.
	seq := findNode(loadPlan(t, "cmi_seqscan.json").Root, "Seq Scan")
	if seq == nil {
		t.Fatal("cmi_seqscan.json has no Seq Scan")
	}
	ex.Root["Plans"] = append(ex.Root["Plans"].([]any), map[string]any(seq))
	exp.Forbid = []planguard.NodeRule{{Node: "Seq Scan", Relations: []string{"channel_messages"}}}
	ch = c.Assess(ex, exp, true)
	wantKinds(t, ch, planguard.FailDriver, planguard.FailForbidden)
	if v := planguard.Decide(false, "", ch.Failures, ch.Retired); v != planguard.VerdictFalsePositive {
		t.Errorf("retired driver + forbidden Seq Scan: verdict %s, want FALSE_POSITIVE", v)
	}
}

func findNode(n planguard.Node, typ string) planguard.Node {
	if n["Node Type"] == typ {
		return n
	}
	kids, _ := n["Plans"].([]any)
	for _, k := range kids {
		if m, ok := k.(map[string]any); ok {
			if f := findNode(m, typ); f != nil {
				return f
			}
		}
	}
	return nil
}

func sortCorpus() *planguard.Corpus {
	return &planguard.Corpus{
		Defaults: planguard.Defaults{BigTables: bigTables, SortAboveLimit: true, SortExempt: []string{"latest_observation"}},
		SortExemptSets: map[string][]planguard.ScanPattern{
			"latest_observation": {{Relation: "packet_observations", IndexCond: "packet_hash = p.packet_hash"}},
			"site_probe":         {{Relation: "packet_sites", IndexCond: "packet_hash = ps.packet_hash"}},
			"site_probe_wrong_index": {{
				Relation: "packet_sites", Index: "idx_packet_sites_site", IndexCond: "packet_hash = ps.packet_hash",
			}},
			"site_probe_under_sort":        {{Relation: "packet_sites", IndexCond: "packet_hash = ps.packet_hash", Sort: "Sort"}},
			"site_probe_under_incremental": {{Relation: "packet_sites", IndexCond: "packet_hash = ps.packet_hash", Sort: "Incremental Sort"}},
		},
	}
}

func TestSortExemptionScopedToScanPattern(t *testing.T) {
	c := sortCorpus()
	exempt := &planguard.Expect{SortExempt: []string{"site_probe"}}

	wantKinds(t, c.Assess(loadPlan(t, "bysites_ok.json"), exempt, false))

	ch := c.Assess(loadPlan(t, "bysites_nolimit.json"), exempt, false)
	wantKinds(t, ch, planguard.FailSortAboveLimit)
	f := ch.Failures[0]
	if !strings.Contains(f.Msg, "packet_sites/idx_packet_sites_site") || strings.Contains(f.Msg, "packet_sites_pkey") {
		t.Errorf("msg = %q, want it to name packet_sites/idx_packet_sites_site only", f.Msg)
	}
	if !reflect.DeepEqual(f.Indexes, []string{"idx_packet_sites_site"}) {
		t.Errorf("indexes = %v, want [idx_packet_sites_site]", f.Indexes)
	}

	// Without the exemption the correct statement fails on the probe scan: the exemption is what clears it.
	ch = c.Assess(loadPlan(t, "bysites_ok.json"), &planguard.Expect{}, false)
	wantKinds(t, ch, planguard.FailSortAboveLimit)
	if !strings.Contains(ch.Failures[0].Msg, "packet_sites/packet_sites_pkey") {
		t.Errorf("msg = %q, want it to name the probe scan", ch.Failures[0].Msg)
	}

	// A pattern's index must match too: one naming the wrong index exempts nothing.
	ch = c.Assess(loadPlan(t, "bysites_ok.json"), &planguard.Expect{SortExempt: []string{"site_probe_wrong_index"}}, false)
	wantKinds(t, ch, planguard.FailSortAboveLimit)

	// A pattern with a sort type exempts the scan only under that sort node: the probe sits under a plain Sort.
	wantKinds(t, c.Assess(loadPlan(t, "bysites_ok.json"), &planguard.Expect{SortExempt: []string{"site_probe_under_sort"}}, false))
	ch = c.Assess(loadPlan(t, "bysites_ok.json"), &planguard.Expect{SortExempt: []string{"site_probe_under_incremental"}}, false)
	wantKinds(t, ch, planguard.FailSortAboveLimit)

	// The defaults set alone covers the latest-observation LATERAL; dropping it fails that scan.
	c.Defaults.SortExempt = nil
	ch = c.Assess(loadPlan(t, "bysites_ok.json"), exempt, false)
	wantKinds(t, ch, planguard.FailSortAboveLimit)
	if !strings.Contains(ch.Failures[0].Msg, "packet_observations/idx_observations_packet") {
		t.Errorf("msg = %q, want it to name the latest-observation scan", ch.Failures[0].Msg)
	}

	// The rule is off unless the defaults turn it on.
	c = sortCorpus()
	c.Defaults.SortAboveLimit = false
	wantKinds(t, c.Assess(loadPlan(t, "bysites_nolimit.json"), &planguard.Expect{}, false))
}

func TestAssertDriverScopedByCTE(t *testing.T) {
	c := &planguard.Corpus{}
	ex := loadPlan(t, "packets_page.json")
	d := planguard.DriverSpec{Scope: "CTE page", Relation: "packets", Index: "idx_packets_last_heard_hash"}

	ch := c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch)
	if want := []string{"packets(p_1):Index Only Scan:idx_packets_last_heard_hash"}; !reflect.DeepEqual(ch.Drivers, want) {
		t.Errorf("Drivers = %v, want %v", ch.Drivers, want)
	}

	d.Scope = ""
	ch = c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch, planguard.FailDriver)
	if !reflect.DeepEqual(ch.Failures[0].Indexes, []string{"packets_pkey"}) {
		t.Errorf("indexes = %v, want [packets_pkey]", ch.Failures[0].Indexes)
	}

	d.Scope = "CTE other"
	ch = c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch, planguard.FailDriver)
	if !strings.Contains(ch.Failures[0].Msg, "no scan") {
		t.Errorf("msg = %q, want a no-scan failure", ch.Failures[0].Msg)
	}
}

func TestIndexCondModeNeutral(t *testing.T) {
	c := &planguard.Corpus{}
	for _, f := range []string{"routes_byiata_custom.json", "routes_byiata_generic.json"} {
		ex := loadPlan(t, f)
		d := planguard.DriverSpec{Relation: "known_routes", Index: "idx_known_routes_iata_keyset", IndexCond: []string{"iata >=", "iata  <="}}
		wantKinds(t, c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true))

		d.IndexCond = []string{"iata >=", "last_seen >"}
		ch := c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
		wantKinds(t, ch, planguard.FailIndexCond)
		if !reflect.DeepEqual(ch.Failures[0].Indexes, []string{"idx_known_routes_iata_keyset"}) {
			t.Errorf("%s: indexes = %v", f, ch.Failures[0].Indexes)
		}
	}
}

func TestIndexCondReadsBitmapChildren(t *testing.T) {
	c := &planguard.Corpus{}
	d := planguard.DriverSpec{
		Relation: "packets", Alias: "p", Nodes: []string{"Bitmap Heap Scan"},
		Index: "idx_packets_scope_last_heard", IndexCond: []string{"scope_id ="},
	}
	ch := c.Assess(loadPlan(t, "cmi_seqscan.json"), &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch)
	if want := []string{"packets(p):Bitmap Heap Scan:idx_packets_scope_last_heard"}; !reflect.DeepEqual(ch.Drivers, want) {
		t.Errorf("Drivers = %v, want %v", ch.Drivers, want)
	}

	d.Nodes = nil // default: Index Scan or Index Only Scan
	wantKinds(t, c.Assess(loadPlan(t, "cmi_seqscan.json"), &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true), planguard.FailDriver)
}

func TestAnyIndexAllowsEmptyPkeyRange(t *testing.T) {
	c := &planguard.Corpus{}
	ex := loadPlan(t, "adverts_past_end.json")
	d := planguard.DriverSpec{Relation: "packet_observations", AnyIndex: []string{"idx_observations_adverts", "packet_observations_pkey"}}
	ch := c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}, Budget: planguard.Budget{Buffers: 10}}, true)
	wantKinds(t, ch)
	if ch.Buffers != 3 {
		t.Errorf("Buffers = %d, want 3", ch.Buffers)
	}

	d = planguard.DriverSpec{Relation: "packet_observations", Index: "idx_observations_adverts"}
	ch = c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch, planguard.FailDriver)
	if !reflect.DeepEqual(ch.Failures[0].Indexes, []string{"packet_observations_pkey"}) {
		t.Errorf("indexes = %v", ch.Failures[0].Indexes)
	}
}

func TestRowsBudget(t *testing.T) {
	c := &planguard.Corpus{}
	ex := loadPlan(t, "packets_page.json") // root Actual Rows 51 × Actual Loops 1
	ch := c.Assess(ex, &planguard.Expect{Budget: planguard.Budget{Rows: 50}}, true)
	wantKinds(t, ch, planguard.FailBudget)
	if ch.Rows != 51 || !strings.Contains(ch.Failures[0].Msg, "rows 51 > 50") {
		t.Errorf("Rows = %d, msg %q", ch.Rows, ch.Failures[0].Msg)
	}
	wantKinds(t, c.Assess(ex, &planguard.Expect{Budget: planguard.Budget{Rows: 51}}, true))

	// Budgets are checked only when the run was analyzed.
	ch = c.Assess(ex, &planguard.Expect{Budget: planguard.Budget{Rows: 50, Buffers: 1, PlanningMs: 0.001, ExecMs: 0.001}}, false)
	wantKinds(t, ch)
	if ch.Rows != 0 || ch.Buffers != 0 {
		t.Errorf("plain run reports Rows %d, Buffers %d", ch.Rows, ch.Buffers)
	}

	ch = c.Assess(ex, &planguard.Expect{Budget: planguard.Budget{PlanningMs: 0.001, ExecMs: 0.001}}, true)
	wantKinds(t, ch, planguard.FailBudget, planguard.FailBudget)
}

func TestForbiddenAndRequiredNodes(t *testing.T) {
	c := &planguard.Corpus{Defaults: planguard.Defaults{
		Forbid: []planguard.NodeRule{{Node: "Seq Scan", Relations: bigTables, Reason: "never scan a big table"}},
	}}
	ex := loadPlan(t, "cmi_seqscan.json")
	wantKinds(t, c.Assess(ex, &planguard.Expect{}, true))

	ch := c.Assess(ex, &planguard.Expect{Forbid: []planguard.NodeRule{{Node: "Seq Scan", Relations: []string{"channel_messages"}}}}, true)
	wantKinds(t, ch, planguard.FailForbidden)
	if !strings.Contains(ch.Failures[0].Msg, "Seq Scan on channel_messages") {
		t.Errorf("msg = %q", ch.Failures[0].Msg)
	}

	exp := &planguard.Expect{Require: []planguard.NodeRule{
		{Node: "Bitmap Index Scan", Relations: []string{"idx_packets_scope_last_heard"}},
		{Node: "Hash Join"},
	}}
	wantKinds(t, c.Assess(ex, exp, true))
	exp.Require = append(exp.Require, planguard.NodeRule{Node: "Merge Join"})
	wantKinds(t, c.Assess(ex, exp, true), planguard.FailRequired)
}

func TestDecide(t *testing.T) {
	retired := []string{"idx_old"}
	f := func(k planguard.FailureKind, ix ...string) planguard.Failure {
		return planguard.Failure{Kind: k, Msg: string(k), Indexes: ix}
	}
	cases := []struct {
		name       string
		expectFail bool
		known      string
		failures   []planguard.Failure
		want       planguard.Verdict
	}{
		{"expect-fail detected", true, "", []planguard.Failure{f(planguard.FailBudget)}, planguard.VerdictDetected},
		{"expect-fail errored", true, "", []planguard.Failure{f(planguard.FailError)}, planguard.VerdictError},
		{"expect-fail errored, known issue", true, "x", []planguard.Failure{f(planguard.FailError)}, planguard.VerdictError},
		{"error, known issue", false, "x", []planguard.Failure{f(planguard.FailError)}, planguard.VerdictError},
		{"retired + error, known issue", false, "x", []planguard.Failure{f(planguard.FailDriver, "idx_old"), f(planguard.FailError)}, planguard.VerdictError},
		{"expect-fail missed", true, "", nil, planguard.VerdictMissed},
		{"clean", false, "", nil, planguard.VerdictOK},
		{"clean known issue", false, "x", nil, planguard.VerdictOK},
		{"retired structural", false, "", []planguard.Failure{
			f(planguard.FailDriver, "idx_old"), f(planguard.FailIndexCond, "idx_old"), f(planguard.FailForbidden, "idx_old"),
			f(planguard.FailRequired, "idx_old"), f(planguard.FailSortAboveLimit, "idx_new", "idx_old"), f(planguard.FailPlanChange, "idx_old"),
		}, planguard.VerdictRetiredIndex},
		{"retired + budget", false, "", []planguard.Failure{f(planguard.FailDriver, "idx_old"), f(planguard.FailBudget)}, planguard.VerdictFalsePositive},
		{"retired + other index", false, "", []planguard.Failure{f(planguard.FailDriver, "idx_old"), f(planguard.FailDriver, "idx_new")}, planguard.VerdictFalsePositive},
		{"retired + no index", false, "", []planguard.Failure{f(planguard.FailDriver, "idx_old"), f(planguard.FailRequired)}, planguard.VerdictFalsePositive},
		{"retired + error", false, "", []planguard.Failure{f(planguard.FailDriver, "idx_old"), f(planguard.FailError)}, planguard.VerdictError},
		{"retired + budget, known issue", false, "x", []planguard.Failure{f(planguard.FailDriver, "idx_old"), f(planguard.FailBudget)}, planguard.VerdictKnownIssue},
		{"known issue", false, "x", []planguard.Failure{f(planguard.FailDriver, "idx_new")}, planguard.VerdictKnownIssue},
		{"false positive", false, "", []planguard.Failure{f(planguard.FailDriver, "idx_new")}, planguard.VerdictFalsePositive},
		{"error", false, "", []planguard.Failure{f(planguard.FailError)}, planguard.VerdictError},
	}
	for _, tc := range cases {
		if got := planguard.Decide(tc.expectFail, tc.known, tc.failures, retired); got != tc.want {
			t.Errorf("%s: Decide = %s, want %s", tc.name, got, tc.want)
		}
	}
	// No retired list: nothing is RETIRED_INDEX.
	if got := planguard.Decide(false, "", []planguard.Failure{f(planguard.FailDriver, "idx_old")}, nil); got != planguard.VerdictFalsePositive {
		t.Errorf("no retired list: Decide = %s, want FALSE_POSITIVE", got)
	}
}

func TestVerdictFailing(t *testing.T) {
	failing := map[planguard.Verdict]bool{
		planguard.VerdictFalsePositive: true, planguard.VerdictMissed: true, planguard.VerdictError: true,
	}
	for _, v := range []planguard.Verdict{
		planguard.VerdictOK, planguard.VerdictDetected, planguard.VerdictMissed, planguard.VerdictFalsePositive,
		planguard.VerdictRetiredIndex, planguard.VerdictKnownIssue, planguard.VerdictSkip, planguard.VerdictError,
	} {
		if v.Failing() != failing[v] {
			t.Errorf("%s.Failing() = %v, want %v", v, v.Failing(), failing[v])
		}
	}
}

func TestSubplanCondsStayOffParentScan(t *testing.T) {
	c := &planguard.Corpus{}
	ex := loadPlan(t, "cmi_seqscan.json")
	heap := findNode(ex.Root, "Bitmap Heap Scan")
	if heap == nil {
		t.Fatal("cmi_seqscan.json has no Bitmap Heap Scan")
	}
	// A per-row bitmap probe run by the scan's filter: a SubPlan child with its own Bitmap Index Scan.
	probe := map[string]any{
		"Node Type": "Bitmap Heap Scan", "Parent Relationship": "SubPlan", "Subplan Name": "SubPlan 1",
		"Relation Name": "packet_observations", "Alias": "po",
		"Recheck Cond": "(observer_id = p.observer_id)",
		"Plans": []any{map[string]any{
			"Node Type": "Bitmap Index Scan", "Parent Relationship": "Outer",
			"Index Name": "idx_observations_observer", "Index Cond": "(observer_id = p.observer_id)",
		}},
	}
	heap["Plans"] = append(heap["Plans"].([]any), probe)

	d := planguard.DriverSpec{
		Relation: "packets", Alias: "p", Nodes: []string{"Bitmap Heap Scan"},
		Index: "idx_packets_scope_last_heard", IndexCond: []string{"scope_id ="},
	}
	ch := c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch)
	if want := []string{"packets(p):Bitmap Heap Scan:idx_packets_scope_last_heard"}; !reflect.DeepEqual(ch.Drivers, want) {
		t.Errorf("Drivers = %v, want %v (the probe's index is not the outer scan's)", ch.Drivers, want)
	}

	d.IndexCond = []string{"observer_id ="} // only in the probe
	ch = c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch, planguard.FailIndexCond)
	if !reflect.DeepEqual(ch.Failures[0].Indexes, []string{"idx_packets_scope_last_heard"}) {
		t.Errorf("indexes = %v, want only the outer scan's", ch.Failures[0].Indexes)
	}

	// A retired index used only by the probe is attributed to the probe, not the outer scan.
	c.Defaults.RetiredIndexes = []string{"idx_observations_observer"}
	d.IndexCond = nil
	d.Index = "idx_packets_other"
	ch = c.Assess(ex, &planguard.Expect{Driver: []planguard.DriverSpec{d}}, true)
	wantKinds(t, ch, planguard.FailDriver)
	if v := planguard.Decide(false, "", ch.Failures, ch.Retired); v != planguard.VerdictFalsePositive {
		t.Errorf("outer driver failure with a retired probe index: verdict %s, want FALSE_POSITIVE", v)
	}
}

func TestBudgetNeedsAnalyzeAndBuffers(t *testing.T) {
	c := &planguard.Corpus{}
	raw, err := os.ReadFile(filepath.Join("testdata", "plans", "packets_page.json"))
	if err != nil {
		t.Fatal(err)
	}
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		t.Fatal(err)
	}
	delete(arr[0], "Execution Time")
	plain, _ := json.Marshal(arr)
	ex, err := planguard.ParseExplain(plain)
	if err != nil {
		t.Fatal(err)
	}
	ch := c.Assess(ex, &planguard.Expect{}, true)
	wantKinds(t, ch, planguard.FailError)
	if !strings.Contains(ch.Failures[0].Msg, "ANALYZE") {
		t.Errorf("msg = %q", ch.Failures[0].Msg)
	}
	wantKinds(t, c.Assess(ex, &planguard.Expect{Budget: planguard.Budget{Buffers: 1}}, false))

	ex = loadPlan(t, "packets_page.json")
	delete(ex.Root, "Shared Hit Blocks")
	delete(ex.Root, "Shared Read Blocks")
	ch = c.Assess(ex, &planguard.Expect{Budget: planguard.Budget{Buffers: 1, Rows: 50}}, true)
	wantKinds(t, ch, planguard.FailError, planguard.FailBudget)
	if !strings.Contains(ch.Failures[0].Msg, "BUFFERS") || !strings.Contains(ch.Failures[1].Msg, "rows") {
		t.Errorf("failures = %+v", ch.Failures)
	}
	if v := planguard.Decide(false, "x", ch.Failures, nil); v != planguard.VerdictError {
		t.Errorf("verdict %s, want ERROR", v)
	}
	// No buffers budget: a plan without BUFFERS is fine.
	wantKinds(t, c.Assess(ex, &planguard.Expect{}, true))
}
