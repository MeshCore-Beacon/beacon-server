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

func drv(id, ix string) planguard.Result {
	return planguard.Result{ID: id, Mode: "custom", Verdict: planguard.VerdictOK, Drivers: []string{"known_routes:Index Scan:" + ix}}
}

func TestFidelityExcludesIndexSetMismatch(t *testing.T) {
	a := []planguard.Result{drv("same", "idx_a"), drv("same2", "idx_b"), drv("differs", "idx_a"), drv("only_a_index", "idx_only_a"), drv("not_in_b", "idx_a")}
	b := []planguard.Result{drv("same", "idx_a"), drv("same2", "idx_b"), drv("differs", "idx_b"), drv("only_a_index", "idx_a")}
	aIx := []string{"idx_a", "idx_b", "idx_only_a"}
	bIx := []string{"idx_a", "idx_b"}
	got := planguard.Fidelity(a, b, aIx, bIx)
	if got.Total != 3 || got.Match != 2 || got.Excluded != 1 {
		t.Fatalf("fidelity = %+v, want 2 of 3 with 1 excluded", got)
	}
	if len(got.Mismatches) != 1 || !strings.Contains(got.Mismatches[0], "differs|custom") {
		t.Errorf("mismatches = %v, want differs|custom", got.Mismatches)
	}

	// The same run seen from b's side: b's plan uses an index a lacks.
	b2 := []planguard.Result{drv("same", "idx_b_only")}
	if got := planguard.Fidelity([]planguard.Result{drv("same", "idx_a")}, b2, []string{"idx_a"}, []string{"idx_a", "idx_b_only"}); got.Excluded != 1 || got.Total != 0 {
		t.Errorf("fidelity = %+v, want the run excluded", got)
	}
}

func TestReportCountsAndFailures(t *testing.T) {
	var rep planguard.Report
	for i, v := range []planguard.Verdict{planguard.VerdictOK, planguard.VerdictFalsePositive, planguard.VerdictMissed, planguard.VerdictError,
		planguard.VerdictRetiredIndex, planguard.VerdictSkip, planguard.VerdictDetected, planguard.VerdictKnownIssue, planguard.VerdictOK} {
		rep.Results = append(rep.Results, planguard.Result{ID: string(rune('a' + i)), Mode: "custom", Verdict: v})
	}
	want := map[planguard.Verdict]int{"ok": 2, "FALSE_POSITIVE": 1, "MISSED": 1, "ERROR": 1, "RETIRED_INDEX": 1, "SKIP": 1, "DETECTED": 1, "KNOWN_ISSUE": 1}
	if got := rep.Counts(); !reflect.DeepEqual(got, want) {
		t.Errorf("counts = %v, want %v", got, want)
	}
	var failing []planguard.Verdict
	for _, r := range rep.Failures() {
		failing = append(failing, r.Verdict)
	}
	if !reflect.DeepEqual(failing, []planguard.Verdict{planguard.VerdictFalsePositive, planguard.VerdictMissed, planguard.VerdictError}) {
		t.Errorf("failures = %v, want FALSE_POSITIVE, MISSED, ERROR", failing)
	}
}

func TestReportFilesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rep := &planguard.Report{Tier: "local", Indexes: []string{"idx_a"}, Results: []planguard.Result{
		{ID: "x", Stmt: "S", Shape: "dense", Mode: "generic", Tier: "local", Verdict: planguard.VerdictFalsePositive,
			Failures: []planguard.Failure{{Kind: planguard.FailDriver, Msg: "driver: a | b"}}, Drivers: []string{"t:Index Scan:idx_a"},
			Signature: []string{"Limit"}, Plan: []byte(`[{"Plan": {"Node Type": "Limit"}}]`)},
		{ID: "y", Mode: "custom", Tier: "local", Verdict: planguard.VerdictSkip, Skip: "pending: f"},
	}}
	if err := rep.WriteFiles(dir); err != nil {
		t.Fatal(err)
	}
	got, err := planguard.ReadResults(filepath.Join(dir, "results.local.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	want := append([]planguard.Result(nil), rep.Results...)
	want[0].Plan = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("read back %+v, want %+v", got, want)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "results.local.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"id":"x"`, `"stmt":"S"`, `"shape":"dense"`, `"mode":"generic"`, `"tier":"local"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("results.jsonl lacks %s", key)
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, "plans", "local", "x.generic.json")); err != nil || !strings.Contains(string(b), "Limit") {
		t.Errorf("plan file: %v %q", err, b)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "summary.local.md")); err != nil || !strings.Contains(string(b), `driver: a \| b`) {
		t.Errorf("summary: %v %q", err, b)
	}
	meta, err := planguard.ReadReportMeta(filepath.Join(dir, "results.local.jsonl"))
	if err != nil || !reflect.DeepEqual(meta.Indexes, []string{"idx_a"}) {
		t.Errorf("report meta: %v %+v", err, meta)
	}
	if _, err := planguard.ReadResults(writeFile(t, "{\"id\":\"x\"}\n{\"id\":")); err == nil {
		t.Error("a truncated results file read without error")
	}
}

func writeFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}
