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

func s(v string) *string { return &v }

// rows builds a RowSet; the value "@@NULL" stands for NULL.
func rows(cols []string, rs ...[]string) planguard.RowSet {
	out := planguard.RowSet{Columns: cols}
	for _, r := range rs {
		row := make([]*string, len(r))
		for i, v := range r {
			if v != "@@NULL" {
				row[i] = s(v)
			}
		}
		out.Rows = append(out.Rows, row)
	}
	return out
}

var tieCols = []string{"id", "k", "latest_observer_name"}

func tieRef() planguard.RowSet {
	return rows(tieCols,
		[]string{"1", "30", "a"},
		[]string{"2", "20", "b"},
		[]string{"3", "20", "c"},
	)
}

func pair(allowed ...string) planguard.DiffPair {
	return planguard.DiffPair{ID: "p", Key: []string{"k"}, Allowed: allowed}
}

func TestDiffComparesAllColumnsByDefault(t *testing.T) {
	// A value changed in a column the pair names nowhere.
	cand := tieRef()
	cand.Rows[1][2] = s("B")
	o := planguard.Compare(tieRef(), cand, pair(), nil, nil)
	if o.Verdict != "different" {
		t.Fatalf("value change in an unnamed column: %+v, want different", o)
	}
	if !strings.Contains(o.Detail, "row 2") || !strings.Contains(o.Detail, "latest_observer_name") {
		t.Errorf("detail %q should name row 2 and the column", o.Detail)
	}

	// NULL and "" differ.
	nullCand := tieRef()
	nullCand.Rows[0][2] = nil
	emptyRef := tieRef()
	emptyRef.Rows[0][2] = s("")
	if o := planguard.Compare(emptyRef, nullCand, pair(), nil, nil); o.Verdict != "different" {
		t.Errorf("NULL vs empty text: %+v, want different", o)
	}

	// omit without latest_observer_tie is a spec error.
	p := pair()
	p.Omit = []string{"latest_observer_name"}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "latest_observer_tie") {
		t.Errorf("omit without latest_observer_tie: Validate() = %v, want an error naming the class", err)
	}
	if o := planguard.Compare(tieRef(), cand, p, nil, nil); o.Verdict != "different" || !strings.Contains(o.Detail, "invalid pair") {
		t.Errorf("Compare on an invalid pair: %+v, want different (invalid pair)", o)
	}

	// latest_observer_tie without omit is a spec error too.
	if err := pair("latest_observer_tie").Validate(); err == nil {
		t.Error("latest_observer_tie without omit: Validate() = nil, want an error")
	}

	// With both, the omitted column may differ; the row is listed.
	p = pair("latest_observer_tie")
	p.Omit = []string{"latest_observer_name"}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	o = planguard.Compare(tieRef(), cand, p, nil, nil)
	if o.Verdict != "allowed" || !reflect.DeepEqual(o.Classes, []string{"latest_observer_tie(1)"}) {
		t.Fatalf("omit + latest_observer_tie: %+v, want allowed [latest_observer_tie(1)]", o)
	}
	for _, want := range []string{"k=20", `"b"`, `"B"`} {
		if !strings.Contains(o.Detail, want) {
			t.Errorf("detail %q should list the row (%s)", o.Detail, want)
		}
	}

	// The omitted columns are the only relaxed ones: another column still counts.
	cand2 := tieRef()
	cand2.Rows[1][0] = s("9")
	if o := planguard.Compare(tieRef(), cand2, p, nil, nil); o.Verdict != "different" {
		t.Errorf("id changed under omit [latest_observer_name]: %+v, want different", o)
	}

	// A ref column the cand lacks is a difference; an extra cand column is not.
	short := rows([]string{"id", "k"}, []string{"1", "30"}, []string{"2", "20"}, []string{"3", "20"})
	if o := planguard.Compare(tieRef(), short, pair(), nil, nil); o.Verdict != "different" || !strings.Contains(o.Detail, "latest_observer_name") {
		t.Errorf("cand lacks a column: %+v, want different naming it", o)
	}
	if o := planguard.Compare(short, tieRef(), pair(), nil, nil); o.Verdict != "identical" {
		t.Errorf("cand adds a column: %+v, want identical", o)
	}

	// Identical sets are identical.
	if o := planguard.Compare(tieRef(), tieRef(), pair(), nil, nil); o.Verdict != "identical" || len(o.Classes) != 0 {
		t.Errorf("same rows: %+v, want identical", o)
	}
}

func TestDiffTieOrder(t *testing.T) {
	perm := rows(tieCols,
		[]string{"1", "30", "a"},
		[]string{"3", "20", "c"},
		[]string{"2", "20", "b"},
	)
	if o := planguard.Compare(tieRef(), perm, pair(), nil, nil); o.Verdict != "different" {
		t.Errorf("tie permutation without tie_order: %+v, want different", o)
	}
	o := planguard.Compare(tieRef(), perm, pair("tie_order"), nil, nil)
	if o.Verdict != "allowed" || !reflect.DeepEqual(o.Classes, []string{"tie_order(2)"}) {
		t.Errorf("tie permutation with tie_order: %+v, want allowed [tie_order(2)]", o)
	}
	// Rows moved across different keys are not a tie.
	cross := rows(tieCols,
		[]string{"2", "20", "b"},
		[]string{"1", "30", "a"},
		[]string{"3", "20", "c"},
	)
	if o := planguard.Compare(tieRef(), cross, pair("tie_order"), nil, nil); o.Verdict != "different" {
		t.Errorf("rows moved across keys: %+v, want different", o)
	}
	// A tie run whose rows are swapped for another row is not a permutation.
	swapped := rows(tieCols,
		[]string{"1", "30", "a"},
		[]string{"3", "20", "c"},
		[]string{"4", "20", "d"},
	)
	if o := planguard.Compare(tieRef(), swapped, pair("tie_order"), nil, nil); o.Verdict != "different" {
		t.Errorf("tie run with another row: %+v, want different", o)
	}
	// With tie_cut, the last run may keep another member of the tie: both pages end inside it.
	if o := planguard.Compare(tieRef(), swapped, pair("tie_order", "tie_cut"), nil, nil); o.Verdict != "allowed" || !reflect.DeepEqual(o.Classes, []string{"tie_order(2)", "tie_cut(1)"}) {
		t.Errorf("last run cut by the limit with tie_cut: %+v, want allowed [tie_order(2) tie_cut(1)]", o)
	}
	// A run before the last one must still be a permutation.
	early := rows(tieCols,
		[]string{"5", "30", "e"},
		[]string{"2", "20", "b"},
		[]string{"3", "20", "c"},
	)
	if o := planguard.Compare(tieRef(), early, pair("tie_order", "tie_cut"), nil, nil); o.Verdict != "different" {
		t.Errorf("an earlier run with another row: %+v, want different", o)
	}
	if err := pair("tie_cut").Validate(); err == nil {
		t.Error("tie_cut without tie_order: Validate() = nil")
	}
}

func TestDiffOnlySaturatedSuffixRelaxesCount(t *testing.T) {
	// Tied rows permuted AND a duplicated trailing row (ref 3, cand 4).
	permDup := rows(tieCols,
		[]string{"1", "30", "a"},
		[]string{"3", "20", "c"},
		[]string{"2", "20", "b"},
		[]string{"2", "20", "b"},
	)
	if o := planguard.Compare(tieRef(), permDup, pair("tie_order"), nil, nil); o.Verdict != "different" {
		t.Errorf("permutation + duplicate row: %+v, want different", o)
	}
	// A duplicated trailing row only.
	dupOnly := rows(tieCols,
		[]string{"1", "30", "a"},
		[]string{"2", "20", "b"},
		[]string{"3", "20", "c"},
		[]string{"3", "20", "c"},
	)
	o := planguard.Compare(tieRef(), dupOnly, pair("tie_order"), nil, nil)
	if o.Verdict != "different" || !strings.Contains(o.Detail, "row count") {
		t.Errorf("duplicate row: %+v, want different (row count)", o)
	}

	// A saturated ref: flag t, floor at row 2's key; row 3 is below the floor.
	cols := []string{"h", "site_heard_at", "scan_saturated", "scan_floor"}
	ref := rows(cols,
		[]string{"\\xaa", "2026-10-18 15:40:39.570763+00", "t", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xbb", "2026-10-18 15:40:39.5+00", "t", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xcc", "2026-10-18 15:40:38.1+00", "t", "2026-10-18 15:40:39.5+00"},
	)
	candCols := []string{"h", "site_heard_at"}
	sat := planguard.DiffPair{
		ID: "sat", Key: []string{"site_heard_at"}, Allowed: []string{"saturated_suffix"},
		Saturated: &planguard.SaturatedSpec{Flag: "scan_saturated", Floor: "scan_floor"},
	}
	if err := sat.Validate(); err != nil {
		t.Fatal(err)
	}
	missingBelow := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"})
	o = planguard.Compare(ref, missingBelow, sat, nil, nil)
	if o.Verdict != "allowed" || !reflect.DeepEqual(o.Classes, []string{"saturated_suffix(1)"}) {
		t.Errorf("row below the floor missing: %+v, want allowed [saturated_suffix(1)]", o)
	}
	differsBelow := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"}, []string{"\\xdd", "2026-10-18 15:40:37+00"})
	if o := planguard.Compare(ref, differsBelow, sat, nil, nil); o.Verdict != "allowed" {
		t.Errorf("row below the floor differs: %+v, want allowed", o)
	}
	missingAbove := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xcc", "2026-10-18 15:40:38.1+00"})
	if o := planguard.Compare(ref, missingAbove, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("row above the floor missing: %+v, want different", o)
	}
	// The cand rows in place of the relaxed ref rows: below the floor, in order, and new.
	dupTail := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"})
	if o := planguard.Compare(ref, dupTail, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("relaxed tail repeats a row: %+v, want different", o)
	}
	newerTail := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"}, []string{"\\xee", "2026-10-18 15:40:40+00"})
	if o := planguard.Compare(ref, newerTail, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("relaxed tail row newer than the floor and out of order: %+v, want different", o)
	}
	ref4 := rows(cols,
		[]string{"\\xaa", "2026-10-18 15:40:39.570763+00", "t", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xbb", "2026-10-18 15:40:39.5+00", "t", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xcc", "2026-10-18 15:40:38.1+00", "t", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xdd", "2026-10-18 15:40:37+00", "t", "2026-10-18 15:40:39.5+00"},
	)
	unordered := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xff", "2026-10-18 15:40:36+00"}, []string{"\\xee", "2026-10-18 15:40:38+00"})
	if o := planguard.Compare(ref4, unordered, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("relaxed tail rows below the floor out of order: %+v, want different", o)
	}
	dupBelow := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xee", "2026-10-18 15:40:38+00"}, []string{"\\xee", "2026-10-18 15:40:38+00"})
	if o := planguard.Compare(ref4, dupBelow, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("relaxed tail repeats a row below the floor: %+v, want different", o)
	}
	atFloor := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xab", "2026-10-18 15:40:39.5+00"}, []string{"\\xff", "2026-10-18 15:40:36+00"})
	if o := planguard.Compare(ref4, atFloor, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("relaxed tail row at the floor, in order and new: %+v, want different", o)
	}
	ordered := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xee", "2026-10-18 15:40:38+00"}, []string{"\\xff", "2026-10-18 15:40:36+00"})
	if o := planguard.Compare(ref4, ordered, sat, nil, nil); o.Verdict != "allowed" {
		t.Errorf("relaxed tail rows below the floor in order: %+v, want allowed", o)
	}
	extra := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xcc", "2026-10-18 15:40:38.1+00"}, []string{"\\xdd", "2026-10-18 15:40:37+00"})
	if o := planguard.Compare(ref, extra, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("cand adds a row: %+v, want different (only ref rows below the floor are relaxed)", o)
	}
	// saturated_full: a short saturated ref page that the cand fills with rows below the floor, in order and new.
	short := rows(cols,
		[]string{"\\xaa", "2026-10-18 15:40:39.570763+00", "t", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xbb", "2026-10-18 15:40:39.5+00", "t", "2026-10-18 15:40:39.5+00"},
	)
	full := sat
	full.Allowed = []string{"saturated_suffix", "saturated_full"}
	if err := full.Validate(); err != nil {
		t.Fatal(err)
	}
	filled := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xee", "2026-10-18 15:40:38+00"}, []string{"\\xff", "2026-10-18 15:40:36+00"})
	if o := planguard.Compare(short, filled, full, nil, nil); o.Verdict != "allowed" || !reflect.DeepEqual(o.Classes, []string{"saturated_full(2)"}) {
		t.Errorf("short page filled below the floor: %+v, want allowed [saturated_full(2)]", o)
	}
	if o := planguard.Compare(short, filled, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("short page filled without saturated_full: %+v, want different", o)
	}
	aboveFloor := rows(candCols, []string{"\\xaa", "2026-10-18 15:40:39.570763+00"}, []string{"\\xbb", "2026-10-18 15:40:39.5+00"},
		[]string{"\\xab", "2026-10-18 15:40:39.5+00"})
	if o := planguard.Compare(short, aboveFloor, full, nil, nil); o.Verdict != "different" {
		t.Errorf("fill row at the floor: %+v, want different", o)
	}
	if o := planguard.Compare(short, rows(candCols, []string{"\\xbb", "2026-10-18 15:40:39.5+00"}, []string{"\\xee", "2026-10-18 15:40:38+00"}), full, nil, nil); o.Verdict != "different" {
		t.Errorf("ref row missing, cand filled: %+v, want different", o)
	}
	noSat := planguard.DiffPair{ID: "full", Key: []string{"site_heard_at"}, Allowed: []string{"saturated_full"}}
	if err := noSat.Validate(); err == nil {
		t.Error("saturated_full without saturated: Validate() = nil")
	}
	unsat := rows(cols,
		[]string{"\\xaa", "2026-10-18 15:40:39.570763+00", "f", "@@NULL"},
		[]string{"\\xbb", "2026-10-18 15:40:39.5+00", "f", "@@NULL"},
		[]string{"\\xcc", "2026-10-18 15:40:38.1+00", "f", "@@NULL"},
	)
	if o := planguard.Compare(unsat, missingBelow, sat, nil, nil); o.Verdict != "different" {
		t.Errorf("unsaturated ref, cand missing a row: %+v, want different", o)
	}
	// Without the class the same cand is a difference.
	plain := planguard.DiffPair{ID: "sat", Key: []string{"site_heard_at"}}
	if o := planguard.Compare(ref, missingBelow, plain, nil, nil); o.Verdict != "different" {
		t.Errorf("saturated ref without the class: %+v, want different", o)
	}
	// saturated without the class, or the class without saturated, is a spec error.
	bad := sat
	bad.Allowed = nil
	if err := bad.Validate(); err == nil {
		t.Error("saturated without saturated_suffix: Validate() = nil")
	}
	bad = sat
	bad.Saturated = nil
	if err := bad.Validate(); err == nil {
		t.Error("saturated_suffix without saturated: Validate() = nil")
	}
}

func TestPageCursorTieRows(t *testing.T) {
	cols := []string{"packet_hash", "site_heard_at"}
	ref := rows(cols,
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x02", "2026-10-18 15:40:39.4+00"},
		[]string{"\\x03", "2026-10-18 15:40:39.3+00"},
	)
	tb := 2
	p := planguard.DiffPair{ID: "pc", Key: []string{"site_heard_at"}, Allowed: []string{"pagecursor_tie_rows"}, TieBoundary: &tb,
		TieCursor: &planguard.TieCursor{TS: 2, Hash: 3, HashColumn: "packet_hash"}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	params := []string{"{AMS}", "2026-10-18 15:40:39.570+00", "16", "4"}
	candParams := []string{"{AMS}", "2026-10-18 15:40:39.570763+00", "\\x0a", "4"}
	cand := rows(cols,
		[]string{"\\x09", "2026-10-18 15:40:39.570763+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x02", "2026-10-18 15:40:39.4+00"},
	)
	o := planguard.Compare(ref, cand, p, params, candParams)
	if o.Verdict != "allowed" || !reflect.DeepEqual(o.Classes, []string{"pagecursor_tie_rows(1)"}) {
		t.Errorf("a lead row at the cursor's millisecond: %+v, want allowed [pagecursor_tie_rows(1)]", o)
	}
	early := rows(cols,
		[]string{"\\x09", "2026-10-18 15:40:39.569999+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x02", "2026-10-18 15:40:39.4+00"},
	)
	if o := planguard.Compare(ref, early, p, params, candParams); o.Verdict != "different" {
		t.Errorf("a lead row 1 ms earlier: %+v, want different", o)
	}
	if o := planguard.Compare(ref, cand, planguard.DiffPair{ID: "pc", Key: []string{"site_heard_at"}}, params, candParams); o.Verdict != "different" {
		t.Errorf("a lead row without the class: %+v, want different", o)
	}
	// The counts must be equal: a lead row plus all of ref is a difference.
	longer := rows(cols,
		[]string{"\\x09", "2026-10-18 15:40:39.570763+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x02", "2026-10-18 15:40:39.4+00"},
		[]string{"\\x03", "2026-10-18 15:40:39.3+00"},
	)
	if o := planguard.Compare(ref, longer, p, params, candParams); o.Verdict != "different" {
		t.Errorf("lead row + every ref row: %+v, want different", o)
	}
	// cand[k:] must equal ref[:n-k].
	shifted := rows(cols,
		[]string{"\\x09", "2026-10-18 15:40:39.570763+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x03", "2026-10-18 15:40:39.3+00"},
	)
	if o := planguard.Compare(ref, shifted, p, params, candParams); o.Verdict != "different" {
		t.Errorf("lead row, then a row missing: %+v, want different", o)
	}
	// Only rows ref lacks are lead rows: a ref row in the cursor's millisecond is compared as usual.
	atMs := rows(cols,
		[]string{"\\x09", "2026-10-18 15:40:39.570763+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
	)
	if o := planguard.Compare(atMs, atMs, p, params, candParams); o.Verdict != "identical" {
		t.Errorf("ref and cand both hold the row in the cursor's millisecond: %+v, want identical", o)
	}
	// Lead rows must sort at or below the cand's own cursor: a row above it cannot come from that cursor.
	above := rows(cols,
		[]string{"\\x09", "2026-10-18 15:40:39.570999+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x02", "2026-10-18 15:40:39.4+00"},
	)
	if o := planguard.Compare(ref, above, p, params, candParams); o.Verdict != "different" {
		t.Errorf("a lead row at .570999 above the cand cursor .570763: %+v, want different", o)
	}
	sameTS := rows(cols,
		[]string{"\\x0b", "2026-10-18 15:40:39.570763+00"},
		[]string{"\\x01", "2026-10-18 15:40:39.5+00"},
		[]string{"\\x02", "2026-10-18 15:40:39.4+00"},
	)
	if o := planguard.Compare(ref, sameTS, p, params, candParams); o.Verdict != "different" {
		t.Errorf("a lead row at the cursor's ts with a hash above its bound: %+v, want different", o)
	}
	if o := planguard.Compare(ref, cand, p, params, nil); o.Verdict != "different" {
		t.Errorf("no cand params: %+v, want different (the class needs the cand cursor)", o)
	}
	bad := p
	bad.TieBoundary = nil
	if err := bad.Validate(); err == nil {
		t.Error("pagecursor_tie_rows without tie_boundary: Validate() = nil")
	}
	bad = p
	bad.TieCursor = nil
	if err := bad.Validate(); err == nil {
		t.Error("pagecursor_tie_rows without tie_cursor: Validate() = nil")
	}
}

func TestDiffFailsOnParseError(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.jsonl")
	if err := os.WriteFile(bad, []byte("{\"columns\":[\"a\",\"b\"],\"rows\":2}\n[\"1\",\"2\"]\n[\"a\", \"b\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := planguard.ReadCaseFile(bad); err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("unterminated JSON on line 3: err %v, want an error naming line 3", err)
	}

	arity := filepath.Join(dir, "arity.jsonl")
	if err := os.WriteFile(arity, []byte("{\"columns\":[\"a\",\"b\",\"c\"],\"rows\":1}\n[\"1\",\"2\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := planguard.ReadCaseFile(arity); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("row of arity 2 under 3 columns: err %v, want an error naming line 2", err)
	}

	for name, body := range map[string]string{
		"empty":       "",
		"no header":   "[\"1\",\"2\"]\n",
		"truncated":   "{\"columns\":[\"a\"],\"rows\":2}\n[\"1\"]\n",
		"no newline":  "{\"columns\":[\"a\"],\"rows\":1}\n[\"1\"]",
		"number cell": "{\"columns\":[\"a\"],\"rows\":1}\n[1]\n",
		"extra rows":  "{\"columns\":[\"a\"],\"rows\":1}\n[\"1\"]\n[\"2\"]\n",
	} {
		p := filepath.Join(dir, "x.jsonl")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := planguard.ReadCaseFile(p); err == nil {
			t.Errorf("%s: ReadCaseFile accepted %q", name, body)
		}
	}

	// Text that looks like the old format's case marker round-trips, and a later row still counts.
	marker := "x\n@@CASE p|ref|old\nmore"
	ref := rows([]string{"id", "k"}, []string{"1", marker}, []string{"2", "b"})
	cand := rows([]string{"id", "k"}, []string{"1", marker}, []string{"2", "B"})
	rp, cp := filepath.Join(dir, "ref.jsonl"), filepath.Join(dir, "cand.jsonl")
	if err := planguard.WriteCaseFile(rp, ref); err != nil {
		t.Fatal(err)
	}
	if err := planguard.WriteCaseFile(cp, cand); err != nil {
		t.Fatal(err)
	}
	ref2, err := planguard.ReadCaseFile(rp)
	if err != nil {
		t.Fatal(err)
	}
	cand2, err := planguard.ReadCaseFile(cp)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ref2, ref) {
		t.Errorf("marker text did not round-trip: %+v", ref2)
	}
	o := planguard.Compare(ref2, cand2, planguard.DiffPair{ID: "marker", Key: []string{"id"}}, nil, nil)
	if o.Verdict != "different" || !strings.Contains(o.Detail, "row 2") {
		t.Errorf("marker text: %+v, want different at row 2", o)
	}
}

func TestCaseFileRoundTripsHostileText(t *testing.T) {
	hostile := []string{
		`he said "hi", then left`,
		"a,b,c",
		"line1\r\nline2\n",
		"tab\there",
		"unicode: ünïcødé ✓ 🛰",
		"",
		"@@NULL",
		`back\slash \x00 not a nul`,
		"\u0001\u001f control",
		"]}\n{\"columns\":[]}",
		"  ",
	}
	rs := planguard.RowSet{Columns: []string{"v", "n"}}
	for _, h := range hostile {
		rs.Rows = append(rs.Rows, []*string{s(h), nil})
	}
	rs.Rows = append(rs.Rows, []*string{nil, s("")})
	p := filepath.Join(t.TempDir(), "h.jsonl")
	if err := planguard.WriteCaseFile(p, rs); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), "\n"); n != len(rs.Rows)+1 {
		t.Errorf("%d lines, want one per row plus the header (%d)", n, len(rs.Rows)+1)
	}
	got, err := planguard.ReadCaseFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, rs) {
		t.Errorf("round trip differs:\n got %+v\nwant %+v", got, rs)
	}
	// An empty result keeps its columns.
	empty := planguard.RowSet{Columns: []string{"a"}}
	if err := planguard.WriteCaseFile(p, empty); err != nil {
		t.Fatal(err)
	}
	if got, err := planguard.ReadCaseFile(p); err != nil || !reflect.DeepEqual(got.Columns, empty.Columns) || len(got.Rows) != 0 {
		t.Errorf("empty result: %+v, %v", got, err)
	}
	// Text that is not UTF-8 cannot be written losslessly.
	if err := planguard.WriteCaseFile(p, planguard.RowSet{Columns: []string{"a"}, Rows: [][]*string{{s("\xff")}}}); err == nil {
		t.Error("WriteCaseFile accepted invalid UTF-8")
	}
	// A row whose arity differs from the columns is refused on write too.
	if err := planguard.WriteCaseFile(p, planguard.RowSet{Columns: []string{"a"}, Rows: [][]*string{{s("1"), s("2")}}}); err == nil {
		t.Error("WriteCaseFile accepted a row of the wrong arity")
	}
}

func TestDiffSpecValidation(t *testing.T) {
	base := `batches:
  - name: b
    search_path: "t_x, public"
    statements:
      ref: "file:ref.sql"
      cand: "new:ListT"
    pairs:
      - id: p1
        ref: {stmt: ref, params: [1]}
        cands: [{stmt: cand, params: [1]}]
        key: [k]
        allowed: [tie_order]
        expect: {cand: allowed}
`
	fsys := mapFS(map[string]string{"d.yaml": base, "ref.sql": "SELECT 1"})
	spec, err := planguard.LoadDiffSpec(fsys, "d.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Batches) != 1 || len(spec.Batches[0].Pairs) != 1 {
		t.Fatalf("spec %+v", spec)
	}
	for name, edit := range map[string][2]string{
		"unknown class":       {"allowed: [tie_order]", "allowed: [close_enough]"},
		"unknown alias":       {"cands: [{stmt: cand", "cands: [{stmt: nope"},
		"bad expect":          {"expect: {cand: allowed}", "expect: {cand: maybe}"},
		"expect non-cand":     {"expect: {cand: allowed}", "expect: {ref: same}"},
		"bad statement kind":  {`cand: "new:ListT"`, `cand: "head:ListT"`},
		"bad alias name":      {"      cand: \"new:ListT\"", "      Cand-1: \"new:ListT\""},
		"bad pair id":         {"id: p1", "id: ../p1"},
		"bad search_path":     {`search_path: "t_x, public"`, `search_path: "t_x; DROP"`},
		"unknown field":       {"key: [k]", "key: [k]\n        colums: [a]"},
		"tie_order no key":    {"key: [k]", "key: []"},
		"file escapes":        {`ref: "file:ref.sql"`, `ref: "file:../ref.sql"`},
		"require not allowed": {"key: [k]", "key: [k]\n        require: [saturated_suffix]"},
		"sql param two stmts": {"ref: {stmt: ref, params: [1]}", "ref: {stmt: ref, params: [{sql: \"SELECT 1; COMMIT\"}]}"},
		"sql param writes":    {"ref: {stmt: ref, params: [1]}", "ref: {stmt: ref, params: [{sql: \"DELETE FROM t RETURNING 1\"}]}"},
		"sql param and more":  {"ref: {stmt: ref, params: [1]}", "ref: {stmt: ref, params: [{sql: \"SELECT 1\", raw: \"2\"}]}"},
	} {
		body := strings.Replace(base, edit[0], edit[1], 1)
		if body == base {
			t.Fatalf("%s: edit did not apply", name)
		}
		if _, err := planguard.LoadDiffSpec(mapFS(map[string]string{"d.yaml": body, "ref.sql": "SELECT 1"}), "d.yaml"); err == nil {
			t.Errorf("%s: LoadDiffSpec accepted\n%s", name, body)
		}
	}
	// A data-modifying statement is refused before anything runs.
	if _, err := planguard.LoadDiffSpec(mapFS(map[string]string{"d.yaml": base, "ref.sql": "INSERT INTO t VALUES (1)"}), "d.yaml"); err == nil {
		t.Error("LoadDiffSpec accepted an INSERT statement")
	}
	// Statements go over the simple protocol, where a second statement (a COMMIT) would leave the READ ONLY
	// transaction: more than one statement is refused. A ';' in a literal or comment, or one trailing, is fine.
	if _, err := planguard.LoadDiffSpec(mapFS(map[string]string{"d.yaml": base, "ref.sql": "SELECT 1; COMMIT; SELECT 2"}), "d.yaml"); err == nil {
		t.Error("LoadDiffSpec accepted two statements")
	}
	if _, err := planguard.LoadDiffSpec(mapFS(map[string]string{"d.yaml": base, "ref.sql": "SELECT ';' AS a, $$;$$ AS b -- ;\n;\n"}), "d.yaml"); err != nil {
		t.Errorf("';' in literals and comments: %v", err)
	}
	ok := strings.Replace(base, "ref: {stmt: ref, params: [1]}", "ref: {stmt: ref, params: [{sql: \"SELECT 1\"}]}", 1)
	if _, err := planguard.LoadDiffSpec(mapFS(map[string]string{"d.yaml": ok, "ref.sql": "SELECT 1"}), "d.yaml"); err != nil {
		t.Errorf("a {sql: …} param: %v", err)
	}
}

// The committed diff cases load, and every statement they name exists.
func TestRepoDiffCasesLoad(t *testing.T) {
	root := "../../db/planguard"
	files, err := filepath.Glob(filepath.Join(root, "diff", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no diff cases: %v", err)
	}
	sqlc := repoSQLC(t)
	for _, f := range files {
		spec, err := planguard.LoadDiffSpec(dirFS(root), "diff/"+filepath.Base(f))
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range spec.Batches {
			for alias, src := range b.Statements {
				// old: names come from the --old-ref build and may be gone from this one; the run checks them.
				kind, name, _ := strings.Cut(src, ":")
				if kind == "new" && sqlc[name] == "" {
					t.Errorf("%s: %s: %s names no sqlc statement", f, alias, src)
				}
			}
		}
	}
	// No plan IDs or deployment identifiers in the diff cases or the overlays either.
	for _, dir := range []string{"diff", "overlay"} {
		paths, _ := filepath.Glob(filepath.Join(root, dir, "*"))
		if len(paths) == 0 {
			t.Errorf("nothing under %s/%s", root, dir)
		}
		for _, p := range paths {
			body, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			for i, line := range strings.Split(string(body), "\n") {
				if m := planIDs.FindString(line); m != "" {
					t.Errorf("%s:%d holds the plan ID %q", p, i+1, m)
				}
				for _, id := range deploymentIDs(line) {
					t.Errorf("%s:%d holds the deployment identifier %s", p, i+1, id)
				}
			}
		}
	}
}
