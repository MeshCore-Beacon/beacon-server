// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DiffSpec is a diff case file: batches of statement pairs whose results are compared on one snapshot.
type DiffSpec struct {
	Batches []DiffBatch `yaml:"batches"`
}

// DiffBatch runs in one REPEATABLE READ READ ONLY transaction, so every statement in it sees the same data.
type DiffBatch struct {
	Name       string            `yaml:"name"`
	SearchPath string            `yaml:"search_path"`
	Statements map[string]string `yaml:"statements"` // alias → "old:<sqlc name>" | "new:<sqlc name>" | "file:<path>"
	Pairs      []DiffPair        `yaml:"pairs"`
}

// Call is one execution of an aliased statement.
type Call struct {
	Stmt   string `yaml:"stmt"`
	Params []any  `yaml:"params"`
}

// SaturatedSpec names the ref columns that say a legacy scan was saturated (flag = t) and where its scan stopped
// (floor). Rows below the floor are the ones the legacy statement may have got wrong.
type SaturatedSpec struct {
	Flag  string `yaml:"flag"`
	Floor string `yaml:"floor"`
}

// TieCursor names the cand's own page cursor: the params holding its timestamp and hash bound (1-based) and the
// result column holding the hash.
type TieCursor struct {
	TS         int    `yaml:"ts"`
	Hash       int    `yaml:"hash"`
	HashColumn string `yaml:"hash_column"`
}

// DiffPair compares one ref result with one or more cand results.
//
// Every ref column is compared. Each relaxation is a named class that must be listed in Allowed:
//   - tie_order: within each run of equal Key, cand rows are a permutation of ref rows (same count, keys equal
//     position by position);
//   - saturated_suffix: when ref row 1 has Saturated.Flag = t, ref rows whose Key[0] is below Saturated.Floor may
//     be missing from or differ in cand; the only class that relaxes the row count, and only by those rows. The
//     cand rows in their place must be below the floor too, keep the descending key order, and repeat no row;
//   - latest_observer_tie: the Omit columns may differ; each such row is listed;
//   - pagecursor_tie_rows: cand may start with k rows ref lacks whose Key[0], truncated to the millisecond, equals
//     the millisecond of ref param TieBoundary, and which sort at or below the cand's own cursor (TieCursor:
//     (Key[0], hash) ≤ (ts, hash bound)); then cand[k:] must equal ref[:len(ref)-k].
//
// Require lists classes that must fire for every cand: a case that exists to show a class shows it, or fails.
// Param indexes (TieBoundary, TieCursor, Walk.LimitParam, Walk.Cursor[].Param) are 1-based, like $n.
type DiffPair struct {
	ID          string            `yaml:"id"`
	Ref         Call              `yaml:"ref"`
	Cands       []Call            `yaml:"cands"`
	Key         []string          `yaml:"key"`
	Allowed     []string          `yaml:"allowed"`
	Omit        []string          `yaml:"omit"`
	Saturated   *SaturatedSpec    `yaml:"saturated"`
	TieBoundary *int              `yaml:"tie_boundary"`
	TieCursor   *TieCursor        `yaml:"tie_cursor"`
	Require     []string          `yaml:"require"`
	Walk        *WalkSpec         `yaml:"walk"`
	Expect      map[string]string `yaml:"expect"` // cand alias → identical | allowed | same | different; default same
}

// Outcome is the comparison of one ref result with one cand result.
type Outcome struct {
	Verdict string   // identical | allowed | different
	Classes []string // allowed classes that fired, as "name(count)"
	Detail  string   // first differing row, missing/extra rows, column names; the rows a class relaxed
}

// Diff verdicts and classes.
const (
	DiffIdentical = "identical"
	DiffAllowed   = "allowed"
	DiffDifferent = "different"
	DiffSkip      = "skip" // a {sql: …} param found no value on this snapshot; the pair did not run

	ClassTieOrder          = "tie_order"
	ClassSaturatedSuffix   = "saturated_suffix"
	ClassLatestObserverTie = "latest_observer_tie"
	ClassPageCursorTieRows = "pagecursor_tie_rows"
)

// classOrder is the order classes are reported in.
var classOrder = []string{ClassPageCursorTieRows, ClassSaturatedSuffix, ClassTieOrder, ClassLatestObserverTie}

var (
	aliasRe       = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	validExpect   = map[string]bool{DiffIdentical: true, DiffAllowed: true, "same": true, DiffDifferent: true}
	defaultExpect = "same"
)

// LoadDiffSpec reads the diff case file p of fsys and parses it with ParseDiffSpec; file: paths are relative to
// fsys's root.
func LoadDiffSpec(fsys fs.FS, p string) (*DiffSpec, error) {
	raw, err := fs.ReadFile(fsys, p)
	if err != nil {
		return nil, err
	}
	return ParseDiffSpec(raw, p, fsys)
}

// ParseDiffSpec parses and validates a diff case file named p. Unknown fields are errors; file: statements must
// exist in fsys and must not modify data (the batch is READ ONLY as well).
func ParseDiffSpec(raw []byte, p string, fsys fs.FS) (*DiffSpec, error) {
	var spec DiffSpec
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&spec); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s: empty diff spec", p)
		}
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: more than one YAML document", p)
	}
	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	for _, b := range spec.Batches {
		for _, alias := range sortedKeys(b.Statements) {
			kind, rest, _ := strings.Cut(b.Statements[alias], ":")
			if kind != "file" {
				continue
			}
			text, err := fs.ReadFile(fsys, rest)
			if err != nil {
				return nil, fmt.Errorf("%s: batch %s: %s: %w", p, b.Name, alias, err)
			}
			if err := checkSingleSelect(string(text)); err != nil {
				return nil, fmt.Errorf("%s: batch %s: %s: %w", p, b.Name, alias, err)
			}
		}
	}
	return &spec, nil
}

// Validate checks every batch and pair.
func (s *DiffSpec) Validate() error {
	if len(s.Batches) == 0 {
		return errors.New("no batches")
	}
	names := map[string]bool{}
	for _, b := range s.Batches {
		if err := checkFileName(b.Name); err != nil {
			return fmt.Errorf("batch name: %w", err)
		}
		if names[b.Name] {
			return fmt.Errorf("batch %s: duplicate name", b.Name)
		}
		names[b.Name] = true
		if err := b.validate(); err != nil {
			return fmt.Errorf("batch %s: %w", b.Name, err)
		}
	}
	return nil
}

func checkFileName(name string) error {
	if err := CheckName(name); err != nil {
		return err
	}
	if strings.HasPrefix(name, ".") {
		return fmt.Errorf("%q must not start with '.'", name)
	}
	return nil
}

func (b *DiffBatch) validate() error {
	if err := checkSearchPath(b.SearchPath); err != nil {
		return err
	}
	if len(b.Statements) == 0 {
		return errors.New("no statements")
	}
	for _, alias := range sortedKeys(b.Statements) {
		if !aliasRe.MatchString(alias) {
			return fmt.Errorf("statement alias %q must match %s", alias, aliasRe)
		}
		kind, rest, ok := strings.Cut(b.Statements[alias], ":")
		switch {
		case !ok || rest == "":
			return fmt.Errorf("statement %s: %q, want old:<sqlc name>, new:<sqlc name> or file:<path>", alias, b.Statements[alias])
		case kind == "old" || kind == "new":
			if !plainIdentRe.MatchString(rest) {
				return fmt.Errorf("statement %s: %q is not a sqlc name", alias, rest)
			}
		case kind == "file":
			if !fs.ValidPath(rest) {
				return fmt.Errorf("statement %s: %q is not a relative path inside the files directory", alias, rest)
			}
		default:
			return fmt.Errorf("statement %s: unknown source %q (want old, new or file)", alias, kind)
		}
	}
	if len(b.Pairs) == 0 {
		return errors.New("no pairs")
	}
	ids := map[string]bool{}
	for i := range b.Pairs {
		p := &b.Pairs[i]
		if ids[p.ID] {
			return fmt.Errorf("pair %s: duplicate id", p.ID)
		}
		ids[p.ID] = true
		if err := p.Validate(); err != nil {
			return err
		}
		if err := p.checkCalls(b.Statements); err != nil {
			return fmt.Errorf("pair %s: %w", p.ID, err)
		}
	}
	return nil
}

// Validate checks one pair on its own: its id, its classes and the fields each class needs, its walk and its
// expectations. Compare refuses a pair that fails it.
func (p DiffPair) Validate() error {
	if err := checkFileName(p.ID); err != nil {
		return fmt.Errorf("pair id: %w", err)
	}
	fail := func(format string, a ...any) error {
		return fmt.Errorf("pair %s: "+format, append([]any{p.ID}, a...)...)
	}
	allowed := map[string]bool{}
	for _, a := range p.Allowed {
		if !slices.Contains(classOrder, a) {
			return fail("unknown class %q (want %s)", a, strings.Join(classOrder, ", "))
		}
		allowed[a] = true
	}
	if slices.Contains(p.Key, "") {
		return fail("empty key column")
	}
	if allowed[ClassTieOrder] && len(p.Key) == 0 {
		return fail("tie_order needs key")
	}
	if (len(p.Omit) > 0) != allowed[ClassLatestObserverTie] {
		return fail("omit and the class latest_observer_tie go together (omit %v, allowed %v)", p.Omit, p.Allowed)
	}
	for _, c := range p.Omit {
		if slices.Contains(p.Key, c) {
			return fail("omit column %s is a key column", c)
		}
	}
	if (p.Saturated != nil) != allowed[ClassSaturatedSuffix] {
		return fail("saturated and the class saturated_suffix go together")
	}
	if p.Saturated != nil && (p.Saturated.Flag == "" || p.Saturated.Floor == "" || len(p.Key) == 0) {
		return fail("saturated needs flag, floor and a key")
	}
	if (p.TieBoundary != nil) != allowed[ClassPageCursorTieRows] || (p.TieCursor != nil) != allowed[ClassPageCursorTieRows] {
		return fail("tie_boundary, tie_cursor and the class pagecursor_tie_rows go together")
	}
	if tc := p.TieCursor; tc != nil && (tc.TS < 1 || tc.Hash < 1 || tc.TS == tc.Hash || tc.HashColumn == "") {
		return fail("tie_cursor needs two different param numbers (ts, hash) and hash_column")
	}
	for _, c := range p.Require {
		if !allowed[c] {
			return fail("require %s: not an allowed class", c)
		}
	}
	if p.TieBoundary != nil && (*p.TieBoundary < 1 || len(p.Key) == 0) {
		return fail("tie_boundary %d must be a param number ($1 = 1) and needs a key", *p.TieBoundary)
	}
	for alias, want := range p.Expect {
		if !validExpect[want] {
			return fail("expect %s: %q (want identical, allowed, same or different)", alias, want)
		}
	}
	if p.Walk != nil {
		if err := p.Walk.validate(p); err != nil {
			return fail("walk: %v", err)
		}
	}
	return nil
}

// checkCalls checks the pair's statements, params and expectations against the batch.
func (p DiffPair) checkCalls(stmts map[string]string) error {
	if p.Ref.Stmt == "" {
		return errors.New("no ref statement")
	}
	if len(p.Cands) == 0 {
		return errors.New("no cands")
	}
	cands := map[string]bool{}
	for _, c := range p.Cands {
		if cands[c.Stmt] {
			return fmt.Errorf("cand %s is listed twice", c.Stmt)
		}
		cands[c.Stmt] = true
	}
	for _, c := range append([]Call{p.Ref}, p.Cands...) {
		if _, ok := stmts[c.Stmt]; !ok {
			return fmt.Errorf("statement %q is not in the batch", c.Stmt)
		}
		for _, v := range c.Params {
			if err := checkDiffParam(v); err != nil {
				return fmt.Errorf("%s: %w", c.Stmt, err)
			}
		}
	}
	for alias := range p.Expect {
		if !cands[alias] {
			return fmt.Errorf("expect names %s, which is not a cand", alias)
		}
	}
	if p.TieBoundary != nil && *p.TieBoundary > len(p.Ref.Params) {
		return fmt.Errorf("tie_boundary $%d: the ref has %d params", *p.TieBoundary, len(p.Ref.Params))
	}
	if tc := p.TieCursor; tc != nil {
		for _, c := range p.Cands {
			if max(tc.TS, tc.Hash) > len(c.Params) {
				return fmt.Errorf("tie_cursor: %s has %d params", c.Stmt, len(c.Params))
			}
		}
	}
	if p.Walk != nil {
		for _, c := range append([]Call{p.Ref}, p.Cands...) {
			if err := p.Walk.checkParams(len(c.Params)); err != nil {
				return fmt.Errorf("walk: %s: %w", c.Stmt, err)
			}
		}
	}
	return nil
}

// checkDiffParam accepts what renderParam renders without a corpus: scalars, lists, {raw: …} and {sql: …}, a
// single read-only SELECT resolved inside the batch's snapshot (no row or NULL: the pair is skipped).
func checkDiffParam(v any) error {
	switch x := v.(type) {
	case nil, string, int, int64, uint64, float64, bool:
		return nil
	case []any:
		for _, y := range x {
			if _, isMap := y.(map[string]any); isMap {
				return errors.New("a list element must be a plain value")
			}
			if err := checkDiffParam(y); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if _, ok := x["raw"].(string); ok && len(x) == 1 {
			return nil
		}
		if q, ok := x["sql"].(string); ok && len(x) == 1 {
			return checkSingleSelect(q)
		}
		return fmt.Errorf("param %v: want a value, a list, {raw: …} or {sql: …}", x)
	}
	return fmt.Errorf("unsupported param type %T", v)
}

// checkSingleSelect refuses text that is more than one statement or that modifies data. Statements are sent over
// the simple protocol, where a second statement (a COMMIT, say) would run outside the READ ONLY transaction.
func checkSingleSelect(sql string) error {
	if err := checkStatementText(sql); err != nil {
		return err
	}
	if kw := dataModifying(sql); kw != "" {
		return fmt.Errorf("runs %s; diff statements must not modify data", kw)
	}
	return nil
}

// paramText is a param's value as text, the way it is sent before the server types it; res holds the values of
// {sql: …} params.
func paramText(v any, res map[string]*string) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case map[string]any:
		if raw, ok := x["raw"].(string); ok {
			return raw
		}
		if key, _, ok := resolverRef(&Corpus{}, x); ok && res[key] != nil {
			return *res[key]
		}
		return ""
	}
	lit, _, err := renderParam(&Corpus{}, v, nil, nil)
	if err != nil {
		return ""
	}
	if s, ok := unquoteLit(lit); ok {
		return s
	}
	return lit
}

// cell is one value as text for messages.
func cell(v *string) string {
	if v == nil {
		return "NULL"
	}
	s := *v
	if len(s) > 60 {
		s = s[:60] + "…"
	}
	return strconv.Quote(s)
}

// colPlan maps a pair's columns to ref and cand positions.
type colPlan struct {
	names            []string // compared columns, in ref order
	ref, cand        []int
	omit             []string
	omitRef, omitCnd []int
	keyRef, keyCand  []int
	extra            []string // cand columns not in ref
}

func indexOf(cols []string) (map[string]int, error) {
	m := map[string]int{}
	for i, c := range cols {
		if _, dup := m[c]; dup {
			return nil, fmt.Errorf("column %s appears twice", c)
		}
		m[c] = i
	}
	return m, nil
}

func planColumns(ref, cand RowSet, p DiffPair) (*colPlan, error) {
	ri, err := indexOf(ref.Columns)
	if err != nil {
		return nil, fmt.Errorf("ref: %w", err)
	}
	ci, err := indexOf(cand.Columns)
	if err != nil {
		return nil, fmt.Errorf("cand: %w", err)
	}
	cp := &colPlan{omit: p.Omit}
	omitted := map[string]bool{}
	for _, c := range p.Omit {
		r, okR := ri[c]
		d, okC := ci[c]
		if !okR || !okC {
			return nil, fmt.Errorf("omit column %s is not in both results", c)
		}
		omitted[c] = true
		cp.omitRef, cp.omitCnd = append(cp.omitRef, r), append(cp.omitCnd, d)
	}
	// The saturation columns describe the legacy scan, not the row; a cand without them is not missing a value.
	control := map[string]bool{}
	if p.Saturated != nil {
		for _, c := range []string{p.Saturated.Flag, p.Saturated.Floor} {
			if _, ok := ri[c]; !ok {
				return nil, fmt.Errorf("saturated column %s is not in ref", c)
			}
			if _, ok := ci[c]; !ok {
				control[c] = true
			}
		}
	}
	var missing []string
	for i, c := range ref.Columns {
		if omitted[c] || control[c] {
			continue
		}
		j, ok := ci[c]
		if !ok {
			missing = append(missing, c)
			continue
		}
		cp.names, cp.ref, cp.cand = append(cp.names, c), append(cp.ref, i), append(cp.cand, j)
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("cand lacks the ref columns %s", strings.Join(missing, ", "))
	}
	for _, c := range cand.Columns {
		if _, ok := ri[c]; !ok {
			cp.extra = append(cp.extra, c)
		}
	}
	for _, k := range p.Key {
		r, okR := ri[k]
		d, okC := ci[k]
		if !okR || !okC || omitted[k] {
			return nil, fmt.Errorf("key column %s is not a compared column of both results", k)
		}
		cp.keyRef, cp.keyCand = append(cp.keyRef, r), append(cp.keyCand, d)
	}
	return cp, nil
}

// tuple encodes the values at idx unambiguously (NULL apart from any text).
func tuple(row []*string, idx []int) string {
	var b strings.Builder
	for _, i := range idx {
		if row[i] == nil {
			b.WriteString("N;")
			continue
		}
		fmt.Fprintf(&b, "S%d:%s;", len(*row[i]), *row[i])
	}
	return b.String()
}

func (cp *colPlan) keyText(row []*string, idx []int) string {
	parts := make([]string, len(idx))
	for n, i := range idx {
		v := "NULL"
		if row[i] != nil {
			v = *row[i]
		}
		parts[n] = cp.keyName(n) + "=" + v
	}
	return strings.Join(parts, ", ")
}

func (cp *colPlan) keyName(n int) string {
	for c, i := range cp.ref {
		if i == cp.keyRef[n] {
			return cp.names[c]
		}
	}
	return "?"
}

// rowLabel names a ref or cand row in a message: its key, else its first compared value.
func (cp *colPlan) rowLabel(row []*string, isRef bool) string {
	if len(cp.keyRef) > 0 {
		if isRef {
			return "(" + cp.keyText(row, cp.keyRef) + ")"
		}
		return "(" + cp.keyText(row, cp.keyCand) + ")"
	}
	if len(cp.names) == 0 {
		return "()"
	}
	idx := cp.ref[0]
	if !isRef {
		idx = cp.cand[0]
	}
	return "(" + cp.names[0] + "=" + cell(row[idx]) + ")"
}

// parseTime reads a timestamptz in PostgreSQL's ISO text output.
func parseTime(s string) (time.Time, bool) {
	for _, l := range []string{"2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999-07:00:00", time.RFC3339Nano} {
		if t, err := time.Parse(l, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func cellTime(v *string) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	return parseTime(*v)
}

// Compare compares a ref result with a cand result under the pair's classes. refParams and candParams are the
// calls' params as text (pagecursor_tie_rows reads both cursors from them). An invalid pair, or columns that do not
// line up, is different: nothing is relaxed that the pair does not name.
func Compare(ref, cand RowSet, p DiffPair, refParams, candParams []string) Outcome {
	diff := func(format string, a ...any) Outcome {
		return Outcome{Verdict: DiffDifferent, Detail: fmt.Sprintf(format, a...)}
	}
	if err := p.Validate(); err != nil {
		return diff("invalid pair: %v", err)
	}
	cp, err := planColumns(ref, cand, p)
	if err != nil {
		return diff("%v", err)
	}
	allowed := map[string]bool{}
	for _, a := range p.Allowed {
		allowed[a] = true
	}
	rt := make([]string, len(ref.Rows))
	for i, r := range ref.Rows {
		rt[i] = tuple(r, cp.ref)
	}
	ct := make([]string, len(cand.Rows))
	for i, r := range cand.Rows {
		ct[i] = tuple(r, cp.cand)
	}
	counts := map[string]int{}
	var notes []string

	// The aligned ranges: ref rows [r0, r1) against cand rows [c0, c1).
	r0, r1, c0, c1 := 0, len(rt), 0, len(ct)

	if allowed[ClassPageCursorTieRows] {
		if k, ms := leadTieRows(cand, rt, ct, cp, p, refParams, candParams); k > 0 && len(ct) == len(rt) {
			c0, r1 = k, len(rt)-k
			counts[ClassPageCursorTieRows] = k
			notes = append(notes, fmt.Sprintf("pagecursor_tie_rows: cand rows 1..%d are at the cursor's millisecond %s and not in ref", k, ms))
		}
	}

	if allowed[ClassSaturatedSuffix] && r1 > r0 {
		if cut, floor, floorT, ok := saturatedCut(ref, cp, p, r0, r1); ok && cut < r1 {
			n := cut - r0 // ref rows that must match
			if c1-c0 < n || c1-c0 > r1-r0 {
				return diff("row count: ref %d, cand %d; saturated_suffix relaxes only the %d ref rows below the floor %s",
					r1-r0, c1-c0, r1-cut, floor)
			}
			if msg := relaxedTail(cand, ct, cp, c0, c0+n, c1, floorT); msg != "" {
				return diff("saturated_suffix: %s (floor %s)", msg, floor)
			}
			changed := 0
			for i := 0; i < r1-cut; i++ {
				if c0+n+i >= c1 || rt[cut+i] != ct[c0+n+i] {
					changed++
				}
			}
			if changed > 0 {
				counts[ClassSaturatedSuffix] = changed
				notes = append(notes, fmt.Sprintf("saturated_suffix: %d of ref rows %d..%d (below the floor %s) are missing from or differ in cand",
					changed, cut+1, r1, floor))
			}
			r1, c1 = cut, c0+n
		}
	}

	if r1-r0 != c1-c0 {
		return diff("row count: ref %d, cand %d%s", r1-r0, c1-c0, membership(ref, cand, rt[r0:r1], ct[c0:c1], cp, r0, c0))
	}

	// Pair ref rows with cand rows: by position, or within tie runs when the order differs only there.
	pairs := make([]int, r1-r0)
	first := -1
	for i := range pairs {
		pairs[i] = c0 + i
		if first < 0 && rt[r0+i] != ct[c0+i] {
			first = i
		}
	}
	if first >= 0 {
		moved, ok := 0, false
		if allowed[ClassTieOrder] {
			moved, ok = tiePairs(ref, cand, rt, ct, cp, r0, c0, pairs)
		}
		if !ok {
			return diff("first difference at row %d %s: %s%s", r0+first+1, cp.rowLabel(ref.Rows[r0+first], true),
				columnDiffs(ref.Rows[r0+first], cand.Rows[c0+first], cp), membership(ref, cand, rt[r0:r1], ct[c0:c1], cp, r0, c0))
		}
		counts[ClassTieOrder] = moved
		notes = append(notes, fmt.Sprintf("tie_order: %d rows hold another position within their run of equal %s", moved, strings.Join(p.Key, ", ")))
	}

	if len(cp.omit) > 0 {
		var listed []string
		for i, j := range pairs {
			rr, cr := ref.Rows[r0+i], cand.Rows[j]
			var cols []string
			for n, c := range cp.omit {
				if tuple(rr, cp.omitRef[n:n+1]) != tuple(cr, cp.omitCnd[n:n+1]) {
					cols = append(cols, fmt.Sprintf("%s ref %s, cand %s", c, cell(rr[cp.omitRef[n]]), cell(cr[cp.omitCnd[n]])))
				}
			}
			if len(cols) > 0 {
				listed = append(listed, fmt.Sprintf("row %d %s: %s", r0+i+1, cp.rowLabel(rr, true), strings.Join(cols, "; ")))
			}
		}
		if len(listed) > 0 {
			counts[ClassLatestObserverTie] = len(listed)
			notes = append(notes, "latest_observer_tie: "+strings.Join(listed, " | "))
		}
	}

	if len(cp.extra) > 0 {
		notes = append(notes, "cand adds the columns "+strings.Join(cp.extra, ", "))
	}
	o := Outcome{Verdict: DiffIdentical, Detail: strings.Join(notes, "; ")}
	for _, c := range classOrder {
		if n := counts[c]; n > 0 {
			o.Classes = append(o.Classes, fmt.Sprintf("%s(%d)", c, n))
		}
	}
	if len(o.Classes) > 0 {
		o.Verdict = DiffAllowed
	}
	return o
}

// leadTieRows counts the leading cand rows that ref lacks, whose first key, truncated to the millisecond, is the
// millisecond of the ref's cursor param, and which sort at or below the cand's own cursor (ts, hash bound).
func leadTieRows(cand RowSet, rt, ct []string, cp *colPlan, p DiffPair, refParams, candParams []string) (int, string) {
	boundary, tc := *p.TieBoundary, p.TieCursor
	if boundary > len(refParams) || max(tc.TS, tc.Hash) > len(candParams) {
		return 0, ""
	}
	b, ok := parseTime(refParams[boundary-1])
	if !ok {
		return 0, ""
	}
	curTS, ok := parseTime(candParams[tc.TS-1])
	if !ok {
		return 0, ""
	}
	curHash := strings.ToLower(candParams[tc.Hash-1])
	hi := slices.Index(cand.Columns, tc.HashColumn)
	if hi < 0 {
		return 0, ""
	}
	ms := b.Truncate(time.Millisecond)
	inRef := map[string]bool{}
	for _, t := range rt {
		inRef[t] = true
	}
	k := 0
	for k < len(ct) && !inRef[ct[k]] {
		t, ok := cellTime(cand.Rows[k][cp.keyCand[0]])
		if !ok || !t.Truncate(time.Millisecond).Equal(ms) {
			break
		}
		h := cand.Rows[k][hi]
		if t.After(curTS) || t.Equal(curTS) && (h == nil || strings.ToLower(*h) > curHash) {
			break // above the cand's own cursor: no page from that cursor can hold it
		}
		k++
	}
	return k, ms.UTC().Format("2006-01-02 15:04:05.000Z")
}

// relaxedTail checks the cand rows [from, to) that stand in for ref rows below the floor: each is below the floor,
// they keep the descending key order from the cand row before them, and none repeats a matched row or another tail
// row (rows [c0, from) are the matched prefix). It returns why they do not, or "".
func relaxedTail(cand RowSet, ct []string, cp *colPlan, c0, from, to int, floor time.Time) string {
	seen := map[string]bool{}
	for j := c0; j < from; j++ {
		seen[ct[j]] = true
	}
	var prev time.Time
	havePrev := false
	if from > c0 {
		prev, havePrev = cellTime(cand.Rows[from-1][cp.keyCand[0]])
	}
	for j := from; j < to; j++ {
		row := cand.Rows[j]
		t, ok := cellTime(row[cp.keyCand[0]])
		switch {
		case !ok || !t.Before(floor):
			return fmt.Sprintf("cand row %d %s stands in for a ref row below the floor but is not below it", j+1, cp.rowLabel(row, false))
		case havePrev && t.After(prev):
			return fmt.Sprintf("cand row %d %s is newer than the row before it", j+1, cp.rowLabel(row, false))
		case seen[ct[j]]:
			return fmt.Sprintf("cand row %d %s repeats an earlier row", j+1, cp.rowLabel(row, false))
		}
		seen[ct[j]], prev, havePrev = true, t, true
	}
	return ""
}

// saturatedCut returns the first ref row of the trailing run below the floor, when ref row 1 is saturated.
func saturatedCut(ref RowSet, cp *colPlan, p DiffPair, r0, r1 int) (int, string, time.Time, bool) {
	fi := slices.Index(ref.Columns, p.Saturated.Flag)
	li := slices.Index(ref.Columns, p.Saturated.Floor)
	if len(ref.Rows) == 0 {
		return 0, "", time.Time{}, false
	}
	first := ref.Rows[0]
	if first[fi] == nil || *first[fi] != "t" {
		return 0, "", time.Time{}, false
	}
	floor, ok := cellTime(first[li])
	if !ok {
		return 0, "", time.Time{}, false
	}
	cut := r1
	for cut > r0 {
		t, ok := cellTime(ref.Rows[cut-1][cp.keyRef[0]])
		if !ok || !t.Before(floor) {
			break
		}
		cut--
	}
	return cut, *first[li], floor, true
}

// tiePairs checks that the keys are equal position by position and that each run of equal keys holds the same
// rows; it fills pairs (ref position → cand row) and returns how many rows moved.
func tiePairs(ref, cand RowSet, rt, ct []string, cp *colPlan, r0, c0 int, pairs []int) (int, bool) {
	n := len(pairs)
	for i := 0; i < n; i++ {
		if tuple(ref.Rows[r0+i], cp.keyRef) != tuple(cand.Rows[c0+i], cp.keyCand) {
			return 0, false
		}
	}
	moved := 0
	for start := 0; start < n; {
		end := start + 1
		k := tuple(ref.Rows[r0+start], cp.keyRef)
		for end < n && tuple(ref.Rows[r0+end], cp.keyRef) == k {
			end++
		}
		free := map[string][]int{}
		for j := start; j < end; j++ {
			free[ct[c0+j]] = append(free[ct[c0+j]], c0+j)
		}
		for i := start; i < end; i++ {
			// Prefer the cand row in the same position, then the first unused equal one.
			t := rt[r0+i]
			js := free[t]
			if len(js) == 0 {
				return 0, false
			}
			pick := 0
			if x := slices.Index(js, c0+i); x >= 0 {
				pick = x
			} else {
				moved++
			}
			pairs[i] = js[pick]
			free[t] = slices.Delete(js, pick, pick+1)
		}
		start = end
	}
	return moved, true
}

// columnDiffs names the compared columns that differ between two rows, with both values.
func columnDiffs(rr, cr []*string, cp *colPlan) string {
	var out []string
	for n, c := range cp.names {
		if tuple(rr, cp.ref[n:n+1]) != tuple(cr, cp.cand[n:n+1]) {
			out = append(out, fmt.Sprintf("%s ref %s, cand %s", c, cell(rr[cp.ref[n]]), cell(cr[cp.cand[n]])))
		}
	}
	return strings.Join(out, "; ")
}

// membership lists ref rows missing from cand and cand rows not in ref (up to five of each).
func membership(ref, cand RowSet, rt, ct []string, cp *colPlan, r0, c0 int) string {
	count := func(ts []string) map[string]int {
		m := map[string]int{}
		for _, t := range ts {
			m[t]++
		}
		return m
	}
	inC, inR := count(ct), count(rt)
	list := func(ts []string, other map[string]int, rows [][]*string, off int, isRef bool) (string, int) {
		var labels []string
		n := 0
		for i, t := range ts {
			if other[t] > 0 {
				other[t]--
				continue
			}
			n++
			if len(labels) < 5 {
				labels = append(labels, fmt.Sprintf("row %d %s", off+i+1, cp.rowLabel(rows[off+i], isRef)))
			}
		}
		return strings.Join(labels, ", "), n
	}
	miss, nm := list(rt, inC, ref.Rows, r0, true)
	extra, ne := list(ct, inR, cand.Rows, c0, false)
	var out []string
	if nm > 0 {
		out = append(out, fmt.Sprintf("%d ref rows missing from cand: %s", nm, miss))
	}
	if ne > 0 {
		out = append(out, fmt.Sprintf("%d cand rows not in ref: %s", ne, extra))
	}
	if len(out) == 0 {
		return ""
	}
	return "; " + strings.Join(out, "; ")
}

// expectMet reports whether a verdict meets an expectation ("" means same).
func expectMet(want, got string) bool {
	if want == "" {
		want = defaultExpect
	}
	if want == "same" {
		return got == DiffIdentical || got == DiffAllowed
	}
	return want == got
}

// worse returns the stronger of two verdicts: different over allowed over identical.
func worse(a, b string) string {
	rank := map[string]int{DiffIdentical: 0, DiffAllowed: 1, DiffDifferent: 2}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

const (
	diffStmtPrefix = "planguard_d_"
	diffTimeoutS   = 600
)

// RunDiff runs every batch of spec on s and compares each pair's results. old: and new: statements take their text
// from oldSQL and newSQL (sqlc name → text), file: statements from fsys.
//
// Each batch is one BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY transaction (lock_timeout 1 s, statement_timeout
// 600 s). Every alias is PREPAREd as planguard_d_<alias> and EXECUTEd over the simple protocol, so values arrive as
// the server's text. Each result is written to <outDir>/<batch>/<pair>.<role>.<alias>.jsonl (walks:
// <pair>.walk<limit>.<role>.<alias>.jsonl) and read back for the comparison. The batch ends with ROLLBACK and a
// DEALLOCATE of each alias, as separate commands, on every path.
//
// walkLimits are the page sizes to walk the pairs that have a walk at; 0 stands for each walk's web_limit. The
// report is written to <outDir>/diff_report.md. The error is a batch that could not run; the report so far is
// returned with it.
func RunDiff(ctx context.Context, s Session, spec *DiffSpec, oldSQL, newSQL map[string]string, fsys fs.FS, outDir string, walkLimits []int) (*DiffReport, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	texts := make([]map[string]string, len(spec.Batches))
	for i, b := range spec.Batches {
		t, err := batchTexts(b, oldSQL, newSQL, fsys)
		if err != nil {
			return nil, fmt.Errorf("batch %s: %w", b.Name, err)
		}
		texts[i] = t
		for _, p := range b.Pairs {
			if p.Walk != nil && slices.Contains(walkLimits, 0) && p.Walk.WebLimit == 0 {
				return nil, fmt.Errorf("batch %s: pair %s: a web walk needs web_limit", b.Name, p.ID)
			}
		}
	}
	for _, l := range walkLimits {
		if l < 0 {
			return nil, fmt.Errorf("walk limit %d, want ≥ 1 (or 0 for web)", l)
		}
	}
	t0 := time.Now()
	rep := &DiffReport{StartedAt: t0.UTC()}
	var runErr error
	for i, b := range spec.Batches {
		br, err := runBatch(ctx, s, b, texts[i], outDir, walkLimits)
		rep.Batches = append(rep.Batches, br)
		if err != nil {
			runErr = fmt.Errorf("batch %s: %w", b.Name, err)
			break
		}
	}
	rep.DurationMs = msSince(t0)
	if err := os.MkdirAll(outDir, 0o755); err == nil {
		err = os.WriteFile(filepath.Join(outDir, "diff_report.md"), []byte(rep.Markdown()), 0o644)
		if err != nil && runErr == nil {
			runErr = err
		}
	} else if runErr == nil {
		runErr = err
	}
	return rep, runErr
}

// batchTexts returns alias → statement text.
func batchTexts(b DiffBatch, oldSQL, newSQL map[string]string, fsys fs.FS) (map[string]string, error) {
	out := map[string]string{}
	for _, alias := range sortedKeys(b.Statements) {
		kind, rest, _ := strings.Cut(b.Statements[alias], ":")
		var text string
		switch kind {
		case "old", "new":
			src := oldSQL
			if kind == "new" {
				src = newSQL
			}
			var ok bool
			if text, ok = src[rest]; !ok {
				return nil, fmt.Errorf("%s: no statement %s in the %s texts", alias, rest, kind)
			}
		case "file":
			raw, err := fs.ReadFile(fsys, rest)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", alias, err)
			}
			text = string(raw)
		}
		text = trimStmt(text)
		if Normalize(text) == "" {
			return nil, fmt.Errorf("%s: empty statement", alias)
		}
		if err := checkSingleSelect(text); err != nil {
			return nil, fmt.Errorf("%s: %w", alias, err)
		}
		out[alias] = text
	}
	return out, nil
}

// batchRun executes one batch's calls inside its transaction.
type batchRun struct {
	ctx context.Context
	s   Session
	b   DiffBatch
	dir string
	res map[string]*string // {sql: …} param values, resolved in the batch's snapshot
}

func runBatch(ctx context.Context, s Session, b DiffBatch, texts map[string]string, outDir string, walkLimits []int) (BatchReport, error) {
	t0 := time.Now()
	br := BatchReport{Name: b.Name}
	r := &batchRun{ctx: ctx, s: s, b: b, dir: filepath.Join(outDir, b.Name)}
	runErr := func() error {
		if err := os.MkdirAll(r.dir, 0o755); err != nil {
			return err
		}
		setup := []string{
			"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
			fmt.Sprintf("SET LOCAL statement_timeout = '%ds'; SET LOCAL lock_timeout = '1s'", diffTimeoutS),
			"SET LOCAL jit = off; SET LOCAL timezone = 'UTC'; SET LOCAL standard_conforming_strings = on; " +
				"SET LOCAL datestyle = 'ISO, MDY'; SET LOCAL intervalstyle = 'postgres'; SET LOCAL bytea_output = 'hex'; " +
				"SET LOCAL extra_float_digits = 1",
		}
		if b.SearchPath != "" {
			setup = append(setup, "SET LOCAL search_path = "+b.SearchPath)
		}
		for _, alias := range sortedKeys(texts) {
			setup = append(setup, "PREPARE "+diffStmtPrefix+alias+" AS\n"+texts[alias])
		}
		for _, l := range setup {
			if _, err := s.Exec(ctx, l); err != nil {
				return fmt.Errorf("%s: %s", firstLine(l), errText(err))
			}
		}
		if err := r.resolve(); err != nil {
			return err
		}
		for _, p := range b.Pairs {
			prs, err := r.pair(p, walkLimits)
			br.Pairs = append(br.Pairs, prs...)
			if err != nil {
				return fmt.Errorf("pair %s: %w", p.ID, err)
			}
		}
		return nil
	}()
	// Prepared statements outlive ROLLBACK: end the transaction, then deallocate each alias, as separate commands.
	cctx, cancel := cleanupCtx(ctx)
	defer cancel()
	var cleanErr error
	if err := rollback(cctx, s); err != nil {
		cleanErr = fmt.Errorf("rollback: %w", err)
	}
	for _, alias := range sortedKeys(texts) {
		if _, err := s.Exec(cctx, "DEALLOCATE "+diffStmtPrefix+alias); err != nil && !isSQLState(err, "26000") && cleanErr == nil {
			cleanErr = fmt.Errorf("deallocate %s: %w", alias, err)
		}
	}
	br.DurationMs = msSince(t0)
	err := runErr
	if err == nil {
		err = cleanErr
	} else if cleanErr != nil {
		err = fmt.Errorf("%w (and %v)", runErr, cleanErr)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		br.Err = err.Error()
	}
	return br, err
}

// exec runs one call with params already rendered as literals.
func (r *batchRun) exec(alias string, lits []string) (RowSet, error) {
	call := "EXECUTE " + diffStmtPrefix + alias
	if len(lits) > 0 {
		call += "(" + strings.Join(lits, ", ") + ")"
	}
	res, err := r.s.Exec(r.ctx, call)
	if err != nil {
		return RowSet{}, fmt.Errorf("%s: %s", alias, errText(err))
	}
	if len(res) == 0 {
		return RowSet{}, fmt.Errorf("%s: no result", alias)
	}
	last := res[len(res)-1]
	rs := RowSet{Columns: make([]string, len(last.FieldDescriptions))}
	for i, fd := range last.FieldDescriptions {
		rs.Columns[i] = fd.Name
	}
	rs.Rows = make([][]*string, len(last.Rows))
	for i, row := range last.Rows {
		out := make([]*string, len(row))
		for j, v := range row {
			if v != nil {
				t := string(v)
				out[j] = &t
			}
		}
		rs.Rows[i] = out
	}
	return rs, nil
}

// resolve runs every {sql: …} param of the batch once, inside its snapshot. A resolver that fails stops the batch.
func (r *batchRun) resolve() error {
	r.res = map[string]*string{}
	var visit func(v any) error
	visit = func(v any) error {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		key, q, ok := resolverRef(&Corpus{}, m)
		if !ok || q == "" {
			return nil
		}
		if _, done := r.res[key]; done {
			return nil
		}
		res, err := r.s.Exec(r.ctx, resolverSelect(q))
		if err != nil {
			return fmt.Errorf("resolver %s: %s", firstLine(q), errText(err))
		}
		rows := lastRows(res)
		if len(rows) == 1 && len(rows[0]) == 1 && rows[0][0] != nil {
			v := string(rows[0][0])
			r.res[key] = &v
		} else {
			r.res[key] = nil
		}
		return nil
	}
	for _, p := range r.b.Pairs {
		params := slices.Clone(p.Ref.Params)
		for _, c := range p.Cands {
			params = append(params, c.Params...)
		}
		if p.Walk != nil {
			params = append(params, p.Walk.First...)
		}
		for _, v := range params {
			if err := visit(v); err != nil {
				return err
			}
		}
	}
	return nil
}

// renderLits renders params as literals; skip names a {sql: …} param that resolved to no value.
func (r *batchRun) renderLits(params []any) ([]string, string, error) {
	lits := make([]string, len(params))
	for i, v := range params {
		lit, skip, err := renderParam(&Corpus{}, v, r.res, nil)
		if skip != "" || err != nil {
			return nil, skip, err
		}
		lits[i] = lit
	}
	return lits, "", nil
}

// skipReason is why a pair cannot run on this snapshot: one of its {sql: …} params has no value.
func (r *batchRun) skipReason(p DiffPair) (string, error) {
	calls := append([]Call{p.Ref}, p.Cands...)
	if p.Walk != nil {
		calls = append(calls, Call{Stmt: "walk.first", Params: p.Walk.First})
	}
	for _, c := range calls {
		if _, skip, err := r.renderLits(c.Params); skip != "" || err != nil {
			return skip, err
		}
	}
	return "", nil
}

// roundTrip writes rs to its case file and reads it back, so every comparison reads what was written.
func (r *batchRun) roundTrip(name string, rs RowSet) (RowSet, error) {
	path := filepath.Join(r.dir, name+".jsonl")
	if err := WriteCaseFile(path, rs); err != nil {
		return RowSet{}, err
	}
	return ReadCaseFile(path)
}

func (r *batchRun) call(p DiffPair, role string, c Call) (RowSet, error) {
	lits, _, err := r.renderLits(c.Params)
	if err != nil {
		return RowSet{}, err
	}
	rs, err := r.exec(c.Stmt, lits)
	if err != nil {
		return RowSet{}, err
	}
	return r.roundTrip(fmt.Sprintf("%s.%s.%s", p.ID, role, c.Stmt), rs)
}

// fetcher pages through a call: the walk's cursor params from First or the cursor, its limit param limit+FetchExtra.
func (r *batchRun) fetcher(w *WalkSpec, c Call) (PageFetcher, error) {
	base, _, err := r.renderLits(c.Params)
	if err != nil {
		return nil, err
	}
	first, _, err := r.renderLits(w.First)
	if err != nil {
		return nil, err
	}
	return func(_ context.Context, cursor []string, limit int) (RowSet, error) {
		lits := slices.Clone(base)
		for i, cb := range w.Cursor {
			if cursor == nil {
				lits[cb.Param-1] = first[i]
			} else {
				lits[cb.Param-1] = quoteLit(cursor[i])
			}
		}
		lits[w.LimitParam-1] = strconv.Itoa(limit + w.FetchExtra)
		return r.exec(c.Stmt, lits)
	}, nil
}

// pair runs one pair: the ref, each cand and their comparison, then the walks.
func (r *batchRun) pair(p DiffPair, walkLimits []int) ([]PairResult, error) {
	skip, err := r.skipReason(p)
	if err != nil {
		return nil, err
	}
	if skip != "" {
		var out []PairResult
		for _, c := range p.Cands {
			out = append(out, PairResult{Batch: r.b.Name, Pair: p.ID, Ref: p.Ref.Stmt, Cand: c.Stmt, Verdict: DiffSkip,
				Skip: skip, Expect: expectOf(p, c.Stmt)})
		}
		return out, nil
	}
	ref, err := r.call(p, "ref", p.Ref)
	if err != nil {
		return nil, err
	}
	texts := func(params []any) []string {
		out := make([]string, len(params))
		for i, v := range params {
			out[i] = paramText(v, r.res)
		}
		return out
	}
	refParams := texts(p.Ref.Params)
	var out []PairResult
	for _, c := range p.Cands {
		cand, err := r.call(p, "cand", c)
		if err != nil {
			return out, err
		}
		o := Compare(ref, cand, p, refParams, texts(c.Params))
		out = append(out, PairResult{Batch: r.b.Name, Pair: p.ID, Ref: p.Ref.Stmt, Cand: c.Stmt,
			RefRows: len(ref.Rows), CandRows: len(cand.Rows), Outcome: o, Verdict: o.Verdict})
	}
	if p.Walk != nil {
		for _, l := range walkLimits {
			if l == 0 {
				l = p.Walk.WebLimit
			}
			if err := r.walks(p, l, out); err != nil {
				return out, fmt.Errorf("walk %d: %w", l, err)
			}
		}
	}
	for i := range out {
		out[i].Expect = expectOf(p, out[i].Cand)
		out[i].OK = expectMet(out[i].Expect, out[i].Verdict)
		for _, c := range p.Require {
			if !slices.ContainsFunc(out[i].Outcome.Classes, func(x string) bool { return strings.HasPrefix(x, c+"(") }) {
				out[i].OK = false
				out[i].Missing = append(out[i].Missing, c)
			}
		}
	}
	return out, nil
}

func expectOf(p DiffPair, cand string) string {
	if e := p.Expect[cand]; e != "" {
		return e
	}
	return defaultExpect
}

// walks walks the ref (or fetches the oracle) and every cand at one page size.
func (r *batchRun) walks(p DiffPair, limit int, out []PairResult) error {
	w := p.Walk
	cols := w.cursorColumns()
	maxRows := w.maxRows()
	tag := fmt.Sprintf("%s.walk%d", p.ID, limit)
	f, err := r.fetcher(w, p.Ref)
	if err != nil {
		return err
	}
	refLimit := limit
	if w.Oracle {
		refLimit = maxRows
	}
	ref, stuck, err := Walk(r.ctx, f, cols, refLimit, maxRows)
	if err != nil {
		return fmt.Errorf("ref %s: %w", p.Ref.Stmt, err)
	}
	if stuck != "" {
		return fmt.Errorf("ref %s: %s", p.Ref.Stmt, stuck)
	}
	if ref, err = r.roundTrip(tag+".ref."+p.Ref.Stmt, ref); err != nil {
		return err
	}
	for i, c := range p.Cands {
		f, err := r.fetcher(w, c)
		if err != nil {
			return err
		}
		cand, stuck, err := Walk(r.ctx, f, cols, limit, maxRows)
		if err != nil {
			return fmt.Errorf("cand %s: %w", c.Stmt, err)
		}
		if cand, err = r.roundTrip(tag+".cand."+c.Stmt, cand); err != nil {
			return err
		}
		wo, err := CompareWalk(ref, cand, cols)
		if err != nil {
			return fmt.Errorf("cand %s: %w", c.Stmt, err)
		}
		wo.Stuck = stuck
		out[i].Walks = append(out[i].Walks, WalkResult{Limit: limit, RefRows: len(ref.Rows), Outcome: wo})
		if !wo.Clean() {
			out[i].Verdict = worse(out[i].Verdict, DiffDifferent)
		}
	}
	return nil
}
