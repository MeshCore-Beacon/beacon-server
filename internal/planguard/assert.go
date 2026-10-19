// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// FailureKind names the rule a plan broke.
type FailureKind string

// Failure kinds.
const (
	FailDriver         FailureKind = "driver"
	FailIndexCond      FailureKind = "index_cond"
	FailForbidden      FailureKind = "forbidden"
	FailRequired       FailureKind = "required"
	FailSortAboveLimit FailureKind = "sort_above_limit"
	FailBudget         FailureKind = "budget"
	FailPlanChange     FailureKind = "plan_change"
	FailError          FailureKind = "error"
)

// Failure is one broken rule.
type Failure struct {
	Kind    FailureKind `json:"kind"`
	Msg     string      `json:"msg"`
	Indexes []string    `json:"indexes,omitempty"` // indexes of the scan(s) the failure is about
}

// Check is what Assess found in one plan.
type Check struct {
	Failures []Failure `json:"failures,omitempty"`
	Drivers  []string  `json:"drivers,omitempty"` // "relation(alias):node:index" as seen, sorted
	Retired  []string  `json:"retired,omitempty"` // retired indexes anywhere in the plan
	Buffers  int64     `json:"buffers"`           // root shared hit + read (ANALYZE only)
	Rows     int64     `json:"rows"`              // root actual rows × loops (ANALYZE only)
}

// defaultDriverNodes are the scan types a driver may use when its spec names none.
var defaultDriverNodes = []string{"Index Scan", "Index Only Scan"}

// passThrough nodes carry a Limit's bound down to a Sort below them.
var passThrough = []string{"Result", "Gather", "Gather Merge", "Subquery Scan", "Materialize", "Unique"}

type scanAt struct {
	n    Node
	path []Node
}

// Assess checks one plan against exp and the corpus defaults. Budgets are checked only when analyzed.
func (c *Corpus) Assess(ex *Explain, exp *Expect, analyzed bool) Check {
	var ch Check
	root := ex.Root
	if analyzed {
		ch.Buffers = sharedBlocks(root)
		ch.Rows = int64(root.num("Actual Rows") * root.num("Actual Loops"))
	}
	var scans []scanAt
	walk(root, func(n Node, path []Node) {
		if isScan(n.str("Node Type")) {
			scans = append(scans, scanAt{n, path})
		}
		if ix := n.str("Index Name"); ix != "" && slices.Contains(c.Defaults.RetiredIndexes, ix) && !slices.Contains(ch.Retired, ix) {
			ch.Retired = append(ch.Retired, ix)
		}
	})

	for _, d := range exp.Driver {
		c.assessDriver(&ch, d, scans)
	}
	c.assessForbidden(&ch, root, append(append([]NodeRule{}, c.Defaults.Forbid...), exp.Forbid...))
	assessRequired(&ch, root, exp.Require)
	if c.Defaults.SortAboveLimit {
		c.assessSortAboveLimit(&ch, root, c.sortExempt(exp))
	}
	if analyzed {
		assessBudget(&ch, ex, exp.Budget)
	}
	sort.Strings(ch.Drivers)
	return ch
}

func (c *Corpus) assessDriver(ch *Check, d DriverSpec, scans []scanAt) {
	nodes := d.Nodes
	if len(nodes) == 0 {
		nodes = defaultDriverNodes
	}
	label := d.Relation + aliasStr(d.Alias)
	if d.Scope != "" {
		label = d.Scope + ": " + label
	}
	var matched []Node
	for _, s := range scans {
		if s.n.str("Relation Name") != d.Relation || (d.Alias != "" && s.n.str("Alias") != d.Alias) {
			continue
		}
		if d.Scope != "" && !slices.ContainsFunc(s.path, func(a Node) bool { return a.str("Subplan Name") == d.Scope }) {
			continue
		}
		matched = append(matched, s.n)
	}
	if len(matched) == 0 {
		ch.Failures = append(ch.Failures, Failure{Kind: FailDriver, Msg: "driver: no scan of " + label})
		return
	}
	var matchedIxs []string
	for _, m := range matched {
		ixs := indexesOf(m)
		matchedIxs = appendNew(matchedIxs, ixs...)
		typ := m.str("Node Type")
		seen := m.str("Relation Name")
		if a := m.str("Alias"); a != "" && a != seen {
			seen += "(" + a + ")"
		}
		ch.Drivers = append(ch.Drivers, fmt.Sprintf("%s:%s:%s", seen, typ, strings.Join(ixs, "+")))
		if !slices.Contains(nodes, typ) {
			ch.Failures = append(ch.Failures, Failure{Kind: FailDriver, Indexes: ixs,
				Msg: fmt.Sprintf("driver: %s scanned by %s (%s), expected %v", label, typ, strings.Join(ixs, "+"), nodes)})
			continue
		}
		if !indexAllowed(d, ixs) {
			want := d.Index
			if len(d.AnyIndex) > 0 {
				want = strings.Join(append(nonEmpty(d.Index), d.AnyIndex...), " or ")
			}
			ch.Failures = append(ch.Failures, Failure{Kind: FailDriver, Indexes: ixs,
				Msg: fmt.Sprintf("driver: %s uses %s, expected %s", label, strings.Join(ixs, "+"), want)})
		}
	}
	for _, frag := range d.IndexCond {
		if slices.ContainsFunc(matched, func(m Node) bool { return condHas(m, frag) }) {
			continue
		}
		conds := make([]string, len(matched))
		for i, m := range matched {
			conds[i] = condsOf(m)
		}
		ch.Failures = append(ch.Failures, Failure{Kind: FailIndexCond, Indexes: matchedIxs,
			Msg: fmt.Sprintf("index_cond: %q not in %s Index Cond %q", frag, label, strings.Join(conds, " | "))})
	}
}

// indexAllowed: no index named, the one index named, or one of any_index.
func indexAllowed(d DriverSpec, ixs []string) bool {
	if d.Index == "" && len(d.AnyIndex) == 0 {
		return true
	}
	if len(ixs) != 1 {
		return false
	}
	return ixs[0] == d.Index || slices.Contains(d.AnyIndex, ixs[0])
}

func (c *Corpus) assessForbidden(ch *Check, root Node, rules []NodeRule) {
	walk(root, func(n Node, _ []Node) {
		for _, r := range rules {
			if n.str("Node Type") != r.Node {
				continue
			}
			if len(r.Relations) > 0 && !slices.Contains(r.Relations, n.str("Relation Name")) {
				continue
			}
			ch.Failures = append(ch.Failures, Failure{Kind: FailForbidden, Indexes: indexesOf(n),
				Msg: fmt.Sprintf("forbidden: %s on %s%s", r.Node, n.str("Relation Name"), reasonStr(r.Reason))})
		}
	})
}

func assessRequired(ch *Check, root Node, rules []NodeRule) {
	for _, r := range rules {
		found := false
		walk(root, func(n Node, _ []Node) {
			if n.str("Node Type") != r.Node {
				return
			}
			if len(r.Relations) == 0 || slices.Contains(r.Relations, n.str("Relation Name")) || slices.Contains(r.Relations, n.str("Index Name")) {
				found = true
			}
		})
		if !found {
			ch.Failures = append(ch.Failures, Failure{Kind: FailRequired,
				Msg: fmt.Sprintf("required: %s on %v not in plan%s", r.Node, r.Relations, reasonStr(r.Reason))})
		}
	}
}

// sortExempt returns the scan patterns of defaults.sort_exempt and exp.sort_exempt.
func (c *Corpus) sortExempt(exp *Expect) []ScanPattern {
	var out []ScanPattern
	for _, name := range append(append([]string{}, c.Defaults.SortExempt...), exp.SortExempt...) {
		out = append(out, c.SortExemptSets[name]...)
	}
	return out
}

func (p ScanPattern) matches(n Node) bool {
	if n.str("Relation Name") != p.Relation {
		return false
	}
	if p.Index != "" {
		ixs := indexesOf(n)
		if len(ixs) != 1 || ixs[0] != p.Index {
			return false
		}
	}
	return condHas(n, p.IndexCond)
}

// assessSortAboveLimit fails a Sort or Incremental Sort that a Limit reaches through pass-through nodes when its
// input holds a big-table scan no Limit bounds. A Limit bounds the rows its subtree returns, not the rows a
// SubPlan or InitPlan below it is run for, so the bound stops at those. A scan matching a sort-exempt pattern is
// exempt; the exemption covers that scan only.
func (c *Corpus) assessSortAboveLimit(ch *Check, root Node, exempt []ScanPattern) {
	walk(root, func(n Node, path []Node) {
		t := n.str("Node Type")
		if t != "Sort" && t != "Incremental Sort" || !limitAbove(path) {
			return
		}
		var bad, badIxs []string
		var rec func(m Node, bounded bool)
		rec = func(m Node, bounded bool) {
			if pr := m.str("Parent Relationship"); pr == "SubPlan" || pr == "InitPlan" {
				bounded = false
			}
			mt := m.str("Node Type")
			if mt == "Limit" {
				bounded = true
			}
			if isScan(mt) && !bounded && slices.Contains(c.Defaults.BigTables, m.str("Relation Name")) &&
				!slices.ContainsFunc(exempt, func(p ScanPattern) bool { return p.matches(m) }) {
				ixs := indexesOf(m)
				bad = append(bad, m.str("Relation Name")+"/"+strings.Join(ixs, "+"))
				badIxs = appendNew(badIxs, ixs...)
			}
			for _, k := range m.kids() {
				rec(k, bounded)
			}
		}
		for _, k := range n.kids() {
			rec(k, false)
		}
		if len(bad) > 0 {
			ch.Failures = append(ch.Failures, Failure{Kind: FailSortAboveLimit, Indexes: badIxs,
				Msg: fmt.Sprintf("sort_above_limit: %s under Limit over unbounded %v", t, bad)})
		}
	})
}

// limitAbove reports whether the nearest ancestors up to a Limit are all pass-through nodes.
func limitAbove(path []Node) bool {
	for i := len(path) - 1; i >= 0; i-- {
		pt := path[i].str("Node Type")
		if pt == "Limit" {
			return true
		}
		if !slices.Contains(passThrough, pt) {
			return false
		}
	}
	return false
}

// assessBudget checks b against an analyzed run. A plan without the numbers a budget needs is an error, never a
// silent zero: an EXPLAIN without ANALYZE, or a non-zero buffers budget on a plan without BUFFERS.
func assessBudget(ch *Check, ex *Explain, b Budget) {
	if !ex.Analyzed {
		ch.Failures = append(ch.Failures, Failure{Kind: FailError,
			Msg: "error: run is analyzed but the plan has no Execution Time (EXPLAIN without ANALYZE)"})
		return
	}
	if _, ok := ex.Root["Shared Hit Blocks"].(float64); b.Buffers > 0 && !ok {
		ch.Failures = append(ch.Failures, Failure{Kind: FailError,
			Msg: "error: buffers budget set but the plan has no Shared Hit Blocks (EXPLAIN without BUFFERS)"})
		b.Buffers = 0
	}
	add := func(msg string) { ch.Failures = append(ch.Failures, Failure{Kind: FailBudget, Msg: msg}) }
	if b.Buffers > 0 && ch.Buffers > b.Buffers {
		add(fmt.Sprintf("budget: buffers %d > %d", ch.Buffers, b.Buffers))
	}
	if b.PlanningMs > 0 && ex.PlanningMs > b.PlanningMs {
		add(fmt.Sprintf("budget: planning %.2f ms > %.2f", ex.PlanningMs, b.PlanningMs))
	}
	if b.ExecMs > 0 && ex.ExecMs > b.ExecMs {
		add(fmt.Sprintf("budget: exec %.1f ms > %.1f", ex.ExecMs, b.ExecMs))
	}
	if b.Rows > 0 && ch.Rows > b.Rows {
		add(fmt.Sprintf("budget: rows %d > %d", ch.Rows, b.Rows))
	}
}

// Verdict is the outcome of one run.
type Verdict string

// Verdicts.
const (
	VerdictOK            Verdict = "ok"
	VerdictDetected      Verdict = "DETECTED"
	VerdictMissed        Verdict = "MISSED"
	VerdictFalsePositive Verdict = "FALSE_POSITIVE"
	VerdictRetiredIndex  Verdict = "RETIRED_INDEX"
	VerdictKnownIssue    Verdict = "KNOWN_ISSUE"
	VerdictSkip          Verdict = "SKIP"
	VerdictError         Verdict = "ERROR"
)

// Failing reports whether the verdict fails the run: FALSE_POSITIVE, MISSED or ERROR.
func (v Verdict) Failing() bool {
	return v == VerdictFalsePositive || v == VerdictMissed || v == VerdictError
}

// retiredKinds are the failures a retired index can explain. Budget and error failures never are.
var retiredKinds = []FailureKind{FailDriver, FailIndexCond, FailForbidden, FailRequired, FailSortAboveLimit, FailPlanChange}

// Decide returns the verdict for one run. A run that errored has failures == [{Kind: error}] and is always
// ERROR, whatever its expect_fail or known_issue.
func Decide(expectFail bool, knownIssue string, failures []Failure, retired []string) Verdict {
	switch {
	case slices.ContainsFunc(failures, func(f Failure) bool { return f.Kind == FailError }):
		return VerdictError // an errored run always fails, expect_fail and known_issue included
	case expectFail && len(failures) > 0:
		return VerdictDetected
	case expectFail:
		return VerdictMissed
	case len(failures) == 0:
		return VerdictOK
	case allRetired(failures, retired):
		return VerdictRetiredIndex
	case knownIssue != "":
		return VerdictKnownIssue
	}
	return VerdictFalsePositive
}

// allRetired: every failure is a structural one about at least one retired index.
func allRetired(failures []Failure, retired []string) bool {
	for _, f := range failures {
		if !slices.Contains(retiredKinds, f.Kind) {
			return false
		}
		if !slices.ContainsFunc(f.Indexes, func(ix string) bool { return slices.Contains(retired, ix) }) {
			return false
		}
	}
	return true
}

func appendNew(xs []string, add ...string) []string {
	for _, a := range add {
		if !slices.Contains(xs, a) {
			xs = append(xs, a)
		}
	}
	return xs
}

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

func aliasStr(a string) string {
	if a == "" {
		return ""
	}
	return "(" + a + ")"
}

func reasonStr(r string) string {
	if r == "" {
		return ""
	}
	return " (" + r + ")"
}
