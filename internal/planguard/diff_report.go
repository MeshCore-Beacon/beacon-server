// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"fmt"
	"strings"
	"time"
)

// DiffReport is the outcome of RunDiff.
type DiffReport struct {
	StartedAt  time.Time
	DurationMs float64
	Batches    []BatchReport
}

// BatchReport is one batch: its pairs, or the error that stopped it.
type BatchReport struct {
	Name       string
	DurationMs float64
	Err        string
	Pairs      []PairResult
}

// PairResult is one (pair, cand): the snapshot comparison, the walks, and whether the verdict met the expectation.
type PairResult struct {
	Batch, Pair, Ref, Cand string
	RefRows, CandRows      int
	Outcome                Outcome
	Walks                  []WalkResult
	Verdict                string // the snapshot verdict combined with every walk's; skip when the pair did not run
	Skip                   string // why the pair did not run
	Expect                 string
	Missing                []string // required classes that did not fire
	OK                     bool
}

// WalkResult is one cand walk at one page size.
type WalkResult struct {
	Limit   int
	RefRows int
	Outcome WalkOutcome
}

// Failed returns the pairs that missed their expectation.
func (r *DiffReport) Failed() []PairResult {
	var out []PairResult
	for _, b := range r.Batches {
		for _, p := range b.Pairs {
			if !p.OK && p.Verdict != DiffSkip {
				out = append(out, p)
			}
		}
	}
	return out
}

// Skipped returns the pairs that did not run: a {sql: …} param found no value on this snapshot. They prove nothing,
// so a run with skips does not pass either.
func (r *DiffReport) Skipped() []PairResult {
	var out []PairResult
	for _, b := range r.Batches {
		for _, p := range b.Pairs {
			if p.Verdict == DiffSkip {
				out = append(out, p)
			}
		}
	}
	return out
}

// Line is a one-line summary of a pair result.
func (p PairResult) Line() string {
	mark := "ok"
	switch {
	case p.Verdict == DiffSkip:
		return fmt.Sprintf("SKIP %s/%s %s vs %s: %s", p.Batch, p.Pair, p.Ref, p.Cand, p.Skip)
	case !p.OK:
		mark = "FAIL"
	}
	s := fmt.Sprintf("%-4s %s/%s %s vs %s: %s (expect %s), rows %d/%d", mark, p.Batch, p.Pair, p.Ref, p.Cand, p.Verdict, p.Expect, p.RefRows, p.CandRows)
	if len(p.Outcome.Classes) > 0 {
		s += " " + strings.Join(p.Outcome.Classes, " ")
	}
	if len(p.Missing) > 0 {
		s += "; required classes that did not fire: " + strings.Join(p.Missing, ", ")
	}
	for _, w := range p.Walks {
		if !w.Outcome.Clean() {
			s += fmt.Sprintf("; walk %d: first wrong %d, missing %d", w.Limit, w.Outcome.FirstWrong, w.Outcome.Missing)
		}
	}
	return s
}

// Markdown renders the report.
func (r *DiffReport) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# planguard diff\n\nStarted %s, %.1f s. %d pairs missed their expectation, %d were skipped.\n\n",
		r.StartedAt.UTC().Format(time.RFC3339), r.DurationMs/1000, len(r.Failed()), len(r.Skipped()))
	esc := func(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "|", `\|`), "\n", `\n`) }
	for _, bt := range r.Batches {
		fmt.Fprintf(&b, "## %s (one REPEATABLE READ READ ONLY snapshot, %.1f s)\n\n", bt.Name, bt.DurationMs/1000)
		if bt.Err != "" {
			fmt.Fprintf(&b, "**Error:** %s\n\n", esc(bt.Err))
		}
		if len(bt.Pairs) == 0 {
			continue
		}
		b.WriteString("| pair | ref | cand | rows ref/cand | verdict | expect | classes | detail |\n|---|---|---|---|---|---|---|---|\n")
		var walks bool
		for _, p := range bt.Pairs {
			v := p.Verdict
			detail := p.Outcome.Detail
			switch {
			case p.Verdict == DiffSkip:
				v, detail = "**SKIP**", p.Skip
			case !p.OK:
				v = "**" + v + " (FAIL)**"
			}
			if len(p.Missing) > 0 {
				detail = "required classes that did not fire: " + strings.Join(p.Missing, ", ") + "; " + detail
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %d/%d | %s | %s | %s | %s |\n", esc(p.Pair), p.Ref, p.Cand, p.RefRows, p.CandRows,
				v, p.Expect, strings.Join(p.Outcome.Classes, " "), esc(detail))
			walks = walks || len(p.Walks) > 0
		}
		if walks {
			b.WriteString("\n| pair | cand | limit | rows ref/cand | first wrong | missing | duplicates | out of order | stuck |\n|---|---|---|---|---|---|---|---|---|\n")
			for _, p := range bt.Pairs {
				for _, w := range p.Walks {
					o := w.Outcome
					fmt.Fprintf(&b, "| %s | %s | %d | %d/%d | %d | %d | %d | %d | %s |\n", esc(p.Pair), p.Cand, w.Limit, w.RefRows, o.Rows,
						o.FirstWrong, o.Missing, o.Duplicates, o.OutOfOrder, o.Stuck)
				}
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}
