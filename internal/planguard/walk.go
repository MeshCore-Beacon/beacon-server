// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// DefaultWalkRows caps a walk when its spec sets no max_rows.
const DefaultWalkRows = 2000

// WalkSpec pages through a keyset-paginated statement the way a client does.
type WalkSpec struct {
	LimitParam int          `yaml:"limit_param"` // 1-based
	FetchExtra int          `yaml:"fetch_extra"` // 1: the statement takes limit+1, and a longer page means more follow
	Cursor     []CursorBind `yaml:"cursor"`      // param ← column of the last row of the page
	First      []any        `yaml:"first"`       // cursor param values for page 1
	WebLimit   int          `yaml:"web_limit"`   // the client's page size for this endpoint (50 or 200)
	MaxRows    int          `yaml:"max_rows"`    // cap; default 2,000
	// Oracle: the ref is not walked; it is fetched as one page of MaxRows rows from First.
	Oracle bool `yaml:"oracle"`
}

// CursorBind binds a statement param (1-based) to a column of the last row kept.
type CursorBind struct {
	Param  int    `yaml:"param"`
	Column string `yaml:"column"`
}

// PageFetcher returns one page of a walk: the statement run from cursor (nil on page 1), asked for limit rows the
// way the client asks (so a page longer than limit means another page follows).
type PageFetcher func(ctx context.Context, cursor []string, limit int) (RowSet, error)

// WalkOutcome compares a walk with its ref.
type WalkOutcome struct {
	Rows       int    // cand rows walked
	FirstWrong int    // 1-based position of the first row that differs from the ref's row there; 0 if none
	Missing    int    // ref rows the walk never returned
	Duplicates int    // rows the walk returned more than once
	OutOfOrder int    // rows that come after a row the ref puts later
	Stuck      string // "cursor did not advance at page N"
}

// Clean reports whether the walk returned exactly the ref's rows.
func (w WalkOutcome) Clean() bool {
	return w.FirstWrong == 0 && w.Missing == 0 && w.Duplicates == 0 && w.OutOfOrder == 0 && w.Stuck == ""
}

func (w *WalkSpec) maxRows() int {
	if w.MaxRows > 0 {
		return w.MaxRows
	}
	return DefaultWalkRows
}

func (w *WalkSpec) validate(p DiffPair) error {
	if w.LimitParam < 1 {
		return errors.New("limit_param must be a param number ($1 = 1)")
	}
	if w.FetchExtra != 1 {
		return errors.New("fetch_extra must be 1: a walk needs the statement to fetch limit+1 rows to know whether another page follows")
	}
	if len(w.Cursor) == 0 {
		return errors.New("no cursor")
	}
	if len(w.First) != len(w.Cursor) {
		return fmt.Errorf("first has %d values for %d cursor params", len(w.First), len(w.Cursor))
	}
	seen := map[int]bool{w.LimitParam: true}
	for _, c := range w.Cursor {
		if c.Param < 1 || c.Column == "" {
			return fmt.Errorf("cursor %+v: want a param number and a column", c)
		}
		if seen[c.Param] {
			return fmt.Errorf("param $%d is bound twice", c.Param)
		}
		seen[c.Param] = true
	}
	for _, v := range w.First {
		if err := checkDiffParam(v); err != nil {
			return fmt.Errorf("first: %w", err)
		}
	}
	if w.WebLimit < 0 || w.MaxRows < 0 {
		return errors.New("web_limit and max_rows must not be negative")
	}
	return nil
}

// checkParams checks that every bound param exists in a call with n params.
func (w *WalkSpec) checkParams(n int) error {
	if w.LimitParam > n {
		return fmt.Errorf("limit_param $%d, but the call has %d params", w.LimitParam, n)
	}
	for _, c := range w.Cursor {
		if c.Param > n {
			return fmt.Errorf("cursor param $%d, but the call has %d params", c.Param, n)
		}
	}
	return nil
}

func (w *WalkSpec) cursorColumns() []string {
	out := make([]string, len(w.Cursor))
	for i, c := range w.Cursor {
		out[i] = c.Column
	}
	return out
}

// Walk pages through fetch at limit rows a page: page 1 from the first cursor, then from the cursor columns of the
// last row kept. It keeps the first limit rows of each page and stops when a page holds limit rows or fewer, at
// maxRows rows (≤ 0: DefaultWalkRows), or when the cursor repeats; stuck then says where. Each page adds a row or
// ends the walk, so it always ends.
func Walk(ctx context.Context, fetch PageFetcher, cursorCols []string, limit, maxRows int) (RowSet, string, error) {
	if limit < 1 {
		return RowSet{}, "", fmt.Errorf("walk limit %d, want ≥ 1", limit)
	}
	if maxRows <= 0 {
		maxRows = DefaultWalkRows
	}
	var out RowSet
	var cursor []string
	var idx []int
	seen := map[string]bool{}
	for page := 1; ; page++ {
		if err := ctx.Err(); err != nil {
			return out, "", err
		}
		rs, err := fetch(ctx, cursor, limit)
		if err != nil {
			return out, "", fmt.Errorf("page %d: %w", page, err)
		}
		if page == 1 {
			out.Columns = rs.Columns
			for _, c := range cursorCols {
				i := slices.Index(rs.Columns, c)
				if i < 0 {
					return out, "", fmt.Errorf("cursor column %s is not in the result", c)
				}
				idx = append(idx, i)
			}
		} else if !slices.Equal(rs.Columns, out.Columns) {
			return out, "", fmt.Errorf("page %d has the columns %v, page 1 had %v", page, rs.Columns, out.Columns)
		}
		keep := rs.Rows[:min(limit, len(rs.Rows))]
		if room := maxRows - len(out.Rows); len(keep) > room {
			keep = keep[:room]
		}
		out.Rows = append(out.Rows, keep...)
		if len(rs.Rows) <= limit || len(out.Rows) >= maxRows || len(keep) == 0 {
			return out, "", nil
		}
		last := keep[len(keep)-1]
		next := make([]string, len(idx))
		for n, i := range idx {
			if last[i] == nil {
				return out, "", fmt.Errorf("page %d: cursor column %s is NULL", page, cursorCols[n])
			}
			next[n] = *last[i]
		}
		k := tuple(last, idx)
		if seen[k] {
			return out, fmt.Sprintf("cursor did not advance at page %d", page), nil
		}
		seen[k] = true
		cursor = next
	}
}

// CompareWalk compares a walk with its ref. key names the identity columns (the cursor columns; empty: every ref
// column). FirstWrong compares whole rows over the ref's columns. A ref that holds one identity twice cannot be
// compared by identity, so it is an error.
func CompareWalk(ref, cand RowSet, key []string) (WalkOutcome, error) {
	w := WalkOutcome{Rows: len(cand.Rows)}
	full := make([]int, len(ref.Columns))
	fullC := make([]int, len(ref.Columns))
	for i, c := range ref.Columns {
		full[i] = i
		fullC[i] = slices.Index(cand.Columns, c)
	}
	if slices.Contains(fullC, -1) {
		// The walk lacks a ref column: no row matches.
		w.FirstWrong, w.Missing = 1, len(ref.Rows)
		return w, nil
	}
	if len(key) == 0 {
		key = ref.Columns
	}
	var keyR, keyC []int
	for _, k := range key {
		i, j := slices.Index(ref.Columns, k), slices.Index(cand.Columns, k)
		if i < 0 {
			return w, fmt.Errorf("identity column %s is not in the ref", k)
		}
		if j < 0 {
			w.FirstWrong, w.Missing = 1, len(ref.Rows)
			return w, nil
		}
		keyR, keyC = append(keyR, i), append(keyC, j)
	}
	n := min(len(ref.Rows), len(cand.Rows))
	for i := 0; i < n; i++ {
		if tuple(ref.Rows[i], full) != tuple(cand.Rows[i], fullC) {
			w.FirstWrong = i + 1
			break
		}
	}
	if w.FirstWrong == 0 && len(ref.Rows) != len(cand.Rows) {
		w.FirstWrong = n + 1
	}
	pos := map[string]int{}
	for i, r := range ref.Rows {
		k := tuple(r, keyR)
		if first, dup := pos[k]; dup {
			return w, fmt.Errorf("the ref holds rows %d and %d with the same %s", first+1, i+1, strings.Join(key, ", "))
		}
		pos[k] = i
	}
	got := map[string]bool{}
	highest := -1
	for _, r := range cand.Rows {
		k := tuple(r, keyC)
		if got[k] {
			w.Duplicates++
			continue
		}
		got[k] = true
		if p, ok := pos[k]; ok {
			if p < highest {
				w.OutOfOrder++
			} else {
				highest = p
			}
		}
	}
	for k := range pos {
		if !got[k] {
			w.Missing++
		}
	}
	return w, nil
}
