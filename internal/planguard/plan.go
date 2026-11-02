// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Node is one plan node of EXPLAIN (FORMAT JSON) output, as decoded by encoding/json.
type Node map[string]any

func (n Node) str(k string) string {
	s, _ := n[k].(string)
	return s
}

func (n Node) num(k string) float64 {
	f, _ := n[k].(float64)
	return f
}

func (n Node) kids() []Node {
	arr, _ := n["Plans"].([]any)
	out := make([]Node, 0, len(arr))
	for _, x := range arr {
		if m, ok := x.(map[string]any); ok {
			out = append(out, Node(m))
		}
	}
	return out
}

// Explain is one EXPLAIN (FORMAT JSON) result.
type Explain struct {
	Root        Node
	PlanningMs  float64
	ExecMs      float64
	Analyzed    bool  // the output carries an Execution Time (EXPLAIN ANALYZE)
	PlanBuffers int64 // shared hit + read while planning (BUFFERS)
}

// ParseExplain decodes EXPLAIN (FORMAT JSON) text. Empty output, an empty array or a result with no plan is an
// error.
func ParseExplain(b []byte) (*Explain, error) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return nil, errors.New("explain: empty output")
	}
	var arr []map[string]any
	if err := json.Unmarshal(b, &arr); err != nil {
		return nil, fmt.Errorf("explain json: %w (%.200q)", err, b)
	}
	if len(arr) == 0 {
		return nil, errors.New("explain: empty result array")
	}
	top := Node(arr[0])
	root, ok := top["Plan"].(map[string]any)
	if !ok {
		return nil, errors.New("explain: result has no Plan object")
	}
	ex := &Explain{Root: Node(root), PlanningMs: top.num("Planning Time")}
	if v, ok := top["Execution Time"].(float64); ok {
		ex.ExecMs, ex.Analyzed = v, true
	}
	if p, ok := top["Planning"].(map[string]any); ok {
		ex.PlanBuffers = sharedBlocks(Node(p))
	}
	return ex, nil
}

func sharedBlocks(n Node) int64 {
	return int64(n.num("Shared Hit Blocks") + n.num("Shared Read Blocks"))
}

// isSubplan reports whether n hangs under its parent as a SubPlan or InitPlan: a separate query the parent
// runs, not part of the parent's own scan.
func isSubplan(n Node) bool {
	pr := n.str("Parent Relationship")
	return pr == "SubPlan" || pr == "InitPlan"
}

// bitmapIndexScans returns the Bitmap Index Scans that feed a Bitmap Heap Scan (through BitmapAnd/BitmapOr),
// leaving out any inside a SubPlan or InitPlan of the scan.
func bitmapIndexScans(n Node) []Node {
	var out []Node
	var rec func(m Node)
	rec = func(m Node) {
		if m.str("Node Type") == "Bitmap Index Scan" {
			out = append(out, m)
		}
		for _, k := range m.kids() {
			if !isSubplan(k) {
				rec(k)
			}
		}
	}
	rec(n)
	return out
}

// walk visits n and its descendants depth first; path holds n's ancestors, root first.
func walk(n Node, fn func(n Node, path []Node)) {
	var rec func(n Node, path []Node)
	rec = func(n Node, path []Node) {
		fn(n, path)
		next := append(path[:len(path):len(path)], n)
		for _, k := range n.kids() {
			rec(k, next)
		}
	}
	rec(n, nil)
}

func isScan(t string) bool {
	switch t {
	case "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Tid Scan", "Tid Range Scan", "Sample Scan":
		return true
	}
	return false
}

// indexesOf returns the indexes a scan node uses: its Index Name, or for a Bitmap Heap Scan those of the Bitmap
// Index Scans feeding it (not those of its SubPlans).
func indexesOf(n Node) []string {
	if ix := n.str("Index Name"); ix != "" {
		return []string{ix}
	}
	var out []string
	if n.str("Node Type") == "Bitmap Heap Scan" {
		for _, m := range bitmapIndexScans(n) {
			out = append(out, m.str("Index Name"))
		}
	}
	return out
}

// condsOf joins a scan's Index Cond and Recheck Cond and, for a Bitmap Heap Scan, the conds of the Bitmap Index
// Scans feeding it (not those of its SubPlans).
func condsOf(n Node) string {
	var parts []string
	for _, k := range []string{"Index Cond", "Recheck Cond"} {
		if v := n.str(k); v != "" {
			parts = append(parts, v)
		}
	}
	if n.str("Node Type") == "Bitmap Heap Scan" {
		for _, m := range bitmapIndexScans(n) {
			parts = append(parts, m.str("Index Cond"))
		}
	}
	return strings.Join(parts, " && ")
}

var wsRe = regexp.MustCompile(`\s+`)

func normWS(s string) string { return strings.TrimSpace(wsRe.ReplaceAllString(s, " ")) }

// condHas reports whether a scan's conds hold the fragment, whitespace-normalised.
func condHas(n Node, fragment string) bool {
	return strings.Contains(normWS(condsOf(n)), normWS(fragment))
}

// Signature is a plan's shape, one line per node: node type, relation and alias, index, scan direction, partial
// mode, parent relationship, join type, strategy and "Parallel". It holds no costs, row counts or values, so it is the baseline diff key.
func Signature(root Node) []string {
	var out []string
	walk(root, func(n Node, path []Node) {
		var b strings.Builder
		b.WriteString(strings.Repeat("  ", len(path)))
		if pa, _ := n["Parallel Aware"].(bool); pa {
			b.WriteString("Parallel ")
		}
		b.WriteString(n.str("Node Type"))
		if r := n.str("Relation Name"); r != "" {
			b.WriteString(" on " + r)
			if a := n.str("Alias"); a != "" && a != r {
				b.WriteString(" " + a)
			}
		}
		if ix := n.str("Index Name"); ix != "" {
			b.WriteString(" using " + ix)
		}
		if sd := n.str("Scan Direction"); sd != "" {
			b.WriteString(" scan=" + sd)
		}
		if pm := n.str("Partial Mode"); pm != "" {
			b.WriteString(" partial=" + pm)
		}
		if pr := n.str("Parent Relationship"); pr != "" && pr != "Outer" && pr != "Inner" && pr != "Member" {
			b.WriteString(" [" + pr + "]")
		}
		if j := n.str("Join Type"); j != "" && j != "Inner" {
			b.WriteString(" (" + j + ")")
		}
		if st := n.str("Strategy"); st != "" && st != "Plain" {
			b.WriteString(" {" + st + "}")
		}
		out = append(out, b.String())
	})
	return out
}
