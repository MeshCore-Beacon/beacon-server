// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
)

// The prod tier through an operator's psql, for evidence before a merge (the server runs Run directly):
//
//  1. EmitResolvers writes a psql script; its output goes to ParseResolverOutput.
//  2. EmitPlans writes a script of plain EXPLAINs with those values; its output goes to ParsePlanOutput.
//  3. CheckPlans checks the plans and returns the report.
//
// Every block of output starts with a marker line "@@<nonce> <kind> <key>", where nonce is 16 random hex
// characters chosen when the script is written, so text from the mesh cannot forge a marker. The scripts open
// with a "@@<nonce> planguard" header and end with "@@<nonce> END"; output without the end marker is truncated
// and is an error. A resolver block's body is "N" for NULL or "V" followed by the value.

// IndexesKey is the key under which ParsePlanOutput returns the index list block (a JSON array of names).
const IndexesKey = "indexes"

const scriptHead = "SET default_transaction_read_only = on; SET lock_timeout = '1s'; SET statement_timeout = '20s';\n" +
	"\\set QUIET on\n\\set ON_ERROR_STOP off\n\\pset format unaligned\n\\pset tuples_only on\n\\pset pager off\n" +
	"SET jit = off; SET timezone = 'UTC'; SET standard_conforming_strings = on;\n"

var (
	nonceRe       = regexp.MustCompile(`^[0-9a-f]{16}$`)
	scriptHeadRe  = regexp.MustCompile(`^@@([0-9a-f]{16}) planguard$`)
	markerPrefixF = "@@%s "
)

// NewNonce returns 16 hex characters from crypto/rand.
func NewNonce() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// ScriptNonce returns the nonce of the "@@<nonce> planguard" header in a script's output. The header is the
// first line starting with "@@"; it comes before any query output.
func ScriptNonce(out []byte) (string, error) {
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "@@") {
			continue
		}
		m := scriptHeadRe.FindStringSubmatch(strings.TrimRight(line, "\r"))
		if m == nil {
			return "", fmt.Errorf("first marker line %.80q is not a planguard header", line)
		}
		return m[1], nil
	}
	return "", fmt.Errorf("no planguard header in the output")
}

func checkNonce(nonce string) error {
	if !nonceRe.MatchString(nonce) {
		return fmt.Errorf("nonce %q is not 16 hex characters", nonce)
	}
	return nil
}

func scriptStart(w *strings.Builder, nonce, searchPath string) {
	w.WriteString(scriptHead)
	if searchPath != "" {
		w.WriteString("SET search_path = " + searchPath + ";\n")
	}
	fmt.Fprintf(w, "\\echo @@%s planguard\n", nonce)
}

// EmitResolvers writes the psql script that resolves the params of o's selection.
func EmitResolvers(w io.Writer, c *Corpus, o RunOptions, nonce string) error {
	if err := checkNonce(nonce); err != nil {
		return err
	}
	if err := o.validate(c); err != nil {
		return err
	}
	need := neededResolvers(c, c.selection(o), o.IncludePending)
	var b strings.Builder
	scriptStart(&b, nonce, c.baseSearchPath(o))
	for _, name := range sortedKeys(need) {
		if need[name] == "" {
			return fmt.Errorf("resolver %s is not in the corpus", name)
		}
		if err := checkStatementText(need[name]); err != nil {
			return fmt.Errorf("resolver %s: %w", name, err)
		}
		body := fmt.Sprintf("SELECT coalesce('V' || ((%s)::text), 'N');", trimStmt(need[name]))
		if err := checkScriptLines(body); err != nil {
			return fmt.Errorf("resolver %s: %w", name, err)
		}
		fmt.Fprintf(&b, "\\echo @@%s R %s\n%s\n", nonce, name, body)
	}
	fmt.Fprintf(&b, "\\echo @@%s END\n", nonce)
	_, err := io.WriteString(w, b.String())
	return err
}

// EmitPlans writes the psql script of plain EXPLAINs for o's selection with params resolved. Runs that skip, or
// cannot be rendered, are left out; CheckPlans reports them.
func EmitPlans(w io.Writer, c *Corpus, o RunOptions, params map[string]*string, nonce string) error {
	if err := checkNonce(nonce); err != nil {
		return err
	}
	if err := o.validate(c); err != nil {
		return err
	}
	if params == nil {
		params = map[string]*string{}
	}
	var b strings.Builder
	scriptStart(&b, nonce, c.baseSearchPath(o))
	var tables []string
	for _, t := range c.Defaults.BigTables {
		tables = append(tables, quoteLit(t))
	}
	fmt.Fprintf(&b, "\\echo @@%s I %s\n", nonce, IndexesKey)
	if len(tables) == 0 {
		b.WriteString("SELECT '[]';\n")
	} else {
		fmt.Fprintf(&b, "SELECT coalesce(json_agg(DISTINCT ic.relname::text ORDER BY ic.relname::text), '[]') FROM unnest(ARRAY[%s]::text[]) b(name) "+
			"JOIN pg_index i ON i.indrelid = to_regclass(quote_ident(b.name)) JOIN pg_class ic ON ic.oid = i.indexrelid;\n", strings.Join(tables, ", "))
	}
	for _, e := range c.selection(o) {
		for _, mode := range o.modesOf(c, e) {
			sp := c.spec(o, e, mode, params, nil, true)
			if sp.skip != "" || sp.err != "" {
				continue
			}
			for _, l := range append(slices.Clone(sp.setup), sp.explain) {
				if err := checkScriptLines(l); err != nil {
					return fmt.Errorf("%s|%s: %w", e.ID, mode, err)
				}
			}
			for _, l := range sp.setup {
				if strings.HasPrefix(l, "PREPARE ") {
					b.WriteString(l + "\n;\n")
				} else {
					b.WriteString(l + ";\n")
				}
			}
			fmt.Fprintf(&b, "\\echo @@%s P %s|%s\n%s;\nROLLBACK;\nDEALLOCATE %s;\n", nonce, e.ID, mode, sp.explain, stmtName)
		}
	}
	fmt.Fprintf(&b, "\\echo @@%s END\n", nonce)
	_, err := io.WriteString(w, b.String())
	return err
}

var blockLabel = map[string]string{"R": "resolver", "P": "plan", "I": "index list"}

type block struct {
	kind, key string
	lines     []string
}

// parseBlocks splits script output into its marked blocks. Lines before the first block are ignored (psql's own
// output of the opening SETs). The output must end with the end marker.
func parseBlocks(r io.Reader, nonce string, kinds ...string) ([]block, error) {
	if err := checkNonce(nonce); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(bufio.NewReader(r))
	if err != nil {
		return nil, err
	}
	text := strings.TrimSuffix(string(raw), "\n")
	prefix := fmt.Sprintf(markerPrefixF, nonce)
	var out []block
	var cur *block
	ended := false
	seen := map[string]bool{}
	closeCur := func() error {
		if cur == nil {
			return nil
		}
		if len(cur.lines) == 0 {
			return fmt.Errorf("%s %s has no body (did its query fail?)", blockLabel[cur.kind], cur.key)
		}
		out = append(out, *cur)
		cur = nil
		return nil
	}
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, prefix) {
			if cur != nil {
				cur.lines = append(cur.lines, line)
			}
			continue
		}
		if err := closeCur(); err != nil {
			return nil, err
		}
		rest := line[len(prefix):]
		if rest == "planguard" {
			continue
		}
		if rest == "END" {
			ended = true
			break
		}
		kind, key, _ := strings.Cut(rest, " ")
		known := false
		for _, k := range kinds {
			known = known || k == kind
		}
		if !known || key == "" {
			return nil, fmt.Errorf("unexpected marker %q", rest)
		}
		if seen[kind+" "+key] {
			return nil, fmt.Errorf("block %s %s appears twice", kind, key)
		}
		seen[kind+" "+key] = true
		cur = &block{kind: kind, key: key}
	}
	if !ended {
		return nil, fmt.Errorf("output ends without the end marker (truncated?)")
	}
	return out, nil
}

// ParseResolverOutput reads the output of an EmitResolvers script: resolver name → value, nil for NULL.
func ParseResolverOutput(r io.Reader, nonce string) (map[string]*string, error) {
	blocks, err := parseBlocks(r, nonce, "R")
	if err != nil {
		return nil, fmt.Errorf("resolver output: %w", err)
	}
	out := map[string]*string{}
	for _, bl := range blocks {
		body := strings.Join(bl.lines, "\n")
		switch {
		case body == "N":
			out[bl.key] = nil
		case strings.HasPrefix(body, "V"):
			v := body[1:]
			out[bl.key] = &v
		default:
			return nil, fmt.Errorf("resolver output: resolver %s: body %.80q is neither N nor V<value>", bl.key, body)
		}
	}
	return out, nil
}

// ParsePlanOutput reads the output of an EmitPlans script: "<id>|<mode>" → EXPLAIN JSON, and IndexesKey → the
// JSON list of big-table indexes. A body that is not JSON is an error naming its block.
func ParsePlanOutput(r io.Reader, nonce string) (map[string][]byte, error) {
	blocks, err := parseBlocks(r, nonce, "P", "I")
	if err != nil {
		return nil, fmt.Errorf("plan output: %w", err)
	}
	out := map[string][]byte{}
	for _, bl := range blocks {
		key := bl.key
		if bl.kind == "P" {
			id, mode, ok := strings.Cut(key, "|")
			if !ok || id == "" || mode == "" {
				return nil, fmt.Errorf("plan output: block key %q is not <id>|<mode>", key)
			}
		} else if key != IndexesKey {
			return nil, fmt.Errorf("plan output: unexpected index block %q", key)
		}
		body := []byte(strings.Join(bl.lines, "\n"))
		if !json.Valid(body) {
			return nil, fmt.Errorf("plan output: block %s: body is not valid JSON (%.80q)", key, body)
		}
		out[key] = body
	}
	return out, nil
}

// CheckPlans checks plans (from ParsePlanOutput) for o's selection, with o.Params the resolved values. A run with
// no plan in the output is an ERROR; a plan for a run outside the selection is an error.
func CheckPlans(c *Corpus, o RunOptions, plans map[string][]byte) (*Report, error) {
	if err := o.validate(c); err != nil {
		return nil, err
	}
	t0 := time.Now()
	rep := &Report{Tier: o.Tier, StartedAt: t0.UTC(), Settings: map[string]string{}}
	if ix, ok := plans[IndexesKey]; ok {
		if err := json.Unmarshal(ix, &rep.Indexes); err != nil {
			return nil, fmt.Errorf("index block: %w", err)
		}
	}
	params := o.Params
	if params == nil {
		params = map[string]*string{}
	}
	used := map[string]bool{IndexesKey: true}
	for _, e := range c.selection(o) {
		for _, mode := range o.modesOf(c, e) {
			sp := c.spec(o, e, mode, params, nil, true)
			r := sp.r
			key := e.ID + "|" + mode
			switch plan, ok := plans[key]; {
			case sp.skip != "":
				r.Verdict, r.Skip = VerdictSkip, sp.skip
			case sp.err != "":
				c.errored(&r, e, sp.err)
			case !ok:
				c.errored(&r, e, "no plan for "+key+" in the psql output")
			default:
				used[key] = true
				if err := c.judge(&r, sp, plan, o); err != nil {
					return nil, err
				}
			}
			rep.Results = append(rep.Results, r)
		}
	}
	for _, k := range sortedKeys(plans) {
		if !used[k] {
			return nil, fmt.Errorf("the psql output holds a plan for %s, which is not in this selection", k)
		}
	}
	rep.DurationMs = msSince(t0)
	return rep, nil
}
