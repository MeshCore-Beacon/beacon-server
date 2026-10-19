// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Corpus is the plan corpus: which statements to plan, with which parameters, and what each plan must look like.
type Corpus struct {
	Defaults          Defaults                 `yaml:"defaults"`
	Tiers             map[string]TierSettings  `yaml:"tiers"` // fixture | local | prod
	Groups            map[string]Group         `yaml:"groups"`
	Statements        map[string]*Statement    `yaml:"statements"`
	SortExemptSets    map[string][]ScanPattern `yaml:"sort_exempt_sets"`
	AdmissionProfiles map[string][]string      `yaml:"admission_profiles"`
	Resolvers         map[string]string        `yaml:"resolvers"`
	Entries           []*Entry                 `yaml:"entries"`

	byID map[string]*Entry
}

// TierSettings applies to every run on one tier.
type TierSettings struct {
	Settings   map[string]string `yaml:"settings"`    // SET LOCAL per run, e.g. random_page_cost
	SearchPath string            `yaml:"search_path"` // default when a group names none for this tier
}

// Defaults apply to every entry.
type Defaults struct {
	BigTables      []string   `yaml:"big_tables"`
	Executions     int        `yaml:"executions"` // 6
	TimeoutS       int        `yaml:"timeout_s"`  // 60
	Forbid         []NodeRule `yaml:"forbid"`
	SortAboveLimit bool       `yaml:"forbid_sort_above_limit"`
	SortExempt     []string   `yaml:"sort_exempt"` // names of sort_exempt_sets applied to every entry
	RetiredIndexes []string   `yaml:"retired_indexes"`
}

// Group sets a search_path per tier for the statements in it.
type Group struct {
	SearchPath map[string]string `yaml:"search_path"`
}

// Statement is one statement text and how it is planned in production.
type Statement struct {
	SQLC        string `yaml:"sqlc"`         // sqlc statement name; text from db/sqlc
	File        string `yaml:"file"`         // frozen text, relative to the corpus file
	DerivedFrom string `yaml:"derived_from"` // sqlc name whose text must contain File's body (read-only part of a DML)
	Kind        string `yaml:"kind"`         // select (default) | dml
	Group       string `yaml:"group"`
	Plan        string `yaml:"plan"`      // cached | custom | maint | ingest | legacy | harness
	Admission   string `yaml:"admission"` // admission profile; required when plan is cached
	Pending     string `yaml:"pending"`   // feature name; the statement is not merged yet
	SQL         string `yaml:"-"`
}

// ScanPattern matches a scan node by relation, optional index and an Index Cond fragment.
type ScanPattern struct {
	Relation  string `yaml:"relation"`
	Index     string `yaml:"index"`      // optional
	IndexCond string `yaml:"index_cond"` // fragment, mode-neutral
	Reason    string `yaml:"reason"`
}

// NodeRule names a plan node type on some relations.
type NodeRule struct {
	Node      string   `yaml:"node"`
	Relations []string `yaml:"relations"` // "big" expands to defaults.big_tables
	Reason    string   `yaml:"reason"`
}

// DriverSpec is the scan that must drive a plan.
type DriverSpec struct {
	Scope     string   `yaml:"scope"` // Subplan Name, e.g. "CTE page"
	Relation  string   `yaml:"relation"`
	Alias     string   `yaml:"alias"`
	Index     string   `yaml:"index"`
	AnyIndex  []string `yaml:"any_index"` // alternatives (empty pkey range for past-the-end cursors)
	Nodes     []string `yaml:"nodes"`
	IndexCond []string `yaml:"index_cond"`
}

// Budget bounds one run; a zero field is not checked.
type Budget struct {
	Buffers    int64   `yaml:"buffers"`
	PlanningMs float64 `yaml:"planning_ms"`
	ExecMs     float64 `yaml:"exec_ms"`
	Rows       int64   `yaml:"rows"`
}

// Expect is what an entry's plan must satisfy.
type Expect struct {
	Driver     []DriverSpec `yaml:"driver"`
	Require    []NodeRule   `yaml:"require"`
	Forbid     []NodeRule   `yaml:"forbid"`
	SortExempt []string     `yaml:"sort_exempt"` // set names, added to defaults.sort_exempt
	Budget     Budget       `yaml:"budget"`
}

// ModeOverride skips one plan mode or relaxes its expectations.
type ModeOverride struct {
	Skip      string  `yaml:"skip"`
	DegradeTo *Expect `yaml:"degrade_to"`
	Reason    string  `yaml:"reason"`
}

// Entry is one statement with one set of parameters.
type Entry struct {
	ID, Stmt, Shape string
	Shapes          []string                 `yaml:"shapes"` // shape-class tags (admission)
	Params          []any                    `yaml:"params"`
	Tiers           []string                 `yaml:"tiers"`
	Modes           []string                 `yaml:"modes"`
	Executions      *int                     `yaml:"executions"`
	TimeoutS        int                      `yaml:"timeout_s"`
	Analyze         *bool                    `yaml:"analyze"`
	Inherit         string                   `yaml:"inherit"`
	Expect          Expect                   `yaml:"expect"`
	Mode            map[string]*ModeOverride `yaml:"mode"`
	ExpectFail      bool                     `yaml:"expect_fail"`
	KnownIssue      string                   `yaml:"known_issue"`
	Baseline        string                   `yaml:"baseline"` // "" or "unstable: <reason>"
	Note            string                   `yaml:"note"`
}

// Plan modes: how a statement's plan is chosen (plan_cache_mode).
const (
	ModeCustom  = "custom"
	ModeGeneric = "generic"
	ModeAuto    = "auto"
)

var (
	validModes = map[string]bool{ModeCustom: true, ModeGeneric: true, ModeAuto: true}
	validTiers = map[string]bool{"fixture": true, "local": true, "prod": true}
	validKinds = map[string]bool{"select": true, "dml": true}

	// classModes are the modes an entry runs when it names none, by its statement's plan class.
	classModes = map[string][]string{
		"cached":  {ModeCustom, ModeGeneric, ModeAuto}, // must pass every layout
		"custom":  {ModeCustom},                        // sent unnamed; no layout plans it generic
		"maint":   {ModeCustom},                        // maint is force_custom_plan
		"ingest":  {ModeAuto},                          // ingest stays auto
		"legacy":  {ModeCustom, ModeGeneric, ModeAuto}, // today's statements
		"harness": {ModeCustom, ModeGeneric, ModeAuto}, // planted regressions
	}

	componentSchema = regexp.MustCompile(`\bt_[a-z0-9_]+\.`)
)

// LoadCorpus reads the corpus at path from fsys (strict: unknown keys fail) and fills every Statement.SQL:
// sqlc → sqlcText[name]; file/derived → fsys file. A sqlc name missing from sqlcText is an error unless the
// statement is pending. It resolves inherit, validates every reference and the default modes per plan class.
func LoadCorpus(fsys fs.FS, p string, sqlcText map[string]string) (*Corpus, error) {
	raw, err := fs.ReadFile(fsys, p)
	if err != nil {
		return nil, fmt.Errorf("read corpus: %w", err)
	}
	var c Corpus
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s: empty corpus", p)
		}
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: more than one YAML document", p)
	}
	if err := c.load(fsys, path.Dir(p), sqlcText); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	return &c, nil
}

// Entry returns the entry with this id, or nil.
func (c *Corpus) Entry(id string) *Entry { return c.byID[id] }

// ModesOf returns the plan modes e runs in: its own modes, else its statement's plan class defaults.
func (c *Corpus) ModesOf(e *Entry) []string {
	if len(e.Modes) > 0 {
		return e.Modes
	}
	if st := c.Statements[e.Stmt]; st != nil {
		return classModes[st.Plan]
	}
	return nil
}

func (c *Corpus) load(fsys fs.FS, dir string, sqlcText map[string]string) error {
	if err := c.checkTopLevel(); err != nil {
		return err
	}
	if err := c.loadStatements(fsys, dir, sqlcText); err != nil {
		return err
	}
	c.byID = make(map[string]*Entry, len(c.Entries))
	for i, e := range c.Entries {
		if e == nil || e.ID == "" {
			return fmt.Errorf("entry %d: missing id", i)
		}
		if _, dup := c.byID[e.ID]; dup {
			return fmt.Errorf("duplicate entry id %q", e.ID)
		}
		c.byID[e.ID] = e
	}
	if err := c.resolveInherit(); err != nil {
		return err
	}
	for _, e := range c.Entries {
		if err := c.checkEntry(e); err != nil {
			return fmt.Errorf("entry %q: %w", e.ID, err)
		}
	}
	return nil
}

func (c *Corpus) checkTopLevel() error {
	for name := range c.Tiers {
		if !validTiers[name] {
			return fmt.Errorf("tiers: unknown tier %q (want fixture, local or prod)", name)
		}
	}
	for _, g := range sortedKeys(c.Groups) {
		for tier := range c.Groups[g].SearchPath {
			if !validTiers[tier] {
				return fmt.Errorf("group %q: unknown tier %q", g, tier)
			}
		}
	}
	for _, name := range sortedKeys(c.SortExemptSets) {
		for i, sp := range c.SortExemptSets[name] {
			if sp.Relation == "" {
				return fmt.Errorf("sort_exempt_sets %q[%d]: missing relation", name, i)
			}
			if err := checkModeNeutral(sp.IndexCond); err != nil {
				return fmt.Errorf("sort_exempt_sets %q[%d]: %w", name, i, err)
			}
		}
	}
	for _, name := range c.Defaults.SortExempt {
		if _, ok := c.SortExemptSets[name]; !ok {
			return fmt.Errorf("defaults: sort_exempt %q is not in sort_exempt_sets", name)
		}
	}
	for _, name := range sortedKeys(c.Resolvers) {
		if m := componentSchema.FindString(c.Resolvers[name]); m != "" {
			return fmt.Errorf("resolver %q reads schema %q; resolvers read Beacon tables only", name, m)
		}
	}
	var err error
	if c.Defaults.Forbid, err = c.expandBig(c.Defaults.Forbid); err != nil {
		return fmt.Errorf("defaults: %w", err)
	}
	return nil
}

func (c *Corpus) loadStatements(fsys fs.FS, dir string, sqlcText map[string]string) error {
	for _, name := range sortedKeys(c.Statements) {
		st := c.Statements[name]
		if st == nil {
			return fmt.Errorf("statement %q: empty", name)
		}
		if err := c.loadStatement(fsys, dir, st, sqlcText); err != nil {
			return fmt.Errorf("statement %q: %w", name, err)
		}
	}
	return nil
}

func (c *Corpus) loadStatement(fsys fs.FS, dir string, st *Statement, sqlcText map[string]string) error {
	if st.Kind == "" {
		st.Kind = "select"
	}
	if !validKinds[st.Kind] {
		return fmt.Errorf("unknown kind %q (want select or dml)", st.Kind)
	}
	if _, ok := classModes[st.Plan]; !ok {
		return fmt.Errorf("unknown plan %q (want cached, custom, maint, ingest, legacy or harness)", st.Plan)
	}
	if st.Plan == "cached" {
		if st.Admission == "" {
			return errors.New("plan cached needs an admission profile")
		}
		if _, ok := c.AdmissionProfiles[st.Admission]; !ok {
			return fmt.Errorf("admission profile %q is not in admission_profiles", st.Admission)
		}
	} else if st.Admission != "" {
		return fmt.Errorf("admission %q is only for plan cached", st.Admission)
	}
	if st.Group != "" {
		if _, ok := c.Groups[st.Group]; !ok {
			return fmt.Errorf("unknown group %q", st.Group)
		}
	}
	switch {
	case st.SQLC != "" && st.File != "":
		return errors.New("sqlc and file are exclusive")
	case st.SQLC != "":
		if st.DerivedFrom != "" {
			return errors.New("derived_from needs a file, not sqlc")
		}
		text, ok := sqlcText[st.SQLC]
		if !ok && st.Pending == "" {
			return fmt.Errorf("sqlc statement %q does not exist (mark it pending if it is not merged yet)", st.SQLC)
		}
		st.SQL = text
	case st.File != "":
		body, err := fs.ReadFile(fsys, path.Join(dir, st.File))
		if err != nil {
			return fmt.Errorf("file: %w", err)
		}
		st.SQL = string(body)
		if Normalize(st.SQL) == "" {
			return fmt.Errorf("file %s holds no statement", st.File)
		}
		if st.DerivedFrom != "" {
			src, ok := sqlcText[st.DerivedFrom]
			switch {
			case !ok && st.Pending == "":
				return fmt.Errorf("derived_from sqlc statement %q does not exist", st.DerivedFrom)
			case ok && !DerivedBodyMatches(src, st.SQL):
				return fmt.Errorf("file %s is no longer part of sqlc statement %q (lock clause aside)", st.File, st.DerivedFrom)
			}
		}
	default:
		return errors.New("needs sqlc or file")
	}
	return nil
}

func (c *Corpus) resolveInherit() error {
	done := map[string]bool{}
	var resolve func(e *Entry, seen []string) error
	resolve = func(e *Entry, seen []string) error {
		if done[e.ID] || e.Inherit == "" {
			done[e.ID] = true
			return nil
		}
		for _, id := range seen {
			if id == e.ID {
				return fmt.Errorf("inherit cycle: %s", strings.Join(append(seen, e.ID), " → "))
			}
		}
		parent := c.byID[e.Inherit]
		if parent == nil {
			return fmt.Errorf("entry %q: inherit names unknown entry %q", e.ID, e.Inherit)
		}
		if err := resolve(parent, append(seen, e.ID)); err != nil {
			return err
		}
		inherit(e, parent)
		done[e.ID] = true
		return nil
	}
	for _, e := range c.Entries {
		if err := resolve(e, nil); err != nil {
			return err
		}
	}
	return nil
}

// inherit fills e's empty expect, mode, modes and tiers fields from parent; e's own values win.
func inherit(e, parent *Entry) {
	if len(e.Tiers) == 0 {
		e.Tiers = append([]string(nil), parent.Tiers...)
	}
	if len(e.Modes) == 0 {
		e.Modes = append([]string(nil), parent.Modes...)
	}
	for k, v := range parent.Mode {
		if e.Mode == nil {
			e.Mode = map[string]*ModeOverride{}
		}
		if _, own := e.Mode[k]; !own {
			e.Mode[k] = v
		}
	}
	x, p := &e.Expect, parent.Expect
	if x.Driver == nil {
		x.Driver = p.Driver
	}
	if x.Require == nil {
		x.Require = p.Require
	}
	if x.Forbid == nil {
		x.Forbid = p.Forbid
	}
	if x.SortExempt == nil {
		x.SortExempt = p.SortExempt
	}
	if x.Budget.Buffers == 0 {
		x.Budget.Buffers = p.Budget.Buffers
	}
	if x.Budget.PlanningMs == 0 {
		x.Budget.PlanningMs = p.Budget.PlanningMs
	}
	if x.Budget.ExecMs == 0 {
		x.Budget.ExecMs = p.Budget.ExecMs
	}
	if x.Budget.Rows == 0 {
		x.Budget.Rows = p.Budget.Rows
	}
}

func (c *Corpus) checkEntry(e *Entry) error {
	if e.Stmt == "" {
		return errors.New("missing stmt")
	}
	if _, ok := c.Statements[e.Stmt]; !ok {
		return fmt.Errorf("unknown stmt %q", e.Stmt)
	}
	for _, t := range e.Tiers {
		if !validTiers[t] {
			return fmt.Errorf("unknown tier %q", t)
		}
	}
	for _, m := range e.Modes {
		if !validModes[m] {
			return fmt.Errorf("unknown mode %q (want custom, generic or auto)", m)
		}
	}
	if e.Baseline != "" && !strings.HasPrefix(e.Baseline, "unstable: ") {
		return fmt.Errorf("baseline %q must be empty or \"unstable: <reason>\"", e.Baseline)
	}
	if err := c.checkExpect(&e.Expect); err != nil {
		return err
	}
	for _, m := range sortedKeys(e.Mode) {
		if !validModes[m] {
			return fmt.Errorf("mode %q: unknown mode (want custom, generic or auto)", m)
		}
		o := e.Mode[m]
		if o == nil {
			return fmt.Errorf("mode %q: empty override", m)
		}
		if o.Skip != "" && o.DegradeTo != nil {
			return fmt.Errorf("mode %q: skip and degrade_to are exclusive", m)
		}
		if o.DegradeTo != nil {
			if err := c.checkExpect(o.DegradeTo); err != nil {
				return fmt.Errorf("mode %q degrade_to: %w", m, err)
			}
		}
	}
	return nil
}

// checkExpect validates x and expands "big" in its node rules in place. Inherited rules are shared with the
// parent, so the expansion is idempotent.
func (c *Corpus) checkExpect(x *Expect) error {
	for _, name := range x.SortExempt {
		if _, ok := c.SortExemptSets[name]; !ok {
			return fmt.Errorf("sort_exempt %q is not in sort_exempt_sets", name)
		}
	}
	for i, d := range x.Driver {
		for _, f := range d.IndexCond {
			if err := checkModeNeutral(f); err != nil {
				return fmt.Errorf("driver %d: %w", i, err)
			}
		}
	}
	var err error
	if x.Require, err = c.expandBig(x.Require); err != nil {
		return err
	}
	if x.Forbid, err = c.expandBig(x.Forbid); err != nil {
		return err
	}
	return nil
}

func (c *Corpus) expandBig(rules []NodeRule) ([]NodeRule, error) {
	for i := range rules {
		var rels []string
		changed := false
		for _, r := range rules[i].Relations {
			if r != "big" {
				rels = append(rels, r)
				continue
			}
			if len(c.Defaults.BigTables) == 0 {
				return nil, fmt.Errorf("%s rule names relations [big] but defaults.big_tables is empty", rules[i].Node)
			}
			rels = append(rels, c.Defaults.BigTables...)
			changed = true
		}
		if changed {
			rules[i].Relations = rels
		}
	}
	return rules, nil
}

// checkModeNeutral rejects an Index Cond fragment holding a literal: a quote, or a number outside a $n
// parameter. Custom plans inline the parameter values and generic plans show $n, so only a fragment without
// literals matches both. Digits inside identifiers (idx_v2, t1.col) are names, not literals.
func checkModeNeutral(fragment string) error {
	if strings.ContainsAny(fragment, `'"`) {
		return fmt.Errorf("index_cond %q holds a literal (a quote); fragments must be mode-neutral", fragment)
	}
	for i := 0; i < len(fragment); i++ {
		ch := fragment[i]
		if ch < '0' || ch > '9' {
			continue
		}
		if i > 0 && isIdentOrParam(fragment[i-1]) {
			continue
		}
		return fmt.Errorf("index_cond %q holds a literal (a number); fragments must be mode-neutral", fragment)
	}
	return nil
}

func isIdentOrParam(b byte) bool {
	return b == '$' || b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
