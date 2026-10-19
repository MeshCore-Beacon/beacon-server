// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// BaselineMode says what a run does with the committed plan signatures.
type BaselineMode string

// Baseline modes.
const (
	BaselineCheck BaselineMode = "check" // compare each signature with its baseline file
	BaselineWrite BaselineMode = "write" // write each signature to its baseline file
	BaselineOff   BaselineMode = "off"
)

// RunOptions selects and configures the runs of one Run.
type RunOptions struct {
	Tier           string // fixture | local | prod
	Only, Skip     *regexp.Regexp
	Modes          []string // override every entry's modes
	IncludePending bool     // run pending statements too (local overlays only)
	SearchPath     string   // override every group's search_path
	Baseline       BaselineMode
	BaselineDir    string             // read: db/planguard/baseline/<tier>; write: the same path on disk
	Params         map[string]*string // pre-resolved (prod via emit); nil → resolve on the session
	Progress       func(Result)       // optional; called after each run
}

const (
	stmtName       = "planguard_q"
	autoExecutions = 6  // plan_cache_mode auto picks a generic plan after five custom ones
	prodTimeoutS   = 20 // statement_timeout cap for resolvers and plans on the prod tier
	localResolverS = 120
	defaultTimeout = 60
)

// plannerSettings are the GUCs a report records, as SHOWn when the run starts.
var plannerSettings = []string{
	"server_version", "random_page_cost", "seq_page_cost", "effective_cache_size", "effective_io_concurrency",
	"work_mem", "default_statistics_target", "cpu_tuple_cost", "cpu_index_tuple_cost", "cpu_operator_cost",
	"jit", "plan_cache_mode", "max_parallel_workers_per_gather", "from_collapse_limit", "join_collapse_limit",
	"geqo_threshold", "enable_seqscan", "enable_indexscan", "enable_indexonlyscan", "enable_bitmapscan",
	"enable_sort", "enable_incremental_sort", "enable_hashjoin", "enable_mergejoin", "enable_nestloop",
	"enable_hashagg", "enable_material", "enable_memoize",
}

var (
	plainIdentRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	quotedIdentRe = regexp.MustCompile(`^"[^"$]+"$`)
	gucNameRe     = regexp.MustCompile(`^[a-z_][a-z0-9_.]*$`)
)

// Run plans every selected (entry, mode) on s, one READ ONLY transaction per run, and checks each plan. It never
// writes: each run ends with ROLLBACK and DEALLOCATE as separate commands, on every path. A run that fails gives
// an ERROR result and the next run goes on; an error is returned only when the session itself cannot go on (or
// ctx ends), together with the results so far.
func Run(ctx context.Context, c *Corpus, s Session, o RunOptions) (*Report, error) {
	if err := o.validate(c); err != nil {
		return nil, err
	}
	t0 := time.Now()
	rep := &Report{Tier: o.Tier, StartedAt: t0.UTC(), Settings: map[string]string{}}
	finish := func(err error) (*Report, error) {
		rep.DurationMs = msSince(t0)
		return rep, err
	}
	if err := readMeta(ctx, s, c, o, rep); err != nil {
		return finish(err)
	}
	entries := c.selection(o)
	res := o.Params
	resErrs := map[string]string{}
	if res == nil {
		var err error
		if res, resErrs, err = resolve(ctx, s, c, o, entries); err != nil {
			return finish(err)
		}
	}
	for _, e := range entries {
		for _, mode := range o.modesOf(c, e) {
			sp := c.spec(o, e, mode, res, resErrs, false)
			r, err := c.runOne(ctx, s, o, sp)
			rep.Results = append(rep.Results, r)
			if o.Progress != nil {
				o.Progress(r)
			}
			if err != nil {
				return finish(err)
			}
		}
	}
	return finish(nil)
}

func (o *RunOptions) validate(c *Corpus) error {
	if !validTiers[o.Tier] {
		return fmt.Errorf("unknown tier %q (want fixture, local or prod)", o.Tier)
	}
	for _, m := range o.Modes {
		if !validModes[m] {
			return fmt.Errorf("unknown mode %q (want custom, generic or auto)", m)
		}
	}
	if o.Tier == "prod" || o.Baseline == "" {
		o.Baseline = BaselineOff
	}
	switch o.Baseline {
	case BaselineOff:
	case BaselineCheck, BaselineWrite:
		if o.BaselineDir == "" {
			return fmt.Errorf("baseline %s needs a baseline directory", o.Baseline)
		}
	default:
		return fmt.Errorf("unknown baseline mode %q (want check, write or off)", o.Baseline)
	}
	if err := checkSearchPath(o.SearchPath); err != nil {
		return err
	}
	for _, e := range c.Entries {
		if err := CheckName(e.ID); err != nil {
			return fmt.Errorf("entry id: %w", err)
		}
	}
	for name := range c.Resolvers {
		if err := CheckName(name); err != nil {
			return fmt.Errorf("resolver name: %w", err)
		}
	}
	for name, ts := range c.Tiers {
		if err := checkSearchPath(ts.SearchPath); err != nil {
			return fmt.Errorf("tier %s: %w", name, err)
		}
		for k := range ts.Settings {
			if !gucNameRe.MatchString(k) {
				return fmt.Errorf("tier %s: setting name %q is not a GUC name", name, k)
			}
		}
	}
	for name, g := range c.Groups {
		for tier, sp := range g.SearchPath {
			if err := checkSearchPath(sp); err != nil {
				return fmt.Errorf("group %s: %s: %w", name, tier, err)
			}
		}
	}
	return nil
}

// checkSearchPath accepts "" (no SET) or a comma list of plain identifiers and double-quoted identifiers without
// '$'. The list is sent unquoted in SET search_path, so nothing else may appear in it.
func checkSearchPath(sp string) error {
	if sp == "" {
		return nil
	}
	for _, el := range strings.Split(sp, ",") {
		el = strings.TrimSpace(el)
		if !plainIdentRe.MatchString(el) && !quotedIdentRe.MatchString(el) {
			return fmt.Errorf("search_path %q: element %q is not a plain or double-quoted identifier (no $)", sp, el)
		}
	}
	return nil
}

// selection returns the entries that run on o.Tier and pass Only and Skip, in corpus order. An entry with no
// tiers runs on every tier.
func (c *Corpus) selection(o RunOptions) []*Entry {
	var out []*Entry
	for _, e := range c.Entries {
		if len(e.Tiers) > 0 && !slices.Contains(e.Tiers, o.Tier) {
			continue
		}
		if o.Skip != nil && o.Skip.MatchString(e.ID) {
			continue
		}
		if o.Only != nil && !o.Only.MatchString(e.ID) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (o RunOptions) modesOf(c *Corpus, e *Entry) []string {
	if len(o.Modes) > 0 {
		return o.Modes
	}
	return c.ModesOf(e)
}

// baseSearchPath is the search_path of resolvers and of the report's index list: the override, else the tier's.
// "" inherits the session's.
func (c *Corpus) baseSearchPath(o RunOptions) string {
	if o.SearchPath != "" {
		return o.SearchPath
	}
	return c.Tiers[o.Tier].SearchPath
}

// searchPath is a statement's search_path on o.Tier: the override, its group's, else the tier's. "" means no SET,
// so the run inherits the session's.
func (c *Corpus) searchPath(o RunOptions, st *Statement) string {
	if o.SearchPath != "" {
		return o.SearchPath
	}
	if g, ok := c.Groups[st.Group]; ok {
		if sp, ok := g.SearchPath[o.Tier]; ok {
			return sp
		}
	}
	return c.Tiers[o.Tier].SearchPath
}

// readMeta fills rep.Settings and rep.Indexes in one READ ONLY transaction.
func readMeta(ctx context.Context, s Session, c *Corpus, o RunOptions, rep *Report) error {
	lines := []string{"BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
		"SET LOCAL lock_timeout = '1s'; SET LOCAL statement_timeout = '20s'; SET LOCAL standard_conforming_strings = on"}
	if sp := c.baseSearchPath(o); sp != "" {
		lines = append(lines, "SET LOCAL search_path = "+sp)
	}
	var names []string
	for _, n := range plannerSettings {
		names = append(names, quoteLit(n))
	}
	settingsQ := "SELECT n, current_setting(n, true) FROM unnest(ARRAY[" + strings.Join(names, ", ") + "]::text[]) n"
	err := func() error {
		for _, l := range lines {
			if _, err := s.Exec(ctx, l); err != nil {
				return err
			}
		}
		res, err := s.Exec(ctx, settingsQ)
		if err != nil {
			return err
		}
		for _, row := range lastRows(res) {
			if len(row) == 2 && row[1] != nil {
				rep.Settings[string(row[0])] = string(row[1])
			}
		}
		rep.Indexes = []string{}
		if len(c.Defaults.BigTables) == 0 {
			return nil
		}
		var tables []string
		for _, t := range c.Defaults.BigTables {
			tables = append(tables, quoteLit(t))
		}
		res, err = s.Exec(ctx, "SELECT DISTINCT ic.relname::text FROM unnest(ARRAY["+strings.Join(tables, ", ")+
			"]::text[]) b(name) JOIN pg_index i ON i.indrelid = to_regclass(quote_ident(b.name)) "+
			"JOIN pg_class ic ON ic.oid = i.indexrelid ORDER BY 1")
		if err != nil {
			return err
		}
		for _, row := range lastRows(res) {
			rep.Indexes = append(rep.Indexes, string(row[0]))
		}
		return nil
	}()
	if rbErr := cleanupRollback(ctx, s); rbErr != nil {
		return fmt.Errorf("read settings: rollback: %w", rbErr)
	}
	if err != nil {
		return fmt.Errorf("read settings and indexes: %s", errText(err))
	}
	return nil
}

// resolverRef is a param's resolver: a named one ({r: name}) or inline SQL ({sql: ...}, keyed by its hash).
func resolverRef(c *Corpus, m map[string]any) (key, sql string, ok bool) {
	if r, isStr := m["r"].(string); isStr {
		return r, c.Resolvers[r], true
	}
	if q, isStr := m["sql"].(string); isStr {
		h := sha1.Sum([]byte(q))
		return "sql_" + hex.EncodeToString(h[:6]), q, true
	}
	return "", "", false
}

// neededResolvers returns key → SQL for every resolver the entries' params use. An unknown name maps to "".
// Entries of pending statements are left out unless includePending: they skip, so their resolvers never run.
func neededResolvers(c *Corpus, entries []*Entry, includePending bool) map[string]string {
	need := map[string]string{}
	var visit func(v any)
	visit = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if k, q, ok := resolverRef(c, x); ok {
				need[k] = q
			}
		case []any:
			for _, y := range x {
				visit(y)
			}
		}
	}
	for _, e := range entries {
		if st := c.Statements[e.Stmt]; st != nil && st.Pending != "" && !includePending {
			continue
		}
		for _, p := range e.Params {
			visit(p)
		}
	}
	return need
}

func resolverSelect(sql string) string {
	return "SELECT coalesce((" + trimStmt(sql) + ")::text, NULL)"
}

// resolve runs the entries' resolvers in one READ ONLY transaction, each under a savepoint so that one failing
// resolver fails only its own entries. The error is for a session that cannot go on.
func resolve(ctx context.Context, s Session, c *Corpus, o RunOptions, entries []*Entry) (map[string]*string, map[string]string, error) {
	need := neededResolvers(c, entries, o.IncludePending)
	vals, errs := map[string]*string{}, map[string]string{}
	if len(need) == 0 {
		return vals, errs, nil
	}
	timeout := localResolverS
	if o.Tier == "prod" {
		timeout = prodTimeoutS
	}
	setup := []string{
		"BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
		fmt.Sprintf("SET LOCAL statement_timeout = '%ds'; SET LOCAL lock_timeout = '1s'", timeout),
		"SET LOCAL jit = off; SET LOCAL timezone = 'UTC'; SET LOCAL standard_conforming_strings = on",
	}
	if sp := c.baseSearchPath(o); sp != "" {
		setup = append(setup, "SET LOCAL search_path = "+sp)
	}
	err := func() error {
		for _, l := range setup {
			if _, err := s.Exec(ctx, l); err != nil {
				return err
			}
		}
		for _, name := range sortedKeys(need) {
			if need[name] == "" {
				errs[name] = "no such resolver in the corpus"
				continue
			}
			if err := checkStatementText(need[name]); err != nil {
				errs[name] = err.Error()
				continue
			}
			if _, err := s.Exec(ctx, "SAVEPOINT planguard_r"); err != nil {
				return err
			}
			res, err := s.Exec(ctx, resolverSelect(need[name]))
			if err != nil {
				if ctx.Err() != nil {
					return err
				}
				errs[name] = errText(err)
				if _, err := s.Exec(ctx, "ROLLBACK TO SAVEPOINT planguard_r"); err != nil {
					return err
				}
				continue
			}
			rows := lastRows(res)
			if len(rows) != 1 || len(rows[0]) != 1 {
				errs[name] = fmt.Sprintf("returned %d rows", len(rows))
				if _, err := s.Exec(ctx, "RELEASE SAVEPOINT planguard_r"); err != nil {
					return err
				}
				continue
			}
			if rows[0][0] != nil {
				v := string(rows[0][0])
				vals[name] = &v
			} else {
				vals[name] = nil
			}
			if _, err := s.Exec(ctx, "RELEASE SAVEPOINT planguard_r"); err != nil {
				return err
			}
		}
		return nil
	}()
	if rbErr := cleanupRollback(ctx, s); rbErr != nil {
		return nil, nil, fmt.Errorf("resolvers: rollback: %w", rbErr)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("resolvers: %s", errText(err))
	}
	return vals, errs, nil
}

// runSpec is one (entry, mode) run, rendered.
type runSpec struct {
	e        *Entry
	st       *Statement
	r        Result // identity and params
	skip     string // the run is not made
	err      string // the run cannot be made: ERROR
	exp      *Expect
	analyzed bool
	setup    []string // BEGIN, SET LOCAL ..., PREPARE
	execs    int
	call     string // EXECUTE planguard_q(...)
	explain  string
}

func modeGUC(m string) string {
	switch m {
	case ModeCustom:
		return "force_custom_plan"
	case ModeGeneric:
		return "force_generic_plan"
	}
	return "auto"
}

// spec renders one run. plain forces plain EXPLAIN with no executions (the psql path).
func (c *Corpus) spec(o RunOptions, e *Entry, mode string, res map[string]*string, resErrs map[string]string, plain bool) runSpec {
	st := c.Statements[e.Stmt]
	sp := runSpec{e: e, st: st, r: Result{ID: e.ID, Stmt: e.Stmt, Shape: e.Shape, Mode: mode, Tier: o.Tier}, exp: &e.Expect}
	if st.Pending != "" && !o.IncludePending {
		sp.skip = "pending: " + st.Pending
		return sp
	}
	if ov := e.Mode[mode]; ov != nil {
		if ov.Skip != "" {
			sp.skip = ov.Skip
			return sp
		}
		if ov.DegradeTo != nil {
			sp.exp, sp.r.Degraded = ov.DegradeTo, ov.Reason
			if sp.r.Degraded == "" {
				sp.r.Degraded = "degrade_to"
			}
		}
	}
	params := make([]string, 0, len(e.Params))
	for _, p := range e.Params {
		v, skip, err := renderParam(c, p, res, resErrs)
		if skip != "" {
			sp.skip = skip
			return sp
		}
		if err != nil {
			sp.err = err.Error()
			return sp
		}
		params = append(params, v)
	}
	sp.r.Params = params
	text := trimStmt(st.SQL)
	if text == "" {
		sp.err = fmt.Sprintf("statement %s has no text on this build", e.Stmt)
		return sp
	}
	if err := checkStatementText(text); err != nil {
		sp.err = fmt.Sprintf("statement %s: %v", e.Stmt, err)
		return sp
	}

	sp.analyzed = !plain && o.Tier == "local" && st.Kind == "select" && (e.Analyze == nil || *e.Analyze)
	if !plain && st.Kind == "select" && o.Tier != "prod" {
		if sp.analyzed {
			sp.execs = c.Defaults.Executions
			if e.Executions != nil {
				sp.execs = *e.Executions
			}
		}
		if mode == ModeAuto && sp.execs < autoExecutions {
			sp.execs = autoExecutions
		}
	}
	timeout := c.Defaults.TimeoutS
	if e.TimeoutS > 0 {
		timeout = e.TimeoutS
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if o.Tier == "prod" && timeout > prodTimeoutS {
		timeout = prodTimeoutS
	}

	l2 := "SET LOCAL jit = off; SET LOCAL timezone = 'UTC'; SET LOCAL standard_conforming_strings = on"
	if path := c.searchPath(o, st); path != "" {
		l2 += "; SET LOCAL search_path = " + path
	}
	sp.setup = []string{
		"BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
		l2,
		fmt.Sprintf("SET LOCAL statement_timeout = '%ds'; SET LOCAL lock_timeout = '1s'", timeout),
		"SET LOCAL plan_cache_mode = " + modeGUC(mode),
	}
	switch st.Plan {
	case "cached", "custom", "maint":
		sp.setup = append(sp.setup, "SET LOCAL max_parallel_workers_per_gather = 0")
	}
	if ts := c.Tiers[o.Tier].Settings; len(ts) > 0 {
		var sets []string
		for _, k := range sortedKeys(ts) {
			sets = append(sets, "SET LOCAL "+k+" = "+quoteLit(ts[k]))
		}
		sp.setup = append(sp.setup, strings.Join(sets, "; "))
	}
	sp.setup = append(sp.setup, "PREPARE "+stmtName+" AS\n"+text)
	sp.call = "EXECUTE " + stmtName
	if len(params) > 0 {
		sp.call += "(" + strings.Join(params, ", ") + ")"
	}
	opts := "FORMAT JSON"
	if sp.analyzed {
		opts = "ANALYZE, BUFFERS, FORMAT JSON"
	}
	sp.explain = "EXPLAIN (" + opts + ") " + sp.call
	return sp
}

// runOne makes one run. The error is for a session that cannot go on (the rollback or deallocate failed, or ctx
// ended); the Result is filled either way.
func (c *Corpus) runOne(ctx context.Context, s Session, o RunOptions, sp runSpec) (Result, error) {
	r := sp.r
	switch {
	case sp.skip != "":
		r.Verdict, r.Skip = VerdictSkip, sp.skip
		return r, nil
	case sp.err != "":
		c.errored(&r, sp.e, sp.err)
		return r, nil
	}
	t0 := time.Now()
	var plan []byte
	runErr := func() error {
		for _, l := range sp.setup {
			if _, err := s.Exec(ctx, l); err != nil {
				return err
			}
		}
		for i := 0; i < sp.execs; i++ {
			if _, err := s.Exec(ctx, sp.call); err != nil {
				return err
			}
		}
		res, err := s.Exec(ctx, sp.explain)
		if err != nil {
			return err
		}
		var lines []string
		for _, row := range lastRows(res) {
			if len(row) > 0 {
				lines = append(lines, string(row[0]))
			}
		}
		plan = []byte(strings.Join(lines, "\n"))
		return nil
	}()
	r.WallMs = msSince(t0)
	// Prepared statements outlive ROLLBACK: end the transaction, then deallocate, as separate commands.
	cctx, cancel := cleanupCtx(ctx)
	defer cancel()
	rbErr := rollback(cctx, s)
	_, daErr := s.Exec(cctx, "DEALLOCATE "+stmtName)
	if isSQLState(daErr, "26000") { // never prepared: the run failed before PREPARE
		daErr = nil
	}
	if runErr != nil {
		c.errored(&r, sp.e, errText(runErr))
	} else if err := c.judge(&r, sp, plan, o); err != nil {
		return r, err
	}
	switch {
	case rbErr != nil:
		return r, fmt.Errorf("%s|%s: rollback: %w", r.ID, r.Mode, rbErr)
	case daErr != nil:
		return r, fmt.Errorf("%s|%s: deallocate: %w", r.ID, r.Mode, daErr)
	case ctx.Err() != nil:
		return r, ctx.Err()
	}
	return r, nil
}

func (c *Corpus) errored(r *Result, e *Entry, msg string) {
	r.Failures = []Failure{{Kind: FailError, Msg: "error: " + msg}}
	r.Verdict = Decide(e.ExpectFail, e.KnownIssue, r.Failures, nil)
}

// judge checks one plan and decides the run's verdict. The error is a baseline file that cannot be read or written.
func (c *Corpus) judge(r *Result, sp runSpec, plan []byte, o RunOptions) error {
	ex, err := ParseExplain(plan)
	if err != nil {
		c.errored(r, sp.e, err.Error())
		return nil
	}
	ch := c.Assess(ex, sp.exp, sp.analyzed)
	r.Failures, r.Drivers, r.Retired = ch.Failures, ch.Drivers, ch.Retired
	r.Buffers, r.Rows = ch.Buffers, ch.Rows
	r.PlanningMs, r.ExecMs = ex.PlanningMs, ex.ExecMs
	r.Signature = Signature(ex.Root)
	r.Plan = append(json.RawMessage(nil), plan...)
	if err := baselineStep(r, sp.e, o); err != nil {
		return err
	}
	r.Verdict = Decide(sp.e.ExpectFail, sp.e.KnownIssueIn(r.Mode), r.Failures, r.Retired)
	return nil
}

// baselineStep compares r's signature with, or writes it to, <BaselineDir>/<id>.<mode>.json. Expect-fail entries
// and entries marked unstable are left out.
func baselineStep(r *Result, e *Entry, o RunOptions) error {
	if o.Baseline == BaselineOff || e.ExpectFail || e.Baseline != "" {
		return nil
	}
	path := filepath.Join(o.BaselineDir, r.ID+"."+r.Mode+".json")
	if o.Baseline == BaselineWrite {
		if err := os.MkdirAll(o.BaselineDir, 0o755); err != nil {
			return err
		}
		b, err := json.MarshalIndent(r.Signature, "", " ")
		if err != nil {
			return err
		}
		return os.WriteFile(path, append(b, '\n'), 0o644)
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		r.PlanChange = []string{"no baseline"}
		r.Failures = append(r.Failures, Failure{Kind: FailPlanChange, Msg: "plan_change: no baseline"})
		return nil
	}
	if err != nil {
		return fmt.Errorf("baseline %s: %w", path, err)
	}
	var base []string
	if err := json.Unmarshal(b, &base); err != nil {
		return fmt.Errorf("baseline %s: %w", path, err)
	}
	if !slices.Equal(base, r.Signature) {
		r.PlanChange = lineDiff(base, r.Signature)
		r.Failures = append(r.Failures, Failure{Kind: FailPlanChange, Msg: "plan_change: signature differs from baseline (see plan_change)"})
	}
	return nil
}

// lineDiff lists the lines that differ, position by position, as "- old" / "+ new".
func lineDiff(a, b []string) []string {
	var out []string
	for i := 0; i < max(len(a), len(b)); i++ {
		var x, y string
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			out = append(out, "- "+x, "+ "+y)
		}
	}
	return out
}

// renderParam renders one param as an untyped literal, so the server infers its type as it does for pgx:
// strings quoted, lists as '{…}' array literals, null as NULL, {raw: …} verbatim, a resolver as its quoted value.
// skip is set when a resolver has no value on this tier.
func renderParam(c *Corpus, v any, res map[string]*string, resErrs map[string]string) (lit, skip string, err error) {
	switch x := v.(type) {
	case nil:
		return "NULL", "", nil
	case string:
		return quoteLit(x), "", nil
	case int, int64, uint64, float64, bool:
		return fmt.Sprint(x), "", nil
	case []any:
		parts := make([]string, 0, len(x))
		for _, y := range x {
			switch el := y.(type) {
			case map[string]any:
				lit, skip, err := renderParam(c, el, res, resErrs)
				if skip != "" || err != nil {
					return "", skip, err
				}
				if strings.HasPrefix(lit, "'") {
					s, _ := unquoteLit(lit)
					parts = append(parts, arrayElem(s))
				} else {
					parts = append(parts, lit)
				}
			default:
				parts = append(parts, arrayElem(el))
			}
		}
		return quoteLit("{" + strings.Join(parts, ",") + "}"), "", nil
	case map[string]any:
		if raw, ok := x["raw"].(string); ok {
			return raw, "", nil
		}
		key, _, ok := resolverRef(c, x)
		if !ok {
			return "", "", fmt.Errorf("param %v is not a value, {raw}, {r} or {sql}", x)
		}
		if msg, failed := resErrs[key]; failed {
			return "", "", fmt.Errorf("resolver %s: %s", key, msg)
		}
		val, ok := res[key]
		if !ok {
			return "", "", fmt.Errorf("resolver %s was not resolved", key)
		}
		if val == nil {
			return "", "resolver " + key + " returned no value on this tier", nil
		}
		return quoteLit(*val), "", nil
	}
	return "", "", fmt.Errorf("unsupported param type %T", v)
}

// arrayElem renders one element of an array literal.
func arrayElem(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return `"` + strings.ReplaceAll(strings.ReplaceAll(x, `\`, `\\`), `"`, `\"`) + `"`
	}
	return fmt.Sprint(v)
}

func quoteLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func unquoteLit(s string) (string, bool) {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), true
}

// trimStmt drops surrounding space and one trailing semicolon.
func trimStmt(sql string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
}

// cleanupCtx outlives ctx's cancellation, so a transaction still ends when the run is interrupted.
func cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
}

func cleanupRollback(ctx context.Context, s Session) error {
	cctx, cancel := cleanupCtx(ctx)
	defer cancel()
	return rollback(cctx, s)
}

func rollback(ctx context.Context, s Session) error {
	_, err := s.Exec(ctx, "ROLLBACK")
	return err
}

func lastRows(res []*pgconn.Result) [][][]byte {
	if len(res) == 0 {
		return nil
	}
	return res[len(res)-1].Rows
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
