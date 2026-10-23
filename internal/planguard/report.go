// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// Result is one run: one entry in one plan mode on one tier.
type Result struct {
	ID         string          `json:"id"`
	Stmt       string          `json:"stmt"`
	Shape      string          `json:"shape"`
	Mode       string          `json:"mode"`
	Tier       string          `json:"tier"`
	Verdict    Verdict         `json:"verdict"`
	Failures   []Failure       `json:"failures,omitempty"`
	Drivers    []string        `json:"drivers,omitempty"`
	Retired    []string        `json:"retired,omitempty"`
	Buffers    int64           `json:"buffers"`
	Rows       int64           `json:"rows"`
	PlanningMs float64         `json:"planning_ms"`
	ExecMs     float64         `json:"exec_ms"`
	WallMs     float64         `json:"wall_ms"`
	Degraded   string          `json:"degraded,omitempty"`
	Skip       string          `json:"skip,omitempty"`
	Params     []string        `json:"params,omitempty"`
	Signature  []string        `json:"signature,omitempty"`
	PlanChange []string        `json:"plan_change,omitempty"`
	Plan       json.RawMessage `json:"-"`
}

// Report is one Run.
type Report struct {
	Tier       string            `json:"tier"`
	StartedAt  time.Time         `json:"started_at"`
	DurationMs float64           `json:"duration_ms"`
	Settings   map[string]string `json:"settings"` // planner GUCs as SHOWn at start
	Indexes    []string          `json:"indexes"`  // indexes on big_tables in the run's search_path
	Results    []Result          `json:"results"`
}

// Counts returns runs per verdict.
func (r *Report) Counts() map[Verdict]int {
	out := map[Verdict]int{}
	for _, x := range r.Results {
		out[x.Verdict]++
	}
	return out
}

// Failures returns the failing runs (FALSE_POSITIVE, MISSED, ERROR), in run order.
func (r *Report) Failures() []Result {
	var out []Result
	for _, x := range r.Results {
		if x.Verdict.Failing() {
			out = append(out, x)
		}
	}
	return out
}

// WriteFiles writes results.<tier>.jsonl, summary.<tier>.md, report.<tier>.json (the report without its results)
// and plans/<tier>/<id>.<mode>.json into dir.
func (r *Report) WriteFiles(dir string) error {
	plans := filepath.Join(dir, "plans", r.Tier)
	if err := os.MkdirAll(plans, 0o755); err != nil {
		return err
	}
	var jl bytes.Buffer
	enc := json.NewEncoder(&jl)
	for _, x := range r.Results {
		if err := enc.Encode(x); err != nil {
			return err
		}
		if len(x.Plan) > 0 {
			if err := os.WriteFile(filepath.Join(plans, x.ID+"."+x.Mode+".json"), x.Plan, 0o644); err != nil {
				return err
			}
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "results."+r.Tier+".jsonl"), jl.Bytes(), 0o644); err != nil {
		return err
	}
	meta := *r
	meta.Results = nil
	b, err := json.MarshalIndent(meta, "", " ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report."+r.Tier+".json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "summary."+r.Tier+".md"), []byte(r.summary()), 0o644)
}

// CountsString renders Counts sorted by verdict, like fmt prints a map.
func (r *Report) CountsString() string {
	return fmt.Sprint(r.Counts())
}

func (r *Report) summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# planguard %s: %d runs in %.1f s; verdicts %s\n\n", r.Tier, len(r.Results), r.DurationMs/1000, r.CountsString())
	b.WriteString("| entry | mode | verdict | buffers | plan ms | exec ms | wall ms | drivers | failures |\n")
	b.WriteString("|---|---|---|---:|---:|---:|---:|---|---|\n")
	esc := func(s string) string {
		return strings.ReplaceAll(strings.ReplaceAll(s, "|", `\|`), "\n", " ")
	}
	for _, x := range r.Results {
		var fl []string
		for _, f := range x.Failures {
			fl = append(fl, esc(f.Msg))
		}
		note := strings.Join(fl, "<br>")
		if x.Skip != "" {
			note = "skip: " + esc(x.Skip)
		}
		if len(x.Retired) > 0 {
			note = "(plan uses retired " + strings.Join(x.Retired, ",") + ") " + note
		}
		if x.Degraded != "" {
			note = "(degrade_to: " + esc(x.Degraded) + ") " + note
		}
		var drv []string
		for _, d := range x.Drivers {
			drv = append(drv, esc(d))
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %d | %.2f | %.1f | %.0f | %s | %s |\n", esc(x.ID), x.Mode, x.Verdict, x.Buffers,
			x.PlanningMs, x.ExecMs, x.WallMs, strings.Join(drv, "<br>"), note)
	}
	return b.String()
}

// Line is the one-line form of a run that the CLI prints.
func (x Result) Line() string {
	var fl []string
	for _, f := range x.Failures {
		fl = append(fl, f.Msg)
	}
	if len(fl) > 2 {
		fl = append(fl[:2:2], fmt.Sprintf("(+%d)", len(x.Failures)-2))
	}
	note := strings.Join(fl, " ; ")
	if x.Skip != "" {
		note = "skip: " + x.Skip
	}
	return fmt.Sprintf("%-58s %-8s %-14s B=%-8d %7.0fms %s", x.ID, x.Mode, x.Verdict, x.Buffers, x.WallMs, note)
}

// ReadResults reads a results.<tier>.jsonl file. A line that does not decode is an error.
func ReadResults(path string) ([]Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Result
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 64<<20)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var r Result
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&r); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, n, err)
		}
		out = append(out, r)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return out, nil
}

// ReadReportMeta reads the report.<tier>.json that WriteFiles put beside a results.<tier>.jsonl.
func ReadReportMeta(resultsPath string) (*Report, error) {
	base := filepath.Base(resultsPath)
	if !strings.HasPrefix(base, "results.") || !strings.HasSuffix(base, ".jsonl") {
		return nil, fmt.Errorf("%s: not a results.<tier>.jsonl file", resultsPath)
	}
	tier := strings.TrimSuffix(strings.TrimPrefix(base, "results."), ".jsonl")
	b, err := os.ReadFile(filepath.Join(filepath.Dir(resultsPath), "report."+tier+".json"))
	if err != nil {
		return nil, err
	}
	var r Report
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("report.%s.json: %w", tier, err)
	}
	return &r, nil
}

// FidelityResult compares the drivers of two runs of the same corpus.
type FidelityResult struct {
	Match, Total int
	Excluded     int
	Mismatches   []string
}

// Fidelity compares the driving index sets per (entry, mode) of two runs. Runs where either plan uses an index
// absent from the other report's Indexes are excluded and counted. A nil index list is unknown and excludes
// nothing. Only runs present in both, neither skipped nor errored and with a driver on at least one side, count.
func Fidelity(a, b []Result, aIndexes, bIndexes []string) FidelityResult {
	var out FidelityResult
	bBy := map[string]Result{}
	for _, r := range b {
		bBy[r.ID+"|"+r.Mode] = r
	}
	absent := func(ixs, have []string) bool {
		return have != nil && slices.ContainsFunc(ixs, func(ix string) bool { return !slices.Contains(have, ix) })
	}
	for _, ra := range a {
		key := ra.ID + "|" + ra.Mode
		rb, ok := bBy[key]
		if !ok || !compared(ra) || !compared(rb) {
			continue
		}
		ia, ib := driverIndexes(ra), driverIndexes(rb)
		if len(ia) == 0 && len(ib) == 0 {
			continue
		}
		if absent(ia, bIndexes) || absent(ib, aIndexes) {
			out.Excluded++
			continue
		}
		out.Total++
		if slices.Equal(ia, ib) {
			out.Match++
		} else {
			out.Mismatches = append(out.Mismatches, fmt.Sprintf("%s: %v vs %v", key, ia, ib))
		}
	}
	return out
}

func compared(r Result) bool { return r.Verdict != VerdictSkip && r.Verdict != VerdictError }

// driverIndexes returns the sorted, distinct indexes of a run's drivers ("relation:node:ix+ix").
func driverIndexes(r Result) []string {
	var out []string
	for _, d := range r.Drivers {
		i := strings.LastIndexByte(d, ':')
		if i < 0 {
			continue
		}
		for _, ix := range strings.Split(d[i+1:], "+") {
			if ix != "" && !slices.Contains(out, ix) {
				out = append(out, ix)
			}
		}
	}
	sort.Strings(out)
	return out
}
