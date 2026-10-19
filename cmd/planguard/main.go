// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command planguard checks that the planner keeps choosing the plans Beacon's statements were designed for.
//
//	planguard run --tier fixture|local|prod [--corpus db/planguard/corpus.yaml] [--dsn $PLANGUARD_DSN]
//	              [--out planguard-out] [--only RE] [--skip RE] [--modes m,..] [--include-pending] [--search-path P]
//	              [--baseline check|write|off] [--emit resolvers|plans --params FILE] [--plans FILE --params FILE]
//	              [--fidelity-against results.jsonl]
//
// Exit status: 0 when no run fails, 1 when one does (or the run cannot be made), 2 on usage errors.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
	"github.com/jackc/pgx/v5/pgconn"
)

const usage = "usage: planguard run --tier fixture|local|prod [flags] (planguard run -h for the flags)"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	switch args[0] {
	case "run":
		return runCmd(args[1:], stdout, stderr)
	case "gen-policy", "diff", "storediff":
		fmt.Fprintf(stderr, "planguard %s: not implemented\n", args[0])
		return 1
	case "-h", "-help", "--help", "help":
		fmt.Fprintln(stderr, usage)
		return 2
	}
	fmt.Fprintf(stderr, "planguard: unknown subcommand %q\n%s\n", args[0], usage)
	return 2
}

type runFlags struct {
	tier, corpus, dsn, out, only, skip, modes, searchPath, baseline, emit, params, plans, fidelity string
	includePending                                                                                 bool
}

func usageErr(stderr io.Writer, format string, a ...any) int {
	fmt.Fprintf(stderr, "planguard run: "+format+"\n", a...)
	return 2
}

func runCmd(args []string, stdout, stderr io.Writer) int {
	var f runFlags
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.tier, "tier", "", "fixture | local | prod (required)")
	fs.StringVar(&f.corpus, "corpus", "db/planguard/corpus.yaml", "corpus file; baselines live in its baseline/<tier> directory")
	fs.StringVar(&f.dsn, "dsn", os.Getenv("PLANGUARD_DSN"), "PostgreSQL DSN (default $PLANGUARD_DSN)")
	fs.StringVar(&f.out, "out", "planguard-out", "output directory")
	fs.StringVar(&f.only, "only", "", "regexp: run only entries whose id matches")
	fs.StringVar(&f.skip, "skip", "", "regexp: leave out entries whose id matches")
	fs.StringVar(&f.modes, "modes", "", "comma list of plan modes overriding every entry's")
	fs.BoolVar(&f.includePending, "include-pending", false, "run pending statements too (local overlays only)")
	fs.StringVar(&f.searchPath, "search-path", "", "override every group's search_path")
	fs.StringVar(&f.baseline, "baseline", "", "check | write | off (default check; prod is always off)")
	fs.StringVar(&f.emit, "emit", "", "resolvers | plans: write a read-only psql script to stdout instead of running")
	fs.StringVar(&f.params, "params", "", "psql output of the --emit resolvers script")
	fs.StringVar(&f.plans, "plans", "", "psql output of the --emit plans script: check it instead of running")
	fs.StringVar(&f.fidelity, "fidelity-against", "", "results.<tier>.jsonl of another run to compare drivers with")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		return usageErr(stderr, "unexpected argument %q", fs.Arg(0))
	}
	o, code := f.options(stderr)
	if code != 0 {
		return code
	}

	sqlc, err := planguard.LoadStatements("db/sqlc")
	if err != nil {
		fmt.Fprintln(stderr, "planguard:", err)
		return 1
	}
	c, err := planguard.LoadCorpus(os.DirFS(filepath.Dir(f.corpus)), filepath.Base(f.corpus), sqlc)
	if err != nil {
		fmt.Fprintln(stderr, "planguard:", err)
		return 1
	}
	if f.params != "" {
		if o.Params, err = readResolverOutput(f.params); err != nil {
			fmt.Fprintln(stderr, "planguard:", err)
			return 1
		}
	}

	switch {
	case f.emit == "resolvers":
		return emit(stderr, func(nonce string) error { return planguard.EmitResolvers(stdout, c, o, nonce) })
	case f.emit == "plans":
		return emit(stderr, func(nonce string) error { return planguard.EmitPlans(stdout, c, o, o.Params, nonce) })
	case f.plans != "":
		raw, err := os.ReadFile(f.plans)
		if err == nil {
			var nonce string
			if nonce, err = planguard.ScriptNonce(raw); err == nil {
				var plans map[string][]byte
				if plans, err = planguard.ParsePlanOutput(strings.NewReader(string(raw)), nonce); err == nil {
					var rep *planguard.Report
					rep, err = planguard.CheckPlans(c, o, plans)
					if err == nil {
						for _, r := range rep.Results {
							fmt.Fprintln(stderr, r.Line())
						}
						return finish(rep, nil, f, stderr)
					}
				}
			}
		}
		fmt.Fprintf(stderr, "planguard: %s: %v\n", f.plans, err)
		return 1
	}

	if f.dsn == "" {
		return usageErr(stderr, "--dsn (or $PLANGUARD_DSN) is required to run")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pc, err := pgconn.Connect(ctx, f.dsn)
	if err != nil {
		fmt.Fprintln(stderr, "planguard: connect:", err)
		return 1
	}
	defer pc.Close(context.Background())
	o.Progress = func(r planguard.Result) { fmt.Fprintln(stderr, r.Line()) }
	rep, err := planguard.Run(ctx, c, planguard.NewSession(pc), o)
	if rep == nil {
		fmt.Fprintln(stderr, "planguard:", err)
		return 1
	}
	return finish(rep, err, f, stderr)
}

// options checks the flags and builds the run options; code is 2 on a usage error.
func (f runFlags) options(stderr io.Writer) (planguard.RunOptions, int) {
	o := planguard.RunOptions{Tier: f.tier, IncludePending: f.includePending, SearchPath: f.searchPath}
	switch f.tier {
	case "":
		return o, usageErr(stderr, "--tier is required (fixture, local or prod)")
	case "fixture", "local", "prod":
	default:
		return o, usageErr(stderr, "unknown --tier %q (want fixture, local or prod)", f.tier)
	}
	switch f.baseline {
	case "":
		o.Baseline = planguard.BaselineCheck
	case "check", "write", "off":
		o.Baseline = planguard.BaselineMode(f.baseline)
	default:
		return o, usageErr(stderr, "unknown --baseline %q (want check, write or off)", f.baseline)
	}
	if f.tier == "prod" {
		o.Baseline = planguard.BaselineOff
	}
	o.BaselineDir = filepath.Join(filepath.Dir(f.corpus), "baseline", f.tier)
	var err error
	if f.only != "" {
		if o.Only, err = regexp.Compile(f.only); err != nil {
			return o, usageErr(stderr, "--only: %v", err)
		}
	}
	if f.skip != "" {
		if o.Skip, err = regexp.Compile(f.skip); err != nil {
			return o, usageErr(stderr, "--skip: %v", err)
		}
	}
	if f.modes != "" {
		for _, m := range strings.Split(f.modes, ",") {
			switch m = strings.TrimSpace(m); m {
			case "custom", "generic", "auto":
				o.Modes = append(o.Modes, m)
			default:
				return o, usageErr(stderr, "--modes: unknown mode %q (want custom, generic or auto)", m)
			}
		}
	}
	if (f.emit != "" || f.plans != "") && f.tier != "prod" {
		return o, usageErr(stderr, "--emit and --plans are the prod tier's psql path; use them with --tier prod")
	}
	switch f.emit {
	case "", "resolvers":
	case "plans":
		if f.params == "" {
			return o, usageErr(stderr, "--emit plans needs --params (the psql output of --emit resolvers)")
		}
	default:
		return o, usageErr(stderr, "unknown --emit %q (want resolvers or plans)", f.emit)
	}
	if f.plans != "" {
		if f.emit != "" {
			return o, usageErr(stderr, "--plans and --emit are exclusive")
		}
		if f.params == "" {
			return o, usageErr(stderr, "--plans needs --params (the psql output of --emit resolvers)")
		}
	}
	return o, 0
}

func emit(stderr io.Writer, write func(nonce string) error) int {
	nonce, err := planguard.NewNonce()
	if err == nil {
		err = write(nonce)
	}
	if err != nil {
		fmt.Fprintln(stderr, "planguard:", err)
		return 1
	}
	return 0
}

func readResolverOutput(path string) (map[string]*string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	nonce, err := planguard.ScriptNonce(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	res, err := planguard.ParseResolverOutput(strings.NewReader(string(raw)), nonce)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return res, nil
}

// finish writes the report files, prints the totals and the fidelity, and returns the exit status.
func finish(rep *planguard.Report, runErr error, f runFlags, stderr io.Writer) int {
	code := 0
	if err := rep.WriteFiles(f.out); err != nil {
		fmt.Fprintln(stderr, "planguard: write report:", err)
		code = 1
	}
	fmt.Fprintf(stderr, "%d runs in %.1f s: %s\n", len(rep.Results), rep.DurationMs/1000, rep.CountsString())
	if f.fidelity != "" {
		other, err := planguard.ReadResults(f.fidelity)
		if err != nil {
			fmt.Fprintln(stderr, "planguard: fidelity:", err)
			code = 1
		} else {
			var otherIx []string
			if meta, err := planguard.ReadReportMeta(f.fidelity); err == nil {
				otherIx = meta.Indexes
			} else if errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(stderr, "planguard: fidelity: no report file beside %s; its index set is unknown, so nothing is excluded for it\n", f.fidelity)
			} else {
				fmt.Fprintln(stderr, "planguard: fidelity:", err)
				code = 1
			}
			fr := planguard.Fidelity(rep.Results, other, rep.Indexes, otherIx)
			pct := 0.0
			if fr.Total > 0 {
				pct = 100 * float64(fr.Match) / float64(fr.Total)
			}
			fmt.Fprintf(stderr, "fidelity: %d of %d driver sets match (%.1f %%), %d excluded for index-set differences\n", fr.Match, fr.Total, pct, fr.Excluded)
			for _, m := range fr.Mismatches {
				fmt.Fprintln(stderr, "  differs:", m)
			}
		}
	}
	if runErr != nil {
		fmt.Fprintln(stderr, "planguard: run stopped:", runErr)
		code = 1
	}
	if n := len(rep.Failures()); n > 0 {
		fmt.Fprintf(stderr, "%d failing runs\n", n)
		code = 1
	}
	return code
}
