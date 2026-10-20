// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

// prodRunTimeoutS bounds each EXPLAIN of the in-server prod tier; resolvers keep the tier's 20 s.
const prodRunTimeoutS = 5

// ProdChecker loads the embedded corpus once (db.SQLCSources, pgdata.Files) and runs tier prod on one
// connection acquired from pool: modes custom and generic (auto needs executions; the prod tier never executes),
// resolvers with statement_timeout 20 s, EXPLAINs with 5 s, lock_timeout 1 s, every transaction READ ONLY. It
// never runs EXPLAIN ANALYZE and changes no session setting: every SET is LOCAL and every prepared statement is
// deallocated. The prod tier names no search_path, so the runs plan in the pool's own.
type ProdChecker struct {
	corpus *Corpus
	pool   *pgxpool.Pool
	opts   RunOptions
}

// NewProdChecker loads the corpus (corpus.yaml at the root of corpus) with the statement texts of the sqlc
// files in sqlc (sqlc/*.sql.go).
func NewProdChecker(pool *pgxpool.Pool, sqlc fs.FS, corpus fs.FS) (*ProdChecker, error) {
	text, err := LoadStatementsFS(sqlc, "sqlc/*.sql.go")
	if err != nil {
		return nil, fmt.Errorf("plan check: statements: %w", err)
	}
	c, err := LoadCorpus(corpus, "corpus.yaml", text)
	if err != nil {
		return nil, fmt.Errorf("plan check: corpus: %w", err)
	}
	p := &ProdChecker{corpus: c, pool: pool, opts: RunOptions{
		Tier:        "prod",
		KeepModes:   []string{ModeCustom, ModeGeneric},
		RunTimeoutS: prodRunTimeoutS,
		Baseline:    BaselineOff,
	}}
	if err := p.opts.validate(c); err != nil {
		return nil, fmt.Errorf("plan check: %w", err)
	}
	return p, nil
}

// Run checks every prod entry once. The error is for a run that could not finish (no connection, or a session
// that cannot go on); the report then holds the runs made so far.
func (p *ProdChecker) Run(ctx context.Context) (*Report, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan check: acquire: %w", err)
	}
	pc := conn.Conn().PgConn()
	rep, err := Run(ctx, p.corpus, NewSession(pc), p.opts)
	if err != nil || pc.IsClosed() || pc.TxStatus() != 'I' {
		// The session may hold a transaction or a prepared statement: never hand it back to the pool.
		cctx, cancel := cleanupCtx(ctx)
		_ = conn.Hijack().Close(cctx)
		cancel()
	} else {
		conn.Release()
	}
	if err != nil {
		return rep, fmt.Errorf("plan check: %w", err)
	}
	return rep, nil
}

// API is the report as GET /api/v1/admin/db/planguard serves it.
func (r *Report) API() api.PlanguardReport {
	out := api.PlanguardReport{
		Tier:       r.Tier,
		StartedAt:  r.StartedAt,
		DurationMs: r.DurationMs,
		Clean:      true,
		Counts:     map[string]int{},
		Failures:   []api.PlanguardRun{},
		Runs:       make([]api.PlanguardRun, 0, len(r.Results)),
	}
	for _, x := range r.Results {
		run := api.PlanguardRun{Entry: x.ID, Stmt: x.Stmt, Mode: x.Mode, Verdict: string(x.Verdict),
			Drivers: x.Drivers, Skip: x.Skip}
		for _, f := range x.Failures {
			run.Failures = append(run.Failures, f.Msg)
		}
		out.Counts[string(x.Verdict)]++
		out.Runs = append(out.Runs, run)
		if x.Verdict.Failing() {
			out.Clean = false
			out.Failures = append(out.Failures, run)
		}
	}
	return out
}
