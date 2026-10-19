// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// sqlcName returns X from a leading "-- name: X :kind" line, or "" (tracer label "unnamed").
// sqlc always writes that header at column 0 of the statement text.
func sqlcName(sql string) string {
	rest, ok := strings.CutPrefix(sql, "-- name: ")
	if !ok {
		return ""
	}
	line, _, _ := strings.Cut(rest, "\n")
	f := strings.Fields(line)
	if len(f) < 2 || !strings.HasPrefix(f[1], ":") {
		return ""
	}
	return f[0]
}

type queryStartKey struct{}

type acquireStartKey struct{}

type queryStart struct {
	at        time.Time
	statement string
}

// tracer records statement time and connection acquire time for one pool.
type tracer struct {
	pool string
}

func newTracer(pool string) *tracer { return &tracer{pool: pool} }

var (
	_ pgx.QueryTracer       = (*tracer)(nil)
	_ pgxpool.AcquireTracer = (*tracer)(nil)
)

// TraceQueryStart runs at the start of Query, QueryRow and Exec.
func (t *tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	name := sqlcName(data.SQL)
	if name == "" {
		name = "unnamed"
	}
	return context.WithValue(ctx, queryStartKey{}, queryStart{at: time.Now(), statement: name})
}

// TraceQueryEnd runs when Exec returns, or when the rows of a Query or QueryRow are closed.
func (t *tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	s, ok := ctx.Value(queryStartKey{}).(queryStart)
	if !ok {
		return
	}
	metrics.DBTime.WithLabelValues(s.statement, t.pool).Observe(time.Since(s.at).Seconds())
}

// TraceAcquireStart runs at the start of a pool Acquire.
func (t *tracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return context.WithValue(ctx, acquireStartKey{}, time.Now())
}

// TraceAcquireEnd runs when the Acquire returns, with or without a connection.
func (t *tracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireEndData) {
	at, ok := ctx.Value(acquireStartKey{}).(time.Time)
	if !ok {
		return
	}
	metrics.PoolAcquire.WithLabelValues(t.pool).Observe(time.Since(at).Seconds())
}
