// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pwReadKey struct{}

// pwReadTracer runs gap once, at the start or the end of the first statement that reads a request password.
type pwReadTracer struct {
	once  *sync.Once
	atEnd bool
	gap   func()
}

func (tr pwReadTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	match := strings.Contains(d.SQL, "password_ct")
	if match && !tr.atEnd {
		tr.once.Do(tr.gap)
	}
	return context.WithValue(ctx, pwReadKey{}, match)
}

func (tr pwReadTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if match, _ := ctx.Value(pwReadKey{}).(bool); match && tr.atEnd {
		tr.once.Do(tr.gap)
	}
}

// Approval is revoked and a new password elected while an owners answer is being built: the answer never seals the
// new password to the revoked observer.
func TestOwnersPasswordFromOneSnapshotPostgres(t *testing.T) {
	for _, atEnd := range []bool{false, true} {
		t.Run(map[bool]string{false: "before the read", true: "after the read"}[atEnd], func(t *testing.T) {
			w := newReqWorld(t, config.RequestObserversApproved)
			u := newUser(t, w.ctx, w.s, "alice")
			o := newReqDevice(1)
			node := w.node(0xA1)
			pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, o.pub), w.now.Add(-time.Hour), 5)
			w.claim(o, 1)
			if err := w.s.SetPollingObserver(w.ctx, o.pub, false, true, ""); err != nil {
				t.Fatal(err)
			}
			r, err := w.reqs.Create(w.ctx, u.ID, node, []byte("old"))
			if err != nil {
				t.Fatal(err)
			}
			if e, ok := w.owners(o, 0xA1); !ok || e.PW == nil {
				t.Fatalf("approved observer not given the password: %+v", e)
			}
			var gapErr error
			gap := func() {
				if gapErr = w.s.SetPollingObserver(w.ctx, o.pub, false, false, ""); gapErr != nil {
					return
				}
				w.now = w.now.Add(time.Minute)
				_, gapErr = w.reqs.Update(w.ctx, u.ID, r.ID, polling.RequestUpdate{SetPassword: true, Password: []byte("new-secret")})
			}
			cfg := w.pool.Config()
			cfg.ConnConfig.Tracer = pwReadTracer{once: &sync.Once{}, atEnd: atEnd, gap: gap}
			traced, err := pgxpool.NewWithConfig(w.ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer traced.Close()
			ts := New(traced, traced, 0, 0)
			svc := polling.NewService(ts, config.ResolvedPolling{Enabled: true, RequestObservers: config.RequestObserversApproved},
				func() time.Time { return w.now }, slog.New(slog.DiscardHandler))
			svc.SetRequests(ts, w.sealer)
			ans, err := svc.Owners(w.ctx, polling.Caller{Observer: o.pub})
			if gapErr != nil {
				t.Fatal(gapErr)
			}
			if err != nil {
				return
			}
			if e, ok := ans.Nodes[hex.EncodeToString(pollKey(0xA1))]; ok && e.PW != nil {
				if got := w.opens(o, 0xA1, e); got != "old" {
					t.Fatalf("revoked observer was sealed %q, a password that never existed while it was approved", got)
				}
			}
			if e, ok := w.owners(o, 0xA1); ok && e.PW != nil {
				t.Fatalf("revoked observer still offered a password: %+v", e)
			}
		})
	}
}
