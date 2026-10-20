// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"context"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/db"
	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/jackc/pgx/v5/pgconn"
)

type sentSession struct{ sent []string }

func (s *sentSession) Exec(_ context.Context, sql string) ([]*pgconn.Result, error) {
	s.sent = append(s.sent, sql)
	return []*pgconn.Result{{}}, nil
}

// The prod checker plans custom and generic only, caps each EXPLAIN at 5 s, never ANALYZEs and sets nothing
// beyond its own transactions.
func TestProdCheckerOptions(t *testing.T) {
	p, err := NewProdChecker(nil, db.SQLCSources, pgdata.Files)
	if err != nil {
		t.Fatal(err)
	}
	s := &sentSession{}
	rep, err := Run(context.Background(), p.corpus, s, p.opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Results) == 0 {
		t.Fatal("no runs")
	}
	modes := map[string]int{}
	for _, r := range rep.Results {
		modes[r.Mode]++
	}
	if modes[ModeAuto] != 0 || modes[ModeCustom] == 0 || modes[ModeGeneric] == 0 {
		t.Errorf("modes = %v, want custom and generic only", modes)
	}
	prepares, resolvers := 0, 0
	for i, sql := range s.sent {
		if strings.Contains(sql, "ANALYZE") {
			t.Errorf("sent %q", firstLine(sql))
		}
		if strings.Contains(sql, "search_path") {
			t.Errorf("sent %q: the prod tier inherits the pool's search_path", firstLine(sql))
		}
		if strings.HasPrefix(sql, "SET ") && !strings.HasPrefix(sql, "SET LOCAL ") {
			t.Errorf("session-level SET %q", sql)
		}
		if strings.HasPrefix(sql, "PREPARE ") {
			prepares++
			if !timeoutBefore(s.sent[:i], "SET LOCAL statement_timeout = '5s'") {
				t.Errorf("PREPARE %d not under a 5 s statement_timeout", prepares)
			}
		}
		if strings.HasPrefix(sql, "SAVEPOINT planguard_r") && resolvers == 0 {
			resolvers++
			if !timeoutBefore(s.sent[:i], "SET LOCAL statement_timeout = '20s'") {
				t.Error("resolvers not under a 20 s statement_timeout")
			}
		}
	}
	if prepares == 0 || resolvers == 0 {
		t.Errorf("prepares %d resolvers %d, want both", prepares, resolvers)
	}
}

// timeoutBefore reports whether the transaction that sent's last BEGIN opened set want.
func timeoutBefore(sent []string, want string) bool {
	for i := len(sent) - 1; i >= 0; i-- {
		if strings.Contains(sent[i], want) {
			return true
		}
		if strings.HasPrefix(sent[i], "BEGIN") {
			return false
		}
	}
	return false
}
