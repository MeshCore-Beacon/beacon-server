// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestConcurrentIndexName(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
		ok   bool
	}{
		{"bare 027 style with comments", `-- RunMigrations executes each file outside an explicit transaction. Keep this
-- as one statement so other subscribers can keep writing during the build.
-- If interrupted, check pg_index.indisvalid and remove this named index before
-- retrying; do not silently accept an invalid index with IF NOT EXISTS.
CREATE INDEX CONCURRENTLY idx_known_routes_iata_last_seen
ON known_routes (iata, last_seen DESC);`, "idx_known_routes_iata_last_seen", true},
		{"if not exists", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_a ON t (c);", "idx_a", true},
		{"unique", "CREATE UNIQUE INDEX CONCURRENTLY idx_u ON t (c);", "idx_u", true},
		{"quoted", `CREATE INDEX CONCURRENTLY "Idx_Q" ON t (c);`, "Idx_Q", true},
		{"schema qualified", "CREATE INDEX CONCURRENTLY public.idx_s ON t (c);", "public.idx_s", true},
		{"lowercase multiline", "create index\n  concurrently\n  idx_l\n  on t (c);", "idx_l", true},
		{"plain create index", "CREATE INDEX idx_p ON t (c);", "", false},
		{"only in comment", "-- CREATE INDEX CONCURRENTLY idx_c ON t (c);\nSELECT 1;", "", false},
		{"empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := concurrentIndexName(tc.sql)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestIsDuplicateRelation(t *testing.T) {
	dup := &pgconn.PgError{Code: "42P07"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"42P07", dup, true},
		{"wrapped 42P07", fmt.Errorf("apply: %w", dup), true},
		{"other sqlstate", &pgconn.PgError{Code: "42501"}, false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDuplicateRelation(tc.err); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
