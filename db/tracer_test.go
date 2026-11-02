// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "testing"

func TestSqlcName(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"-- name: ListPackets :many\nSELECT 1", "ListPackets"},
		{"-- name: X :exec", "X"},
		{"SELECT 1", ""},
		{"  -- name: X :one", ""}, // sqlc always writes the header at column 0
		{"", ""},
	} {
		if got := sqlcName(tc.sql); got != tc.want {
			t.Errorf("sqlcName(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}
