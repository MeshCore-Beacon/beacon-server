// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package backup

import (
	"strings"
	"testing"
)

func TestCheckClientVersion(t *testing.T) {
	for _, tc := range []struct {
		output string
		server int
		valid  bool
	}{
		{"pg_dump (PostgreSQL) 16.10\n", 160010, true},
		{"pg_dump (PostgreSQL) 17.6 (Debian 17.6)\n", 160010, true},
		{"pg_dump (PostgreSQL) 16.10\n", 170006, false},
		{"pg_dump (PostgreSQL) 16.10\n", 180000, false},
		{"pg_dump (PostgreSQL) 18.0\n", 180000, true},
		{"PRIVATE_CANARY", 160010, false},
		{"pg_dump (PostgreSQL) 16.bad", 160010, false},
		{"pg_dump (PostgreSQL) 16.10\n", 0, false},
	} {
		err := checkClientVersion([]byte(tc.output), tc.server)
		if (err == nil) != tc.valid || (err != nil && strings.Contains(err.Error(), "PRIVATE_CANARY")) {
			t.Fatalf("server %d: %v", tc.server, err)
		}
	}
}
