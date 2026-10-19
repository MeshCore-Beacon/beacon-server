// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import "testing"

func TestDataModifying(t *testing.T) {
	cases := map[string]string{
		"SELECT 1": "",
		"-- name: X :one\nDELETE FROM t WHERE id = $1":                                      "DELETE",
		"/* c */ insert into t values (1)":                                                  "INSERT",
		"UPDATE t SET a = 1":                                                                "UPDATE",
		"MERGE INTO t USING s ON true WHEN MATCHED THEN DO NOTHING":                         "MERGE",
		"WITH d AS (DELETE FROM t RETURNING 1) SELECT count(*) FROM d":                      "DELETE",
		"WITH x AS MATERIALIZED (SELECT 1), u AS (UPDATE t SET a = 1 RETURNING 1) SELECT 1": "UPDATE",
		"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x":                                "INSERT",
		"WITH e AS (SELECT id FROM t FOR UPDATE SKIP LOCKED) SELECT * FROM e":               "",
		"WITH e AS (SELECT id FROM t FOR NO KEY UPDATE) SELECT * FROM e":                    "",
		"SELECT 'delete', \"update\" FROM t -- insert":                                      "",
		"WITH x AS (SELECT $$delete$$ AS s) SELECT * FROM x":                                "",
		"SELECT * FROM t WHERE a IN (SELECT 1) FOR UPDATE":                                  "",
	}
	for sql, want := range cases {
		if got := dataModifying(sql); got != want {
			t.Errorf("dataModifying(%q) = %q, want %q", sql, got, want)
		}
	}
}
