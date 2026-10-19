// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import "testing"

func TestHasStatementSeparator(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT 1":                                   false,
		"SELECT 1;":                                  false,
		"SELECT 1;\n  ":                              false,
		"SELECT ';' AS a, \"x;y\" AS b":              false,
		"SELECT $$;$$, $q$ ; $q$":                    false,
		"SELECT 1 -- ; trailing\n":                   false,
		"SELECT 1 /* ; */":                           false,
		"SELECT 1; COMMIT":                           true,
		"SELECT 1;;":                                 true,
		"SELECT 1; COMMIT; DELETE FROM nodes":        true,
		"SELECT (SELECT 1); INSERT INTO t VALUES(1)": true,
	} {
		if got := hasStatementSeparator(sql); got != want {
			t.Errorf("hasStatementSeparator(%q) = %v, want %v", sql, got, want)
		}
	}
}

// Every sqlc statement is one statement, with no trailing ';' in its constant, so the guard passes all of them.
func TestSQLCTextsAreSingleStatements(t *testing.T) {
	texts, err := LoadStatements("../../db/sqlc")
	if err != nil {
		t.Fatal(err)
	}
	for name, sql := range texts {
		if err := checkOneStatement(sql); err != nil {
			t.Errorf("sqlc %s: %v", name, err)
		}
		if trimStmt(sql) != sql && trimStmt(sql)+";" == sql {
			t.Errorf("sqlc %s ends with ';'", name)
		}
	}
	if len(texts) < 100 {
		t.Errorf("only %d sqlc statements", len(texts))
	}
}
