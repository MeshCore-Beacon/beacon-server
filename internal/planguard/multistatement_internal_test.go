// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"strings"
	"testing"
)

func TestStatementProblem(t *testing.T) {
	for sql, want := range map[string]string{
		"SELECT 1":                                   "",
		"SELECT 1;":                                  "",
		"SELECT 1;\n  ":                              "",
		"SELECT ';' AS a, \"x;y\" AS b":              "",
		"SELECT $$;$$, $q$ ; \\! id $q$":             "",
		"SELECT 1 -- ; trailing\n":                   "",
		"SELECT 1 /* ; /* nested */ */":              "",
		"SELECT a$b FROM t WHERE x = $1":             "",
		"SELECT '\\x0102'::bytea, '\\'":              "", // standard_conforming_strings: the backslash is plain text
		"SELECT 'it''s'":                             "",
		"SELECT 1; COMMIT":                           "more than one statement",
		"SELECT 1;;":                                 "more than one statement",
		"SELECT 1; COMMIT; DELETE FROM nodes":        "more than one statement",
		"SELECT (SELECT 1); INSERT INTO t VALUES(1)": "more than one statement",
		"SELECT E'\\';'":                             "escape string",
		"SELECT e'\\';' ; COMMIT":                    "escape string",
		"SELECT x FROM t WHERE y = E'a'":             "escape string",
		"SELECT 1\n\\! id":                           "backslash outside a literal",
		"SELECT 1 \\gexec":                           "backslash outside a literal",
		"SELECT 1 -- \\! id":                         "backslash in a comment",
		"SELECT 1 /* \\o /tmp/x */":                  "backslash in a comment",
		"SELECT \"a\\b\"":                            "backslash in a quoted identifier",
		"SELECT 'abc":                                "does not end",
		"SELECT $q$abc":                              "does not end",
		"SELECT 1 /* abc":                            "does not end",
	} {
		got := statementProblem(sql)
		if (want == "") != (got == "") || !strings.Contains(got, want) {
			t.Errorf("statementProblem(%q) = %q, want %q", sql, got, want)
		}
	}
}

func TestCheckScriptLines(t *testing.T) {
	if err := checkScriptLines("SELECT '\\x01'\nFROM t"); err != nil {
		t.Errorf("backslash inside a line: %v", err)
	}
	for _, body := range []string{"SELECT 'a\n\\! id\n'", "SELECT 1\n   \\o /tmp/x"} {
		if err := checkScriptLines(body); err == nil {
			t.Errorf("checkScriptLines(%q) = nil, want a line starting with a backslash", body)
		}
	}
}

// Every sqlc statement passes the guard, with no trailing ';' in its constant.
func TestSQLCTextsPassTheStatementGuard(t *testing.T) {
	texts, err := LoadStatements("../../db/sqlc")
	if err != nil {
		t.Fatal(err)
	}
	for name, sql := range texts {
		if err := checkStatementText(sql); err != nil {
			t.Errorf("sqlc %s: %v", name, err)
		}
		if strings.HasSuffix(strings.TrimSpace(sql), ";") {
			t.Errorf("sqlc %s ends with ';'", name)
		}
	}
	if len(texts) < 100 {
		t.Errorf("only %d sqlc statements", len(texts))
	}
}
