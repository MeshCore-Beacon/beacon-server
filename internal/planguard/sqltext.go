// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package planguard checks that the planner keeps choosing the plans Beacon's statements were designed for.
package planguard

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing/fstest"
)

var sqlcHeader = regexp.MustCompile(`^-- name: (\w+) :(\w+)`)

// LoadStatements parses every *.sql.go file in dir (db/sqlc) and returns sqlc name → the constant's text,
// "-- name: X :kind" header included. Errors: a parse error, a duplicate name, or no statement at all.
func LoadStatements(dir string) (map[string]string, error) {
	return LoadStatementsFS(os.DirFS(dir), "*.sql.go")
}

// LoadStatementsFS is LoadStatements over the files of fsys that match pattern, e.g. "sqlc/*.sql.go".
func LoadStatementsFS(fsys fs.FS, pattern string) (map[string]string, error) {
	files, err := fs.Glob(fsys, pattern)
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", pattern, err)
	}
	sort.Strings(files)
	out := map[string]string{}
	from := map[string]string{}
	fset := token.NewFileSet()
	for _, name := range files {
		src, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					lit, ok := v.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					text, err := strconv.Unquote(lit.Value)
					if err != nil {
						return nil, fmt.Errorf("%s: unquote %s: %w", name, fset.Position(lit.Pos()), err)
					}
					m := sqlcHeader.FindStringSubmatch(text)
					if m == nil {
						continue
					}
					if prev, dup := from[m[1]]; dup {
						return nil, fmt.Errorf("duplicate sqlc statement %q in %s and %s", m[1], prev, name)
					}
					from[m[1]] = name
					out[m[1]] = text
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no statement found in %d files matching %s", len(files), pattern)
	}
	return out, nil
}

// LoadStatementsAtRef reads db/sqlc at a git revision of the repository in repoDir (git ls-tree, then git show
// <ref>:db/sqlc/<file>) and parses it like LoadStatements.
func LoadStatementsAtRef(repoDir, ref string) (map[string]string, error) {
	if ref == "" || strings.HasPrefix(ref, "-") {
		return nil, fmt.Errorf("invalid git ref %q", ref)
	}
	list, err := git(repoDir, "ls-tree", "--full-tree", "--name-only", ref, "--", "db/sqlc/")
	if err != nil {
		return nil, err
	}
	files := fstest.MapFS{}
	for _, p := range strings.Split(strings.TrimSpace(string(list)), "\n") {
		if !strings.HasSuffix(p, ".sql.go") {
			continue
		}
		body, err := git(repoDir, "show", ref+":"+p)
		if err != nil {
			return nil, err
		}
		files[path.Base(p)] = &fstest.MapFile{Data: body}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no db/sqlc/*.sql.go at %s", ref)
	}
	return LoadStatementsFS(files, "*.sql.go")
}

func git(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

var (
	paramRef   = regexp.MustCompile(`\$[0-9]+`)
	whitespace = regexp.MustCompile(`\s+`)
)

// Normalize strips -- comments, collapses whitespace and trailing ';', and replaces $n by $? (renumbering-proof).
// A "--" inside a quoted literal or identifier is kept.
func Normalize(sql string) string {
	s := stripLineComments(sql)
	s = paramRef.ReplaceAllString(s, "$$?")
	s = strings.TrimSpace(whitespace.ReplaceAllString(s, " "))
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return s
}

func stripLineComments(sql string) string {
	var b strings.Builder
	var quote byte // ' or " while inside a quoted run
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0 // a doubled quote reopens on the next byte
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			if i < len(sql) {
				b.WriteByte('\n')
			}
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

var lockClause = regexp.MustCompile(`(?i)\s*\bFOR\s+UPDATE(\s+OF\s+\w+(\s*,\s*\w+)*)?\s+SKIP\s+LOCKED\b`)

// StripLockClause removes every "FOR UPDATE [OF x[, y]] SKIP LOCKED" from sql.
func StripLockClause(sql string) string {
	return lockClause.ReplaceAllString(sql, "")
}

// DerivedBodyMatches reports whether body, the frozen read-only part of a DML statement, is still part of that
// statement's source text once its lock clause is removed. Both sides are normalized, so parameter numbers,
// comments and whitespace do not matter, and one edited token does.
func DerivedBodyMatches(source, body string) bool {
	b := Normalize(body)
	return b != "" && strings.Contains(Normalize(StripLockClause(source)), b)
}
