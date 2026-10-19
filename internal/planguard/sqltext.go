// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package planguard checks that the planner keeps choosing the plans Beacon's statements were designed for.
package planguard

import (
	"bytes"
	"errors"
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

var (
	sqlcHeader = regexp.MustCompile(`^-- name: (\w+) :(\w+)`)
	rawHeader  = regexp.MustCompile(`-- name: \w+ :\w+`)
)

// foldString returns the value of a string literal or a + chain of string literals (sqlc writes a backtick
// inside a statement as `...` + "`" + `...`). ok is false for any other expression.
func foldString(e ast.Expr) (string, bool, error) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false, nil
		}
		s, err := strconv.Unquote(x.Value)
		if err != nil {
			return "", false, fmt.Errorf("unquote: %w", err)
		}
		return s, true, nil
	case *ast.ParenExpr:
		return foldString(x.X)
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", false, nil
		}
		l, ok, err := foldString(x.X)
		if err != nil || !ok {
			return "", false, err
		}
		r, ok, err := foldString(x.Y)
		if err != nil || !ok {
			return "", false, err
		}
		return l + r, true, nil
	}
	return "", false, nil
}

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
		extracted := 0
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
					text, ok, err := foldString(v)
					if err != nil {
						return nil, fmt.Errorf("%s: %s: %w", name, fset.Position(v.Pos()), err)
					}
					if !ok {
						continue
					}
					m := sqlcHeader.FindStringSubmatch(text)
					if m == nil {
						continue
					}
					extracted++
					if prev, dup := from[m[1]]; dup {
						return nil, fmt.Errorf("duplicate sqlc statement %q in %s and %s", m[1], prev, name)
					}
					from[m[1]] = name
					out[m[1]] = text
				}
			}
		}
		if headers := len(rawHeader.FindAll(src, -1)); headers != extracted {
			return nil, fmt.Errorf("%s: %d statement headers in the file but %d extracted; a statement constant is not a foldable string", name, headers, extracted)
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

// statementProblem scans sql, after one trailing ';' is dropped, and names the first thing that makes it unsafe to
// send over the simple protocol or into a psql script, or returns "":
//   - a ';' outside literals, quoted identifiers, dollar quotes and comments: a second statement (a COMMIT, say)
//     would run outside the READ ONLY transaction;
//   - an escape string (E'...'): a backslash there escapes a quote, so the scan could not tell where it ends;
//   - a backslash anywhere but inside a standard single-quoted literal or a dollar quote: psql reads it as a
//     meta-command (\! runs a shell, \o writes a file);
//   - a literal, quoted identifier, dollar quote or comment that does not end.
func statementProblem(sql string) string {
	s := trimStmt(sql)
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '-' && strings.HasPrefix(s[i:], "--"):
			end := len(s)
			if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
				end = i + j
			}
			if strings.IndexByte(s[i:end], '\\') >= 0 {
				return "a backslash in a comment (psql reads it as a meta-command)"
			}
			i = end
		case c == '/' && strings.HasPrefix(s[i:], "/*"):
			depth, j := 1, i+2
			for j < len(s) && depth > 0 {
				switch {
				case strings.HasPrefix(s[j:], "/*"):
					depth, j = depth+1, j+2
				case strings.HasPrefix(s[j:], "*/"):
					depth, j = depth-1, j+2
				case s[j] == '\\':
					return "a backslash in a comment (psql reads it as a meta-command)"
				default:
					j++
				}
			}
			if depth > 0 {
				return "a comment that does not end"
			}
			i = j
		case c == '\'':
			if i > 0 && (s[i-1] == 'E' || s[i-1] == 'e') && (i == 1 || !isIdent(s[i-2])) {
				return "an E'' escape string (a backslash there escapes a quote)"
			}
			j := i + 1
			for ; ; j++ {
				if j >= len(s) {
					return "a string literal that does not end"
				}
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						j++
						continue
					}
					break
				}
			}
			i = j + 1
		case c == '"':
			j := i + 1
			for ; ; j++ {
				if j >= len(s) {
					return "a quoted identifier that does not end"
				}
				if s[j] == '\\' {
					return "a backslash in a quoted identifier (psql reads it as a meta-command)"
				}
				if s[j] == '"' {
					if j+1 < len(s) && s[j+1] == '"' {
						j++
						continue
					}
					break
				}
			}
			i = j + 1
		case c == '$':
			j := i + 1
			for j < len(s) && isIdent(s[j]) && !(j == i+1 && s[j] >= '0' && s[j] <= '9') {
				j++
			}
			if j < len(s) && s[j] == '$' {
				tag := s[i : j+1]
				k := strings.Index(s[j+1:], tag)
				if k < 0 {
					return "a dollar quote that does not end"
				}
				i = j + 1 + k + len(tag)
			} else {
				i = j
			}
		case isIdent(c):
			for i < len(s) && (isIdent(s[i]) || s[i] == '$') {
				i++
			}
		case c == '\\':
			return "a backslash outside a literal (psql reads it as a meta-command)"
		case c == ';':
			return "more than one statement (a ';' outside literals and comments)"
		default:
			i++
		}
	}
	return ""
}

// checkStatementText refuses statement, resolver and inline {sql} text that statementProblem objects to.
func checkStatementText(sql string) error {
	if p := statementProblem(sql); p != "" {
		return errors.New(p)
	}
	return nil
}

// checkScriptLines refuses an emitted SQL body with a line that starts with a backslash once trimmed: psql would
// run it as a meta-command even where the SQL lexer would read it as part of a literal.
func checkScriptLines(body string) error {
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "\\") {
			return fmt.Errorf("a line starts with a backslash (a psql meta-command): %q", firstLine(strings.TrimSpace(l)))
		}
	}
	return nil
}

// StripLockClause removes every "FOR UPDATE [OF x[, y]] SKIP LOCKED" from sql.
func StripLockClause(sql string) string {
	return lockClause.ReplaceAllString(sql, "")
}

// DerivedBodyMatches reports whether body, the frozen read-only part of a DML statement, is still part of that
// statement's source text once its lock clause is removed. Both sides are normalized, so parameter numbers,
// comments and whitespace do not matter, and one edited token does.
//
// The match must sit on token boundaries: it starts the source or follows whitespace or "(", and it ends the
// source or is followed by ")" or ";", directly or after whitespace. A body that stops short of a clause
// ("LIMIT $1" inside "LIMIT $5 OFFSET $6") does not match.
func DerivedBodyMatches(source, body string) bool {
	b := Normalize(body)
	if b == "" {
		return false
	}
	src := Normalize(StripLockClause(source))
	for from := 0; ; {
		i := strings.Index(src[from:], b)
		if i < 0 {
			return false
		}
		start := from + i
		end := start + len(b)
		if boundaryBefore(src, start) && boundaryAfter(src, end) {
			return true
		}
		from = start + 1
	}
}

func boundaryBefore(s string, i int) bool {
	return i == 0 || s[i-1] == ' ' || s[i-1] == '('
}

func boundaryAfter(s string, i int) bool {
	if i < len(s) && s[i] == ' ' {
		i++
	}
	return i == len(s) || s[i] == ')' || s[i] == ';'
}
