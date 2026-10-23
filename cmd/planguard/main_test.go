// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIUsage(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string // in stderr
	}{
		{"no args", nil, "usage"},
		{"unknown subcommand", []string{"frobnicate"}, "frobnicate"},
		{"run without tier", []string{"run"}, "--tier"},
		{"run with unknown tier", []string{"run", "--tier", "staging"}, "staging"},
		{"bad baseline", []string{"run", "--tier", "local", "--baseline", "maybe"}, "--baseline"},
		{"bad only", []string{"run", "--tier", "local", "--only", "("}, "--only"},
		{"emit plans without params", []string{"run", "--tier", "prod", "--emit", "plans"}, "--params"},
		{"emit on local", []string{"run", "--tier", "local", "--emit", "resolvers"}, "--tier prod"},
		{"plans on fixture", []string{"run", "--tier", "fixture", "--plans", "p.out", "--params", "r.out"}, "--tier prod"},
		{"unknown flag", []string{"run", "--tier", "local", "--nope"}, "nope"},
		{"gen-policy unknown flag", []string{"gen-policy", "--nope"}, "nope"},
		{"gen-policy argument", []string{"gen-policy", "extra"}, "extra"},
		{"diff without cases", []string{"diff", "--dsn", "x"}, "--cases"},
		{"diff bad walk", []string{"diff", "--cases", "c.yaml", "--walk", "1,0"}, "--walk"},
		{"diff bad walk word", []string{"diff", "--cases", "c.yaml", "--walk", "web,all"}, "all"},
		{"diff unknown flag", []string{"diff", "--nope"}, "nope"},
		{"storediff without new", []string{"storediff", "--old", "http://a", "--cases", "c.yaml"}, "--new"},
		{"storediff bad url", []string{"storediff", "--old", "a:8481", "--new", "http://b", "--cases", "c.yaml"}, "a:8481"},
		{"storediff bad walk", []string{"storediff", "--old", "http://a", "--new", "http://b", "--cases", "c.yaml", "--walk", "0"}, "--walk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errb bytes.Buffer
			if code := run(tc.args, &out, &errb); code != 2 {
				t.Errorf("exit %d, want 2 (stderr %q)", code, errb.String())
			}
			if !strings.Contains(errb.String(), tc.want) {
				t.Errorf("stderr %q, want it to name %q", errb.String(), tc.want)
			}
		})
	}
}

// gen-policy finds the module root from a subdirectory. It writes to a temporary file, which must equal the
// committed cached_gen.go; the committed file is never written.
func TestGenPolicyFromSubdirectory(t *testing.T) {
	want, err := os.ReadFile("../../db/planpolicy/cached_gen.go")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "cached_gen.go")
	t.Chdir("../../internal")
	var stdout, errb bytes.Buffer
	if code := run([]string{"gen-policy", "--out", out}, &stdout, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("generated policy differs from the committed db/planpolicy/cached_gen.go:\n%s", got)
	}
}

// storediff exits 0 when every page matches and 1 when one differs, and writes its report under --out.
func TestStoreDiffCLI(t *testing.T) {
	serve := func(name string) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"items":[{"id":1,"name":%q}],"hasMore":false}`, name)
		}))
		t.Cleanup(s.Close)
		return s
	}
	dir := t.TempDir()
	cases := filepath.Join(dir, "cases.yaml")
	if err := os.WriteFile(cases, []byte("cases:\n  - id: things\n    path: /api/v1/things\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, b, c := serve("x"), serve("x"), serve("y")
	var out, errb bytes.Buffer
	if code := run([]string{"storediff", "--old", a.URL, "--new", b.URL, "--cases", cases, "--walk", "1,web", "--out", dir}, &out, &errb); code != 0 {
		t.Fatalf("same servers: exit %d, stderr %s", code, errb.String())
	}
	report, err := os.ReadFile(filepath.Join(dir, "storediff_report.md"))
	if err != nil || !strings.Contains(string(report), "| things | 50 | 1 | identical |") {
		t.Errorf("report %q (%v)", report, err)
	}
	errb.Reset()
	if code := run([]string{"storediff", "--old", a.URL, "--new", c.URL, "--cases", cases}, &out, &errb); code != 1 ||
		!strings.Contains(errb.String(), `$.items[0].name: old "x", new "y"`) {
		t.Errorf("different servers: exit %d, stderr %s", code, errb.String())
	}
}
