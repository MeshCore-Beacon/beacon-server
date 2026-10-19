// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
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

// gen-policy finds the module root from a subdirectory and reports the committed file as current.
func TestGenPolicyFromSubdirectory(t *testing.T) {
	t.Chdir("../../internal")
	var out, errb bytes.Buffer
	if code := run([]string{"gen-policy"}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "cached_gen.go is up to date") {
		t.Errorf("stdout %q, want the committed file up to date", out.String())
	}
}
