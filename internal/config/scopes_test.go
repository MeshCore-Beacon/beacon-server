// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestScopesConfig(t *testing.T) {
	valid := "regions:\n  - slug: ottawa\n    iatas: [YOW, YGK]\n  - slug: bc\n    iatas: [YVR]\nscopes:\n  - name: on\n    region: ottawa\n  - name: \"#west\"\n    region: bc\n  - name: \"$ottawa\"\n    region: ottawa\n"
	for _, tc := range []struct {
		name, text string
		bad        bool
	}{
		{"valid", valid, false},
		{"missing region", strings.Replace(valid, "    region: bc\n", "", 1), true},
		{"unknown region", strings.Replace(valid, "region: bc", "region: prairies", 1), true},
		{"empty name", strings.Replace(valid, "name: on", "name: \"\"", 1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(p)
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v", err)
			}
			if tc.bad {
				return
			}
			want := map[string][]string{"YOW": {"#on", "$ottawa"}, "YGK": {"#on", "$ottawa"}, "YVR": {"#west"}}
			if got := cfg.ManualScopeMembers(); !reflect.DeepEqual(got, want) {
				t.Fatalf("ManualScopeMembers = %v, want %v", got, want)
			}
		})
	}
}
