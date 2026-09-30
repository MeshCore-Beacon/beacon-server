// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateBorderNoGeometry(t *testing.T) {
	for _, raw := range []string{`{"type":"Feature","geometry":null}`, `{"type":"Feature"}`} {
		if _, err := ValidateBorder([]byte(raw)); err == nil {
			t.Fatal("missing geometry accepted")
		}
	}
}

func TestLoadLocalBorders(t *testing.T) {
	if got, err := LoadLocalBorders(&Config{}); err != nil || got != nil {
		t.Fatalf("disabled: %v %v", got, err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	for _, tc := range []struct {
		text string
		bad  bool
	}{
		{"nodes:\n  mark_foreign: true\n", true},
		{"nodes:\n  mark_foreign: true\nregions:\n  - slug: a\n    iatas: [AAA]\nmeshmapper:\n  zones:\n    enabled: true\n", false},
	} {
		if err := os.WriteFile(path, []byte(tc.text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); (err != nil) != tc.bad {
			t.Fatalf("%q: %v", tc.text, err)
		}
	}
	border := filepath.Join(dir, "region.json")
	raw := `{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[10,10],[20,10],[20,20],[10,20],[10,10]]],[[[30,10],[40,10],[40,20],[30,20],[30,10]]]]}}`
	if err := os.WriteFile(border, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("nodes:\n  mark_foreign: true\niatas:\n  AAA:\n    borderFile: region.json\n  BBB:\n    name: Airport without an operating border\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	files, err := LoadLocalBorders(cfg)
	if err != nil || len(files) != 1 {
		t.Fatal(files, err)
	}
	local, err := BuildLocalBorders(files, nil)
	if err != nil {
		t.Fatal(err)
	}
	if *local.PossiblyForeign(2, new(15.0), new(15.0)) || *local.PossiblyForeign(2, new(15.0), new(35.0)) || !*local.PossiblyForeign(2, new(25.0), new(25.0)) {
		t.Fatal("polygon union not used")
	}
	imported := map[string]json.RawMessage{"AAA": json.RawMessage(`{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[50,50],[60,50],[60,60],[50,60],[50,50]]]}}`)}
	replaced, err := BuildLocalBorders(files, imported)
	if err != nil {
		t.Fatal(err)
	}
	if !*replaced.PossiblyForeign(2, new(15.0), new(15.0)) || *replaced.PossiblyForeign(2, new(55.0), new(55.0)) {
		t.Fatal("imported boundary did not replace the file border")
	}
	if len(files) != 1 || string(files["AAA"]) == string(imported["AAA"]) {
		t.Fatal("build changed its inputs")
	}
	if empty, err := BuildLocalBorders(nil, nil); err != nil || empty != nil {
		t.Fatal(empty, err)
	}
	for _, bad := range []string{`not json`, `{"type":"Feature","geometry":null}`, `{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[179,10],[-179,10],[-179,20],[179,20],[179,10]]]}}`} {
		if err := os.WriteFile(border, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadLocalBorders(cfg); err == nil || !strings.Contains(err.Error(), "nodes.mark_foreign") {
			t.Fatalf("bad boundary did not fail clearly: %v", err)
		}
	}
	if err := os.Remove(border); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLocalBorders(cfg); err == nil {
		t.Fatal("missing file accepted")
	}
	if *local.PossiblyForeign(2, new(15.0), new(15.0)) {
		t.Fatal("running classifier changed with its source file")
	}
}
