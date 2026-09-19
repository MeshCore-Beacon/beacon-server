// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
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
	if _, err := LoadLocalBorders(&Config{Nodes: NodesConfig{MarkForeign: true}}); err == nil {
		t.Fatal("enabled with no border")
	}
	dir := t.TempDir()
	border := filepath.Join(dir, "region.json")
	raw := `{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[10,10],[20,10],[20,20],[10,20],[10,10]]],[[[30,10],[40,10],[40,20],[30,20],[30,10]]]]}}`
	if err := os.WriteFile(border, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("nodes:\n  mark_foreign: true\niatas:\n  AAA:\n    borderFile: region.json\n  BBB:\n    name: Airport without an operating border\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	local, err := LoadLocalBorders(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if *local.PossiblyForeign(2, new(15.0), new(15.0)) || *local.PossiblyForeign(2, new(15.0), new(35.0)) || !*local.PossiblyForeign(2, new(25.0), new(25.0)) {
		t.Fatal("polygon union not used")
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
