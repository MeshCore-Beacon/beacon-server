// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

type stubSeeder struct {
	regions        map[string]int32
	iatas          []string
	regionIATAs    map[int32][]string
	channelRegions []string
	scopes         []string
	borders        map[string]json.RawMessage
	upsertErr      error
}

func newStubSeeder() *stubSeeder {
	return &stubSeeder{
		regions:     make(map[string]int32),
		regionIATAs: make(map[int32][]string),
		borders:     make(map[string]json.RawMessage),
	}
}

func (s *stubSeeder) UpsertIATA(_ context.Context, iata string) error {
	s.iatas = append(s.iatas, iata)
	return s.upsertErr
}

func (s *stubSeeder) UpsertIATADetails(_ context.Context, iata, _ string, _, _ *float64) error {
	s.iatas = append(s.iatas, iata)
	return s.upsertErr
}

func (s *stubSeeder) UpsertIATABorder(_ context.Context, iata string, border json.RawMessage) error {
	s.borders[iata] = border
	return s.upsertErr
}

func (s *stubSeeder) UpsertRegion(_ context.Context, slug, _, _ string, _ int, _, _ *float64, _ *int) (int32, error) {
	id := int32(len(s.regions) + 1)
	s.regions[slug] = id
	return id, s.upsertErr
}

func (s *stubSeeder) SetRegionIATAs(_ context.Context, regionID int32, iatas []string) error {
	s.regionIATAs[regionID] = iatas
	return s.upsertErr
}

func (s *stubSeeder) SetChannelConfigScopes(_ context.Context, fingerprints [][]byte, regions []string) error {
	s.channelRegions = regions
	return s.upsertErr
}

func (s *stubSeeder) UpsertTransportScope(_ context.Context, name, _ string, _, _ []byte) error {
	s.scopes = append(s.scopes, name)
	return s.upsertErr
}

func TestSeed_IATAsAndRegions(t *testing.T) {
	cfg := &Config{
		IATAs: map[string]IATAConfig{
			"YVR": {Name: "Vancouver"},
		},
		Regions: []RegionConfig{
			{Slug: "bc", Name: "British Columbia", IATAs: []string{"YVR", "YYJ"}},
		},
	}

	db := newStubSeeder()
	if err := Seed(context.Background(), cfg, db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := db.regions["bc"]; !ok {
		t.Error("expected bc region to be upserted")
	}
	if len(db.regionIATAs[1]) != 2 {
		t.Errorf("expected 2 IATAs for region 1, got %d", len(db.regionIATAs[1]))
	}
}

func TestSeed_BorderFile_ValidatesAndUpserts(t *testing.T) {
	dir := t.TempDir()
	borderPath := dir + "/yow.geojson"
	if err := os.WriteFile(borderPath, []byte(`{
		"type": "Feature",
		"properties": {"name": "Ottawa"},
		"geometry": {
			"type": "Polygon",
			"coordinates": [[[-76.4,45.0],[-75.2,45.0],[-75.2,45.6],[-76.4,45.6],[-76.4,45.0]]]
		}
	}`), 0o644); err != nil {
		t.Fatalf("failed to write test border file: %v", err)
	}

	cfg := &Config{
		IATAs: map[string]IATAConfig{
			"YOW": {Name: "Ottawa", BorderFile: borderPath},
		},
	}
	db := newStubSeeder()
	if err := Seed(context.Background(), cfg, db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	border, ok := db.borders["YOW"]
	if !ok {
		t.Fatal("expected a border to be upserted for YOW")
	}
	var feat map[string]any
	if err := json.Unmarshal(border, &feat); err != nil {
		t.Fatalf("stored border is not valid JSON: %v", err)
	}
	if _, ok := feat["bbox"]; !ok {
		t.Error("expected stored border to have a computed bbox")
	}
}

func TestSeed_BorderFile_InvalidGeometryFailsClosed(t *testing.T) {
	dir := t.TempDir()
	borderPath := dir + "/bad.geojson"
	// unclosed ring
	if err := os.WriteFile(borderPath, []byte(`{
		"type": "Feature",
		"properties": {},
		"geometry": {
			"type": "Polygon",
			"coordinates": [[[-76.4,45.0],[-75.2,45.0],[-75.2,45.6]]]
		}
	}`), 0o644); err != nil {
		t.Fatalf("failed to write test border file: %v", err)
	}

	cfg := &Config{
		IATAs: map[string]IATAConfig{
			"YOW": {Name: "Ottawa", BorderFile: borderPath},
		},
	}
	db := newStubSeeder()
	if err := Seed(context.Background(), cfg, db); err == nil {
		t.Fatal("expected Seed to fail on an invalid border file")
	}
	if _, ok := db.borders["YOW"]; ok {
		t.Error("expected no border to be upserted when validation fails")
	}
}

func TestSeed_BorderFile_MissingFileFailsClosed(t *testing.T) {
	cfg := &Config{
		IATAs: map[string]IATAConfig{
			"YOW": {Name: "Ottawa", BorderFile: "/nonexistent/path/border.geojson"},
		},
	}
	db := newStubSeeder()
	if err := Seed(context.Background(), cfg, db); err == nil {
		t.Fatal("expected Seed to fail when the border file doesn't exist")
	}
}

func TestSeed_Scopes(t *testing.T) {
	cfg := &Config{
		Scopes: []ScopeConfig{
			{Name: "bc"},
			{Name: "#west"},
		},
	}

	db := newStubSeeder()
	if err := Seed(context.Background(), cfg, db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(db.scopes) != 2 {
		t.Fatalf("expected 2 scopes, got %d", len(db.scopes))
	}
	if db.scopes[0] != "#bc" {
		t.Errorf("expected #bc, got %s", db.scopes[0])
	}
	if db.scopes[1] != "#west" {
		t.Errorf("expected #west, got %s", db.scopes[1])
	}
}

func TestSeed_DBError(t *testing.T) {
	cfg := &Config{
		IATAs: map[string]IATAConfig{
			"YVR": {Name: "Vancouver"},
		},
	}

	db := newStubSeeder()
	db.upsertErr = errors.New("db error")

	if err := Seed(context.Background(), cfg, db); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestNormalizeScopeName_WithHash(t *testing.T) {
	if scopestore.FromName("#bc").Name != "#bc" {
		t.Error("expected #bc unchanged")
	}
}

func TestNormalizeScopeName_WithDollar(t *testing.T) {
	if scopestore.FromName("$bc").Name != "$bc" {
		t.Error("expected $bc unchanged")
	}
}

func TestNormalizeScopeName_WithoutPrefix(t *testing.T) {
	if scopestore.FromName("bc").Name != "#bc" {
		t.Error("expected bc to become #bc")
	}
}

func TestNormalizeScopeName_Empty(t *testing.T) {
	if scopestore.FromName("").Name != "#" {
		t.Error("expected empty string to become #")
	}
}

func TestDeriveScopeKey_Length(t *testing.T) {
	key := scopestore.FromName("#bc").TransportKey
	if len(key) != 16 {
		t.Errorf("expected 16 bytes, got %d", len(key))
	}
}

func TestDeriveScopeKey_Deterministic(t *testing.T) {
	a := scopestore.FromName("#bc").TransportKey
	b := scopestore.FromName("#bc").TransportKey
	if hex.EncodeToString(a) != hex.EncodeToString(b) {
		t.Error("expected same key for same input")
	}
}

func TestDeriveScopeKey_KnownValue(t *testing.T) {
	// SHA256("#bc")[:16] — pin the exact derivation so changes are caught
	key := scopestore.FromName("#bc").TransportKey
	got := hex.EncodeToString(key)
	// generate this once: echo -n "#bc" | sha256sum | cut -c1-32
	const want = "84509cfe73d94f7f6a8299e6bcdb8a3c"
	if got != want {
		t.Errorf("deriveScopeKey(\"#bc\") = %s, want %s", got, want)
	}
}

func TestDeriveScopeKey_DifferentInputs(t *testing.T) {
	a := scopestore.FromName("#bc").TransportKey
	b := scopestore.FromName("#other").TransportKey
	if hex.EncodeToString(a) == hex.EncodeToString(b) {
		t.Error("expected different keys for different inputs")
	}
}

func TestSeed_ChannelScopes(t *testing.T) {
	cfg := &Config{
		Regions:     []RegionConfig{{Slug: "bc", IATAs: []string{"YVR"}}},
		ChannelKeys: ChannelKeysConfig{Hashtags: []HashtagConfig{{Name: "meshcore"}, {Name: "vancouver", Region: "bc"}}},
	}
	db := newStubSeeder()
	if err := Seed(context.Background(), cfg, db); err != nil {
		t.Fatal(err)
	}
	if len(db.channelRegions) != 2 || db.channelRegions[0] != "" || db.channelRegions[1] != "bc" {
		t.Fatalf("channel regions = %q", db.channelRegions)
	}
}
