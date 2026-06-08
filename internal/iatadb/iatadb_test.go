// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package iatadb_test

import (
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
)

func TestLookup_KnownIATA(t *testing.T) {
	entry, ok := iatadb.Lookup("YVR")
	if !ok {
		t.Fatal("expected YVR to be found")
	}
	if entry.Country != "CA" {
		t.Errorf("expected country CA, got %s", entry.Country)
	}
	if entry.Continent != "NA" {
		t.Errorf("expected continent NA, got %s", entry.Continent)
	}
}

func TestLookup_UnknownIATA(t *testing.T) {
	entry, ok := iatadb.Lookup("ZZZZZ")
	if ok {
		t.Fatal("expected ZZZZZ not to be found")
	}
	if entry.Country != "" {
		t.Errorf("expected country to be empty, got %s", entry.Country)
	}
	if entry.Continent != "" {
		t.Errorf("expected continent to be empty, got %s", entry.Continent)
	}
}

func TestCountryFor_Known(t *testing.T) {
	country := iatadb.CountryFor("YVR")
	if country != "CA" {
		t.Fatalf("expected country for YVR to be CA, got %s", country)
	}
}

func TestCountryFor_Unknown(t *testing.T) {
	country := iatadb.CountryFor("ZZZZZ")
	if country != "" {
		t.Fatalf("expected no country for IATA ZZZZZ, got %s", country)
	}
}

func TestContinentFor_Known(t *testing.T) {
	continent := iatadb.ContinentFor("YVR")
	if continent != "NA" {
		t.Fatalf("expected continent for YVR to be NA, got %s", continent)
	}
}

func TestContinentFor_Unknown(t *testing.T) {
	continent := iatadb.ContinentFor("ZZZZZ")
	if continent != "" {
		t.Fatalf("expected no continient for IATA ZZZZZ, got %s", continent)
	}
}

func TestBuildAllowedSet_Empty(t *testing.T) {
	result := iatadb.BuildAllowedSet(nil, nil)
	if result != nil {
		t.Error("expected nil for empty filter")
	}
}

func TestBuildAllowedSet_ByCountry(t *testing.T) {
	allowed := iatadb.BuildAllowedSet([]string{"CA"}, nil)
	if allowed == nil {
		t.Fatal("expected non-nil result")
	}
	if _, ok := allowed["YVR"]; !ok {
		t.Error("expected YVR to be allowed")
	}
	if _, ok := allowed["JFK"]; ok {
		t.Error("expected JFK to be excluded")
	}
}

func TestBuildAllowedSet_ByContinent(t *testing.T) {
	allowed := iatadb.BuildAllowedSet(nil, []string{"NA"})
	if allowed == nil {
		t.Fatal("expected non-nil result")
	}
	if _, ok := allowed["YVR"]; !ok {
		t.Error("expected YVR to be allowed")
	}
	if _, ok := allowed["LHR"]; ok {
		t.Error("expected LHR (EU) to be excluded")
	}
}

func TestBuildAllowedSet_ORSemantics(t *testing.T) {
	allowed := iatadb.BuildAllowedSet([]string{"CA"}, []string{"EU"})
	if allowed == nil {
		t.Fatal("expected non-nil result")
	}
	if _, ok := allowed["YVR"]; !ok {
		t.Error("expected YVR (CA) to be allowed")
	}
	if _, ok := allowed["LHR"]; !ok {
		t.Error("expected LHR (EU) to be allowed")
	}
	if _, ok := allowed["GRU"]; ok {
		t.Error("expected GRU (SA/BR) to be excluded")
	}
}
