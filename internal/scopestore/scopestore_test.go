// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package scopestore

import (
	"testing"
)

func TestNew_Empty(t *testing.T) {
	s := New()
	if len(s.Entries()) != 0 {
		t.Errorf("expected empty store, got %d entries", len(s.Entries()))
	}
}

func TestLoad_ReplacesEntries(t *testing.T) {
	s := New()
	s.Load([]Entry{
		{Name: "#bc", TransportKey: []byte{0x01}, KeyFingerprint: []byte{0x02}},
	})
	entries := s.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "#bc" {
		t.Errorf("expected #bc, got %s", entries[0].Name)
	}

	// replace with new entries
	s.Load([]Entry{
		{Name: "#west", TransportKey: []byte{0x03}, KeyFingerprint: []byte{0x04}},
		{Name: "#east", TransportKey: []byte{0x05}, KeyFingerprint: []byte{0x06}},
	})
	entries = s.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after reload, got %d", len(entries))
	}
}

func TestEntriesRetainsImmutableSnapshotWithoutAllocation(t *testing.T) {
	s := New()
	s.Load([]Entry{{Name: "#bc"}})
	previous := s.Entries()
	if allocations := testing.AllocsPerRun(100, func() {
		if len(s.Entries()) != 1 {
			t.Fatal("missing entries")
		}
	}); allocations != 0 {
		t.Fatalf("snapshot read allocates: %v", allocations)
	}
	s.Load([]Entry{{Name: "#on"}})
	if previous[0].Name != "#bc" || s.Entries()[0].Name != "#on" {
		t.Fatal("replacement mutated the previous reader snapshot")
	}
}
