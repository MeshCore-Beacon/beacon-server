// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package scopestore provides an in-memory lookup of transport scope keys
// loaded from the database at startup.
package scopestore

import (
	"crypto/sha256"
	"slices"
	"strings"
	"sync"
)

// Entry holds a single transport scope key and its metadata.
type Entry struct {
	Name           string
	TransportKey   []byte   // 16 bytes
	KeyFingerprint []byte   // 8 bytes
	IATAs          []string // nil for manual keys; imported candidates are regional
}

// FromName uses the same case-sensitive derivation for manual and imported scopes.
func FromName(name string) Entry {
	if !strings.HasPrefix(name, "#") && !strings.HasPrefix(name, "$") {
		name = "#" + name
	}
	h := sha256.Sum256([]byte(name))
	key := h[:16]
	fingerprint := sha256.Sum256(key)
	return Entry{Name: name, TransportKey: key, KeyFingerprint: fingerprint[:8]}
}

// ScopeStore holds all known transport scope keys in memory.
type ScopeStore struct {
	mu      sync.RWMutex
	entries []Entry
	// IATA -> scope names, for region-filtered listings only; matching uses entries.
	manual, catalogue map[string][]string
}

// New creates an empty ScopeStore.
func New() *ScopeStore {
	return &ScopeStore{}
}

// Load publishes a new immutable snapshot. The caller must not mutate it after publication.
func (s *ScopeStore) Load(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
}

// Entries returns the current immutable snapshot. Callers must not modify it.
func (s *ScopeStore) Entries() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries
}

// SetManualMembers publishes configured region membership of manual scopes.
func (s *ScopeStore) SetManualMembers(byIATA map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manual = byIATA
}

// SetCatalogueMembers publishes imported catalogue membership, including names a manual scope overrides.
func (s *ScopeStore) SetCatalogueMembers(byIATA map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalogue = byIATA
}

// NamesForIATAs returns the sorted names of scopes that belong to any of the given IATAs.
func (s *ScopeStore) NamesForIATAs(iatas []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := []string{}
	for _, iata := range iatas {
		names = append(names, s.manual[iata]...)
		names = append(names, s.catalogue[iata]...)
	}
	slices.Sort(names)
	return slices.Compact(names)
}
