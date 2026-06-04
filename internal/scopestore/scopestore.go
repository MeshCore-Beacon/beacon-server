// Package scopestore provides an in-memory lookup of transport scope keys
// loaded from the database at startup.
package scopestore

import "sync"

// Entry holds a single transport scope key and its metadata.
type Entry struct {
	Name           string
	TransportKey   []byte // 16 bytes
	KeyFingerprint []byte // 8 bytes
}

// ScopeStore holds all known transport scope keys in memory.
type ScopeStore struct {
	mu      sync.RWMutex
	entries []Entry
}

// New creates an empty ScopeStore.
func New() *ScopeStore {
	return &ScopeStore{}
}

// Load replaces all entries — call on startup after DB seeding.
func (s *ScopeStore) Load(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
}

// Entries returns a copy of all loaded entries.
func (s *ScopeStore) Entries() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Entry, len(s.entries))
	copy(result, s.entries)
	return result
}
