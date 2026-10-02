// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package keystore provides channel key lookup for the ingest pipeline.
// Config keys are fixed at startup; MeshMapper imports are swapped in at runtime.
package keystore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sync/atomic"
)

// Entry holds a resolved channel key along with its metadata.
type Entry struct {
	Key         []byte // raw key bytes (16 bytes for hashtag channels)
	Fingerprint []byte // first 8 bytes of SHA256(key)
	Hashtag     string // set if derived from a hashtag, empty otherwise
	Name        string // display name, may be empty
}

// MapKeyStore maps channel hash hex → list of known key entries.
// Multiple entries per hash are supported to handle 1-byte hash collisions.
type MapKeyStore struct {
	config   map[string][]Entry
	imported map[string][]Entry
	entries  atomic.Pointer[map[string][]Entry] // config + imported, read lock-free by ingest
}

// NewMapKeyStore creates a keystore from the configured entries.
func NewMapKeyStore(entries map[string][]Entry) *MapKeyStore {
	store := &MapKeyStore{config: make(map[string][]Entry), imported: map[string][]Entry{}}
	for hash, list := range entries {
		store.config[hash] = append(store.config[hash], list...)
	}
	store.entries.Store(&store.config)
	return store
}

// GetKey returns all known key entries for the given channel hash byte.
// Returns nil if no keys are known for this hash.
func (s *MapKeyStore) GetKey(channelHash []byte) []Entry {
	return (*s.entries.Load())[hex.EncodeToString(channelHash)]
}

// SetImported replaces the imported keys and returns the channel hashes of keys
// that weren't known before. Config keys win; one writer at a time.
func (s *MapKeyStore) SetImported(entries []Entry) [][]byte {
	merged := make(map[string][]Entry, len(s.config))
	for hash, list := range s.config {
		merged[hash] = list
	}
	imported := map[string][]Entry{}
	var added [][]byte
	for _, e := range entries {
		hash := sha256.Sum256(e.Key)
		hashHex := hex.EncodeToString(hash[:1])
		if EntryExists(merged[hashHex], e) {
			continue
		}
		merged[hashHex] = append(slices.Clone(merged[hashHex]), e)
		imported[hashHex] = append(imported[hashHex], e)
		if !EntryExists(s.imported[hashHex], e) {
			added = append(added, hash[:1])
		}
	}
	s.imported = imported
	s.entries.Store(&merged)
	return added
}

// DeriveHashtagKey derives the channel secret and hash for a hashtag name.
//
//	secret       = SHA256("#" + tag)[:16]
//	channel_hash = SHA256(secret)[0]
//	fingerprint  = SHA256(secret)[:8]
func DeriveHashtagKey(tag string) (secret []byte, channelHash byte, fingerprint []byte) {
	input := sha256.Sum256([]byte("#" + tag))
	secret = input[:16]
	secretHash := sha256.Sum256(secret)
	channelHash = secretHash[0]
	fingerprint = secretHash[:8]
	return
}

// Fingerprint returns the first 8 bytes of SHA256(key).
func Fingerprint(key []byte) []byte {
	h := sha256.Sum256(key)
	return h[:8]
}

func EntryExists(entries []Entry, e Entry) bool {
	for _, existing := range entries {
		if bytes.Equal(existing.Key, e.Key) {
			return true
		}
	}
	return false
}
