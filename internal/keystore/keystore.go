// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package keystore provides channel key lookup for the ingest pipeline.
// Keys are loaded from config at startup and never written at runtime.
// Future: add a DB-backed fallback that checks the channel_keys table.
package keystore

import (
	"crypto/sha256"
	"encoding/hex"
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
	entries map[string][]Entry // channel hash hex → entries
}

// NewMapKeyStore creates a keystore from a pre-built map of entries.
// Use BuildFromConfig to construct entries from a config.ChannelKeysConfig.
func NewMapKeyStore(entries map[string][]Entry) *MapKeyStore {
	store := &MapKeyStore{entries: make(map[string][]Entry)}
	for hash, list := range entries {
		store.entries[hash] = append(store.entries[hash], list...)
	}
	return store
}

// GetKey returns all known key entries for the given channel hash byte.
// Returns nil if no keys are known for this hash.
func (s *MapKeyStore) GetKey(channelHash []byte) []Entry {
	return s.entries[hex.EncodeToString(channelHash)]
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
