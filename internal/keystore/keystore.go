// Package keystore provides channel key lookup for the ingest pipeline.
// Keys are loaded from config at startup and never written at runtime.
// Future: add a DB-backed fallback that checks the channel_keys table.
package keystore

import (
	"encoding/hex"
)

type MapKeyStore struct {
	keys map[string][][]byte // channel hash hex string → key bytes
}

func NewMapKeyStore(keys map[string][][]byte) *MapKeyStore {
	store := &MapKeyStore{keys: make(map[string][][]byte)}
	for hash, keyList := range keys {
		store.keys[hash] = append(store.keys[hash], keyList...)
	}
	return store
}

func (s *MapKeyStore) GetKey(channelHash []byte) [][]byte {
	return s.keys[hex.EncodeToString(channelHash)]
}
