// Package keystore provides channel key lookup for the ingest pipeline.
// Keys are loaded from config at startup and never written at runtime.
// Future: add a DB-backed fallback that checks the channel_keys table.
package keystore

import (
	"encoding/hex"
	"maps"
)

type MapKeyStore struct {
	keys map[string][]byte // channel hash hex string → key bytes
}

var defaultKeys = map[string][]byte{
	"11": {0x8b, 0x33, 0x87, 0xe9, 0xc5, 0xcd, 0xea, 0x6a, 0xc9, 0xe5, 0xed, 0xba, 0xa1, 0x15, 0xcd, 0x72}, // public channel
}

func NewMapKeyStore(extra map[string][]byte) *MapKeyStore {
	keys := make(map[string][]byte)
	maps.Copy(keys, defaultKeys)
	maps.Copy(keys, extra)
	return &MapKeyStore{keys}
}

func (s *MapKeyStore) GetKey(channelHash []byte) []byte {
	return s.keys[hex.EncodeToString(channelHash)]
}
