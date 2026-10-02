// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package keystore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestDeriveHashtagKey_SecretLength(t *testing.T) {
	secret, _, _ := DeriveHashtagKey("bc")
	if len(secret) != 16 {
		t.Errorf("expected 16 byte secret, got %d", len(secret))
	}
}

func TestDeriveHashtagKey_Deterministic(t *testing.T) {
	s1, h1, f1 := DeriveHashtagKey("bc")
	s2, h2, f2 := DeriveHashtagKey("bc")
	if !bytes.Equal(s1, s2) {
		t.Error("secret not deterministic")
	}
	if h1 != h2 {
		t.Error("channelHash not deterministic")
	}
	if !bytes.Equal(f1, f2) {
		t.Error("fingerprint not deterministic")
	}
}

func TestDeriveHashtagKey_DifferentInputs(t *testing.T) {
	s1, _, _ := DeriveHashtagKey("bc")
	s2, _, _ := DeriveHashtagKey("other")
	if bytes.Equal(s1, s2) {
		t.Error("expected different secrets for different inputs")
	}
}

func TestDeriveHashtagKey_DerivationSpec(t *testing.T) {
	// secret = SHA256("#bc")[:16]
	// channel_hash = SHA256(secret)[0]
	// fingerprint = SHA256(secret)[:8]
	tag := "bc"
	input := sha256.Sum256([]byte("#" + tag))
	expectedSecret := input[:16]
	expectedSecretHash := sha256.Sum256(expectedSecret)
	expectedChannelHash := expectedSecretHash[0]
	expectedFingerprint := expectedSecretHash[:8]

	secret, channelHash, fingerprint := DeriveHashtagKey(tag)

	if !bytes.Equal(secret, expectedSecret) {
		t.Errorf("secret mismatch: got %s, want %s", hex.EncodeToString(secret), hex.EncodeToString(expectedSecret))
	}
	if channelHash != expectedChannelHash {
		t.Errorf("channelHash mismatch: got %02x, want %02x", channelHash, expectedChannelHash)
	}
	if !bytes.Equal(fingerprint, expectedFingerprint) {
		t.Errorf("fingerprint mismatch: got %s, want %s", hex.EncodeToString(fingerprint), hex.EncodeToString(expectedFingerprint))
	}
}

func TestFingerprint_Length(t *testing.T) {
	fp := Fingerprint([]byte("somekey"))
	if len(fp) != 8 {
		t.Errorf("expected 8 bytes, got %d", len(fp))
	}
}

func TestFingerprint_Deterministic(t *testing.T) {
	key := []byte("somekey")
	if !bytes.Equal(Fingerprint(key), Fingerprint(key)) {
		t.Error("fingerprint not deterministic")
	}
}

func TestFingerprint_MatchesSHA256Prefix(t *testing.T) {
	key := []byte("somekey")
	h := sha256.Sum256(key)
	expected := h[:8]
	if !bytes.Equal(Fingerprint(key), expected) {
		t.Error("fingerprint does not match SHA256(key)[:8]")
	}
}

func TestNewMapKeyStore_Empty(t *testing.T) {
	s := NewMapKeyStore(nil)
	if s.GetKey([]byte{0x01}) != nil {
		t.Error("expected nil for unknown hash")
	}
}

func TestNewMapKeyStore_GetKey_Hit(t *testing.T) {
	key := []byte{0x01, 0x02, 0x03}
	hash := []byte{0xab}
	entries := map[string][]Entry{
		hex.EncodeToString(hash): {{Key: key, Name: "test"}},
	}
	s := NewMapKeyStore(entries)
	result := s.GetKey(hash)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	if !bytes.Equal(result[0].Key, key) {
		t.Errorf("expected key %v, got %v", key, result[0].Key)
	}
}

func TestNewMapKeyStore_GetKey_Miss(t *testing.T) {
	s := NewMapKeyStore(map[string][]Entry{
		"ab": {{Key: []byte{0x01}}},
	})
	if s.GetKey([]byte{0xcd}) != nil {
		t.Error("expected nil for unknown hash")
	}
}

func TestNewMapKeyStore_MultipleEntriesPerHash(t *testing.T) {
	hash := []byte{0xab}
	entries := map[string][]Entry{
		hex.EncodeToString(hash): {
			{Key: []byte{0x01}, Name: "first"},
			{Key: []byte{0x02}, Name: "second"},
		},
	}
	s := NewMapKeyStore(entries)
	result := s.GetKey(hash)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
}

func TestEntryExists_Found(t *testing.T) {
	key := []byte{0x01, 0x02}
	entries := []Entry{{Key: key}}
	if !EntryExists(entries, Entry{Key: key}) {
		t.Error("expected entry to be found")
	}
}

func TestEntryExists_NotFound(t *testing.T) {
	entries := []Entry{{Key: []byte{0x01}}}
	if EntryExists(entries, Entry{Key: []byte{0x02}}) {
		t.Error("expected entry not to be found")
	}
}

func TestEntryExists_Empty(t *testing.T) {
	if EntryExists(nil, Entry{Key: []byte{0x01}}) {
		t.Error("expected false for empty slice")
	}
}

func TestSetImported_ConfigWinsAndNewHashesReported(t *testing.T) {
	secret, hash, fp := DeriveHashtagKey("meshcore")
	hashHex := hex.EncodeToString([]byte{hash})
	configured := Entry{Key: secret, Fingerprint: fp, Hashtag: "meshcore", Name: "Configured"}
	store := NewMapKeyStore(map[string][]Entry{hashHex: {configured}})

	dupSecret, _, dupFP := DeriveHashtagKey("meshcore")
	newSecret, newHash, newFP := DeriveHashtagKey("ottawa-mesh")
	added := store.SetImported([]Entry{
		{Key: dupSecret, Fingerprint: dupFP, Hashtag: "meshcore", Name: "#meshcore"},
		{Key: newSecret, Fingerprint: newFP, Hashtag: "ottawa-mesh", Name: "#ottawa-mesh"},
	})
	if len(added) != 1 || added[0][0] != newHash {
		t.Fatalf("new hashes = %x", added)
	}
	if got := store.GetKey([]byte{hash}); len(got) != 1 || got[0].Name != "Configured" {
		t.Fatalf("config entry replaced: %+v", got)
	}
	if got := store.GetKey([]byte{newHash}); len(got) != 1 || got[0].Hashtag != "ottawa-mesh" {
		t.Fatalf("imported entry missing: %+v", got)
	}
	if added = store.SetImported([]Entry{{Key: newSecret, Fingerprint: newFP, Hashtag: "ottawa-mesh"}}); len(added) != 0 {
		t.Fatal("unchanged import reported new hashes", added)
	}
	store.SetImported(nil)
	if store.GetKey([]byte{newHash}) != nil || len(store.GetKey([]byte{hash})) != 1 {
		t.Fatal("clearing imports touched config keys or kept the import")
	}
}
