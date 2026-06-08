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
