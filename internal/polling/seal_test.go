// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/google/uuid"
)

func testSealer(t *testing.T, fill byte) *Sealer {
	t.Helper()
	s, err := NewSealer(bytes.Repeat([]byte{fill}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSealRoundTripAtRest(t *testing.T) {
	s := testSealer(t, 1)
	id, target := uuid.New(), bytes.Repeat([]byte{9}, 32)
	ct, nonce, err := s.Seal(id, target, []byte("guest"))
	if err != nil {
		t.Fatal(err)
	}
	if len(nonce) != 12 || bytes.Contains(ct, []byte("guest")) {
		t.Fatalf("nonce %d, plaintext visible: %v", len(nonce), bytes.Contains(ct, []byte("guest")))
	}
	got, err := s.Open(id, target, ct, nonce)
	if err != nil || string(got) != "guest" {
		t.Fatalf("open: %q %v", got, err)
	}
	ct2, nonce2, _ := s.Seal(id, target, []byte("guest"))
	if bytes.Equal(nonce, nonce2) || bytes.Equal(ct, ct2) {
		t.Error("nonce reused")
	}
	// Bound to the request and the target: a copied row does not open elsewhere.
	if _, err := s.Open(uuid.New(), target, ct, nonce); err == nil {
		t.Error("opened under another request id")
	}
	if _, err := s.Open(id, bytes.Repeat([]byte{8}, 32), ct, nonce); err == nil {
		t.Error("opened under another target")
	}
	if _, err := testSealer(t, 2).Open(id, target, ct, nonce); err == nil {
		t.Error("opened under another key")
	}
	bad := append([]byte{}, ct...)
	bad[0] ^= 1
	if _, err := s.Open(id, target, bad, nonce); err == nil {
		t.Error("opened a changed ciphertext")
	}
	if _, err := s.Open(id, target, ct, nonce[:5]); err == nil {
		t.Error("short nonce")
	}
}

func TestParseSecretKey(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding} {
		got, err := ParseSecretKey(" " + enc.EncodeToString(key) + "\n")
		if err != nil || !bytes.Equal(got, key) {
			t.Errorf("%v: %v", enc, err)
		}
	}
	for _, bad := range []string{"", "not base64!", base64.StdEncoding.EncodeToString(key[:31]), base64.StdEncoding.EncodeToString(append(key, 1))} {
		if _, err := ParseSecretKey(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := NewSealer(key[:16]); err == nil {
		t.Error("short key")
	}
}
