// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"

	"github.com/google/uuid"
)

// Sealer encrypts guest passwords at rest with AES-256-GCM under BEACON_SECRET_KEY. Each row is bound to its
// request id and target key, so a copied row does not open anywhere else.
type Sealer struct{ aead cipher.AEAD }

// ParseSecretKey reads the 32 random bytes from their base64 form (standard or URL, padded or not).
func ParseSecretKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			if len(b) != 32 {
				return nil, errors.New("secret key must be 32 bytes")
			}
			return b, nil
		}
	}
	return nil, errors.New("secret key is not base64")
}

// NewSealer takes the 32-byte key.
func NewSealer(key []byte) (*Sealer, error) {
	if len(key) != 32 {
		return nil, errors.New("secret key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: aead}, nil
}

func sealAAD(requestID uuid.UUID, target []byte) []byte {
	return append(append([]byte{}, requestID[:]...), target...)
}

// Seal returns the ciphertext and its random 12-byte nonce.
func (s *Sealer) Seal(requestID uuid.UUID, target, password []byte) (ct, nonce []byte, err error) {
	nonce = make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	return s.aead.Seal(nil, nonce, password, sealAAD(requestID, target)), nonce, nil
}

// Open reverses Seal. The error says nothing about why it failed.
func (s *Sealer) Open(requestID uuid.UUID, target, ct, nonce []byte) ([]byte, error) {
	if len(nonce) != s.aead.NonceSize() {
		return nil, errors.New("polling: password does not open")
	}
	pw, err := s.aead.Open(nil, nonce, ct, sealAAD(requestID, target))
	if err != nil {
		return nil, errors.New("polling: password does not open")
	}
	return pw, nil
}
