// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"crypto/aes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"strconv"
)

// Envelope is the guest password sealed to one observer, as the firmware reads it from the owners answer
// (spec section 4.4). Hex is lower case.
type Envelope struct {
	EPK string `json:"epk"`
	CT  string `json:"ct"`
	Tag string `json:"tag"`
	Ver uint32 `json:"ver"`
}

// MaxGuestPassword is the longest password the envelope carries: one AES block, zero padded.
const MaxGuestPassword = 15

const envelopeLabel = "beacon-poll-pw/v1|"

var (
	ErrEnvelopePassword = errors.New("polling: password must be 1 to 15 bytes with no zero byte")
	ErrEnvelopeKey      = errors.New("polling: bad public key")
	errEnvelopeOpen     = errors.New("polling: envelope does not open")
)

var fieldP = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(19))

// ValidGuestPassword reports whether pw fits the envelope.
func ValidGuestPassword(pw []byte) bool {
	if len(pw) < 1 || len(pw) > MaxGuestPassword {
		return false
	}
	for _, b := range pw {
		if b == 0 {
			return false
		}
	}
	return true
}

// montgomeryU maps an Ed25519 public key to the X25519 u coordinate: u = (1+y)/(1-y) with bit 255 cleared.
func montgomeryU(edPub []byte) ([]byte, error) {
	if len(edPub) != 32 {
		return nil, ErrEnvelopeKey
	}
	le := make([]byte, 32)
	for i := range le {
		le[31-i] = edPub[i] // big-endian for big.Int
	}
	le[0] &= 0x7f
	y := new(big.Int).SetBytes(le)
	y.Mod(y, fieldP)
	den := new(big.Int).Sub(big.NewInt(1), y)
	den.Mod(den, fieldP)
	if den.Sign() == 0 {
		return nil, ErrEnvelopeKey
	}
	num := new(big.Int).Add(big.NewInt(1), y)
	u := num.Mul(num, den.ModInverse(den, fieldP))
	u.Mod(u, fieldP)
	be := u.FillBytes(make([]byte, 32))
	out := make([]byte, 32)
	for i := range out {
		out[31-i] = be[i]
	}
	return out, nil
}

// x25519 computes the shared secret and refuses an all-zero result (a low-order peer key).
func x25519(scalar, u []byte) ([]byte, error) {
	priv, err := ecdh.X25519().NewPrivateKey(scalar)
	if err != nil {
		return nil, err
	}
	pub, err := ecdh.X25519().NewPublicKey(u)
	if err != nil {
		return nil, ErrEnvelopeKey
	}
	s, err := priv.ECDH(pub)
	if err != nil {
		return nil, ErrEnvelopeKey
	}
	return s, nil
}

// expandSeed is the Ed25519 secret scalar for a seed: the first half of SHA-512, clamped.
func expandSeed(seed []byte) []byte {
	h := sha512.Sum512(seed)
	h[0] &= 248
	h[31] &= 63
	h[31] |= 64
	return h[:]
}

// envelopeSecret is the Beacon side of the key agreement: the one-time key against the observer's node key.
func envelopeSecret(ephSeed, observerPub []byte) ([]byte, error) {
	u, err := montgomeryU(observerPub)
	if err != nil {
		return nil, err
	}
	return x25519(expandSeed(ephSeed)[:32], u)
}

// envelopeKeys derives the AES key (first half) and the MAC key (second half).
func envelopeKeys(secret, observer, target, epk []byte, ver uint32) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(envelopeLabel + hex.EncodeToString(observer) + "|" + hex.EncodeToString(target) + "|" +
		hex.EncodeToString(epk) + "|" + strconv.FormatUint(uint64(ver), 10)))
	return m.Sum(nil)
}

func envelopeTag(macKey, ct []byte) []byte {
	m := hmac.New(sha256.New, macKey)
	m.Write(ct)
	return m.Sum(nil)
}

// SealEnvelope seals password for observer (its node public key) polling target, at password version ver. rnd
// supplies the 32-byte one-time seed; every call needs a fresh one.
func SealEnvelope(rnd io.Reader, observer, target []byte, ver uint32, password []byte) (Envelope, error) {
	if !ValidGuestPassword(password) {
		return Envelope{}, ErrEnvelopePassword
	}
	if len(observer) != 32 || len(target) != 32 {
		return Envelope{}, ErrEnvelopeKey
	}
	seed := make([]byte, ed25519.SeedSize)
	if _, err := io.ReadFull(rnd, seed); err != nil {
		return Envelope{}, err
	}
	secret, err := envelopeSecret(seed, observer)
	if err != nil {
		return Envelope{}, err
	}
	epk := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	k := envelopeKeys(secret, observer, target, epk, ver)
	block, err := aes.NewCipher(k[:16])
	if err != nil {
		return Envelope{}, err
	}
	var plain, ct [16]byte
	copy(plain[:], password)
	block.Encrypt(ct[:], plain[:])
	return Envelope{
		EPK: hex.EncodeToString(epk), CT: hex.EncodeToString(ct[:]),
		Tag: hex.EncodeToString(envelopeTag(k[16:], ct[:])), Ver: ver,
	}, nil
}
