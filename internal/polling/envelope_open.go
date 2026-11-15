// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"crypto/aes"
	"crypto/hmac"
	"encoding/hex"
)

// The observer's side of the envelope, written from the firmware's openPwEnvelope. Beacon never opens an envelope
// itself; this exists so tests can read what Beacon sends the way a device does.

// openerSecret is the device's key agreement: its expanded private key (64 bytes, scalar first) against the
// one-time public key.
func openerSecret(observerPrv, epk []byte) ([]byte, error) {
	if len(observerPrv) != 64 {
		return nil, ErrEnvelopeKey
	}
	u, err := montgomeryU(epk)
	if err != nil {
		return nil, err
	}
	return x25519(observerPrv[:32], u)
}

// OpenEnvelope opens env with the observer's expanded private key, as the device does. The password is 1 to 15
// bytes, and the rest of the block must be zero.
func OpenEnvelope(observerPrv, observerPub, target []byte, env Envelope) ([]byte, error) {
	epk, err := hex.DecodeString(env.EPK)
	if err != nil {
		return nil, errEnvelopeOpen
	}
	secret, err := openerSecret(observerPrv, epk)
	if err != nil {
		return nil, err
	}
	return openWithSecret(secret, observerPub, target, env)
}

func openWithSecret(secret, observer, target []byte, env Envelope) ([]byte, error) {
	epk, err1 := hex.DecodeString(env.EPK)
	ct, err2 := hex.DecodeString(env.CT)
	tag, err3 := hex.DecodeString(env.Tag)
	if err1 != nil || err2 != nil || err3 != nil || len(epk) != 32 || len(ct) != 16 || len(tag) != 32 {
		return nil, errEnvelopeOpen
	}
	k := envelopeKeys(secret, observer, target, epk, env.Ver)
	if !hmac.Equal(envelopeTag(k[16:], ct), tag) {
		return nil, errEnvelopeOpen
	}
	block, err := aes.NewCipher(k[:16])
	if err != nil {
		return nil, err
	}
	var plain [16]byte
	block.Decrypt(plain[:], ct)
	n := 0
	for n < 16 && plain[n] != 0 {
		n++
	}
	if n < 1 || n > MaxGuestPassword {
		return nil, errEnvelopeOpen
	}
	for _, b := range plain[n:] {
		if b != 0 {
			return nil, errEnvelopeOpen
		}
	}
	return append([]byte(nil), plain[:n]...), nil
}
