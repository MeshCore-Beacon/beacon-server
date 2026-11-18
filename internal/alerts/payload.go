// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

const (
	// PayloadKeySize is the AES-256 key the app generates for each device.
	PayloadKeySize = 32
	// MaxCiphertext is the most the push server accepts for one notification, once decoded.
	MaxCiphertext    = 2048
	payloadAADPrefix = "beacon-push/v1:"
)

// notice is what the app decrypts. It carries facts, not wording, so a server cannot put arbitrary text under the
// app's name: the app writes the sentence itself.
type notice struct {
	V          int    `json:"v"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Value      *int64 `json:"value"`
	Detail     string `json:"detail"`
	TargetKind string `json:"targetKind"`
	PublicKey  string `json:"publicKey"`
	ID         string `json:"id"`
	TempUnit   string `json:"tempUnit"`
}

// NoticePayload is the plaintext of one push notice. tempUnit is the instance unit, "f" or "c".
func NoticePayload(r OutboxRow, tempUnit string) []byte {
	n := notice{V: 1, Kind: r.Kind, Name: SanitizeName(r.Name, r.Key), Detail: r.Detail, TargetKind: r.TargetKind,
		PublicKey: hex.EncodeToString(r.Key), TempUnit: "c"}
	if r.HasValue && HasFigure(r.Kind) {
		v := r.Value
		n.Value = &v
	}
	if r.TargetID != uuid.Nil {
		n.ID = r.TargetID.String()
	}
	if tempUnit == "f" {
		n.TempUnit = "f"
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(n) // a struct of strings and numbers always encodes
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}

// HasFigure is false for the kinds whose sentence gives no figure; their notices carry a null value whatever is
// stored (the evaluator stores uptime for a reboot and zero for the others).
func HasFigure(kind string) bool {
	switch kind {
	case KindBackOnline, KindRebooted, KindRequestAttention:
		return false
	}
	return true
}

// SealNotice encrypts a notice for one device with AES-256-GCM: nonce(12) || ciphertext || tag(16), bound to this
// server's Push ID so another server's notice cannot be replayed as ours.
func SealNotice(key []byte, pushID string, plaintext []byte) ([]byte, error) {
	if len(key) != PayloadKeySize {
		return nil, errors.New("alerts: payload key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, []byte(payloadAADPrefix+pushID)), nil
}
