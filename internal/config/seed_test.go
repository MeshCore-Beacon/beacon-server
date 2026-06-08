// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/hex"
	"testing"
)

func TestNormalizeScopeName_WithHash(t *testing.T) {
	if normalizeScopeName("#bc") != "#bc" {
		t.Error("expected #bc unchanged")
	}
}

func TestNormalizeScopeName_WithDollar(t *testing.T) {
	if normalizeScopeName("$bc") != "$bc" {
		t.Error("expected $bc unchanged")
	}
}

func TestNormalizeScopeName_WithoutPrefix(t *testing.T) {
	if normalizeScopeName("bc") != "#bc" {
		t.Error("expected bc to become #bc")
	}
}

func TestNormalizeScopeName_Empty(t *testing.T) {
	if normalizeScopeName("") != "#" {
		t.Error("expected empty string to become #")
	}
}

func TestDeriveScopeKey_Length(t *testing.T) {
	key := deriveScopeKey("#bc")
	if len(key) != 16 {
		t.Errorf("expected 16 bytes, got %d", len(key))
	}
}

func TestDeriveScopeKey_Deterministic(t *testing.T) {
	a := deriveScopeKey("#bc")
	b := deriveScopeKey("#bc")
	if hex.EncodeToString(a) != hex.EncodeToString(b) {
		t.Error("expected same key for same input")
	}
}

func TestDeriveScopeKey_KnownValue(t *testing.T) {
	// SHA256("#bc")[:16] — pin the exact derivation so changes are caught
	key := deriveScopeKey("#bc")
	got := hex.EncodeToString(key)
	// generate this once: echo -n "#bc" | sha256sum | cut -c1-32
	const want = "84509cfe73d94f7f6a8299e6bcdb8a3c"
	if got != want {
		t.Errorf("deriveScopeKey(\"#bc\") = %s, want %s", got, want)
	}
}

func TestDeriveScopeKey_DifferentInputs(t *testing.T) {
	a := deriveScopeKey("#bc")
	b := deriveScopeKey("#other")
	if hex.EncodeToString(a) == hex.EncodeToString(b) {
		t.Error("expected different keys for different inputs")
	}
}
