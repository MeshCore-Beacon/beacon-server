// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/binary"
	"encoding/json"
	"testing"
)

func TestParseNumber_Float(t *testing.T) {
	raw := json.RawMessage(`3.14`)
	if parseNumber(raw) != 3.14 {
		t.Errorf("expected 3.14, got %f", parseNumber(raw))
	}
}

func TestParseNumber_QuotedString(t *testing.T) {
	raw := json.RawMessage(`"42.5"`)
	if parseNumber(raw) != 42.5 {
		t.Errorf("expected 42.5, got %f", parseNumber(raw))
	}
}

func TestParseNumber_Empty(t *testing.T) {
	if parseNumber(json.RawMessage(``)) != 0 {
		t.Error("expected 0 for empty input")
	}
}

func TestParseNumber_Invalid(t *testing.T) {
	if parseNumber(json.RawMessage(`"notanumber"`)) != 0 {
		t.Error("expected 0 for unparseable string")
	}
}

func TestParseNumber_Integer(t *testing.T) {
	raw := json.RawMessage(`7`)
	if parseNumber(raw) != 7 {
		t.Errorf("expected 7, got %f", parseNumber(raw))
	}
}

func TestNormalizeObserverType_OrgPrefix(t *testing.T) {
	if normalizeObserverType("meshcore-dev/meshcore-ha") != "meshcore-ha" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcore-dev/meshcore-ha"))
	}
}

func TestNormalizeObserverType_VersionSuffix(t *testing.T) {
	if normalizeObserverType("meshcoretomqtt:1.1.0") != "meshcoretomqtt" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcoretomqtt:1.1.0"))
	}
}

func TestNormalizeObserverType_BuildSuffix(t *testing.T) {
	// org/name format — LastIndex strips to just the name portion
	if normalizeObserverType("meshcore-dev/meshcoretomqtt") != "meshcoretomqtt" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcore-dev/meshcoretomqtt"))
	}
}

func TestNormalizeObserverType_Plain(t *testing.T) {
	if normalizeObserverType("meshcoretomqtt") != "meshcoretomqtt" {
		t.Errorf("unexpected: %s", normalizeObserverType("meshcoretomqtt"))
	}
}

func TestNormalizeObserverType_Empty(t *testing.T) {
	if normalizeObserverType("") != "" {
		t.Error("expected empty string for empty input")
	}
}

func TestInferObserverType_SourceTakesPriority(t *testing.T) {
	got := inferObserverType("meshcore-dev/meshcore-ha", "some-version")
	if got != "meshcore-ha" {
		t.Errorf("expected meshcore-ha, got %s", got)
	}
}

func TestInferObserverType_FallsBackToClientVersion(t *testing.T) {
	got := inferObserverType("", "custom-firmware-1.0")
	if got != "custom-firmware-1.0" {
		t.Errorf("expected custom-firmware-1.0, got %s", got)
	}
}

func TestInferObserverType_BothEmpty(t *testing.T) {
	if inferObserverType("", "") != "" {
		t.Error("expected empty string when both inputs are empty")
	}
}

func TestUint32ToBytes_KnownValue(t *testing.T) {
	b := uint32ToBytes(0x01020304)
	// little-endian: least significant byte first
	expected := []byte{0x04, 0x03, 0x02, 0x01}
	for i, v := range expected {
		if b[i] != v {
			t.Errorf("byte %d: expected %02x, got %02x", i, v, b[i])
		}
	}
}

func TestUint32ToBytes_Zero(t *testing.T) {
	b := uint32ToBytes(0)
	if len(b) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(b))
	}
	for _, v := range b {
		if v != 0 {
			t.Error("expected all zero bytes")
		}
	}
}

func TestUint32ToBytes_RoundTrip(t *testing.T) {
	v := uint32(0xDEADBEEF)
	b := uint32ToBytes(v)
	got := binary.LittleEndian.Uint32(b)
	if got != v {
		t.Errorf("round trip failed: expected %x, got %x", v, got)
	}
}
