// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/google/uuid"
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

func TestComputeTransportCode_Deterministic(t *testing.T) {
	key := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	c1 := computeTransportCode(key, 4, payload)
	c2 := computeTransportCode(key, 4, payload)
	if c1 != c2 {
		t.Error("expected deterministic output")
	}
}

func TestComputeTransportCode_DifferentPayloadTypes(t *testing.T) {
	key := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	payload := []byte{0xde, 0xad}
	c1 := computeTransportCode(key, 4, payload)
	c2 := computeTransportCode(key, 5, payload)
	if c1 == c2 {
		t.Error("expected different codes for different payload types")
	}
}

func TestComputeTransportCode_NeverReturnsReservedValues(t *testing.T) {
	key := make([]byte, 16)
	for i := 0; i < 256; i++ {
		key[0] = byte(i)
		for j := 0; j < 16; j++ {
			code := computeTransportCode(key, uint8(j), []byte{byte(i), byte(j)})
			if code == 0x0000 {
				t.Errorf("got reserved 0x0000 for key[0]=%d payloadType=%d", i, j)
			}
			if code == 0xFFFF {
				t.Errorf("got reserved 0xFFFF for key[0]=%d payloadType=%d", i, j)
			}
		}
	}
}

// stubDB implements only the methods needed for capability detection tests.
type stubDB struct {
	setCapabilityCalls []setCapabilityCall

	upsertNodeCalls            int
	upsertChannelCalls         int
	upsertChannelHashOnlyCalls int
	upsertChannelIATACalls     int
	upsertTraceIATACalls       int
	observationInserted        bool
}

type setCapabilityCall struct {
	nodeID uuid.UUID
	paths  bool
	traces bool
}

func (s *stubDB) SetNodeCapability(_ context.Context, nodeID uuid.UUID, paths, traces bool) error {
	s.setCapabilityCalls = append(s.setCapabilityCalls, setCapabilityCall{nodeID, paths, traces})
	return nil
}

// no-op implementations for remaining DB interface methods
func (s *stubDB) UpsertObserver(_ context.Context, _ []byte) (uuid.UUID, string, error) {
	return uuid.Nil, "", nil
}
func (s *stubDB) UpsertObserverBroker(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (s *stubDB) UpsertIATA(_ context.Context, _ string) error                        { return nil }
func (s *stubDB) UpsertPacket(_ context.Context, _ UpsertPacketParams) (bool, error) {
	return false, nil
}
func (s *stubDB) SetPacketDecrypted(_ context.Context, _ []byte) error { return nil }
func (s *stubDB) InsertObservation(_ context.Context, _ InsertObservationParams) (bool, error) {
	return s.observationInserted, nil
}
func (s *stubDB) SetNodeDefaultScope(_ context.Context, _ uuid.UUID, _ int32) error { return nil }
func (s *stubDB) UpsertNode(_ context.Context, _ UpsertNodeParams, _ RadioSettings) (uuid.UUID, error) {
	s.upsertNodeCalls++
	return uuid.Nil, nil
}
func (s *stubDB) UpsertNodeIATA(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (s *stubDB) UpsertNodeShortID(_ context.Context, _ uuid.UUID, _ string, _ []byte) error {
	return nil
}

func (s *stubDB) GetNodeByPubkey(_ context.Context, _ []byte) (uuid.UUID, error) {
	return uuid.Nil, errors.New("not found")
}

func (s *stubDB) GetNodesByIDs(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	return nil, nil
}

func (s *stubDB) InsertChannelMessage(_ context.Context, _ InsertChannelMessageParams) (bool, error) {
	return false, nil
}

func (s *stubDB) UpdateObserverStatus(_ context.Context, _ UpdateObserverStatusParams) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (s *stubDB) GetObserverLastIATA(_ context.Context, _ uuid.UUID) (string, error) { return "", nil }

func (s *stubDB) InsertObserverTelemetry(_ context.Context, _ uuid.UUID, _ time.Time, _ *int32, _, _ *float32, _ float32, _ int64, _, _, _ *int32) error {
	return nil
}

func (s *stubDB) GetObserverRadio(_ context.Context, _ uuid.UUID) (RadioSettings, error) {
	return RadioSettings{}, nil
}
func (s *stubDB) IsObserverByPubkey(_ context.Context, _ []byte) bool { return false }
func (s *stubDB) GetObserverScopes(_ context.Context, _ uuid.UUID) ([]string, error) {
	return nil, nil
}

func (s *stubDB) ResolvePathHashes(_ context.Context, _ string, _ [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	return nil, nil
}

func (s *stubDB) UpsertChannel(_ context.Context, _ []byte, _ []byte, _, _ string) (int, error) {
	s.upsertChannelCalls++
	return 0, nil
}

func (s *stubDB) UpsertChannelHashOnly(_ context.Context, _ []byte) (int, error) {
	s.upsertChannelHashOnlyCalls++
	return 0, nil
}

func (s *stubDB) UpsertChannelIATA(_ context.Context, _ []byte, _ string, _ time.Time) error {
	s.upsertChannelIATACalls++
	return nil
}

func (s *stubDB) UpsertTraceIATA(_ context.Context, _ []byte, _ string, _ time.Time) error {
	s.upsertTraceIATACalls++
	return nil
}

func (s *stubDB) GetPacketObservationCount(_ context.Context, _ []byte) (int64, error) {
	return 0, nil
}

func (s *stubDB) GetTransportScopeByName(_ context.Context, _ string) (int32, error) { return 0, nil }
func (s *stubDB) UpsertObserverScope(_ context.Context, _ uuid.UUID, _ int32) error  { return nil }
func (s *stubDB) UpsertKnownRoute(_ context.Context, _ []uuid.UUID, _ [][]byte, _ string, _ int32) error {
	return nil
}

func (s *stubDB) UpsertNodeNeighbor(_ context.Context, _, _ uuid.UUID, _ string, _ *float32) error {
	return nil
}

func newTestWorker() (*Worker, *stubDB) {
	db := &stubDB{}
	w := &Worker{
		cfg:    Config{BrokerName: "test"},
		db:     db,
		hub:    hub.New(),
		scopes: &stubScopes{},
		keys:   &stubKeys{},
	}
	return w, db
}

type stubScopes struct{}

func (s *stubScopes) Entries() []scopestore.Entry { return nil }

type stubKeys struct{}

func (s *stubKeys) GetKey(_ []byte) []keystore.Entry { return nil }

func TestRunCapabilityDetection_HashSizeOne_DoesNothing(t *testing.T) {
	w, db := newTestWorker()
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	w.runCapabilityDetection(context.Background(), 4, 1, []uuid.UUID{nodeID})
	if len(db.setCapabilityCalls) != 0 {
		t.Errorf("expected no capability calls for hashSize 1, got %d", len(db.setCapabilityCalls))
	}
}

func TestRunCapabilityDetection_NonTrace_HashSize2_SetsPaths(t *testing.T) {
	w, db := newTestWorker()
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	w.runCapabilityDetection(context.Background(), 4, 2, []uuid.UUID{nodeID})
	if len(db.setCapabilityCalls) != 1 {
		t.Fatalf("expected 1 capability call, got %d", len(db.setCapabilityCalls))
	}
	if !db.setCapabilityCalls[0].paths {
		t.Error("expected paths=true for non-trace hashSize 2")
	}
	if db.setCapabilityCalls[0].traces {
		t.Error("expected traces=false for non-trace hashSize 2")
	}
}

func TestRunCapabilityDetection_Trace_HashSize2_SetsTraces(t *testing.T) {
	w, db := newTestWorker()
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	w.runCapabilityDetection(context.Background(), 0x09, 2, []uuid.UUID{nodeID})
	if len(db.setCapabilityCalls) != 1 {
		t.Fatalf("expected 1 capability call, got %d", len(db.setCapabilityCalls))
	}
	if db.setCapabilityCalls[0].paths {
		t.Error("expected paths=false for trace hashSize 2")
	}
	if !db.setCapabilityCalls[0].traces {
		t.Error("expected traces=true for trace hashSize 2")
	}
}

func TestRunCapabilityDetection_MultipleNodes(t *testing.T) {
	w, db := newTestWorker()
	node1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	node2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	w.runCapabilityDetection(context.Background(), 4, 2, []uuid.UUID{node1, node2})
	if len(db.setCapabilityCalls) != 2 {
		t.Errorf("expected 2 capability calls, got %d", len(db.setCapabilityCalls))
	}
}

func TestRunCapabilityDetection_NoNodes_DoesNothing(t *testing.T) {
	w, db := newTestWorker()
	w.runCapabilityDetection(context.Background(), 4, 2, nil)
	if len(db.setCapabilityCalls) != 0 {
		t.Errorf("expected no capability calls for empty node list, got %d", len(db.setCapabilityCalls))
	}
}
