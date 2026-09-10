// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/lora"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

// frameCaptureDB records what handlePacket stored so a test can rebuild the
// frame from the persisted columns and compare it to the bytes off the wire.
type frameCaptureDB struct {
	*stubDB
	radio    RadioSettings
	packets  []UpsertPacketParams
	observed []InsertObservationParams
}

func (s *frameCaptureDB) UpsertPacket(_ context.Context, p UpsertPacketParams) (bool, error) {
	s.packets = append(s.packets, p)
	return true, nil
}

func (s *frameCaptureDB) InsertObservation(_ context.Context, o InsertObservationParams) (bool, error) {
	s.observed = append(s.observed, o)
	return true, nil
}

func (s *frameCaptureDB) GetObserverRadio(context.Context, uuid.UUID) (RadioSettings, error) {
	return s.radio, nil
}

// grpTxtPayload is a stand-in encrypted payload; its bytes never need decoding here.
func grpTxtPayload(t *testing.T) []byte {
	t.Helper()
	return buildGrpTxtPacket(t, 0x1a, make([]byte, 16)).Payload
}

// pathBytes returns n distinct hashes of hashSize bytes each.
func pathBytes(hashCount, hashSize int) []byte {
	path := make([]byte, 0, hashCount*hashSize)
	for i := range hashCount {
		for j := range hashSize {
			path = append(path, byte(0x10+i*hashSize+j))
		}
	}
	return path
}

// pathLengthByte packs hash size and hop count the way MeshCore does.
func pathLengthByte(hashCount, hashSize int) uint8 {
	return uint8(hashSize-1)<<6 | uint8(hashCount)
}

// TestFrameLengthReconstruction proves lora.FrameLength rebuilds the on-air byte
// count from the stored columns alone, across the path/transport-code shapes.
func TestFrameLengthReconstruction(t *testing.T) {
	for _, tc := range []struct {
		name               string
		packet             *meshcore.Packet
		wantTransportCodes bool
	}{
		{
			name: "flood with 1-byte hash path",
			packet: &meshcore.Packet{
				Header:     meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeGrpTxt, 0),
				PathLength: pathLengthByte(2, 1),
				Path:       pathBytes(2, 1),
				Payload:    grpTxtPayload(t),
			},
		},
		{
			name: "transport flood with transport codes",
			packet: &meshcore.Packet{
				Header:         meshcore.MakeHeader(meshcore.RouteTypeTransportFlood, meshcore.PayloadTypeGrpTxt, 0),
				PathLength:     pathLengthByte(1, 1),
				Path:           pathBytes(1, 1),
				Payload:        grpTxtPayload(t),
				TransportCode1: 0xbeef,
				TransportCode2: 0xf00d,
			},
			wantTransportCodes: true,
		},
		{
			name: "flood with 2-byte hash path",
			packet: &meshcore.Packet{
				Header:     meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeGrpTxt, 0),
				PathLength: pathLengthByte(3, 2),
				Path:       pathBytes(3, 2),
				Payload:    grpTxtPayload(t),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := tc.packet.ToBytes()
			if err != nil {
				t.Fatalf("packet to bytes: %v", err)
			}
			w, base := newTestWorker()
			db := &frameCaptureDB{stubDB: base, radio: RadioSettings{FreqMHz: 910.525, SF: 7, BWKHz: 62.5, CR: 5}}
			w.db = db

			w.handlePacket(context.Background(), "YOW", "0102", packetEnvelope(t, tc.packet))

			if len(db.packets) != 1 || len(db.observed) != 1 {
				t.Fatalf("stored %d packets and %d observations, want 1 of each", len(db.packets), len(db.observed))
			}
			up, obs := db.packets[0], db.observed[0]
			// Pin the captured shape: without this a lost transport-code capture would
			// quietly turn this case into the plain-flood one and still pass.
			if gotCodes := len(up.TransportCodes) == 4; gotCodes != tc.wantTransportCodes {
				t.Fatalf("captured %d transport-code bytes, want transport codes = %v", len(up.TransportCodes), tc.wantTransportCodes)
			}
			got := lora.FrameLength(len(up.TransportCodes) == 4, len(obs.PathBytes), len(up.RawPayload))
			if got != len(raw) {
				t.Errorf("FrameLength = %d, want %d (wire bytes)", got, len(raw))
			}
			if obs.AirtimeMs == nil {
				t.Error("AirtimeMs is nil, want a cost for SF7/62.5/CR5")
			}
		})
	}
}

// TestObservationAirtimeUnknownRadio covers the observer that never reported its
// radio: the columns are zero, so there is no airtime to store.
func TestObservationAirtimeUnknownRadio(t *testing.T) {
	w, base := newTestWorker()
	db := &frameCaptureDB{stubDB: base} // zero RadioSettings
	w.db = db

	w.handlePacket(context.Background(), "YOW", "0102", packetEnvelope(t, buildGrpTxtPacket(t, 0x1a, make([]byte, 16))))

	if len(db.observed) != 1 {
		t.Fatalf("stored %d observations, want 1", len(db.observed))
	}
	if got := db.observed[0].AirtimeMs; got != nil {
		t.Errorf("AirtimeMs = %v, want nil when the radio settings are unknown", *got)
	}
}
