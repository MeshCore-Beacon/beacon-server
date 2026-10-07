package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type traceSafetyDB struct {
	stubDB
	saved                          UpsertPacketParams
	resolutions, neighbors, routes int
}

func (s *traceSafetyDB) UpsertPacket(_ context.Context, p UpsertPacketParams) (bool, error) {
	s.saved = p
	return true, nil
}
func (s *traceSafetyDB) ResolveTracePathHashes(_ context.Context, _ string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) > 0 {
		s.resolutions++
	}
	out := map[string][]api.ResolvedPathEntry{}
	for i, h := range hashes {
		id := uuid.UUID{}
		id[15] = byte(i + 1)
		out[hex.EncodeToString(h)] = []api.ResolvedPathEntry{{NodeID: id}}
	}
	return out, nil
}
func (s *traceSafetyDB) UpsertNodeNeighbor(_ context.Context, _, _ uuid.UUID, _ string, _ *float32, _ *string) error {
	s.neighbors++
	return nil
}
func (s *traceSafetyDB) UpsertKnownRoute(_ context.Context, _ []uuid.UUID, _ [][]byte, _ string, _ int32) error {
	s.routes++
	return nil
}

func TestTraceQualityDoesNotPromoteMalformedOrUnvisitedHops(t *testing.T) {
	for _, bad := range []bool{false, true} {
		w, _ := newTestWorker()
		db := &traceSafetyDB{}
		w.db = db
		payload, _ := (&meshcore.Trace{Tag: 7, Flags: 1, PathHashes: []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}}).ToBytes()
		version := byte(0)
		if bad {
			version = 1
		}
		pkt := &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeDirect, meshcore.PayloadTypeTrace, version), PathLength: 2, Path: []byte{0, 252}, Payload: payload}
		w.handlePacket(context.Background(), "YKF", "0102", packetEnvelope(t, pkt))
		if len(db.saved.RawPayload) == 0 || len(db.traceHearings) != 1 {
			t.Fatal("raw evidence lost")
		}
		var parsed parsedTrace
		if err := json.Unmarshal(db.saved.ParsedPayload, &parsed); err != nil {
			t.Fatal(err)
		}
		if bad {
			if parsed.Quality.Status != "suspect" || db.neighbors != 0 || db.resolutions != 0 || len(db.setCapabilityCalls) != 0 {
				t.Fatalf("malformed trace promoted: %+v", db)
			}
		} else if parsed.Quality.Status != "supported" || db.neighbors != 1 || len(db.setCapabilityCalls) != 2 {
			t.Fatalf("valid consumed links/capabilities missing: %+v", db)
		}
		if db.routes != 0 {
			t.Fatal("planned route promoted as observed")
		}
	}
}
