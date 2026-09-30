// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/meshcore-go/meshcore-go"
)

func TestTransportScopeRegionalAndAmbiguousMatches(t *testing.T) {
	entry := scopestore.FromName("yow")
	entry.IATAs = []string{"YOW"}
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	// Fixed HMAC-SHA256 fixture: SHA256("#yow")[:16], payload type 4.
	if got := matchTransportScope([]scopestore.Entry{entry}, "YOW", 4, payload, 21698); got == nil || *got != "#yow" {
		t.Fatal(got)
	}
	if got := matchTransportScope([]scopestore.Entry{entry}, "YVR", 4, payload, 21698); got != nil {
		t.Fatal("imported key escaped source region")
	}
	entry.IATAs = nil
	if got := matchTransportScope([]scopestore.Entry{entry}, "YVR", 4, payload, 21698); got == nil {
		t.Fatal("manual key was region-restricted")
	}
	// Distinct keys with the same short code for this payload must remain unknown.
	a, b := scopestore.FromName("collision-320"), scopestore.FromName("collision-322")
	a.IATAs, b.IATAs = []string{"YOW"}, []string{"YOW"}
	for _, entries := range [][]scopestore.Entry{{a, b}, {b, a}} {
		if computeTransportCode(entries[0].TransportKey, 4, payload) != 37018 {
			t.Fatal("collision fixture changed")
		}
		if got := matchTransportScope(entries, "YOW", 4, payload, 37018); got != nil {
			t.Fatal("ambiguous short code resolved", *got)
		}
	}
}

type importCaptureDB struct{ *frameCaptureDB }

func (s *importCaptureDB) GetTransportScopeByName(context.Context, string) (int32, error) {
	return 123, nil
}

func TestImportedScopeFlowsThroughPacketIngest(t *testing.T) {
	for _, iata := range []string{"YOW", "YVR"} {
		t.Run(iata, func(t *testing.T) {
			w, base := newTestWorker()
			capture := &importCaptureDB{&frameCaptureDB{stubDB: base}}
			w.db = capture
			key := scopestore.FromName("yow")
			key.IATAs = []string{"YOW"}
			store := scopestore.New()
			store.Load([]scopestore.Entry{key})
			w.scopes = store
			packet := buildGrpTxtPacket(t, 0x1a, make([]byte, 16))
			packet.Header = meshcore.MakeHeader(meshcore.RouteTypeTransportFlood, meshcore.PayloadTypeGrpTxt, 0)
			packet.TransportCode1 = computeTransportCode(key.TransportKey, packet.PayloadType(), packet.Payload)
			w.handlePacket(context.Background(), iata, "0102", packetEnvelope(t, packet))
			if len(capture.packets) != 1 {
				t.Fatal("packet not stored")
			}
			got := capture.packets[0].ScopeID
			if iata == "YOW" && (got == nil || *got != 123) {
				t.Fatal("scope not persisted", got)
			}
			if iata == "YVR" && got != nil {
				t.Fatal("scope assigned outside source region", got)
			}
		})
	}
}

func BenchmarkTransportScopeCatalogue(b *testing.B) {
	for _, count := range []int{0, 64, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			entries := []scopestore.Entry{scopestore.FromName("manual")}
			for n := 0; n < count; n++ {
				entry := scopestore.FromName(fmt.Sprintf("scope-%d", n))
				entry.IATAs = []string{fmt.Sprintf("R%02d", n/64)}
				entries = append(entries, entry)
			}
			store := scopestore.New()
			store.Load(entries)
			payload := make([]byte, 128)
			b.ReportAllocs()
			for b.Loop() {
				matchTransportScope(store.Entries(), "R00", 4, payload, 1234)
			}
		})
	}
}

func TestManualScopeWinsTransportCodeCollision(t *testing.T) {
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	manual, imported := scopestore.FromName("collision-320"), scopestore.FromName("collision-322")
	imported.IATAs = []string{"YOW"}
	for _, entries := range [][]scopestore.Entry{{manual, imported}, {imported, manual}, {imported, scopestore.Entry{Name: "ambiguous", TransportKey: imported.TransportKey, IATAs: []string{"YOW"}}, manual}} {
		if got := matchTransportScope(entries, "YOW", 4, payload, 37018); got == nil || *got != manual.Name {
			t.Fatalf("manual match lost to imported collision: %v", got)
		}
	}
	imported.IATAs = nil
	for _, entries := range [][]scopestore.Entry{{manual, imported}, {imported, manual}} {
		if got := matchTransportScope(entries, "YOW", 4, payload, 37018); got == nil || *got != entries[0].Name {
			t.Fatalf("manual-only first-match behavior changed: %v", got)
		}
	}
}
