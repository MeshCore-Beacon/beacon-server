// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/google/uuid"
)

type repeatHarness struct {
	t      *testing.T
	ctx    context.Context
	w      *Worker
	db     *stubDB
	client *hub.Client
}

func newRepeatHarness(t *testing.T, includeRepeats bool) *repeatHarness {
	w, db := newTestWorker()
	db.observationCount = 3
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	go w.hub.Run()
	client := w.hub.NewClient()
	t.Cleanup(func() { w.hub.Remove(client) })
	w.hub.AddScope(client, "all", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation, hub.EventObserverStatus}})
	if includeRepeats {
		w.hub.Configure(client, hub.ClientOptions{IncludeRepeats: true})
		for !w.hub.RepeatsWanted() {
			time.Sleep(time.Millisecond)
		}
	}
	waitForSummarySubscriber(t, ctx, w.hub, client)
	return &repeatHarness{t: t, ctx: ctx, w: w, db: db, client: client}
}

// hear feeds one hearing of the same group text over path and returns the packetObservation
// payloads it produced.
func (r *repeatHarness) hear(inserted bool, path ...byte) []json.RawMessage {
	r.t.Helper()
	packet := buildGrpTxtPacket(r.t, 0x1a, make([]byte, 16))
	packet.Path = path
	packet.PathLength = byte(len(path))
	r.db.observationInserted = inserted
	r.w.handlePacket(r.ctx, "YOW", "0102", packetEnvelope(r.t, packet))

	// Hub delivery is FIFO, so everything handlePacket sent arrives before this marker.
	r.w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus})
	var got []json.RawMessage
	for {
		select {
		case evt := <-r.client.Send:
			if evt.Type == hub.EventObserverStatus {
				return got
			}
			got = append(got, evt.Payload)
		case <-r.ctx.Done():
			r.t.Fatal("marker event not delivered")
		}
	}
}

func packetFields(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var evt struct {
		Packet map[string]json.RawMessage `json:"packet"`
	}
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatal(err)
	}
	return evt.Packet
}

func TestHandlePacket_Repeats(t *testing.T) {
	r := newRepeatHarness(t, true)

	first := r.hear(true, 0x11)
	if len(first) != 1 || strings.Contains(string(first[0]), "isRepeat") {
		t.Fatalf("first hearing: %s", first)
	}
	if got := string(packetFields(t, first[0])["observationCount"]); got != "3" {
		t.Fatalf("first hearing observationCount = %s, want the count InsertObservation returned", got)
	}
	if got := r.hear(false, 0x11); len(got) != 0 {
		t.Fatalf("broker copy of the first hearing sent: %s", got)
	}

	repeat := r.hear(false, 0x22, 0x11)
	if len(repeat) != 1 {
		t.Fatalf("new path sent %d events", len(repeat))
	}
	fields := packetFields(t, repeat[0])
	if string(fields["isRepeat"]) != "true" || string(fields["isFirstObservation"]) != "false" || string(fields["observationCount"]) != "0" {
		t.Fatalf("repeat packet fields: %v", fields)
	}
	if got := r.hear(false, 0x22, 0x11); len(got) != 0 {
		t.Fatalf("same path sent twice: %s", got)
	}
}

func TestHandlePacket_RepeatsOffByDefault(t *testing.T) {
	r := newRepeatHarness(t, false)
	if got := r.hear(true, 0x11); len(got) != 1 || strings.Contains(string(got[0]), "isRepeat") {
		t.Fatalf("first hearing: %s", got)
	}
	if got := r.hear(false, 0x22, 0x11); len(got) != 0 {
		t.Fatalf("repeat sent with nobody opted in: %s", got)
	}
}

// A broker copy racing through the inserted hearing's DB work must find its path already
// recorded, otherwise it would be streamed as a repeat of the first hearing.
func TestHandlePacket_PathMarkedRightAfterInsert(t *testing.T) {
	r := newRepeatHarness(t, true)
	packet := buildGrpTxtPacket(t, 0x1a, make([]byte, 16))
	packet.Path = []byte{0x11}
	packet.PathLength = 1
	hash := packet.PacketHash()
	var repeat bool
	r.db.dbHook = func() {
		// stubDB.UpsertObserver returns uuid.Nil, so that is the observer id ingest marks with
		repeat = r.w.hub.MarkSent(hash[:], uuid.Nil[:], []byte{0x22})
	}
	r.db.observationInserted = true
	r.w.handlePacket(r.ctx, "YOW", "0102", packetEnvelope(r.t, packet))
	if !repeat {
		t.Fatal("path was not marked right after the insert")
	}
}

// A broker copy can lose the insert race yet mark its path first; it is still the first hearing.
func TestHandlePacket_BrokerCopyMarkedFirstIsNotARepeat(t *testing.T) {
	r := newRepeatHarness(t, true)
	if got := r.hear(false, 0x11); len(got) != 0 {
		t.Fatalf("broker copy streamed as a repeat: %s", got)
	}
	if got := r.hear(true, 0x11); len(got) != 1 || strings.Contains(string(got[0]), "isRepeat") {
		t.Fatalf("inserted hearing: %s", got)
	}
	if got := r.hear(false, 0x22, 0x11); len(got) != 1 {
		t.Fatalf("later path sent %d events", len(got))
	}
}

func TestHandlePacket_PathsRecordedWithoutSubscribers(t *testing.T) {
	r := newRepeatHarness(t, false)
	if got := r.hear(true, 0x11); len(got) != 1 {
		t.Fatalf("first hearing: %s", got)
	}
	r.w.hub.Configure(r.client, hub.ClientOptions{IncludeRepeats: true})
	for !r.w.hub.RepeatsWanted() {
		if r.ctx.Err() != nil {
			t.Fatal("repeats opt-in never registered")
		}
		time.Sleep(time.Millisecond)
	}
	if got := r.hear(false, 0x11); len(got) != 0 {
		t.Fatalf("same path streamed as repeat after late opt-in: %s", got)
	}
}

func TestHandlePacket_TraceNeverRepeats(t *testing.T) {
	r := newRepeatHarness(t, true)
	packet := buildTracePacket(t)
	packet.Path = []byte{0x05, 0x06}
	packet.PathLength = 2
	r.db.observationInserted = true
	r.w.handlePacket(r.ctx, "YOW", "0102", packetEnvelope(r.t, packet))
	r.db.observationInserted = false
	packet.Path = append([]byte{}, packet.Path...)
	packet.Path[0] ^= 0xff // a different per-hop SNR byte, same route
	r.w.handlePacket(r.ctx, "YOW", "0102", packetEnvelope(r.t, packet))
	r.w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus})
	repeats := 0
	for {
		select {
		case evt := <-r.client.Send:
			if evt.Type == hub.EventObserverStatus {
				if repeats != 0 {
					t.Fatalf("TRACE copy streamed as %d repeat(s)", repeats)
				}
				return
			}
			if strings.Contains(string(evt.Payload), "isRepeat") {
				repeats++
			}
		case <-r.ctx.Done():
			t.Fatal("marker event not delivered")
		}
	}
}
