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
)

type countQueryDB struct {
	*stubDB
	countQueries int
}

func (s *countQueryDB) GetPacketObservationCount(context.Context, []byte) (int64, error) {
	s.countQueries++
	return 1, nil
}

type repeatHarness struct {
	t      *testing.T
	ctx    context.Context
	w      *Worker
	db     *countQueryDB
	client *hub.Client
}

func newRepeatHarness(t *testing.T, includeRepeats bool) *repeatHarness {
	w, base := newTestWorker()
	db := &countQueryDB{stubDB: base}
	w.db = db
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
	if r.db.countQueries != 1 {
		t.Fatalf("first hearing ran %d count queries", r.db.countQueries)
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
	if r.db.countQueries != 1 {
		t.Fatalf("repeat ran a count query (%d total)", r.db.countQueries)
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
	if r.db.countQueries != 1 {
		t.Fatalf("duplicate ran a count query (%d total)", r.db.countQueries)
	}
}
