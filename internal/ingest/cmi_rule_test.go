// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/meshcore-go/meshcore-go"
)

// A live group-text hearing records its site unless this call created the message, whatever the decrypt result.
func TestStoreGroupTextAddsSiteUnlessCreated(t *testing.T) {
	psk := make([]byte, 16)
	payload := encryptedGroupText(t, 0x11, psk, "Fixture", "test")
	keyed := &mapKeys{entries: map[byte][]keystore.Entry{0x11: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}}
	hash := []byte{0xab, 0xcd}
	cases := []struct {
		name    string
		keys    ChannelKeyStore
		payload []byte
		setup   func(*stubDB)
		adds    int
		result  bool
		err     bool
	}{
		{name: "created", keys: keyed, payload: payload, setup: func(s *stubDB) { s.insertChannelMessageResult = true }, result: true},
		{name: "duplicate", keys: keyed, payload: payload, adds: 1, result: true},
		{name: "no key", keys: &mapKeys{}, payload: payload, adds: 1},
		{name: "upsert channel error", keys: keyed, payload: payload, setup: func(s *stubDB) { s.upsertChannelErr = errors.New("boom") }, adds: 1, err: true},
		{name: "payload does not decode", keys: keyed, payload: []byte{0x11}, adds: 1, err: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := &stubDB{}
			if c.setup != nil {
				c.setup(db)
			}
			result, err := StoreGroupText(context.Background(), db, c.keys, hash, c.payload, "YOW")
			if (err != nil) != c.err || (result != nil) != c.result {
				t.Fatalf("result %+v, err %v", result, err)
			}
			want := slices.Repeat([]addCall{{string(hash), "YOW"}}, c.adds)
			if !slices.Equal(db.addCalls, want) {
				t.Fatalf("adds = %v, want %v", db.addCalls, want)
			}
		})
	}
}

// An Add error is logged only: the new message is still broadcast and a keyless hearing still records its channel.
func TestGroupTextAddErrorStillBroadcasts(t *testing.T) {
	w, db := newTestWorker()
	db.addErr = errors.New("add failed")
	psk := make([]byte, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go w.hub.Run()
	client := w.hub.NewClient()
	defer w.hub.Remove(client)
	w.hub.AddScope(client, "scope", hub.Scope{Events: []hub.EventType{hub.EventChannelMessage, hub.EventObserverStatus}})
	waitForSummarySubscriber(t, ctx, w.hub, client)
	packet := &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeGrpTxt, 0), Payload: encryptedGroupText(t, 0x11, psk, "Fixture", "test")}

	// No key: the Add fails, the hash-only channel is still recorded.
	w.handlePayloadTypeSideEffects(ctx, packet, "YOW", []byte{1}, RadioSettings{}, nil, nil, nil, 0)
	if db.upsertChannelHashOnlyCalls != 1 || len(db.addCalls) != 1 {
		t.Fatalf("hash-only upserts %d, adds %d", db.upsertChannelHashOnlyCalls, len(db.addCalls))
	}

	// A duplicate with a key: the Add fails, nothing is broadcast, nothing panics.
	w.keys = &mapKeys{entries: map[byte][]keystore.Entry{0x11: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}}
	w.handlePayloadTypeSideEffects(ctx, packet, "YUL", []byte{2}, RadioSettings{}, nil, nil, nil, 0)
	if len(db.addCalls) != 2 {
		t.Fatalf("adds = %v", db.addCalls)
	}

	// A new message: no Add, and the event is broadcast.
	db.insertChannelMessageResult = true
	w.handlePayloadTypeSideEffects(ctx, packet, "YYZ", []byte{3}, RadioSettings{}, nil, nil, nil, 0)
	select {
	case event := <-client.Send:
		var got channelMessageEvent
		if err := json.Unmarshal(event.Payload, &got); err != nil {
			t.Fatal(err)
		}
		if got.ID != 1 || got.Content != "test" || got.ChannelHash != "11" {
			t.Fatalf("event = %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("no channelMessage event")
	}
	if len(db.addCalls) != 2 {
		t.Fatalf("a created message added its site: %v", db.addCalls)
	}
}

// A stored group-text hearing records its site right after the observation insert, before any other DB call, so a
// message that already exists gains the site at once; a duplicate hearing records nothing.
func TestGroupTextObservationAddsSiteAtOnce(t *testing.T) {
	w, db := newTestWorker()
	db.observationInserted = true
	adds := -1
	db.dbHook = func() { adds = len(db.addCalls) }
	packet := buildGrpTxtPacket(t, 0x1a, make([]byte, 16))
	w.handlePacket(context.Background(), "YOW", "0102", packetEnvelope(t, packet))
	if adds != 1 || db.addCalls[0].iata != "YOW" {
		t.Fatalf("adds before the next DB call = %d (%v), want 1 at YOW", adds, db.addCalls)
	}

	w, db = newTestWorker()
	db.observationInserted = false
	w.handlePacket(context.Background(), "YOW", "0102", packetEnvelope(t, packet))
	if len(db.addCalls) != 0 {
		t.Fatalf("a duplicate hearing added %v", db.addCalls)
	}
}
