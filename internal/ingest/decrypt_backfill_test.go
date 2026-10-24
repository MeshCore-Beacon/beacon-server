// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
)

// resumableDB lists only the packets not yet marked decrypted, as the database does, and counts stored messages
// per packet.
type resumableDB struct {
	*stubDB
	mu        sync.Mutex
	packets   []UndecryptedPacket
	decrypted map[string]bool
	stored    map[string]int
	lists     int
	release   chan struct{} // when set, each list waits for it
}

func (d *resumableDB) ListUndecryptedGroupTextPackets(context.Context) ([]UndecryptedPacket, error) {
	if d.release != nil {
		<-d.release
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lists++
	var out []UndecryptedPacket
	for _, p := range d.packets {
		if !d.decrypted[string(p.PacketHash)] {
			out = append(out, p)
		}
	}
	return out, nil
}

func (d *resumableDB) InsertChannelMessage(_ context.Context, m InsertChannelMessageParams) (*InsertedChannelMessage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stored[string(m.PacketHash)]++
	if d.stored[string(m.PacketHash)] > 1 {
		return nil, nil
	}
	return &InsertedChannelMessage{ID: int64(len(d.stored)), ScopeStatus: "unavailable"}, nil
}

func (d *resumableDB) SetPacketDecrypted(_ context.Context, hash []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.decrypted[string(hash)] = true
	return nil
}

func (d *resumableDB) listed() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lists
}

func newResumableDB(t *testing.T, n int) (*resumableDB, *mapKeys) {
	psk := make([]byte, 16)
	raw := encryptedGroupText(t, 0x44, psk, "ded", "hi")
	d := &resumableDB{stubDB: &stubDB{}, decrypted: map[string]bool{}, stored: map[string]int{}}
	for i := range n {
		h := make([]byte, 8)
		binary.BigEndian.PutUint64(h, uint64(i+1))
		d.packets = append(d.packets, UndecryptedPacket{PacketHash: h, RawPayload: raw})
	}
	return d, &mapKeys{entries: map[byte][]keystore.Entry{0x44: {{Key: psk, Fingerprint: []byte{0xAA}}}}}
}

// pacerFunc paces with a function.
type pacerFunc func(ctx context.Context, elapsed time.Duration) bool

func (f pacerFunc) Wait(ctx context.Context, elapsed time.Duration) bool { return f(ctx, elapsed) }

// A run paces between its batches, not after the last.
func TestDecryptBackfillPacesBatches(t *testing.T) {
	d, keys := newResumableDB(t, 2*decryptBackfillBatch+200)
	waits := 0
	b := NewDecryptBackfill(d, keys, pacerFunc(func(context.Context, time.Duration) bool { waits++; return true }))
	if n := b.run(context.Background(), true, nil); n != len(d.packets) {
		t.Fatalf("decrypted %d of %d", n, len(d.packets))
	}
	if waits != 2 {
		t.Fatalf("%d pauses for three batches, want 2", waits)
	}
}

// A run stopped between batches (shutdown) leaves the rest undecrypted; the next run decrypts exactly the rest,
// and no packet's message is stored twice.
func TestDecryptBackfillStopsAndResumes(t *testing.T) {
	d, keys := newResumableDB(t, 2*decryptBackfillBatch+200)
	ctx, cancel := context.WithCancel(context.Background())
	stop := NewDecryptBackfill(d, keys, pacerFunc(func(context.Context, time.Duration) bool { cancel(); return false }))
	if n := stop.run(ctx, true, nil); n != decryptBackfillBatch {
		t.Fatalf("stopped run decrypted %d, want one batch, %d", n, decryptBackfillBatch)
	}
	if n := NewDecryptBackfill(d, keys, nil).run(context.Background(), true, nil); n != len(d.packets)-decryptBackfillBatch {
		t.Fatalf("resumed run decrypted %d, want the other %d", n, len(d.packets)-decryptBackfillBatch)
	}
	for _, p := range d.packets {
		if d.stored[string(p.PacketHash)] != 1 {
			t.Fatalf("packet %x stored %d times", p.PacketHash, d.stored[string(p.PacketHash)])
		}
	}
}

// Run serves requests one run at a time: requests made during a run merge into one more run, and a request for
// every channel covers one for some.
func TestDecryptBackfillMergesRequests(t *testing.T) {
	d, keys := newResumableDB(t, 3)
	d.release = make(chan struct{})
	b := NewDecryptBackfill(d, keys, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()
	b.RequestAll() // returns at once: the caller (boot) never waits for the database
	d.release <- struct{}{}
	b.RequestHashes([][]byte{{0x44}})
	b.RequestAll()
	b.RequestAll()
	d.release <- struct{}{}
	deadline := time.Now().Add(5 * time.Second)
	for d.listed() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if d.listed() != 2 || d.backfillHashes != nil {
		t.Fatalf("%d full runs and hash runs %x; want the boot run and one merged run", d.listed(), d.backfillHashes)
	}
	if len(d.stored) != 3 {
		t.Fatalf("stored %d messages, want 3", len(d.stored))
	}
}

// A live hearing that decrypts a packet stored before it (its key arrived since, or the background backfill has
// not reached it) stores the message as historical, as the backfill would, so its earlier hours are re-rolled; the
// first hearing of a new packet stays live.
func TestLiveDecryptOfStoredPacketIsHistorical(t *testing.T) {
	psk := make([]byte, 16)
	body := packetEnvelope(t, buildGrpTxtPacket(t, 0x1a, psk))
	for _, known := range []bool{false, true} {
		w, db := newTestWorker()
		db.packetIsNew, db.observationInserted, db.insertChannelMessageResult = !known, true, true
		w.keys = &mapKeys{entries: map[byte][]keystore.Entry{0x1a: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}}
		w.handleMessage(queuedTestMessage{malformedTopicMessage{"meshcore/YOW/" + strings.Repeat("AB", 32) + "/packets"}, body})
		if len(db.channelMessages) != 1 || db.channelMessages[0].Historical != known {
			t.Fatalf("packet stored before: %v; messages %+v, want one with Historical %v", known, db.channelMessages, known)
		}
	}
}
