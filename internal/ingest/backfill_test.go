// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/meshcore-go/meshcore-go"
)

// encryptedGroupText returns the raw GRP_TXT payload bytes for a message encrypted under psk
// -- a real round trip through meshcore-go's own Encrypt, not a hand-rolled fixture.
func encryptedGroupText(t *testing.T, channelHash byte, psk []byte, sender, text string) []byte {
	t.Helper()
	grpTxt, err := (&meshcore.GroupTextPayload{
		Timestamp: 1000,
		Sender:    sender,
		Text:      text,
	}).Encrypt(channelHash, psk)
	if err != nil {
		t.Fatalf("encrypt group text: %v", err)
	}
	raw, err := grpTxt.ToBytes()
	if err != nil {
		t.Fatalf("group text to bytes: %v", err)
	}
	return raw
}

func TestDecryptGroupText_Success(t *testing.T) {
	db := &stubDB{insertChannelMessageResult: true}
	psk := make([]byte, 16)
	channelHash := byte(0x11)
	keys := &mapKeys{entries: map[byte][]keystore.Entry{
		channelHash: {{Key: psk, Fingerprint: []byte{0xAA}, Name: "Public", Hashtag: "public"}},
	}}
	raw := encryptedGroupText(t, channelHash, psk, "ded", "hello")

	result, err := DecryptGroupText(context.Background(), db, keys, []byte{0x01}, raw, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected a non-nil result for a decryptable packet")
	}
	if result.Payload.Sender != "ded" || result.Payload.Text != "hello" {
		t.Errorf("expected decrypted sender=ded text=hello, got sender=%s text=%s", result.Payload.Sender, result.Payload.Text)
	}
	if result.Message == nil {
		t.Error("expected a message when InsertChannelMessage reports a new insert")
	}
	if db.upsertChannelCalls != 1 {
		t.Errorf("expected UpsertChannel to be called once, got %d", db.upsertChannelCalls)
	}
}

func TestDecryptGroupText_NoMatchingKey(t *testing.T) {
	db := &stubDB{}
	channelHash := byte(0x22)
	// Encrypted under a key the store doesn't have -- keys is otherwise empty for this hash.
	raw := encryptedGroupText(t, channelHash, make([]byte, 16), "ded", "hello")
	keys := &mapKeys{entries: map[byte][]keystore.Entry{}}

	result, err := DecryptGroupText(context.Background(), db, keys, []byte{0x02}, raw, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected a nil result when no known key decrypts the packet, got %+v", result)
	}
	if db.upsertChannelCalls != 0 {
		t.Errorf("expected UpsertChannel NOT to be called, got %d calls", db.upsertChannelCalls)
	}
}

func TestDecryptGroupText_WrongKeyForHash(t *testing.T) {
	db := &stubDB{}
	channelHash := byte(0x33)
	raw := encryptedGroupText(t, channelHash, make([]byte, 16), "ded", "hello")
	// A key IS registered for this hash, but it's the wrong one (1-byte hash collisions are
	// expected; DecryptStruct's MAC check is what actually distinguishes them).
	wrongKey := make([]byte, 16)
	wrongKey[0] = 0xFF
	keys := &mapKeys{entries: map[byte][]keystore.Entry{
		channelHash: {{Key: wrongKey, Fingerprint: []byte{0xBB}}},
	}}

	result, err := DecryptGroupText(context.Background(), db, keys, []byte{0x03}, raw, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected a nil result for a MAC mismatch, got %+v", result)
	}
}

func TestDecryptGroupText_MalformedPayload(t *testing.T) {
	db := &stubDB{}
	keys := &mapKeys{}
	_, err := DecryptGroupText(context.Background(), db, keys, []byte{0x04}, []byte{}, false)
	if err == nil {
		t.Fatal("expected an error for an empty/malformed payload")
	}
}

func TestBackfillChannelMessages_DecryptsAndCounts(t *testing.T) {
	psk := make([]byte, 16)
	channelHash := byte(0x44)
	decryptable := encryptedGroupText(t, channelHash, psk, "ded", "hi")
	stillUnknown := encryptedGroupText(t, byte(0x55), make([]byte, 16), "someone", "bye")

	db := &stubDB{
		insertChannelMessageResult: true,
		undecryptedPackets: []UndecryptedPacket{
			{PacketHash: []byte{0x01}, RawPayload: decryptable},
			{PacketHash: []byte{0x02}, RawPayload: stillUnknown},
		},
	}
	keys := &mapKeys{entries: map[byte][]keystore.Entry{
		channelHash: {{Key: psk, Fingerprint: []byte{0xAA}, Name: "Public"}},
	}}

	n, err := BackfillChannelMessages(context.Background(), db, keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 packet newly decrypted (the other has no known key yet), got %d", n)
	}
	if db.upsertChannelCalls != 1 {
		t.Errorf("expected UpsertChannel called once (only for the decryptable packet), got %d", db.upsertChannelCalls)
	}
}

func TestBackfillChannelHashes_OnlyScansNewHashes(t *testing.T) {
	psk := make([]byte, 16)
	db := &stubDB{
		insertChannelMessageResult: true,
		undecryptedByHash:          []UndecryptedPacket{{PacketHash: []byte{0x01}, RawPayload: encryptedGroupText(t, 0x44, psk, "ded", "hi")}},
		undecryptedPackets:         []UndecryptedPacket{{PacketHash: []byte{0x02}, RawPayload: encryptedGroupText(t, 0x44, psk, "x", "y")}},
	}
	keys := &mapKeys{entries: map[byte][]keystore.Entry{0x44: {{Key: psk, Fingerprint: []byte{0xAA}}}}}
	n, err := BackfillChannelHashes(context.Background(), db, keys, [][]byte{{0x44}})
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(db.backfillHashes) != 1 || db.backfillHashes[0][0] != 0x44 {
		t.Fatalf("scanned hashes %x", db.backfillHashes)
	}
}

func TestBackfillChannelMessages_NoUndecryptedPackets(t *testing.T) {
	db := &stubDB{}
	keys := &mapKeys{}
	n, err := BackfillChannelMessages(context.Background(), db, keys)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

func TestBackfillMarksMessagesHistorical(t *testing.T) {
	psk := make([]byte, 16)
	db := &stubDB{
		insertChannelMessageResult: true,
		undecryptedPackets:         []UndecryptedPacket{{PacketHash: []byte{0x01}, RawPayload: encryptedGroupText(t, 0x44, psk, "ded", "hi")}},
	}
	keys := &mapKeys{entries: map[byte][]keystore.Entry{0x44: {{Key: psk, Fingerprint: []byte{0xAA}}}}}
	if n, err := BackfillChannelMessages(context.Background(), db, keys); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(db.channelMessages) != 1 || !db.channelMessages[0].Historical {
		t.Errorf("backfilled messages %+v; want Historical so their rolled hours are re-rolled", db.channelMessages)
	}
	if _, err := DecryptGroupText(context.Background(), db, keys, []byte{0x02}, encryptedGroupText(t, 0x44, psk, "x", "live"), false); err != nil {
		t.Fatal(err)
	}
	if db.channelMessages[1].Historical {
		t.Error("live messages must not queue re-rolls")
	}
}
