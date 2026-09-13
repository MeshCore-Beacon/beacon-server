// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/meshcore-go/meshcore-go"
)

// UndecryptedPacket is a stored GRP_TXT packet that was never successfully decrypted --
// either because no key was known for its channel hash at ingest time, or because the keys
// that were known didn't match. See ListUndecryptedGroupTextPackets.
type UndecryptedPacket struct {
	PacketHash []byte
	RawPayload []byte
}

// DecryptGroupTextResult is the outcome of a successful DecryptGroupText call.
type DecryptGroupTextResult struct {
	Payload     *meshcore.GroupTextPayload
	ChannelID   int
	ChannelHash []byte
	Entry       keystore.Entry
	// NewMessage is false if InsertChannelMessage found the message already existed
	// (packet_hash is unique per message) -- e.g. re-running the backfill twice.
	NewMessage bool
}

// DecryptGroupText attempts to decrypt a GRP_TXT payload against keys and, on success,
// upserts the channel and inserts the decrypted message. Shared by the live per-packet
// ingest path (side_effects.go) and BackfillChannelMessages below, so the two can't drift.
//
// A nil result with a nil error means no known key decrypted this packet -- the caller
// decides what to do with that. The live path falls back to UpsertChannelHashOnly itself;
// BackfillChannelMessages, which only scans packets already recorded that way, just leaves
// them as they are and tries again next boot.
func DecryptGroupText(ctx context.Context, db DB, keys ChannelKeyStore, packetHash, rawPayload []byte) (*DecryptGroupTextResult, error) {
	grpTxt, err := meshcore.GroupTextFromBytes(rawPayload)
	if err != nil {
		return nil, err
	}
	channelHashBytes := []byte{grpTxt.ChannelHash}

	var payload *meshcore.GroupTextPayload
	var usedEntry keystore.Entry
	for _, entry := range keys.GetKey(channelHashBytes) {
		if p, err := grpTxt.DecryptStruct(entry.Key); err == nil {
			payload = p
			usedEntry = entry
			break
		}
	}
	if payload == nil {
		return nil, nil
	}

	channelID, err := db.UpsertChannel(ctx, channelHashBytes, usedEntry.Fingerprint, usedEntry.Name, usedEntry.Hashtag)
	if err != nil {
		return nil, fmt.Errorf("upsert channel: %w", err)
	}
	params := InsertChannelMessageParams{
		ChannelID:  channelID,
		PacketHash: packetHash,
		SenderName: strings.ReplaceAll(strings.ToValidUTF8(payload.Sender, "\uFFFD"), "\x00", ""),
		SentAt:     time.Unix(int64(payload.Timestamp), 0),
		Content:    strings.ReplaceAll(strings.ToValidUTF8(payload.Text, "\uFFFD"), "\x00", ""),
	}
	newMsg, err := db.InsertChannelMessage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("insert channel message: %w", err)
	}
	if newMsg {
		// Non-fatal: the message is stored either way, so just log and continue -- matches
		// the live ingest path's existing behavior of not treating this as a hard failure.
		if err := db.SetPacketDecrypted(ctx, packetHash); err != nil {
			slog.Error(fmt.Sprintf("ingest: failed to set packet decrypted for %s", hex.EncodeToString(packetHash)), "component", "ingest", "error", err)
		}
	}
	return &DecryptGroupTextResult{
		Payload:     payload,
		ChannelID:   channelID,
		ChannelHash: channelHashBytes,
		Entry:       usedEntry,
		NewMessage:  newMsg,
	}, nil
}

// BackfillChannelMessages scans packets already stored but never successfully decrypted and
// retries decryption against the current keystore. Meant to run once at boot, after the
// keystore is built from config: without this, a channel added to the config after its
// packets already arrived would sit undecrypted in the database forever, since nothing else
// ever re-processes old packets when a new key shows up.
//
// Returns the number of packets newly decrypted. Deliberately does not broadcast live WS
// channelMessage events for backfilled messages -- those are historical, not new activity,
// and broadcasting a burst of them on every boot would look like a flood of new messages to
// anyone connected at startup.
func BackfillChannelMessages(ctx context.Context, db DB, keys ChannelKeyStore) (int, error) {
	packets, err := db.ListUndecryptedGroupTextPackets(ctx)
	if err != nil {
		return 0, err
	}
	decrypted := 0
	for _, p := range packets {
		result, err := DecryptGroupText(ctx, db, keys, p.PacketHash, p.RawPayload)
		if err != nil {
			slog.Error(fmt.Sprintf("ingest: backfill: decrypt failed for packet %s", hex.EncodeToString(p.PacketHash)), "component", "ingest", "error", err)
			continue
		}
		if result != nil && result.NewMessage {
			decrypted++
		}
	}
	return decrypted, nil
}
