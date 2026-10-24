// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	// Message is nil when the packet already has a stored channel message.
	Message *InsertedChannelMessage
}

// DecryptGroupText attempts to decrypt a GRP_TXT payload against keys and, on success,
// upserts the channel and inserts the decrypted message. Shared by the live per-packet
// ingest path (side_effects.go) and DecryptBackfill below, so the two can't drift.
//
// A nil result with a nil error means no known key decrypted this packet -- the caller
// decides what to do with that. The live path falls back to UpsertChannelHashOnly itself;
// DecryptBackfill, which only scans packets already recorded that way, just leaves
// them as they are and tries again after the next boot.
func DecryptGroupText(ctx context.Context, db DB, keys ChannelKeyStore, packetHash, rawPayload []byte, historical bool) (*DecryptGroupTextResult, error) {
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
		Historical: historical,
		Content:    strings.ReplaceAll(strings.ToValidUTF8(payload.Text, "\uFFFD"), "\x00", ""),
	}
	newMsg, err := db.InsertChannelMessage(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("insert channel message: %w", err)
	}
	if newMsg != nil {
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
		Message:     newMsg,
	}, nil
}

// StoreGroupText decrypts and stores a live group-text observation's message (DecryptGroupText).
// Whenever this call did not create the message (no key, a decrypt or store error, a payload that does not decode,
// a message that already exists) it records the observation's site with AddChannelMessageIATA, unless siteRecorded
// says an earlier Add for this hearing already found the stored message; an Add error is logged and never returned.
func StoreGroupText(ctx context.Context, db DB, keys ChannelKeyStore, packetHash, rawPayload []byte, iata string, siteRecorded, historical bool) (*DecryptGroupTextResult, error) {
	result, err := DecryptGroupText(ctx, db, keys, packetHash, rawPayload, historical)
	if !siteRecorded && (err != nil || result == nil || result.Message == nil) {
		if _, addErr := db.AddChannelMessageIATA(ctx, packetHash, iata); addErr != nil {
			slog.Error(fmt.Sprintf("ingest: failed to record site %s for channel packet %s", iata, hex.EncodeToString(packetHash)), "component", "ingest", "error", addErr)
		}
	}
	return result, err
}

// decryptBackfillBatch is the packets one paced batch of a decrypt backfill tries.
const decryptBackfillBatch = 500

// Pacer spaces a backfill's batches: Wait sleeps after a batch that took elapsed and reports whether ctx is live.
type Pacer interface {
	Wait(ctx context.Context, elapsed time.Duration) bool
}

// backfill tries packets in batches of decryptBackfillBatch; pace, when set, waits after each batch. It stops
// early when ctx ends, leaving the rest undecrypted for the next run.
func backfill(ctx context.Context, db DB, keys ChannelKeyStore, packets []UndecryptedPacket, pace Pacer) int {
	decrypted := 0
	for start := 0; start < len(packets) && ctx.Err() == nil; start += decryptBackfillBatch {
		began := time.Now()
		for _, p := range packets[start:min(start+decryptBackfillBatch, len(packets))] {
			result, err := DecryptGroupText(ctx, db, keys, p.PacketHash, p.RawPayload, true)
			if err != nil {
				if ctx.Err() != nil {
					return decrypted
				}
				slog.Error(fmt.Sprintf("ingest: backfill: decrypt failed for packet %s", hex.EncodeToString(p.PacketHash)), "component", "ingest", "error", err)
				continue
			}
			if result != nil && result.Message != nil {
				decrypted++
			}
		}
		if pace != nil && start+decryptBackfillBatch < len(packets) && !pace.Wait(ctx, time.Since(began)) {
			return decrypted
		}
	}
	return decrypted
}

// decryptBackfillTimeout bounds one run; what it does not reach waits for the next run (the next boot or key import).
const decryptBackfillTimeout = 30 * time.Minute

// DecryptBackfill retries stored, undecrypted group text against the keystore off the boot path: all of it after
// boot (RequestAll), so a channel added to the config after its packets arrived does not stay undecrypted forever,
// and the channels of imported keys (RequestHashes). One goroutine (Run) does one run at a time,
// in paced batches; requests that arrive meanwhile are merged into the next run. It is resumable by construction:
// each decrypted packet is marked decrypted, and a run lists only packets that are not. Messages it stores are
// historical (their hours are re-rolled) and are not broadcast: they are history, not new activity. A packet heard live meanwhile is decrypted by the
// live path, which stores the message once (packet_hash is unique) and marks it historical too when the packet was
// stored before that hearing.
type DecryptBackfill struct {
	db   DB
	keys ChannelKeyStore
	pace Pacer

	mu     sync.Mutex
	all    bool
	hashes map[byte]struct{}
	wake   chan struct{}
}

// NewDecryptBackfill returns a backfill that paces its batches with pace (nil for none).
func NewDecryptBackfill(db DB, keys ChannelKeyStore, pace Pacer) *DecryptBackfill {
	return &DecryptBackfill{db: db, keys: keys, pace: pace, hashes: map[byte]struct{}{}, wake: make(chan struct{}, 1)}
}

// RequestAll queues a run over every undecrypted group text packet.
func (b *DecryptBackfill) RequestAll() {
	b.mu.Lock()
	b.all = true
	b.mu.Unlock()
	b.poke()
}

// RequestHashes queues a run over the undecrypted packets of the given one-byte channel hashes.
func (b *DecryptBackfill) RequestHashes(hashes [][]byte) {
	b.mu.Lock()
	for _, h := range hashes {
		if len(h) == 1 {
			b.hashes[h[0]] = struct{}{}
		}
	}
	b.mu.Unlock()
	b.poke()
}

func (b *DecryptBackfill) poke() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// take returns and clears the pending request; ok is false when there is none.
func (b *DecryptBackfill) take() (all bool, hashes [][]byte, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	all = b.all
	for h := range b.hashes {
		hashes = append(hashes, []byte{h})
	}
	b.all, b.hashes = false, map[byte]struct{}{}
	return all, hashes, all || len(hashes) > 0
}

// Run serves requests until ctx ends.
func (b *DecryptBackfill) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.wake:
		}
		for {
			all, hashes, ok := b.take()
			if !ok || ctx.Err() != nil {
				break
			}
			b.run(ctx, all, hashes)
		}
	}
}

// run does one run and returns the packets it decrypted.
func (b *DecryptBackfill) run(ctx context.Context, all bool, hashes [][]byte) int {
	ctx, cancel := context.WithTimeout(ctx, decryptBackfillTimeout)
	defer cancel()
	began := time.Now()
	var packets []UndecryptedPacket
	var err error
	if all {
		packets, err = b.db.ListUndecryptedGroupTextPackets(ctx)
	} else {
		packets, err = b.db.ListUndecryptedGroupTextPacketsByHash(ctx, hashes)
	}
	if err != nil {
		if ctx.Err() == nil || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			slog.Error("channel message backfill failed; it runs again after the next boot", "component", "ingest", "error", err)
		}
		return 0
	}
	slog.Info("channel message backfill started", "component", "ingest", "packets", len(packets), "all_channels", all)
	n := backfill(ctx, b.db, b.keys, packets, b.pace)
	attrs := []any{"component", "ingest", "decrypted", n, "packets", len(packets), "duration", time.Since(began).Round(time.Millisecond).String()}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		slog.Warn(fmt.Sprintf("channel message backfill stopped after %s; the rest runs again after the next boot", decryptBackfillTimeout), attrs...)
		return n
	}
	if ctx.Err() == nil {
		slog.Info(fmt.Sprintf("channel message backfill done: %d previously-undecrypted channel message(s)", n), attrs...)
	}
	return n
}
