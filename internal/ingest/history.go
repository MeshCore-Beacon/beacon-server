// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/meshcore-go/meshcore-go"
)

type HistoryStore interface {
	DB
	LoadState(context.Context, string, any) (bool, error)
	SaveState(context.Context, string, any) error
	ListChannelHistoryPage(context.Context, []byte, int32) ([]UndecryptedPacket, error)
}

type historyState struct {
	Revision string `json:"revision"`
	After    string `json:"after"`
	Complete bool   `json:"complete"`
}

// ChannelHistory retries recorded history in bounded primary-key pages after
// startup. Live ingest decrypts new arrivals; a changed key set starts a new pass.
// A failed page never advances the saved cursor. Retried writes are idempotent.
type ChannelHistory struct {
	store HistoryStore
	keys  ChannelKeyStore
	mu    sync.Mutex
}

func NewChannelHistory(store HistoryStore, keys ChannelKeyStore) *ChannelHistory {
	return &ChannelHistory{store: store, keys: keys}
}

func historyKeyRevision(keys ChannelKeyStore) (string, bool) {
	var entries []string
	for i := range 256 {
		for _, entry := range keys.GetKey([]byte{byte(i)}) {
			key := sha256.Sum256(entry.Key)
			entries = append(entries, hex.EncodeToString(key[:]))
		}
	}
	sort.Strings(entries)
	h := sha256.New()
	for _, entry := range entries {
		_, _ = h.Write([]byte(entry))
	}
	return hex.EncodeToString(h.Sum(nil)), len(entries) > 0
}

func (h *ChannelHistory) Run(parent context.Context) error {
	if !h.mu.TryLock() {
		return nil
	}
	defer h.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	revision, hasKeys := historyKeyRevision(h.keys)
	var state historyState
	if _, err := h.store.LoadState(ctx, "channel_history", &state); err != nil {
		return err
	}
	if state.Revision != revision {
		state = historyState{Revision: revision}
		if !hasKeys {
			state.Complete = true
			return h.store.SaveState(ctx, "channel_history", state)
		}
	}
	if state.Complete {
		return nil
	}
	after, err := hex.DecodeString(state.After)
	if err != nil {
		return err
	}
	const pageSize = 500
	page, err := h.store.ListChannelHistoryPage(ctx, after, pageSize)
	if err != nil {
		return err
	}
	decrypted := 0
	for _, packet := range page {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(packet.RawPayload) > 0 {
			// Malformed historical packets are retained, but cannot strand the cursor.
			if _, err := meshcore.GroupTextFromBytes(packet.RawPayload); err == nil {
				result, err := DecryptGroupText(ctx, h.store, h.keys, packet.PacketHash, packet.RawPayload, true)
				if err != nil {
					return err
				}
				if result != nil && result.Message != nil {
					decrypted++
				}
			}
		}
		state.After = hex.EncodeToString(packet.PacketHash)
	}
	state.Complete = len(page) < pageSize
	if err := h.store.SaveState(ctx, "channel_history", state); err != nil {
		return err
	}
	metrics.JobRows.WithLabelValues("channel_history").Add(float64(decrypted))
	metrics.HistoryScanned.Add(float64(len(page)))
	return nil
}
