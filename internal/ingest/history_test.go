// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
)

type historyTestStore struct {
	DB
	state []byte
	fail  bool
	calls int
	after []byte
}

func (s *historyTestStore) LoadState(_ context.Context, _ string, v any) (bool, error) {
	if s.state == nil {
		return false, nil
	}
	return true, json.Unmarshal(s.state, v)
}
func (s *historyTestStore) SaveState(_ context.Context, _ string, v any) error {
	var err error
	s.state, err = json.Marshal(v)
	return err
}
func (s *historyTestStore) ListChannelHistoryPage(_ context.Context, after []byte, n int32) ([]UndecryptedPacket, error) {
	s.calls++
	s.after = append([]byte(nil), after...)
	if n != 500 {
		panic("unbounded history page")
	}
	if s.fail {
		return nil, errors.New("temporary read failure")
	}
	return []UndecryptedPacket{{PacketHash: []byte{7}}}, nil
}
func TestChannelHistoryResumesAndRevisitsChangedKeys(t *testing.T) {
	keys := keystore.NewMapKeyStore(map[string][]keystore.Entry{"11": {{Key: []byte{1}}}})
	s := &historyTestStore{fail: true}
	h := NewChannelHistory(s, keys)
	if h.Run(context.Background()) == nil || s.state != nil {
		t.Fatal("failed page advanced progress")
	}
	s.fail = false
	if err := h.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := NewChannelHistory(s, keys).Run(context.Background()); err != nil || s.calls != 2 {
		t.Fatal("completed pass was not restored")
	}
	keys.SetImported([]keystore.Entry{{Key: []byte{2}}})
	if err := h.Run(context.Background()); err != nil || s.calls != 3 || len(s.after) != 0 {
		t.Fatal("new key did not restart historical scan")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h.Run(ctx) == nil || s.calls != 3 {
		t.Fatal("canceled job accessed database")
	}
}
