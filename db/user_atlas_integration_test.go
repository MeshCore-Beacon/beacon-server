// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

func atlasKey(b byte) []byte { return []byte(strings.Repeat(string([]byte{b}), 32)) }

func TestUserAtlasPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "atlas")

	a, err := s.UserAtlas(ctx, u.ID)
	if err != nil || a.Revision != 0 || a.Range != "24h" || len(a.Pins) != 0 || a.Limit != 12 {
		t.Fatalf("empty: %+v %v", a, err)
	}

	var nodeID, obsID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO nodes (public_key, node_type) VALUES ($1, 2) RETURNING id`, atlasKey(1)).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO observers (public_key) VALUES ($1) RETURNING id`, atlasKey(1)).Scan(&obsID); err != nil {
		t.Fatal(err)
	}
	var obs2 uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO observers (public_key) VALUES ($1) RETURNING id`, atlasKey(2)).Scan(&obs2); err != nil {
		t.Fatal(err)
	}
	tiles := []string{"airtime", "battery", "types", "noise"}
	pins := []api.StoredPin{
		{Kind: "observer", Key: atlasKey(2), Name: "north", Tiles: tiles},
		{Kind: "node", Key: atlasKey(1), Name: "hill", Tiles: tiles, View: "heard"},
		{Kind: "node", Key: atlasKey(3), Name: "unknown"},
	}
	rev, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 0, "3d", pins)
	if err != nil || !ok || rev != 1 {
		t.Fatalf("first write: %d %v %v", rev, ok, err)
	}
	a, err = s.UserAtlas(ctx, u.ID)
	if err != nil || a.Revision != 1 || a.Range != "3d" || len(a.Pins) != 3 {
		t.Fatalf("read: %+v %v", a, err)
	}
	if a.Pins[0].Kind != "observer" || a.Pins[0].ObserverID == nil || *a.Pins[0].ObserverID != obs2.String() || len(a.Pins[0].Tiles) != 4 {
		t.Errorf("observer pin: %+v", a.Pins[0])
	}
	if p := a.Pins[1]; p.ID == nil || *p.ID != nodeID.String() || p.ObserverID == nil || *p.ObserverID != obsID.String() || p.View != "heard" {
		t.Errorf("node that is also an observer: %+v", p)
	}
	if p := a.Pins[2]; p.ID != nil || p.ObserverID != nil || p.Tiles != nil {
		t.Errorf("unknown node: %+v", p)
	}

	// A stale base changes nothing.
	if _, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 0, "24h", nil); err != nil || ok {
		t.Fatalf("stale: ok=%v err=%v", ok, err)
	}
	if b, _ := s.UserAtlas(ctx, u.ID); b.Revision != 1 || len(b.Pins) != 3 || b.Range != "3d" {
		t.Fatalf("stale write changed it: %+v", b)
	}

	// The observer's key gains a node row: the pin reads as a node pin in place.
	if _, err := pool.Exec(ctx, `INSERT INTO nodes (public_key, node_type) VALUES ($1, 2)`, atlasKey(2)); err != nil {
		t.Fatal(err)
	}
	a, _ = s.UserAtlas(ctx, u.ID)
	if a.Pins[0].Kind != "node" || a.Pins[0].PublicKey != hex.EncodeToString(atlasKey(2)) || a.Pins[0].ObserverID == nil || a.Pins[0].ID == nil {
		t.Errorf("upgrade: %+v", a.Pins[0])
	}
	// The node is already pinned as well: the two merge, the node pin keeps its place.
	if _, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 1, "24h", []api.StoredPin{
		{Kind: "node", Key: atlasKey(4), Name: "first"},
		{Kind: "observer", Key: atlasKey(2), Name: "obs"},
		{Kind: "node", Key: atlasKey(2), Name: "node"},
	}); err != nil || !ok {
		t.Fatal(err, ok)
	}
	a, _ = s.UserAtlas(ctx, u.ID)
	if len(a.Pins) != 2 || a.Pins[1].Kind != "node" || a.Pins[1].Name != "node" {
		t.Errorf("merge: %+v", a.Pins)
	}

	// Same key under two kinds is fine; the same kind twice is refused by the table.
	if _, _, err := s.ReplaceUserAtlas(ctx, u.ID, 2, "24h", []api.StoredPin{{Kind: "node", Key: atlasKey(1)}, {Kind: "node", Key: atlasKey(1)}}); err == nil {
		t.Error("duplicate pin was stored")
	}

	keys, err := s.ObserverKeysByIDs(ctx, []uuid.UUID{obs2, uuid.New()})
	if err != nil || len(keys) != 1 || hex.EncodeToString(keys[obs2]) != hex.EncodeToString(atlasKey(2)) {
		t.Fatalf("observer keys: %v %v", keys, err)
	}

	// A user that does not exist.
	if _, _, err := s.ReplaceUserAtlas(ctx, uuid.New(), 0, "24h", nil); !errors.Is(err, users.ErrNotFound) {
		t.Errorf("missing user: %v", err)
	}

	// Deleting the account removes the Atlas.
	if err := s.DeleteUser(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM user_atlas) + (SELECT count(*) FROM user_atlas_pins)`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("left behind: %d %v", n, err)
	}
}

func TestUserAtlasConcurrentWritersPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u := newUser(t, ctx, s, "racer")
	const writers = 20
	var wins, losses atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 0, "24h", []api.StoredPin{{Kind: "node", Key: atlasKey(byte(i + 1)), Name: "w"}})
			switch {
			case err != nil:
				t.Error(err)
			case ok:
				wins.Add(1)
			default:
				losses.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 || losses.Load() != writers-1 {
		t.Fatalf("wins=%d losses=%d", wins.Load(), losses.Load())
	}
	if a, _ := s.UserAtlas(ctx, u.ID); a.Revision != 1 || len(a.Pins) != 1 {
		t.Fatalf("after race: %+v", a)
	}
}
