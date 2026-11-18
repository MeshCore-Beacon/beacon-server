// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

func atlasKey(b byte) []byte { return []byte(strings.Repeat(string([]byte{b}), 32)) }

func TestUserAtlasPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "atlas")

	// The store leaves Limit unset; the handler fills it from config.
	a, err := s.UserAtlas(ctx, u.ID)
	if err != nil || a.Revision != 0 || a.Range != "24h" || len(a.Pins) != 0 || a.Limit != 0 {
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
		{Kind: "node", Key: atlasKey(1), Name: "hill", Tiles: tiles, View: "telemetry", TelemetryTiles: []string{"battery", "noise", "uptime", "packets"}},
		{Kind: "node", Key: atlasKey(3), Name: "unknown"},
	}
	doc, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 0, "3d", pins)
	if err != nil || !ok || doc.Revision != 1 || doc.Range != "3d" || len(doc.Pins) != 3 {
		t.Fatalf("first write: %+v %v %v", doc, ok, err)
	}
	a, err = s.UserAtlas(ctx, u.ID)
	if err != nil || a.Revision != 1 || a.Range != "3d" || len(a.Pins) != 3 {
		t.Fatalf("read: %+v %v", a, err)
	}
	if a.Pins[0].Kind != "observer" || a.Pins[0].ObserverID == nil || *a.Pins[0].ObserverID != obs2.String() || len(a.Pins[0].Tiles) != 4 {
		t.Errorf("observer pin: %+v", a.Pins[0])
	}
	if p := a.Pins[1]; p.ID == nil || *p.ID != nodeID.String() || p.ObserverID == nil || *p.ObserverID != obsID.String() || p.View != "telemetry" || strings.Join(p.TelemetryTiles, ",") != "battery,noise,uptime,packets" || strings.Join(p.Tiles, ",") != strings.Join(tiles, ",") {
		t.Errorf("node that is also an observer: %+v", p)
	}
	if p := a.Pins[2]; p.ID != nil || p.ObserverID != nil || p.Tiles != nil || p.TelemetryTiles != nil {
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

// atlasGap pauses an Atlas read after its header and pins statement.
type atlasGap struct {
	sqlc.Querier
	arrived chan struct{}
	release chan struct{}
}

func (g atlasGap) ReadUserAtlas(ctx context.Context, id uuid.UUID) ([]sqlc.ReadUserAtlasRow, error) {
	r, err := g.Querier.ReadUserAtlas(ctx, id)
	g.arrived <- struct{}{}
	<-g.release
	return r, err
}

// A write that lands in the middle of a read must not give the reader one revision with the other's pins.
func TestUserAtlasReadIsOneSnapshotPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u := newUser(t, ctx, s, "snap")
	if _, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 0, "24h", []api.StoredPin{{Kind: "node", Key: atlasKey(1), Name: "old"}}); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	g := atlasGap{Querier: s.q, arrived: make(chan struct{}, 1), release: make(chan struct{})}
	s.q = g
	type res struct {
		a   api.Atlas
		err error
	}
	done := make(chan res, 1)
	go func() {
		a, err := s.UserAtlas(ctx, u.ID)
		done <- res{a, err}
	}()
	select {
	case <-g.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the read never reached the gap")
	}
	if _, ok, err := s.ReplaceUserAtlas(ctx, u.ID, 1, "3d", []api.StoredPin{{Kind: "node", Key: atlasKey(2), Name: "new"}}); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	close(g.release)
	r := <-done
	if r.err != nil || len(r.a.Pins) != 1 {
		t.Fatalf("%+v %v", r.a, r.err)
	}
	if got, want := r.a.Pins[0].Name, map[int64]string{1: "old", 2: "new"}[r.a.Revision]; got != want || (r.a.Revision == 1) != (r.a.Range == "24h") {
		t.Fatalf("revision %d range %s with pin %q", r.a.Revision, r.a.Range, got)
	}
}

// A pin stored before the Telemetry view existed reads back unchanged, and the new view value is accepted.
func TestUserAtlasLegacyPinPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "legacy")
	if _, err := pool.Exec(ctx, `INSERT INTO user_atlas (user_id, revision) VALUES ($1, 1)`, u.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO user_atlas_pins (user_id, position, kind, public_key, name, tiles, view)
		VALUES ($1, 0, 'node', $2, 'old', ARRAY['airtime','battery','types','noise'], 'observer')`, u.ID, atlasKey(1)); err != nil {
		t.Fatal(err)
	}
	a, err := s.UserAtlas(ctx, u.ID)
	if err != nil || len(a.Pins) != 1 || a.Pins[0].View != "observer" || a.Pins[0].TelemetryTiles != nil || len(a.Pins[0].Tiles) != 4 {
		t.Fatalf("legacy pin: %+v %v", a, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE user_atlas_pins SET view = 'bogus' WHERE user_id = $1`, u.ID); err == nil {
		t.Error("an unknown view is still refused")
	}
}
