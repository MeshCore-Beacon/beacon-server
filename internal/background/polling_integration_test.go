// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
)

func TestPollingElectAllPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	store := db.New(pool, pool, 0, 0)
	if _, err := pool.Exec(ctx, `INSERT INTO iata_codes (iata) VALUES ('YVR')`); err != nil {
		t.Fatal(err)
	}
	key := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	target := key(0xA)
	o1, o2 := key(1), key(2)
	now := time.Now().UTC().Truncate(time.Second)
	ids := map[byte]uuid.UUID{}
	for _, o := range [][]byte{o1, o2} {
		id, err := store.ObserverID(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		ids[o[0]] = id
		claim := polling.Claim{Origin: "o", Observer: o, Timestamp: now, Client: "fw", PolicyVersion: "p",
			Nodes: []polling.ClaimNode{{Target: target, KeyMode: "own"}}}
		if ok, err := store.ReplacePollingClaim(ctx, claim, id, now); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	// Zero-hop adverts: the second observer hears the target better.
	for o, snr := range map[byte]float32{1: 2, 2: 9} {
		if _, err := pool.Exec(ctx, `WITH p AS (
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,$1,'\x00','\x00',$2,$2) RETURNING packet_hash)
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr)
SELECT packet_hash,$3,'YVR',$2,0,1,0,4,-90,$4 FROM p`, target, now, ids[o], snr); err != nil {
			t.Fatal(err)
		}
	}
	svc := polling.NewService(store, config.ResolvedPolling{Enabled: true}, time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	task := pollTask(t, PollingTasks(svc, NewPollingMaint(store), config.ResolvedPolling{}), "polling_election")
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	cur, err := store.CurrentPoller(ctx, target)
	if err != nil || !bytes.Equal(cur, o2) {
		t.Fatalf("rank 0 is %x, want the second observer (%v)", cur, err)
	}
}
