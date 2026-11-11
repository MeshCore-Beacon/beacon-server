// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"math"
	"slices"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
)

// Pages of any size walk a node's observations newest packet first, each once, and a cursor that is gone ends the list.
func TestNodeObservationPagesPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	// Packet 5 is old but heard again last, so it holds the newest observation id; 6 was never heard;
	// 3 and 4 share a first hearing.
	mustExec(t, ctx, tx, `
INSERT INTO observers(id,public_key) SELECT ('00000000-0000-0000-0000-00000000010' || i)::uuid, int4send(i) FROM generate_series(1,3) i;
INSERT INTO nodes(id,public_key,node_type,name) VALUES ('00000000-0000-0000-0000-0000000000a1','\xa1',2,'a'),
 ('00000000-0000-0000-0000-0000000000b1','\xb1',2,'noise'), ('00000000-0000-0000-0000-0000000000c1','\xc1',2,'quiet');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT hash,4,0,1,origin,'\x00','\x00',date_trunc('second',now())-mins*INTERVAL '1 minute',now() FROM (VALUES
 ('\x01'::bytea,'\xa1'::bytea,10), ('\x02','\xa1',20), ('\x03','\xa1',30), ('\x04','\xa1',30), ('\x05','\xa1',7200), ('\x06','\xa1',15),
 ('\x11','\xb1',5), ('\x12','\xb1',25), ('\x13',NULL,12)) v(hash,origin,mins);
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT id,hash,('00000000-0000-0000-0000-00000000010' || obs)::uuid,'YVR',now(),0,1,0 FROM (VALUES
 (1,'\x05'::bytea,1), (2,'\x03',1), (3,'\x04',1), (4,'\x12',1), (5,'\x02',1), (6,'\x03',2), (7,'\x04',2), (8,'\x13',1),
 (9,'\x01',1), (10,'\x11',1), (11,'\x01',2), (12,'\x11',2), (13,'\x01',3), (14,'\x12',3), (15,'\x05',2)) v(id,hash,obs)`)
	store := &Store{q: sqlc.New(tx)}
	node := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	want := []int64{13, 11, 9, 5, 7, 3, 6, 2, 15, 1}
	if oracle := queryInt64s(t, ctx, tx, `SELECT po.id FROM packet_observations po JOIN packets p USING (packet_hash)
		WHERE p.origin_pubkey = '\xa1' ORDER BY p.first_heard_at DESC, p.packet_hash DESC, po.id DESC`); !slices.Equal(oracle, want) {
		t.Fatalf("fixture order %v, want %v", oracle, want)
	}
	for _, limit := range []int32{1, 2, 3, 4, 9, 10, 11, 50} {
		var got []int64
		var cursor int64
		for page := 0; ; page++ {
			p, err := store.ListNodeObservations(ctx, node, cursor, limit)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range p.Items {
				got = append(got, item.ID)
			}
			if len(p.Items) > int(limit) || p.HasMore != (p.NextCursor != nil) || (p.HasMore && len(p.Items) != int(limit)) {
				t.Fatalf("limit %d page %d: %d items, hasMore %v, nextCursor %v", limit, page, len(p.Items), p.HasMore, p.NextCursor)
			}
			if !p.HasMore {
				break
			}
			if page > len(want) {
				t.Fatalf("limit %d: still paging after %d pages", limit, page)
			}
			cursor = *p.NextCursor
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d: got %v, want %v", limit, got, want)
		}
	}

	first, err := store.ListNodeObservations(ctx, node, 0, 2)
	if err != nil || first.NextCursor == nil || *first.NextCursor != 11 {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	if first.Items[0].PacketHash != "01" || first.Items[0].IATA != "YVR" || first.Items[0].PayloadType != 4 || first.Items[0].HopCount == nil {
		t.Fatalf("first item %+v", first.Items[0])
	}
	// A cursor on another node's observation continues from that packet's place among this node's.
	if p, err := store.ListNodeObservations(ctx, node, 4, 3); err != nil || len(p.Items) != 3 || p.Items[0].ID != 7 {
		t.Fatalf("foreign cursor: %+v, %v", p, err)
	}
	// Retention took the cursor's observation: the list ends there.
	mustExec(t, ctx, tx, `DELETE FROM packet_observations WHERE id = 11`)
	for _, gone := range []int64{11, -1, math.MaxInt64} {
		p, err := store.ListNodeObservations(ctx, node, gone, 2)
		if err != nil || p.Items == nil || len(p.Items) != 0 || p.HasMore || p.NextCursor != nil {
			t.Fatalf("cursor %d: %+v, %v", gone, p, err)
		}
	}
	for _, empty := range []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-0000000000c1"), uuid.New()} {
		p, err := store.ListNodeObservations(ctx, empty, 0, 50)
		if err != nil || p.Items == nil || len(p.Items) != 0 || p.HasMore || p.NextCursor != nil {
			t.Fatalf("node %s: %+v, %v", empty, p, err)
		}
	}
}
