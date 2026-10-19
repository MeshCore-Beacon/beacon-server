// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

// Packets 1..2,100 of types 2, 4, 5 and 9 (packet i has type [2,4,5,9][i%4]), one observation each.
// The sequence is set first, so the 2,020 observations take contiguous ids 2,147,483,641 onward and
// cross 2^31. Every third of the first 2,000 and all of the last 20 carry no payload type; the rest
// carry their packet's, except the first, which holds 5 while its packet is 4.
const payloadTypeBackfillFixture = `
INSERT INTO iata_codes(iata) VALUES ('YOW');
INSERT INTO observers(id,public_key) VALUES ('00000000-0000-0000-0000-0000000000a1','\xa1');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT int4send(i),(ARRAY[2,4,5,9])[1 + i % 4],0,1,'\x00','\x00',now(),now() FROM generate_series(1,2100) i;
SELECT setval('packet_observations_id_seq', 2147483640);
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type)
SELECT p.packet_hash,'00000000-0000-0000-0000-0000000000a1','YOW','2026-01-01'::timestamptz+i*interval '1 second',0,1,0,
 CASE WHEN i = 1 THEN 5 WHEN i % 3 = 0 OR i > 2000 THEN NULL ELSE p.payload_type END
FROM generate_series(1,2020) i JOIN packets p ON p.packet_hash = int4send(i) ORDER BY i;`

// backfillFromRecorder is the real store; it records the fromID and rows typed of every batch.
type backfillFromRecorder struct {
	*db.Store
	from  []int64
	typed []int64
}

func (r *backfillFromRecorder) BackfillObservationPayloadTypes(ctx context.Context, fromID, toID int64, maxRows int32) (int64, int64, error) {
	r.from = append(r.from, fromID)
	n, next, err := r.Store.BackfillObservationPayloadTypes(ctx, fromID, toID, maxRows)
	r.typed = append(r.typed, n)
	return n, next, err
}

// TestPayloadTypeBackfillPostgres drives the task on a real store through ingest and retention
// between runs, then checks it typed exactly the untyped rows, and that it resumes after a crash.
func TestPayloadTypeBackfillPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	store := db.New(pool, pool, 0, 0)
	if _, err := pool.Exec(ctx, payloadTypeBackfillFixture); err != nil {
		t.Fatal(err)
	}
	const (
		first    int64 = 2_147_483_641
		mismatch       = first // holds 5; its packet is 4
		span     int64 = 500
		maxRows  int32 = 50_000
	)

	count := func(query string, args ...any) int64 {
		t.Helper()
		var n int64
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	idList := func(query string, args ...any) []int64 {
		t.Helper()
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	state := func() db.ObservationPayloadTypeState {
		t.Helper()
		var st db.ObservationPayloadTypeState
		if ok, err := store.LoadState(ctx, db.ObservationPayloadTypeKey, &st); err != nil || !ok {
			t.Fatalf("state found %v, err %v", ok, err)
		}
		return st
	}
	xmins := func() map[int64]string {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT id, xmin::text FROM packet_observations`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[int64]string{}
		for rows.Next() {
			var id int64
			var x string
			if err := rows.Scan(&id, &x); err != nil {
				t.Fatal(err)
			}
			out[id] = x
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	// runUntilDone runs task, calling between after the first run, and returns the runs taken.
	runUntilDone := func(task Task, between func()) int {
		t.Helper()
		for i := 1; i <= 20; i++ {
			if err := task.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if i == 1 && between != nil {
				between()
			}
			if state().Done {
				return i
			}
		}
		t.Fatal("not done after 20 runs")
		return 0
	}
	// typedFromPacket checks no row is untyped and every row but the mismatch carries its packet's type.
	typedFromPacket := func(when string) {
		t.Helper()
		if n := count(`SELECT count(*) FROM packet_observations WHERE payload_type IS NULL`); n != 0 {
			t.Fatalf("%s: %d rows still untyped", when, n)
		}
		wrong := idList(`SELECT po.id FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.payload_type IS DISTINCT FROM p.payload_type ORDER BY po.id`)
		if len(wrong) != 1 || wrong[0] != mismatch {
			t.Fatalf("%s: rows differing from their packet %v, want only %d", when, wrong, mismatch)
		}
		if n := count(`SELECT payload_type FROM packet_observations WHERE id = $1`, mismatch); n != 5 {
			t.Fatalf("%s: the typed row that differs from its packet now holds %d, want 5", when, n)
		}
	}

	untyped := idList(`SELECT id FROM packet_observations WHERE payload_type IS NULL ORDER BY id`)
	if len(untyped) != 686 || untyped[len(untyped)-1] != first+2019 {
		t.Fatalf("fixture: %d untyped rows, last %d", len(untyped), untyped[len(untyped)-1])
	}
	if n := count(`SELECT count(*) FROM packet_observations WHERE payload_type IS NULL AND id >= 2147483648`); n < 600 {
		t.Fatalf("fixture: %d untyped rows at or above 2^31, want at least 600", n)
	}

	// Run 1 types [first, first+1500). Before run 2, retention deletes the packet of an untyped row in
	// the next range (the observation goes with it), and ingest adds 5 typed rows above the max.
	var deleted, maxAtLastRun int64
	runs := runUntilDone(payloadTypeBackfillTask(store, time.Minute, span, maxRows, 3), func() {
		if st := state(); st != (db.ObservationPayloadTypeState{NextID: first + 3*span}) {
			t.Fatalf("after run 1: state %+v", st)
		}
		var hash []byte
		if err := pool.QueryRow(ctx, `SELECT id, packet_hash FROM packet_observations WHERE id >= $1 AND payload_type IS NULL ORDER BY id LIMIT 1`,
			first+3*span).Scan(&deleted, &hash); err != nil {
			t.Fatal(err)
		}
		if tag, err := pool.Exec(ctx, `DELETE FROM packets WHERE packet_hash = $1`, hash); err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("deleted %d packets (err %v)", tag.RowsAffected(), err)
		}
		if n := count(`SELECT count(*) FROM packet_observations WHERE id = $1`, deleted); n != 0 {
			t.Fatalf("observation %d outlived its packet", deleted)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type)
SELECT p.packet_hash,'00000000-0000-0000-0000-0000000000a1','YOW','2026-01-02'::timestamptz+i*interval '1 second',0,1,0,p.payload_type
FROM generate_series(2021,2025) i JOIN packets p ON p.packet_hash = int4send(i) ORDER BY i`); err != nil {
			t.Fatal(err)
		}
		maxAtLastRun = count(`SELECT max(id) FROM packet_observations`)
	})
	if runs != 2 || deleted == 0 || maxAtLastRun != first+2024 {
		t.Fatalf("done after %d runs; deleted observation %d; max %d", runs, deleted, maxAtLastRun)
	}
	if st := state(); st != (db.ObservationPayloadTypeState{NextID: maxAtLastRun + 1, Done: true}) {
		t.Fatalf("final state %+v, want next_id %d, done", st, maxAtLastRun+1)
	}
	typedFromPacket("after the walk")
	if n := count(`SELECT count(*) FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.id = ANY($1) AND po.payload_type = p.payload_type`, untyped); n != int64(len(untyped)-1) {
		t.Fatalf("%d of the %d formerly untyped rows (one deleted) carry their packet's type", n, len(untyped)-1)
	}

	// Done: a run and a direct batch over the whole range write nothing.
	before := xmins()
	if err := payloadTypeBackfillTask(store, time.Minute, span, maxRows, 3).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n, next, err := store.BackfillObservationPayloadTypes(ctx, first, maxAtLastRun+1, maxRows); err != nil || n != 0 || next != maxAtLastRun+1 {
		t.Fatalf("a batch over typed rows wrote %d rows, next %d (err %v)", n, next, err)
	}
	if after := xmins(); !maps.Equal(before, after) {
		t.Fatal("rows rewritten after the walk finished")
	}

	// Crash resume: untype 50 rows, restart the walk, run one batch, then continue with a new task value.
	tag, err := pool.Exec(ctx, `UPDATE packet_observations SET payload_type = NULL WHERE id IN (
 SELECT id FROM packet_observations WHERE id <> $1 AND id % 37 = 0 ORDER BY id LIMIT 50)`, mismatch)
	if err != nil || tag.RowsAffected() != 50 {
		t.Fatalf("untyped %d rows (err %v)", tag.RowsAffected(), err)
	}
	if err := store.SaveState(ctx, db.ObservationPayloadTypeKey, db.ObservationPayloadTypeState{NextID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := payloadTypeBackfillTask(store, time.Minute, span, maxRows, 1).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st := state(); st != (db.ObservationPayloadTypeState{NextID: first + span}) {
		t.Fatalf("after one batch: state %+v, want next_id %d", st, first+span)
	}
	rec := &backfillFromRecorder{Store: store}
	runUntilDone(payloadTypeBackfillTask(rec, time.Minute, span, maxRows, 1), nil)
	if len(rec.from) == 0 || rec.from[0] != first+span {
		t.Fatalf("resumed batches from %v, want the first at %d", rec.from, first+span)
	}
	if st := state(); st != (db.ObservationPayloadTypeState{NextID: maxAtLastRun + 1, Done: true}) {
		t.Fatalf("state after resume %+v", st)
	}
	typedFromPacket("after the resume")

	// A dense span: 300 untyped rows in a row and a cap of 100 take three capped statements, each
	// typing the lowest untyped ids and continuing after the last one.
	if tag, err := pool.Exec(ctx, `UPDATE packet_observations SET payload_type = NULL WHERE id > $1 AND id <= $1 + 300`, first); err != nil || tag.RowsAffected() != 300 {
		t.Fatalf("untyped %d rows (err %v)", tag.RowsAffected(), err)
	}
	if err := store.SaveState(ctx, db.ObservationPayloadTypeKey, db.ObservationPayloadTypeState{NextID: 1}); err != nil {
		t.Fatal(err)
	}
	rec = &backfillFromRecorder{Store: store}
	capped := payloadTypeBackfillTask(rec, time.Minute, span, 100, 2)
	if err := capped.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st := state(); st != (db.ObservationPayloadTypeState{NextID: first + 201}) {
		t.Fatalf("after two capped batches: state %+v, want next_id %d", st, first+201)
	}
	if !slices.Equal(rec.from, []int64{first, first + 101}) || !slices.Equal(rec.typed, []int64{100, 100}) {
		t.Fatalf("capped batches from %v typed %v", rec.from, rec.typed)
	}
	if left := idList(`SELECT id FROM packet_observations WHERE payload_type IS NULL ORDER BY id`); len(left) != 100 || left[0] != first+201 || left[99] != first+300 {
		t.Fatalf("after two capped batches %d rows untyped, want ids %d..%d", len(left), first+201, first+300)
	}
	runUntilDone(capped, nil)
	if !slices.Equal(rec.from[:4], []int64{first, first + 101, first + 201, first + 301}) || rec.typed[2] != 100 || rec.typed[3] != 0 {
		t.Fatalf("capped batches from %v typed %v", rec.from, rec.typed)
	}
	if st := state(); st != (db.ObservationPayloadTypeState{NextID: maxAtLastRun + 1, Done: true}) {
		t.Fatalf("state after the capped walk %+v", st)
	}
	typedFromPacket("after the capped walk")
}
