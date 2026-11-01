// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/obscount"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The ingest process's activity counts must give the observer chart exactly what re-reading the raw rows gives.
func TestObserverActivityLiveCountsMatchRawPostgres(t *testing.T) {
	ctx, tx, raw := directoryFixture(t)
	if _, err := tx.Exec(ctx, "ALTER TABLE observers ADD COLUMN region_scope_checked_at timestamptz"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	start := now.Add(-26 * time.Hour) // the counter covers slots from start + 31 minutes
	clock := start
	counts := obscount.NewActivity(func() time.Time { return clock })
	live := &Store{q: sqlc.New(tx), obsActivity: counts}

	rng := rand.New(rand.NewPCG(5, 9))
	observers := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")}
	store := func(observer uuid.UUID, at time.Time, counted bool) {
		t.Helper()
		payloadType := []int16{1, 2, 4, 5, 6, 9}[rng.IntN(6)]
		rssi, snr := int16(-40-rng.IntN(80)), float32(rng.IntN(140)-80)/4 // SNR comes in quarter dB
		if rng.IntN(10) == 0 {
			rssi, snr = 0, 0 // no signal reported
		}
		rssiValue, snrValue := &rssi, &snr
		if rng.IntN(5) == 0 {
			rssiValue = nil
		}
		if rng.IntN(5) == 0 {
			snrValue = nil
		}
		var airtime *float32
		if rng.IntN(4) != 0 {
			a := float32(rng.Float64() * 1500)
			airtime = &a
		}
		activityObservation(t, ctx, tx, observer, at, payloadType, rssiValue, snrValue, airtime)
		if counted {
			counts.Add(at, observer, payloadType, rssiValue, snrValue, airtime)
		}
	}
	slot := obscount.ActivitySlot
	for range 1500 {
		at := start.Add(-2*time.Hour + time.Duration(rng.Int64N(int64(28*time.Hour)))).Truncate(time.Microsecond)
		// Rows heard before the counter started were stored by an earlier process; later ones are counted.
		store(observers[rng.IntN(2)], at, !at.Before(start))
	}
	for _, edge := range []time.Time{now.Add(-6 * time.Hour).Truncate(slot), now.Add(-12 * time.Hour).Truncate(slot)} {
		store(observers[0], edge, true)
		store(observers[0], edge.Add(-time.Microsecond), true)
	}
	// Heard after the request's end but in its slot (ingest accepts timestamps up to 30 minutes ahead).
	store(observers[0], now.Add(2*time.Second), true)
	store(observers[1], now.Add(-3*time.Hour).Truncate(slot).Add(time.Second), true) // and after a fixed end's
	clock = now

	type view struct {
		Points       []api.ObserverActivityPoint
		PayloadTypes []api.PayloadBreakdownItem
		Recorded     int64
		Start, End   int64
	}
	get := func(s *Store, observer uuid.UUID, window, interval time.Duration, until time.Time) view {
		t.Helper()
		a, err := s.GetObserverActivity(ctx, observer, window, interval, until)
		if err != nil {
			t.Fatal(err)
		}
		return view{a.Points, a.PayloadTypes, a.Summary.RecordedPackets, a.WindowStart, a.WindowEnd}
	}
	thinned := false
	compare := func(label string, observer uuid.UUID, window, interval time.Duration, until time.Time) {
		t.Helper()
		for try := 0; ; try++ {
			want, got := get(raw, observer, window, interval, until), get(live, observer, window, interval, until)
			if want.Start != got.Start && try == 0 { // the clock crossed a bucket between the two reads
				continue
			}
			want.End, got.End = 0, 0
			w, _ := json.Marshal(want)
			g, _ := json.Marshal(got)
			if string(w) != string(g) {
				t.Fatalf("%s: live\n%s\nraw\n%s", label, g, w)
			}
			if len(want.Points) == 0 && !thinned {
				t.Fatalf("%s: empty fixture", label)
			}
			return
		}
	}
	cases := func(prefix string) {
		for _, observer := range observers {
			for _, c := range []struct {
				window, interval time.Duration
				until            time.Time
			}{
				{24 * time.Hour, slot, time.Time{}},
				{48 * time.Hour, slot, time.Time{}},
				{6 * time.Hour, slot, time.Time{}},
				{24 * time.Hour, 5 * time.Minute, time.Time{}},
				{24 * time.Hour, slot, now.Add(-3 * time.Hour)},
				{12 * time.Hour, slot, now.Add(-5*time.Hour - 7*time.Minute)},
			} {
				compare(fmt.Sprintf("%s %s %s/%s until %v", prefix, observer, c.window, c.interval, c.until), observer, c.window, c.interval, c.until)
			}
		}
	}
	cases("all stored")

	// The live path is really taken: thin the raw rows of a counted slot and the counts still hold them.
	thin := now.Add(-4 * time.Hour).Truncate(slot)
	if _, err := tx.Exec(ctx, `DELETE FROM packet_observations WHERE heard_at >= $1 AND heard_at < $2`, thin, thin.Add(slot)); err != nil {
		t.Fatal(err)
	}
	if a, b := get(raw, observers[0], 24*time.Hour, slot, now), get(live, observers[0], 24*time.Hour, slot, now); a.Recorded == b.Recorded {
		t.Fatal("the live store re-read the raw rows")
	}

	thinned = true
	// Retention deleted everything up to some packet time: those slots and the 30 minutes after come from raw rows.
	cutoff := now.Add(-2*time.Hour - 10*time.Minute)
	for _, q := range []string{`DELETE FROM packet_observations WHERE heard_at <= $1`, `UPDATE analytics_raw_state SET raw_deleted_before = $1`} {
		if _, err := tx.Exec(ctx, q, cutoff); err != nil {
			t.Fatal(err)
		}
	}
	if a, b := get(raw, observers[0], 24*time.Hour, slot, now), get(live, observers[0], 24*time.Hour, slot, now); a.Recorded != b.Recorded {
		t.Fatalf("after retention: live %d observations, raw %d", b.Recorded, a.Recorded)
	}
	cases("after retention")
}

func activityObservation(t *testing.T, ctx context.Context, tx pgx.Tx, observer uuid.UUID, at time.Time, payloadType int16, rssi *int16, snr *float32, airtime *float32) {
	t.Helper()
	_, err := tx.Exec(ctx, `WITH packet AS (
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
 VALUES (decode(md5(gen_random_uuid()::text),'hex'),$4,0,1,'\x00','\x00',$1,$1) RETURNING packet_hash
 ) INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr,airtime_ms)
 SELECT packet_hash,$2,'YVR',$1,0,1,0,$4,$5,$6,$3 FROM packet`, at, observer, airtime, payloadType, rssi, snr)
	if err != nil {
		t.Fatal(err)
	}
}
