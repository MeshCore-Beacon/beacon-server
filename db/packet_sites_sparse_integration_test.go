// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// sparseObservations stores three clusters of observations with 100,000 unused ids between them, the way a long
// retention run leaves a few old observations far below the live ids.
func (f *psFixture) sparseObservations() {
	f.t.Helper()
	stream := f.packets(60)
	third := len(stream) / 3
	f.unsited(stream[:third]...)
	f.exec("SELECT setval('packet_observations_id_seq', (SELECT max(id) FROM packet_observations) + 100000)")
	f.unsited(stream[third : 2*third]...)
	f.exec("SELECT setval('packet_observations_id_seq', (SELECT max(id) FROM packet_observations) + 100000)")
	f.unsited(stream[2*third:]...)
}

// observationIDs lists every observation id, ascending.
func (f *psFixture) observationIDs() []int64 {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, "SELECT id FROM packet_observations ORDER BY id")
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			f.t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return ids
}

// wantSlicesCover checks that fills run newest first without overlap, that each starts at an observation, and
// that together they cover every observation id.
func wantSlicesCover(t *testing.T, fills [][2]int64, ids []int64) {
	t.Helper()
	exists := map[int64]bool{}
	for _, id := range ids {
		exists[id] = true
	}
	covered := 0
	for i, r := range fills {
		if !exists[r[1]] || (i > 0 && r[1] > fills[i-1][0]) {
			t.Fatalf("fill %d is %v: %v", i, r, fills)
		}
		for _, id := range ids {
			if id > r[0] && id <= r[1] {
				covered++
			}
		}
	}
	if covered != len(ids) {
		t.Fatalf("fills %v cover %d of %d observations", fills, covered, len(ids))
	}
}

// A backfill slice holds a slice of observations however sparse their ids are, so long id ranges retention left
// nearly empty cost what their rows cost, and the slices fill exactly what a dense walk does.
func TestPacketSitesBackfillSlicesSparseIDsPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.sparseObservations()
	ids := f.observationIDs()
	p := f.begin()
	p.slice = 50
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	st, err := p.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete {
		t.Fatalf("Backfill = %+v, %v", st, err)
	}
	// 50 observations a slice, where walking 50 ids a slice would take more than 4,000.
	if want := (len(ids) + 49) / 50; len(p.fills) != want {
		t.Fatalf("%d fills for %d observations in three clusters, want %d: %v", len(p.fills), len(ids), want, p.fills)
	}
	wantSlicesCover(t, p.fills, ids)
	f.wantClean("after the sparse backfill")
}

// A backfill stopped by a failed slice resumes on the next call from its saved position and still covers every
// observation once.
func TestPacketSitesBackfillResumesAcrossGapsPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.sparseObservations()
	ids := f.observationIDs()
	p := f.begin()
	p.slice = 50
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	fill, calls := p.fillStmt, 0
	p.fillStmt = func(ctx context.Context, after, through int64) (int64, error) {
		if calls++; calls == 3 {
			return 0, errors.New("disk on fire")
		}
		return fill(ctx, after, through)
	}
	if _, err := p.Backfill(f.ctx, time.Minute); err == nil {
		t.Fatal("Backfill ignored a failed slice")
	}
	done := p.fills[:2]
	var saved PacketSitesState
	if found, err := f.store.LoadState(f.ctx, "packet_sites", &saved); err != nil || !found || saved.Complete || saved.BackfilledDownToID != done[1][0] {
		t.Fatalf("saved %+v, %v, %v; want the position after two slices, %d", saved, found, err, done[1][0])
	}
	p.Close(f.ctx)

	q := f.begin()
	q.slice = 50
	st, err := q.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete {
		t.Fatalf("resumed Backfill = %+v, %v", st, err)
	}
	if q.fills[0][1] != saved.BackfilledDownToID {
		t.Fatalf("resumed at %v, want the saved position %d", q.fills[0], saved.BackfilledDownToID)
	}
	wantSlicesCover(t, append(append([][2]int64{}, done...), q.fills...), ids)
	f.wantClean("after the resumed backfill")
}

// Progress counts estimated observations: after ANALYZE the estimate of the whole range is close to the row count,
// and the empty ranges between clusters count for nearly nothing.
func TestPacketSitesProgressCountsRowsPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.sparseObservations()
	f.exec("ANALYZE packet_observations")
	ids := f.observationIDs()
	p := f.begin()
	p.loadIDs(f.ctx)
	lo, hi := ids[0], ids[len(ids)-1]
	all, ok := p.ids.estimate(lo-1, hi)
	if !ok || all < int64(len(ids))*9/10 || all > int64(len(ids))*11/10 {
		t.Fatalf("estimate of all %d observations = %d, %v", len(ids), all, ok)
	}
	third := len(ids) / 3
	if gap, _ := p.ids.estimate(ids[third-1], ids[third]-1); gap > int64(len(ids))/20 {
		t.Fatalf("the empty 100,000 ids between clusters count %d of %d observations", gap, len(ids))
	}
	// The 10 observations at or below the first of the second cluster reach back across the gap.
	if n, err := sqlc.New(f.pool).ObservationIDBelow(f.ctx, sqlc.ObservationIDBelowParams{ThroughID: ids[third], N: 10}); err != nil || n != ids[third-10] {
		t.Fatalf("ObservationIDBelow across the gap = %d, %v; want %d", n, err, ids[third-10])
	}
	if n, err := sqlc.New(f.pool).ObservationIDBelow(f.ctx, sqlc.ObservationIDBelowParams{ThroughID: ids[5], N: 6}); err != nil || n != 0 {
		t.Fatalf("ObservationIDBelow past the oldest = %d, %v; want 0", n, err)
	}

}

// The audit samples observations, not ids: over three equal clusters 100,000 ids apart each cluster gets about a
// third of the audited slices, and every slice holds a full sample, where slices of ids would mostly land in the
// empty ranges between them.
func TestPacketSitesAuditSamplesRowsPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.sparseObservations()
	f.exec("ANALYZE packet_observations")
	ids := f.observationIDs()
	p := f.begin()
	p.slice = 50
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	p.sample = 20
	third := len(ids) / 3
	clusters := [3]int{}
	const draws = 60
	for i := range draws {
		p.fills = nil
		if n, err := p.Audit(f.ctx, float64(i)/draws); err != nil || n != 0 {
			t.Fatalf("audit %d: %d, %v", i, n, err)
		}
		r := p.fills[0]
		held := 0
		for k, id := range ids {
			if id > r[0] && id <= r[1] {
				held++
				clusters[min(k/third, 2)]++
			}
		}
		if held != 20 {
			t.Fatalf("audit %d slice %v holds %d observations, want 20", i, r, held)
		}
	}
	for c, n := range clusters {
		if share := float64(n) / (draws * 20); share < 0.2 || share > 0.47 {
			t.Fatalf("cluster %d got %.0f %% of the audited observations %v, want about a third", c, 100*share, clusters)
		}
	}
}

// The verification's random slice below the newest is a sample of observations too: it holds a full sample even
// when the ids below are mostly empty.
func TestPacketSitesVerifySamplesRowsPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.sparseObservations()
	f.exec("ANALYZE packet_observations")
	ids := f.observationIDs()
	p := f.begin()
	p.slice, p.sample = 50, 20
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	// Raise the watermark far above the observations, so the newest 500,000-id slice is empty and everything is
	// below it.
	p.state.ReconHi = ids[len(ids)-1] + packetSitesSlice + 1
	for range 10 {
		if v, err := p.Verify(f.ctx); err != nil || !v.OK() {
			t.Fatalf("Verify = %+v, %v", v, err)
		}
		r := p.checks[len(p.checks)-1]
		held := 0
		for _, id := range ids {
			if id > r[0] && id <= r[1] {
				held++
			}
		}
		if held != 20 {
			t.Fatalf("sampled range %v holds %d observations, want 20", r, held)
		}
	}
}
