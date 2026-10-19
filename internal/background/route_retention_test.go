// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

type expiredCall struct {
	after  db.RouteKey
	cutoff time.Time
	batch  int32
}

type graceCall struct {
	after  db.RouteKey
	cutoff time.Time
	minObs int64
	batch  int32
	res    db.GraceSliceResult
}

// fakeRetainer records every call. Grace slices end where the SQL ends them; candidates,
// last keys and unresolved instants are scripted per call (1-based), defaulting to empty.
type fakeRetainer struct {
	state      map[string]json.RawMessage
	expired    []expiredCall
	expiredRes []struct {
		selected int64
		last     db.RouteKey
	}
	grace      []graceCall
	candidates map[int]int64
	last       map[int]db.RouteKey
	unresolved map[int]time.Time
	graceErr   map[int]error
	windowEnd  map[int]time.Time // overrides the SQL's window end
	expiredErr error
	loadErr    error
	saveErr    error
	neighErr   error
	saves      int
	neighbors  int
}

func newFakeRetainer() *fakeRetainer {
	return &fakeRetainer{
		state:      map[string]json.RawMessage{},
		candidates: map[int]int64{},
		last:       map[int]db.RouteKey{},
		unresolved: map[int]time.Time{},
		graceErr:   map[int]error{},
		windowEnd:  map[int]time.Time{},
	}
}

func (f *fakeRetainer) DeleteExpiredRoutes(_ context.Context, after db.RouteKey, cutoff time.Time, batch int32) (int64, db.RouteKey, error) {
	f.expired = append(f.expired, expiredCall{after, cutoff, batch})
	if f.expiredErr != nil {
		return 0, db.RouteKey{}, f.expiredErr
	}
	if i := len(f.expired) - 1; i < len(f.expiredRes) {
		return f.expiredRes[i].selected, f.expiredRes[i].last, nil
	}
	return 0, db.RouteKey{}, nil
}

// testCeilMs is ceil_ms of the cutoff as Postgres receives it (floored to µs).
func testCeilMs(t time.Time) time.Time {
	t = t.Truncate(time.Microsecond)
	f := t.Truncate(time.Millisecond)
	if f.Before(t) {
		return f.Add(time.Millisecond)
	}
	return f
}

func (f *fakeRetainer) DeleteGraceRoutesSlice(_ context.Context, after db.RouteKey, cutoff time.Time, minObs int64, batch int32) (db.GraceSliceResult, error) {
	n := len(f.grace) + 1
	if n > 10_000 {
		return db.GraceSliceResult{}, errors.New("grace walk does not terminate")
	}
	if after.TS.IsZero() {
		return db.GraceSliceResult{}, errors.New("grace walk from -infinity")
	}
	if err := f.graceErr[n]; err != nil {
		f.grace = append(f.grace, graceCall{after: after, cutoff: cutoff, minObs: minObs, batch: batch})
		return db.GraceSliceResult{}, err
	}
	end := testCeilMs(cutoff)
	if w := after.TS.Add(db.GraceWindow); w.Before(end) {
		end = w
	}
	if w, ok := f.windowEnd[n]; ok {
		end = w
	}
	res := db.GraceSliceResult{Candidates: f.candidates[n], Deleted: f.candidates[n], Last: f.last[n], WindowEnd: end}
	if u, ok := f.unresolved[n]; ok {
		res.Unresolved = &u
	}
	f.grace = append(f.grace, graceCall{after, cutoff, minObs, batch, res})
	return res, nil
}

func (f *fakeRetainer) LoadState(_ context.Context, key string, v any) (bool, error) {
	if f.loadErr != nil {
		return false, f.loadErr
	}
	raw, ok := f.state[key]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(raw, v)
}

func (f *fakeRetainer) SaveState(_ context.Context, key string, v any) error {
	f.saves++
	if f.saveErr != nil {
		return f.saveErr
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f.state[key] = raw
	return nil
}

func (f *fakeRetainer) ReconfirmNeighbors(context.Context) error {
	f.neighbors++
	return f.neighErr
}

func (f *fakeRetainer) put(t *testing.T, s routesGraceState) {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	f.state[RoutesGraceKey] = raw
}

func (f *fakeRetainer) saved(t *testing.T) routesGraceState {
	t.Helper()
	var s routesGraceState
	raw, ok := f.state[RoutesGraceKey]
	if !ok {
		t.Fatal("routes_grace not saved")
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRouteRetentionLoop(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 34, 56, 586_400, time.UTC) // µs and ns in now, as in production
	const (
		retention       = 14 * 24 * time.Hour
		grace           = 7 * 24 * time.Hour
		minObs    int64 = 3
		batch     int32 = 7
	)
	R, G := now.Add(-retention).Truncate(time.Microsecond), now.Add(-grace).Truncate(time.Microsecond) // as the task floors them
	floorR, floorG, ceilG := R.Truncate(time.Millisecond), G.Truncate(time.Millisecond), testCeilMs(G)
	clock := func() time.Time { return now }
	run := func(t *testing.T, f *fakeRetainer, retention, grace time.Duration) error {
		t.Helper()
		return routeRetentionTask(f, retention, grace, minObs, time.Hour, clock, batch).Run(context.Background())
	}
	mustRun := func(t *testing.T, f *fakeRetainer) {
		t.Helper()
		if err := run(t, f, retention, grace); err != nil {
			t.Fatal(err)
		}
	}
	incremental := routesGraceState{Watermark: floorG.Add(-time.Hour), MinObservations: minObs, Grace: grace.String(), LastFullPass: now.Add(-24 * time.Hour)}

	t.Run("first run is a full pass", func(t *testing.T) {
		f := newFakeRetainer()
		mustRun(t, f)
		if len(f.grace) != 29 {
			t.Fatalf("%d grace calls, want 29", len(f.grace))
		}
		if !f.grace[0].after.TS.Equal(floorR) || f.grace[0].after.ID != 0 {
			t.Fatalf("first after %+v, want {%v 0}", f.grace[0].after, floorR)
		}
		for i, c := range f.grace {
			if c.res.WindowEnd.After(c.after.TS.Add(6 * time.Hour)) {
				t.Errorf("call %d window %v..%v wider than 6h", i+1, c.after.TS, c.res.WindowEnd)
			}
			if !c.cutoff.Equal(G) || c.minObs != minObs || c.batch != batch {
				t.Errorf("call %d args %+v", i+1, c)
			}
			if i > 0 && (!c.after.TS.Equal(f.grace[i-1].res.WindowEnd) || c.after.ID != 0) {
				t.Errorf("call %d after %+v, want {%v 0}", i+1, c.after, f.grace[i-1].res.WindowEnd)
			}
		}
		if !f.grace[27].res.WindowEnd.Equal(floorG) {
			t.Errorf("window 28 ends %v, want floor_ms(G) %v", f.grace[27].res.WindowEnd, floorG)
		}
		if end := f.grace[28].res.WindowEnd; !end.Equal(ceilG) {
			t.Errorf("last window ends %v, want ceil_ms(G) %v", end, ceilG)
		}
		want := routesGraceState{Watermark: floorG, MinObservations: 3, Grace: "168h0m0s", LastFullPass: now.Truncate(time.Hour)}
		if got := f.saved(t); got != want {
			t.Errorf("saved %+v, want %+v", got, want)
		}
		if f.neighbors != 1 {
			t.Errorf("neighbors called %d times", f.neighbors)
		}
	})

	t.Run("incremental from the watermark", func(t *testing.T) {
		f := newFakeRetainer()
		f.put(t, incremental)
		mustRun(t, f)
		if len(f.grace) != 1 || !f.grace[0].after.TS.Equal(incremental.Watermark) {
			t.Fatalf("grace calls %+v, want 1 from %v", f.grace, incremental.Watermark)
		}
		if got := f.saved(t); !got.LastFullPass.Equal(incremental.LastFullPass) {
			t.Errorf("LastFullPass %v, want carried %v", got.LastFullPass, incremental.LastFullPass)
		}
	})

	t.Run("watermark is floor ms of G", func(t *testing.T) {
		f := newFakeRetainer()
		f.put(t, incremental)
		mustRun(t, f)
		if w := f.saved(t).Watermark; !w.Equal(floorG) || w.Equal(ceilG) {
			t.Errorf("watermark %v, want floor_ms(G) %v (not ceil %v)", w, floorG, ceilG)
		}
	})

	t.Run("full batch stays in the window", func(t *testing.T) {
		f := newFakeRetainer()
		f.put(t, incremental)
		T := incremental.Watermark.Add(time.Minute)
		f.candidates[1], f.last[1] = int64(batch), db.RouteKey{TS: T, ID: 9}
		mustRun(t, f)
		if len(f.grace) != 2 || f.grace[1].after != (db.RouteKey{TS: T, ID: 9}) {
			t.Fatalf("grace calls %+v, want call 2 after {%v 9}", f.grace, T)
		}
	})

	t.Run("unresolved holds the watermark", func(t *testing.T) {
		f := newFakeRetainer()
		u2, u5 := floorR.Add(8*time.Hour), floorR.Add(7*time.Hour)
		f.unresolved[2], f.unresolved[5] = u2, u5
		mustRun(t, f)
		if w := f.saved(t).Watermark; !w.Equal(u5) {
			t.Errorf("watermark %v, want %v", w, u5)
		}
	})

	t.Run("policy change forces a full pass", func(t *testing.T) {
		for _, s := range []routesGraceState{
			{Watermark: incremental.Watermark, MinObservations: 4, Grace: grace.String(), LastFullPass: incremental.LastFullPass},
			{Watermark: incremental.Watermark, MinObservations: minObs, Grace: "120h0m0s", LastFullPass: incremental.LastFullPass},
		} {
			f := newFakeRetainer()
			f.put(t, s)
			mustRun(t, f)
			if !f.grace[0].after.TS.Equal(floorR) {
				t.Errorf("%+v: start %v, want floor_ms(R) %v", s, f.grace[0].after.TS, floorR)
			}
			if got := f.saved(t); got.MinObservations != minObs || got.Grace != grace.String() || !got.LastFullPass.Equal(now.Truncate(time.Hour)) {
				t.Errorf("%+v: saved %+v", s, got)
			}
		}
	})

	t.Run("weekly full pass", func(t *testing.T) {
		for _, c := range []struct {
			last time.Time
			full bool
		}{
			{now.Truncate(time.Hour).Add(-7 * 24 * time.Hour), true},
			{now.Truncate(time.Hour).Add(-7*24*time.Hour + time.Hour), false},
		} {
			f := newFakeRetainer()
			s := incremental
			s.LastFullPass = c.last
			f.put(t, s)
			mustRun(t, f)
			start, saved := f.grace[0].after.TS, f.saved(t).LastFullPass
			if c.full && (!start.Equal(floorR) || !saved.Equal(now.Truncate(time.Hour))) {
				t.Errorf("last %v: start %v, LastFullPass %v; want full pass", c.last, start, saved)
			}
			if !c.full && (!start.Equal(s.Watermark) || !saved.Equal(c.last)) {
				t.Errorf("last %v: start %v, LastFullPass %v; want incremental", c.last, start, saved)
			}
		}
	})

	t.Run("grace not shorter than retention", func(t *testing.T) {
		f := newFakeRetainer()
		if err := run(t, f, 10*24*time.Hour, 15*24*time.Hour); err != nil {
			t.Fatal(err)
		}
		if len(f.grace) != 1 || f.grace[0].res.Candidates != 0 {
			t.Fatalf("grace calls %+v, want exactly 1 empty", f.grace)
		}
	})

	t.Run("error saves nothing", func(t *testing.T) {
		f := newFakeRetainer()
		want := errors.New("statement timeout")
		f.graceErr[3] = want
		if err := run(t, f, retention, grace); !errors.Is(err, want) {
			t.Fatalf("error %v, want %v", err, want)
		}
		if len(f.grace) != 3 || f.saves != 0 || f.neighbors != 0 {
			t.Errorf("calls %d, saves %d, neighbors %d after error", len(f.grace), f.saves, f.neighbors)
		}
	})

	t.Run("retention half pages", func(t *testing.T) {
		f := newFakeRetainer()
		k1, k2 := db.RouteKey{TS: R.Add(-48 * time.Hour), ID: 5}, db.RouteKey{TS: R.Add(-24 * time.Hour), ID: 2}
		f.expiredRes = []struct {
			selected int64
			last     db.RouteKey
		}{{int64(batch), k1}, {int64(batch), k2}, {3, db.RouteKey{TS: R.Add(-time.Hour), ID: 1}}}
		mustRun(t, f)
		if len(f.expired) != 3 {
			t.Fatalf("%d retention calls, want 3", len(f.expired))
		}
		for i, want := range []db.RouteKey{{}, k1, k2} {
			if c := f.expired[i]; c.after != want || !c.cutoff.Equal(R) || c.batch != batch {
				t.Errorf("call %d %+v, want after %+v", i+1, c, want)
			}
		}
	})

	t.Run("cutoffs are today's exact instants", func(t *testing.T) {
		f := newFakeRetainer()
		ret := 168*time.Hour + 600*time.Microsecond
		if err := run(t, f, ret, 24*time.Hour); err != nil {
			t.Fatal(err)
		}
		// R and G are floored to µs once, as pgx sends them; nothing else is truncated.
		exact := now.Add(-ret).Truncate(time.Microsecond)
		if !f.expired[0].cutoff.Equal(exact) {
			t.Errorf("retention cutoff %v, want %v", f.expired[0].cutoff, exact)
		}
		if g := now.Add(-24 * time.Hour).Truncate(time.Microsecond); !f.grace[0].cutoff.Equal(g) {
			t.Errorf("grace cutoff %v, want %v", f.grace[0].cutoff, g)
		}
		if start := f.grace[0].after.TS; !start.Equal(exact.Truncate(time.Millisecond)) {
			t.Errorf("grace start %v", start)
		}
	})
}

// Sub-µs parts of now: G's µs floor can sit on a whole ms, so ceil_ms(G as sent) < G exactly.
func TestRouteRetentionSubMicrosecondNow(t *testing.T) {
	for _, ns := range []int{400, 999} {
		now := time.Date(2026, 1, 15, 12, 34, 56, ns, time.UTC)
		f := newFakeRetainer()
		clock := func() time.Time { return now }
		if err := routeRetentionTask(f, 14*24*time.Hour, 7*24*time.Hour, 3, time.Hour, clock, 7).Run(context.Background()); err != nil {
			t.Fatalf("+%dns: %v", ns, err)
		}
		// floor_ms(R)+7d is G's µs floor, a whole ms: 28 windows, the last ending at ceil_ms(G) itself.
		if len(f.grace) != 28 {
			t.Errorf("+%dns: %d grace calls, want 28", ns, len(f.grace))
		}
		if f.saves != 1 || f.neighbors != 1 {
			t.Errorf("+%dns: saves %d, neighbors %d", ns, f.saves, f.neighbors)
		}
	}
}

func TestRouteRetentionErrors(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 34, 56, 586_400, time.UTC)
	boom := errors.New("boom")
	run := func(f *fakeRetainer) error {
		return routeRetentionTask(f, 14*24*time.Hour, 7*24*time.Hour, 3, time.Hour, func() time.Time { return now }, 7).Run(context.Background())
	}
	for _, c := range []struct {
		name             string
		set              func(*fakeRetainer)
		saves, neighbors int
	}{
		{"expired", func(f *fakeRetainer) { f.expiredErr = boom }, 0, 0},
		{"load state", func(f *fakeRetainer) { f.loadErr = boom }, 0, 0},
		{"save state", func(f *fakeRetainer) { f.saveErr = boom }, 1, 0},
		{"neighbors", func(f *fakeRetainer) { f.neighErr = boom }, 1, 1},
	} {
		f := newFakeRetainer()
		c.set(f)
		if err := run(f); !errors.Is(err, boom) {
			t.Errorf("%s: error %v", c.name, err)
		}
		if f.saves != c.saves || f.neighbors != c.neighbors {
			t.Errorf("%s: saves %d, neighbors %d; want %d, %d", c.name, f.saves, f.neighbors, c.saves, c.neighbors)
		}
	}
	t.Run("no progress", func(t *testing.T) {
		f := newFakeRetainer()
		R := now.Add(-14 * 24 * time.Hour).Truncate(time.Millisecond)
		f.windowEnd[2] = R.Add(6 * time.Hour) // call 2 starts there: the window does not advance
		err := run(f)
		if err == nil || len(f.grace) != 2 || f.saves != 0 || f.neighbors != 0 {
			t.Fatalf("error %v, calls %d, saves %d, neighbors %d", err, len(f.grace), f.saves, f.neighbors)
		}
	})
}

func TestRouteRetentionClockJump(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 34, 56, 586_400, time.UTC)
	f := newFakeRetainer()
	f.put(t, routesGraceState{
		Watermark: now.Add(-7*24*time.Hour - time.Hour).Truncate(time.Millisecond), MinObservations: 3,
		Grace: (7 * 24 * time.Hour).String(), LastFullPass: now.Add(48 * time.Hour), // a pass "in the future"
	})
	if err := routeRetentionTask(f, 14*24*time.Hour, 7*24*time.Hour, 3, time.Hour, func() time.Time { return now }, 7).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if R := now.Add(-14 * 24 * time.Hour).Truncate(time.Millisecond); !f.grace[0].after.TS.Equal(R) {
		t.Errorf("start %v, want full pass from %v", f.grace[0].after.TS, R)
	}
	if got := f.saved(t).LastFullPass; !got.Equal(now.Truncate(time.Hour)) {
		t.Errorf("LastFullPass %v, want %v", got, now.Truncate(time.Hour))
	}
}

func TestRouteRetentionTaskName(t *testing.T) {
	task := RouteRetentionTask(newFakeRetainer(), time.Hour, time.Hour, 3, 2*time.Hour)
	if task.Name != "route_retention" || task.Interval != 2*time.Hour {
		t.Errorf("task %q every %v", task.Name, task.Interval)
	}
}
