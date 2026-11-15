// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

// failOnWarn fails the test on any warning, so a failed election or claim inside the service cannot pass unseen.
type failOnWarn struct{ t *testing.T }

func (h failOnWarn) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }
func (h failOnWarn) Handle(_ context.Context, r slog.Record) error {
	h.t.Errorf("service warned: %s", r.Message)
	return nil
}
func (h failOnWarn) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h failOnWarn) WithGroup(string) slog.Handler      { return h }

// simDevice is an observer as the firmware runs it: local targets take seats first, and an owners answer drops the
// requested targets it does not list before it adds new ones into the free seats.
type simDevice struct {
	dev   reqDevice
	seats int
	local map[string]bool
	held  map[string]bool
	gone  bool
}

func (d *simDevice) free() int { return max(d.seats-len(d.local)-len(d.held), 0) }

func (d *simDevice) reconcile(a polling.OwnersAnswer) {
	listed := map[string]bool{}
	for k, e := range a.Nodes {
		if e.Requested {
			listed[k] = true
		}
	}
	for k := range d.held {
		if !listed[k] || d.local[k] {
			delete(d.held, k)
		}
	}
	for k := range listed {
		if !d.held[k] && !d.local[k] && d.free() > 0 {
			d.held[k] = true
		}
	}
}

// Random claims, owners fetches, elections, request and account changes, cleanup and the offline task, with the
// seat, rank and status rules checked against the database after every step.
func TestRequestedInterleavingsKeepInvariantsPostgres(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4, 5}
	if s := os.Getenv("BEACON_INTERLEAVE_SEED"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = []uint64{n}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) { runInterleaving(t, seed, 400) })
	}
}

func runInterleaving(t *testing.T, seed uint64, steps int) {
	rng := rand.New(rand.NewPCG(seed, seed))
	w := newReqWorld(t, "")
	log := slog.New(failOnWarn{t})
	cfg := config.ResolvedPolling{Enabled: true, RequestObservers: config.RequestObserversAll}
	w.svc = polling.NewService(w.s, cfg, func() time.Time { return w.now }, log)
	w.svc.SetRequests(w.s, w.sealer)
	w.reqs = polling.NewRequestService(w.s, w.sealer, w.svc, 2, func() time.Time { return w.now }, log)

	targets := []byte{0xA1, 0xA2, 0xA3, 0xA4}
	nodeIDs := map[byte]uuid.UUID{}
	for _, k := range targets {
		nodeIDs[k] = w.node(k)
	}
	var devs []*simDevice
	for i := 1; i <= 3; i++ {
		d := &simDevice{dev: newReqDevice(byte(i)), seats: 1 + rng.IntN(3), local: map[string]bool{}, held: map[string]bool{}}
		oid := pollObserver(t, w.ctx, w.s, d.dev.pub)
		for _, k := range targets {
			if rng.IntN(4) > 0 {
				pollHearAdvert(t, w.ctx, w.pool, pollKey(k), oid, w.now.Add(-time.Hour), float32(rng.IntN(10)))
			}
		}
		if rng.IntN(3) == 0 {
			d.local[hex.EncodeToString(pollKey(targets[rng.IntN(len(targets))]))] = true
		}
		devs = append(devs, d)
	}
	var people []users.User
	for i := 0; i < 3; i++ {
		people = append(people, newUser(t, w.ctx, w.s, fmt.Sprintf("u%d", i)))
	}
	made := 3

	claim := func(d *simDevice) {
		var nodes []string
		for k := range d.local {
			b, _ := hex.DecodeString(k)
			nodes = append(nodes, reqNode(b, false, "ok"))
		}
		for k := range d.held {
			b, _ := hex.DecodeString(k)
			state := []string{"ok", "ok", "ok", "pending", "login_failed", "admin_stop"}[rng.IntN(6)]
			nodes = append(nodes, reqNode(b, true, state))
		}
		d.gone = false
		w.svc.HandleClaim(w.ctx, hex.EncodeToString(d.dev.pub), reqClaimJSON(d.dev.pub, w.now, d.free(), nodes...))
	}
	pickRequest := func() (users.User, polling.RequestRow, bool) {
		u := people[rng.IntN(len(people))]
		rows, err := w.reqs.List(w.ctx, u.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			return u, polling.RequestRow{}, false
		}
		return u, rows[rng.IntN(len(rows))], true
	}
	expected := func(err error) {
		t.Helper()
		for _, ok := range []error{polling.ErrRequestLimit, polling.ErrAlreadyRequested, polling.ErrNoPasswordBlocked, polling.ErrPasswordInvalid} {
			if errors.Is(err, ok) {
				return
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	trace := os.Getenv("BEACON_INTERLEAVE_TRACE") != ""
	var seen struct{ steps, active, seated, stopped int }
	for step := 0; step < steps && !t.Failed(); step++ {
		w.now = w.now.Add(time.Minute)
		var what string
		switch op := rng.IntN(12); op {
		case 0, 1, 2:
			d := devs[rng.IntN(len(devs))]
			what = "claim " + hex.EncodeToString(d.dev.pub[:4])
			claim(d)
		case 3, 4:
			d := devs[rng.IntN(len(devs))]
			what = "owners " + hex.EncodeToString(d.dev.pub[:4])
			a, err := w.svc.Owners(w.ctx, polling.Caller{Observer: d.dev.pub})
			if err != nil {
				t.Fatal(err)
			}
			d.reconcile(a)
			if msg := checkDeviceHolds(t, w, d); msg != "" {
				t.Fatalf("seed %d step %d (%s): %s", seed, step, what, msg)
			}
		case 5:
			what = "elect all"
			if err := w.svc.ElectAll(w.ctx); err != nil {
				t.Fatal(err)
			}
		case 6:
			u, k := people[rng.IntN(len(people))], targets[rng.IntN(len(targets))]
			var pw []byte
			if rng.IntN(3) > 0 {
				pw = []byte(fmt.Sprint("pw", rng.IntN(100)))
			}
			what = fmt.Sprintf("create %s %x blank=%v", u.Username, k, pw == nil)
			_, err := w.reqs.Create(w.ctx, u.ID, nodeIDs[k], pw)
			expected(err)
		case 7:
			u, r, ok := pickRequest()
			if !ok {
				continue
			}
			up := polling.RequestUpdate{Enable: rng.IntN(2) == 0}
			if !up.Enable || rng.IntN(2) == 0 {
				up.SetPassword = true
				if rng.IntN(4) > 0 {
					up.Password = []byte(fmt.Sprint("new", rng.IntN(100)))
				}
			}
			what = fmt.Sprintf("update %s %+v", u.Username, up)
			_, err := w.reqs.Update(w.ctx, u.ID, r.ID, up)
			expected(err)
		case 8:
			u, r, ok := pickRequest()
			if !ok {
				continue
			}
			what = "delete request of " + u.Username
			expected(w.reqs.Delete(w.ctx, u.ID, r.ID))
		case 9:
			i := rng.IntN(len(people))
			what = "delete user " + people[i].Username
			if err := w.s.DeleteUser(w.ctx, people[i].ID); err != nil {
				t.Fatal(err)
			}
			made++
			people[i] = newUser(t, w.ctx, w.s, fmt.Sprintf("u%d", made))
		case 10:
			// An observer gone for a day, then the hourly cleanup.
			d := devs[rng.IntN(len(devs))]
			what = "cleanup after " + hex.EncodeToString(d.dev.pub[:4]) + " left"
			if _, err := w.pool.Exec(w.ctx, `UPDATE polling_claims SET received_at = $1 WHERE observer_public_key = $2`, w.now.Add(-25*time.Hour), d.dev.pub); err != nil {
				t.Fatal(err)
			}
			d.gone, d.held = true, map[string]bool{}
			if err := w.s.DeleteExpiredPolling(w.ctx, w.now.Add(-24*time.Hour), w.now.Add(-30*24*time.Hour)); err != nil {
				t.Fatal(err)
			}
		case 11:
			k := targets[rng.IntN(len(targets))]
			what = fmt.Sprintf("offline %x", k)
			if _, err := w.pool.Exec(w.ctx, `UPDATE nodes SET last_seen = $1 WHERE public_key = $2`, w.now.Add(-10*24*time.Hour), pollKey(k)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.pool.Exec(w.ctx, `UPDATE telemetry_requests SET enabled_at = $1 WHERE target_public_key = $2`, w.now.Add(-9*24*time.Hour), pollKey(k)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.s.DisableOfflineRequests(w.ctx, w.now.Add(-7*24*time.Hour), w.now, polling.ReasonOffline(7)); err != nil {
				t.Fatal(err)
			}
		}
		if trace {
			t.Logf("step %d: %s", step, what)
		}
		if msg := checkRequestInvariants(t, w); msg != "" {
			t.Fatalf("seed %d step %d (%s): %s", seed, step, what, msg)
		}
		var active, seated, stopped int
		if err := w.pool.QueryRow(w.ctx, `SELECT (SELECT count(*) FROM telemetry_requests WHERE status = 'active'),
			(SELECT count(*) FROM polling_assignments WHERE requested),
			(SELECT count(*) FROM telemetry_requests WHERE status IN ('login_failed', 'admin_stopped', 'disabled'))`).Scan(&active, &seated, &stopped); err != nil {
			t.Fatal(err)
		}
		seen.steps++
		seen.active += active
		seen.seated += seated
		seen.stopped += stopped
	}
	// The run is only worth something if requests were seated, active and stopped along the way.
	t.Logf("%+v", seen)
	if seen.active == 0 || seen.seated == 0 || seen.stopped == 0 {
		t.Fatalf("the run never reached the states it checks: %+v", seen)
	}
}

// checkDeviceHolds compares what a device with a live claim holds after an owners answer with the requested
// assignments the database gives it: the answer must hand over every one of them.
func checkDeviceHolds(t *testing.T, w *reqWorld, d *simDevice) string {
	t.Helper()
	rows, err := w.pool.Query(w.ctx, `SELECT encode(a.target_public_key, 'hex') FROM polling_assignments a
		JOIN polling_targets pt ON pt.target_public_key = a.target_public_key AND pt.active_request_id IS NOT NULL
		JOIN polling_claims cl ON cl.observer_public_key = a.observer_public_key AND cl.received_at > $2
		WHERE a.observer_public_key = $1 AND a.requested`, d.dev.pub, w.now.Add(-polling.ClaimTTL))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]bool{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		want[k] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 {
		return ""
	}
	if fmt.Sprint(want) != fmt.Sprint(d.held) {
		return fmt.Sprintf("device holds %v, assigned %v", d.held, want)
	}
	return ""
}

// checkRequestInvariants returns what is broken, or "".
func checkRequestInvariants(t *testing.T, w *reqWorld) string {
	t.Helper()
	q := func(sql string, args ...any) []string {
		rows, err := w.pool.Query(w.ctx, sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	// (b) requested assignments of requested targets within the seats of the observer's stored claim, none without
	// one. A claim that lapses by the clock is applied by the next election pass, so liveness is not asked here.
	seats := `COALESCE(CASE WHEN cl.accepts_requests THEN least(8, least(cl.request_slots, 8) +
	              (SELECT least(count(*), 8) FROM polling_candidates c WHERE c.observer_public_key = cl.observer_public_key AND c.requested)) END, 0)`
	if bad := q(`SELECT encode(a.observer_public_key, 'hex') || ' holds ' || count(*) || ' of ' || ` + seats + `
	             FROM polling_assignments a
	             JOIN polling_targets pt ON pt.target_public_key = a.target_public_key AND pt.active_request_id IS NOT NULL
	             LEFT JOIN polling_claims cl ON cl.observer_public_key = a.observer_public_key
	             WHERE a.requested
	             GROUP BY a.observer_public_key, cl.observer_public_key, cl.accepts_requests, cl.request_slots
	             HAVING count(*) > ` + seats); len(bad) > 0 {
		return fmt.Sprint("seats overbooked: ", bad)
	}
	// (c) ranks 0..n-1.
	if bad := q(`SELECT encode(target_public_key, 'hex') || ' ranks ' || string_agg(rank::text, ',' ORDER BY rank)
	             FROM polling_assignments GROUP BY target_public_key
	             HAVING min(rank) <> 0 OR max(rank) <> count(*) - 1 OR count(DISTINCT rank) <> count(*)`); len(bad) > 0 {
		return fmt.Sprint("ranks not dense: ", bad)
	}
	// (d) waiting and active follow the assignments, carry a matching reason, and exist only while the target is
	// requested; the request in use is waiting or active.
	if bad := q(`SELECT encode(r.target_public_key, 'hex') || ' ' || r.status || ' ' || r.status_reason || ' assigned=' ||
	                    EXISTS (SELECT 1 FROM polling_assignments a WHERE a.target_public_key = r.target_public_key) ||
	                    ' requested=' || (pt.active_request_id IS NOT NULL)
	             FROM telemetry_requests r LEFT JOIN polling_targets pt ON pt.target_public_key = r.target_public_key
	             WHERE r.status IN ('waiting', 'active') AND (
	                 pt.active_request_id IS NULL
	                 OR (r.status = 'active') <> EXISTS (SELECT 1 FROM polling_assignments a WHERE a.target_public_key = r.target_public_key)
	                 OR (r.status = 'active' AND r.status_reason <> '')
	                 OR (r.status = 'waiting' AND r.status_reason NOT IN ($1, $2)))`, polling.ReasonNoObserver, polling.ReasonNoSeat); len(bad) > 0 {
		return fmt.Sprint("status out of step: ", bad)
	}
	if bad := q(`SELECT encode(pt.target_public_key, 'hex') || ' uses a ' || r.status || ' request'
	             FROM polling_targets pt JOIN telemetry_requests r ON r.id = pt.active_request_id
	             WHERE r.status NOT IN ('waiting', 'active')`); len(bad) > 0 {
		return fmt.Sprint("request in use is stopped: ", bad)
	}
	return ""
}
