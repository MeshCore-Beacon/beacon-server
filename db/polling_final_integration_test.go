// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"errors"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/jackc/pgx/v5/pgconn"
)

// waitForLockWaiters blocks until n sessions wait on a target lock.
func waitForLockWaiters(t *testing.T, w *reqWorld, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got int
		if err := w.pool.QueryRow(w.ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=25190 AND NOT granted`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions did not wait for the target lock", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// An observer whose claim lands while elections are in flight is not in the set they locked; the one seat it offers
// must still go to one target only.
func TestRequestedClaimArrivesMidElectionPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s := w.ctx, w.s
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	o := newReqDevice(1)
	oid := pollObserver(t, ctx, s, o.pub)
	nOld, nNew := w.node(0xA2), w.node(0xA1)
	for _, tg := range []byte{0xA1, 0xA2} {
		pollHearAdvert(t, ctx, w.pool, pollKey(tg), oid, w.now.Add(-time.Hour), 5)
	}
	if _, err := w.reqs.Create(ctx, alice.ID, nOld, []byte("old")); err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	if _, err := w.reqs.Create(ctx, bob.ID, nNew, []byte("new")); err != nil {
		t.Fatal(err)
	}
	hold, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback(ctx)
	q := sqlc.New(hold)
	for _, tg := range []byte{0xA1, 0xA2} {
		if err := q.LockPollingTarget(ctx, pollKey(tg)); err != nil {
			t.Fatal(err)
		}
	}
	arrived := make(chan byte, 8)
	release := map[byte]chan struct{}{0xA1: make(chan struct{}), 0xA2: make(chan struct{})}
	done := map[byte]chan error{0xA1: make(chan error, 1), 0xA2: make(chan error, 1)}
	for _, tg := range []byte{0xA2, 0xA1} {
		go func() {
			done[tg] <- s.ElectPollingTarget(ctx, pollKey(tg), w.now.Add(-time.Hour), w.now.Add(-24*time.Hour), w.now, func(in polling.ElectionInput) polling.Election {
				el := polling.Decide(in, config.ResolvedPolling{Enabled: true, RequestObservers: config.RequestObserversAll})
				arrived <- tg
				<-release[tg]
				return el
			})
		}()
	}
	waitForLockWaiters(t, w, 2)
	c := polling.Claim{Observer: o.pub, Timestamp: w.now, AcceptsRequests: true, RequestSlots: 1}
	if ok, err := s.ReplacePollingClaim(ctx, c, oid, w.now); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	if err := hold.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Each election is let go as it asks. The younger one is held back a moment so that, without the fix, the older
	// one commits its seat first and the younger one then commits the same seat. With the fix they run one at a time
	// in either order.
	go func() {
		for range 2 {
			tg := <-arrived
			go func() {
				if tg == 0xA1 {
					time.Sleep(200 * time.Millisecond)
				}
				close(release[tg])
			}()
		}
	}()
	for _, tg := range []byte{0xA2, 0xA1} {
		if err := <-done[tg]; err != nil {
			t.Fatal(err)
		}
	}
	if got := assignedTargets(t, w); len(got) > 1 {
		t.Fatalf("one seat, assignments %v", got)
	}
}

// A seat taken back from a younger target leaves its other observers: the best of them becomes the poller.
func TestRequestedPreemptionPromotesAStandbyPostgres(t *testing.T) {
	w, o, _, newReq, _, bob := seatWorld(t)
	if _, err := w.pool.Exec(w.ctx, `DELETE FROM polling_assignments`); err != nil {
		t.Fatal(err)
	}
	second := newReqDevice(2)
	pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, second.pub), w.now.Add(-time.Hour), 1)
	w.claim(second, 1)
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA1)); err != nil {
		t.Fatal(err)
	}
	if e, ok := w.owners(o, 0xA1); !ok || e.Role != "polling" {
		t.Fatalf("initial %+v", e)
	}
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA2)); err != nil {
		t.Fatal(err)
	}
	e, ok := w.owners(second, 0xA1)
	st := w.status(bob.ID, newReq.ID)
	if !ok || e.Role != "polling" || st.PollerID == nil {
		t.Fatalf("a standby was left behind: entry %+v active %s poller %v", e, st.Status, st.PollerID)
	}
}

// Preemption and cleanup both touch a target's assignment rows: they must meet at the target lock first.
func TestRequestedPreemptionAndCleanupDoNotDeadlockPostgres(t *testing.T) {
	w, _, _, _, _, _ := seatWorld(t)
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA1)); err != nil {
		t.Fatal(err)
	}
	cleanup, err := w.pool.Begin(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup.Rollback(w.ctx)
	q := sqlc.New(cleanup)
	if err := q.LockPollingTarget(w.ctx, pollKey(0xA1)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.svc.ElectTarget(w.ctx, pollKey(0xA2)) }()
	waitForLockWaiters(t, w, 1)
	// A preemption that already deleted the row would now wait for this lock, and cleanup deletes the same row.
	cleanupErr := q.DeleteOrphanPollingAssignments(w.ctx, pollKey(0xA1))
	if cleanupErr == nil {
		cleanupErr = cleanup.Commit(w.ctx)
	}
	electionErr := <-done
	var pg *pgconn.PgError
	if errors.As(cleanupErr, &pg) || errors.As(electionErr, &pg) || cleanupErr != nil || electionErr != nil {
		t.Fatalf("cleanup=%v election=%v", cleanupErr, electionErr)
	}
}

// A requested assignment exists before its observer lists the target; cleanup keeps it while that claim is live.
func TestRequestedCleanupKeepsANewAssignmentPostgres(t *testing.T) {
	w, o, _, _, _, _ := seatWorld(t)
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA2)); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.owners(o, 0xA2); !ok {
		t.Fatal("initial assignment absent")
	}
	if err := w.s.DeleteExpiredPolling(w.ctx, w.now.Add(-24*time.Hour), w.now.Add(-31*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.owners(o, 0xA2); !ok {
		t.Fatal("cleanup removed a requested target while its accepting claim is live")
	}
}

// An instance that cannot record what it served must fail the owners request, not answer without the requested
// target: the firmware reads an omitted requested target as withdrawn.
func TestRequestedOwnersOnAReadOnlyStoreFailsPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s := w.ctx, w.s
	alice := newUser(t, ctx, s, "alice")
	o := newReqDevice(1)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, o.pub), w.now.Add(-time.Hour), 5)
	w.claim(o, 1)
	if _, err := w.reqs.Create(ctx, alice.ID, node, []byte("A")); err != nil {
		t.Fatal(err)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET TRANSACTION READ ONLY`); err != nil {
		t.Fatal(err)
	}
	s.q = sqlc.New(tx)
	a, err := w.svc.Owners(ctx, polling.Caller{Observer: o.pub})
	if err == nil {
		t.Fatalf("answered 200 with %d nodes although the served versions could not be recorded", len(a.Nodes))
	}
}
