// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
)

// A request created while the delete waits for its first target locks must not make the delete take a target lock
// after the user lock.
func TestDeleteUserNewTargetNoDeadlockPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	u := newUser(t, w.ctx, w.s, "alice")
	x, y := pollKey(0xA2), pollKey(0xA1)
	r0 := newReq(u.ID, w.now, true)
	if err := w.s.MutateRequestTarget(w.ctx, x, u.ID, func(st *polling.TargetState) error { return st.Create(r0, w.now) }); err != nil {
		t.Fatal(err)
	}
	hold, err := w.pool.Begin(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback(w.ctx)
	if err = sqlc.New(hold).LockPollingTarget(w.ctx, x); err != nil {
		t.Fatal(err)
	}
	atUser := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	deleteGapHook = func() {
		once.Do(func() { close(atUser) })
		<-release
	}
	t.Cleanup(func() { deleteGapHook = nil })
	done := make(chan error, 1)
	go func() { done <- w.s.DeleteUser(w.ctx, u.ID) }()
	waitForLockWaiters(t, w, 1)
	r := newReq(u.ID, w.now, true)
	if err = w.s.MutateRequestTarget(w.ctx, y, u.ID, func(st *polling.TargetState) error { return st.Create(r, w.now) }); err != nil {
		t.Fatal(err)
	}
	edit, err := w.pool.Begin(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer edit.Rollback(w.ctx)
	q := sqlc.New(edit)
	if err = q.LockPollingTarget(w.ctx, y); err != nil {
		t.Fatal(err)
	}
	if err = hold.Commit(w.ctx); err != nil {
		t.Fatal(err)
	}
	<-atUser
	edited := make(chan error, 1)
	go func() {
		err := mutateRequestTarget(w.ctx, q, y, u.ID, func(st *polling.TargetState) error {
			return st.SetPassword(r.ID, []byte("new"), []byte("nonce"), w.now)
		})
		if err == nil {
			err = edit.Commit(w.ctx)
		} else {
			_ = edit.Rollback(w.ctx)
		}
		edited <- err
	}()
	close(release)
	de, ee := <-done, <-edited
	if strings.Contains(fmt.Sprint(de, ee), "deadlock") {
		t.Fatalf("delete=%v; update=%v", de, ee)
	}
	if de != nil {
		t.Fatal(de)
	}
	var n int
	if err := w.pool.QueryRow(w.ctx, `SELECT count(*) FROM telemetry_requests`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("requests left after the delete: %d %v", n, err)
	}
}

// claimGap runs after between storing a claim and returning, as an election landing in that gap would.
type claimGap struct {
	*Store
	after func()
}

func (s *claimGap) ReplacePollingClaim(ctx context.Context, c polling.Claim, id uuid.UUID, at time.Time) (bool, error) {
	ok, err := s.Store.ReplacePollingClaim(ctx, c, id, at)
	if ok && err == nil && s.after != nil {
		s.after()
	}
	return ok, err
}

func (s *claimGap) ReplacePollingClaimReporting(ctx context.Context, c polling.Claim, id uuid.UUID, at time.Time,
	report func(polling.ClaimNode, *polling.TargetState)) (bool, error) {
	ok, err := s.Store.ReplacePollingClaimReporting(ctx, c, id, at, report)
	if ok && err == nil && s.after != nil {
		s.after()
	}
	return ok, err
}

// adminStopWorld has one observer holding one requested target whose password it was served.
func adminStopWorld(t *testing.T) (*reqWorld, reqDevice, uuid.UUID, uuid.UUID) {
	t.Helper()
	w := newReqWorld(t, "")
	u := newUser(t, w.ctx, w.s, "alice")
	n := w.node(0xA1)
	o := newReqDevice(1)
	pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, o.pub), w.now.Add(-time.Hour), 5)
	w.claim(o, 1)
	r, err := w.reqs.Create(w.ctx, u.ID, n, []byte("admin"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := w.owners(o, 0xA1); !ok {
		t.Fatal("setup: not offered")
	}
	return w, o, u.ID, r.ID
}

func checkAdminStopped(t *testing.T, w *reqWorld, user, id uuid.UUID, what string) {
	t.Helper()
	stops, err := w.s.ListPollingAdminStops(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	st := w.status(user, id)
	if st.Status != polling.StatusAdminStopped || st.HasPassword || len(stops) != 1 {
		t.Fatalf("%s: status=%s hasPassword=%v stops=%d", what, st.Status, st.HasPassword, len(stops))
	}
}

// An election between the claim's commit and its report must not take the reporter's authority away first.
func TestAdminStopReportRacesElectionPostgres(t *testing.T) {
	w, o, user, id := adminStopWorld(t)
	st := &claimGap{Store: w.s}
	w.svc = polling.NewService(st, config.ResolvedPolling{Enabled: true, RequestObservers: config.RequestObserversAll}, func() time.Time { return w.now }, slog.New(slog.DiscardHandler))
	w.svc.SetRequests(st, w.sealer)
	st.after = func() {
		if err := w.svc.ElectTarget(w.ctx, pollKey(0xA1)); err != nil {
			t.Error(err)
		}
	}
	w.now = w.now.Add(time.Minute)
	w.claim(o, 0, reqNode(pollKey(0xA1), true, "admin_stop"))
	checkAdminStopped(t, w, user, id, "election after the claim commit")
}

// A report that fails to store must leave the claim unstored, so the device's next claim repeats it and it lands.
func TestAdminStopReportSurvivesTransientFailurePostgres(t *testing.T) {
	w, o, user, id := adminStopWorld(t)
	if _, err := w.pool.Exec(w.ctx, `
		CREATE SEQUENCE fail_once_seq;
		CREATE FUNCTION fail_once() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF nextval('fail_once_seq') = 1 THEN RAISE EXCEPTION 'transient admin stop failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER fail_once BEFORE INSERT ON polling_admin_stops FOR EACH ROW EXECUTE FUNCTION fail_once();`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		w.now = w.now.Add(time.Minute)
		w.claim(o, 0, reqNode(pollKey(0xA1), true, "admin_stop"))
	}
	checkAdminStopped(t, w, user, id, "after two admin-stop claims")
}

// A locally polled target rides on its local poll and takes no requested seat from anyone.
func TestLocalAssignmentDoesNotPreemptRequestPostgres(t *testing.T) {
	w, o, _, young, _, bob := seatWorld(t)
	w.now = w.now.Add(time.Minute)
	w.claim(o, 1, reqNode(pollKey(0xA2), false, "ok"))
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA1)); err != nil {
		t.Fatal(err)
	}
	if st := w.status(bob.ID, young.ID); st.Status != polling.StatusActive {
		t.Fatalf("setup: %+v", st)
	}
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA2)); err != nil {
		t.Fatal(err)
	}
	if st := w.status(bob.ID, young.ID); st.Status != polling.StatusActive {
		t.Fatalf("local target took the requested seat: status=%s reason=%q assignments=%v", st.Status, st.Reason, assignedTargets(t, w))
	}
}

// Cleanup that removes the poller promotes the standby; with nobody left the request waits for an observer.
func TestCleanupKeepsDenseRanksAndStatusPostgres(t *testing.T) {
	for _, standby := range []bool{false, true} {
		t.Run(fmt.Sprint("standby=", standby), func(t *testing.T) {
			w := newReqWorld(t, "")
			u := newUser(t, w.ctx, w.s, "alice")
			n := w.node(0xA1)
			a, b := newReqDevice(1), newReqDevice(2)
			for i, d := range []reqDevice{a, b} {
				if i == 1 && !standby {
					continue
				}
				pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, d.pub), w.now.Add(-time.Hour), float32(9-i*5))
				w.claim(d, 1)
			}
			r, err := w.reqs.Create(w.ctx, u.ID, n, []byte("pw"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = w.pool.Exec(w.ctx, `UPDATE polling_claims SET received_at=$1 WHERE observer_public_key=$2`, w.now.Add(-25*time.Hour), a.pub); err != nil {
				t.Fatal(err)
			}
			if err = w.s.DeleteExpiredPolling(w.ctx, w.now.Add(-24*time.Hour), w.now.Add(-31*24*time.Hour)); err != nil {
				t.Fatal(err)
			}
			st := w.status(u.ID, r.ID)
			if standby {
				e, ok := w.owners(b, 0xA1)
				if !ok || e.Role != "polling" || st.PollerID == nil || st.Status != polling.StatusActive {
					t.Fatalf("surviving owner=%+v; status=%s poller=%v", e, st.Status, st.PollerID)
				}
			} else if st.Status != polling.StatusWaiting || st.Reason != polling.ReasonNoObserver {
				t.Fatalf("no assignments: status=%s reason=%q poller=%v", st.Status, st.Reason, st.PollerID)
			}
		})
	}
}

type approvalReadError struct {
	*Store
	calls int
}

func (s *approvalReadError) PollingObserverFlags(ctx context.Context, o []byte) (bool, bool, error) {
	s.calls++
	if s.calls == 2 {
		return false, false, fmt.Errorf("transient approval read failure")
	}
	return s.Store.PollingObserverFlags(ctx, o)
}

// A failed approval read fails the owners answer instead of withdrawing the requested entries.
func TestOwnersApprovalReadErrorFailsAnswerPostgres(t *testing.T) {
	w := newReqWorld(t, "approved")
	u := newUser(t, w.ctx, w.s, "alice")
	n := w.node(0xA1)
	o := newReqDevice(1)
	pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, o.pub), w.now.Add(-time.Hour), 5)
	if err := w.s.SetPollingObserver(w.ctx, o.pub, false, true, ""); err != nil {
		t.Fatal(err)
	}
	w.claim(o, 1)
	if _, err := w.reqs.Create(w.ctx, u.ID, n, []byte("pw")); err != nil {
		t.Fatal(err)
	}
	st := &approvalReadError{Store: w.s}
	svc := polling.NewService(st, config.ResolvedPolling{Enabled: true, RequestObservers: config.RequestObserversApproved}, func() time.Time { return w.now }, slog.New(slog.DiscardHandler))
	svc.SetRequests(w.s, w.sealer)
	a, err := svc.Owners(w.ctx, polling.Caller{Observer: o.pub})
	if err == nil {
		t.Fatalf("approval lookup failed but owners succeeded with %d entries", len(a.Nodes))
	}
	if _, ok := w.owners(o, 0xA1); !ok {
		t.Fatal("a successful read must still offer the target")
	}
}
