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

// approvalReadError fails the read that carries the observer's approval along with its owner rows.
type approvalReadError struct{ *Store }

func (s *approvalReadError) PollingOwnerRows(context.Context, []byte, time.Time) ([]polling.OwnerRow, error) {
	return nil, fmt.Errorf("transient approval read failure")
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

// A full device holding a younger target is handed the older one that took its seat, in the same answer.
func TestPreemptionOffersReplacementAtZeroFreeSlotsPostgres(t *testing.T) {
	w, o, old, _, alice, _ := seatWorld(t)
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA1)); err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	w.claim(o, 0, reqNode(pollKey(0xA1), true, "ok"))
	if err := w.svc.ElectTarget(w.ctx, pollKey(0xA2)); err != nil {
		t.Fatal(err)
	}
	a, err := w.svc.Owners(w.ctx, polling.Caller{Observer: o.pub})
	if err != nil {
		t.Fatal(err)
	}
	e, ok := a.Nodes[fmt.Sprintf("%x", pollKey(0xA2))]
	if len(a.Nodes) != 1 || !ok || e.PW == nil {
		t.Fatalf("older target status=%s assignments=%v but owners=%+v", w.status(alice.ID, old.ID).Status, assignedTargets(t, w), a.Nodes)
	}
	if got := w.opens(o, 0xA2, e); got != "old" {
		t.Fatalf("replacement carries %q", got)
	}
}

// A failed or admin-stopped request keeps its state through the offline sweep and a later enable.
func TestOfflineKeepsCredentialStopsPostgres(t *testing.T) {
	for _, state := range []string{"login_failed", "admin_stop"} {
		t.Run(state, func(t *testing.T) {
			w := newReqWorld(t, "")
			u := newUser(t, w.ctx, w.s, "alice")
			n := w.node(0xA1)
			o := newReqDevice(1)
			pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, o.pub), w.now.Add(-time.Hour), 5)
			w.claim(o, 1)
			r, err := w.reqs.Create(w.ctx, u.ID, n, []byte("pw"))
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := w.owners(o, 0xA1); !ok {
				t.Fatal("setup")
			}
			w.now = w.now.Add(time.Hour)
			w.claim(o, 0, reqNode(pollKey(0xA1), true, state))
			want := w.status(u.ID, r.ID)
			if _, err = w.pool.Exec(w.ctx, `UPDATE telemetry_requests SET enabled_at=$1`, w.now.Add(-9*24*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if _, err = w.pool.Exec(w.ctx, `UPDATE nodes SET last_seen=$1`, w.now.Add(-10*24*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if _, err = w.s.DisableOfflineRequests(w.ctx, w.now.Add(-7*24*time.Hour), w.now, polling.ReasonOffline(7)); err != nil {
				t.Fatal(err)
			}
			if got := w.status(u.ID, r.ID); got.Status != want.Status || got.Reason != want.Reason {
				t.Fatalf("offline sweep changed %s/%q to %s/%q", want.Status, want.Reason, got.Status, got.Reason)
			}
			if state == "admin_stop" {
				fresh := newReqDevice(2)
				pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), pollObserver(t, w.ctx, w.s, fresh.pub), w.now.Add(-time.Hour), 8)
				w.claim(fresh, 1)
			}
			got, err := w.reqs.Update(w.ctx, u.ID, r.ID, polling.RequestUpdate{Enable: true})
			if err != nil {
				t.Fatal(err)
			}
			var failed bool
			var active *uuid.UUID
			if err = w.pool.QueryRow(w.ctx, `SELECT r.password_failed, t.active_request_id FROM telemetry_requests r
				JOIN polling_targets t ON t.target_public_key = r.target_public_key WHERE r.id=$1`, r.ID).Scan(&failed, &active); err != nil {
				t.Fatal(err)
			}
			if got.Status != want.Status || got.Reason != want.Reason || active != nil || failed != (state == "login_failed") {
				t.Fatalf("enable revived a stopped credential: status=%s reason=%q hasPassword=%v failed=%v active=%v",
					got.Status, got.Reason, got.HasPassword, failed, active)
			}
			if state == "admin_stop" {
				if _, err := w.reqs.Update(w.ctx, u.ID, r.ID, polling.RequestUpdate{SetPassword: true}); err == nil {
					t.Fatal("an admin-stopped request became a blank login")
				}
				if got := w.status(u.ID, r.ID); got.Status != polling.StatusAdminStopped {
					t.Fatalf("refused blank login still changed the request: %s", got.Status)
				}
			}
		})
	}
}

// Deleting a request clears polling_targets.active_request_id through an index, not a scan of every target.
func TestDeletingRequestHasIndexedReverseReferencePostgres(t *testing.T) {
	w := newReqWorld(t, "")
	u := newUser(t, w.ctx, w.s, "alice")
	r := newReq(u.ID, w.now, false)
	if err := w.s.MutateRequestTarget(w.ctx, pollKey(0xA1), u.ID, func(st *polling.TargetState) error { return st.Create(r, w.now) }); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `INSERT INTO polling_targets(target_public_key) SELECT decode(md5(i::text)||md5(i::text),'hex') FROM generate_series(1,100000) i; ANALYZE polling_targets`); err != nil {
		t.Fatal(err)
	}
	rows, err := w.pool.Query(w.ctx, `EXPLAIN UPDATE polling_targets SET active_request_id=NULL WHERE active_request_id=$1`, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if plan := strings.Join(lines, "\n"); strings.Contains(plan, "Seq Scan") {
		t.Fatalf("the reverse reference has no index:\n%s", plan)
	}
}

// A target whose requests are gone keeps its assignment until its next election; it must not take the seat the
// answer owes a target that is still requested.
func TestOwnersOfferSkipsWithdrawnTargetsPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	o := newReqDevice(1)
	oid := pollObserver(t, w.ctx, w.s, o.pub)
	var people []uuid.UUID
	for i, k := range []byte{0xA1, 0xA2, 0xA3} {
		pollHearAdvert(t, w.ctx, w.pool, pollKey(k), oid, w.now.Add(-time.Hour), 5)
		people = append(people, newUser(t, w.ctx, w.s, fmt.Sprint("u", i)).ID)
	}
	w.claim(o, 2)
	for i, k := range []byte{0xA1, 0xA2} {
		if _, err := w.reqs.Create(w.ctx, people[i], w.node(k), []byte("pw")); err != nil {
			t.Fatal(err)
		}
	}
	// Account deletion runs no election, so A1 keeps its assignment for now.
	if err := w.s.DeleteUser(w.ctx, people[0]); err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	if _, err := w.reqs.Create(w.ctx, people[2], w.node(0xA3), []byte("pw")); err != nil {
		t.Fatal(err)
	}
	a, err := w.svc.Owners(w.ctx, polling.Caller{Observer: o.pub})
	if err != nil {
		t.Fatal(err)
	}
	_, has2 := a.Nodes[fmt.Sprintf("%x", pollKey(0xA2))]
	_, has3 := a.Nodes[fmt.Sprintf("%x", pollKey(0xA3))]
	if !has2 || !has3 {
		t.Fatalf("assignments %v but owners lists %d entries (A2 %v, A3 %v)", assignedTargets(t, w), len(a.Nodes), has2, has3)
	}
}

// A target whose requests are all disabled keeps its assignment until its own election; until then its seat must
// not count against a younger target that is still requested.
func TestWithdrawnTargetFreesItsSeatPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	alice, bob := newUser(t, w.ctx, w.s, "alice"), newUser(t, w.ctx, w.s, "bob")
	o := newReqDevice(1)
	oid := pollObserver(t, w.ctx, w.s, o.pub)
	for _, k := range []byte{0xA2, 0xA3} {
		pollHearAdvert(t, w.ctx, w.pool, pollKey(k), oid, w.now.Add(-time.Hour), 5)
	}
	w.claim(o, 1)
	old := w.node(0xA3)
	if _, err := w.reqs.Create(w.ctx, alice.ID, old, []byte("pw")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `UPDATE nodes SET last_seen = $1 WHERE public_key = $2`, w.now.Add(-10*24*time.Hour), pollKey(0xA3)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `UPDATE telemetry_requests SET enabled_at = $1`, w.now.Add(-9*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, err := w.s.DisableOfflineRequests(w.ctx, w.now.Add(-7*24*time.Hour), w.now, polling.ReasonOffline(7)); err != nil || n != 1 {
		t.Fatalf("disabled %d, %v", n, err)
	}
	w.now = w.now.Add(time.Minute)
	r, err := w.reqs.Create(w.ctx, bob.ID, w.node(0xA2), []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	if st := w.status(bob.ID, r.ID); st.Status != polling.StatusActive {
		t.Fatalf("younger request kept out by a disabled target's seat: status=%s reason=%q", st.Status, st.Reason)
	}
}
