// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

func TestPollingObserverTargetsPostgres(t *testing.T) {
	// Observers report their own telemetry: no request, no election and no owners entry polls one.
	w := newReqWorld(t, "")
	ctx, s, pool := w.ctx, w.s, w.pool
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	o := newReqDevice(1)
	requested, local := byte(0xA1), byte(0xA2)
	n1 := w.node(requested)
	n2 := w.node(local)
	heard := w.now.Add(-time.Hour)
	for _, tg := range []byte{requested, local} {
		pollHearAdvert(t, ctx, pool, pollKey(tg), pollObserver(t, ctx, s, o.pub), heard, 9)
	}
	w.claim(o, 2, reqNode(pollKey(local), false, "ok"))
	r1, err := w.reqs.Create(ctx, alice.ID, n1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.status(alice.ID, r1.ID); got.Status != polling.StatusActive {
		t.Fatalf("before: %+v", got)
	}
	if e, ok := w.owners(o, requested); !ok || e.Role != "polling" || !e.Requested {
		t.Fatalf("requested before: %+v", e)
	}
	if e, ok := w.owners(o, local); !ok || e.Role != "polling" {
		t.Fatalf("local before: %+v", e)
	}

	// Both repeaters start reporting as observers.
	pollObserver(t, ctx, s, pollKey(requested))
	pollObserver(t, ctx, s, pollKey(local))

	if _, err := w.reqs.Create(ctx, bob.ID, n1, nil); !errors.Is(err, polling.ErrObserverNode) {
		t.Fatalf("create on an observer: %v", err)
	}
	if _, err := w.reqs.Create(ctx, bob.ID, n2, nil); !errors.Is(err, polling.ErrObserverNode) {
		t.Fatalf("create on a local observer: %v", err)
	}

	// The election clears both, so the owners answer no longer has the device poll either.
	if err := w.svc.ElectAll(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM polling_assignments`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("assignments left: %d %v", n, err)
	}
	if e, ok := w.owners(o, requested); ok {
		t.Fatalf("requested observer still handed out: %+v", e)
	}
	if e, ok := w.owners(o, local); !ok || e.Role != "excluded" {
		t.Fatalf("claimed observer not excluded: %+v %v", e, ok)
	}
	// A new claim re-elects through HandleClaim and still assigns nothing.
	w.now = w.now.Add(time.Minute)
	w.claim(o, 2, reqNode(pollKey(local), false, "ok"))
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM polling_assignments`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("assignments after a claim: %d %v", n, err)
	}
	targets, err := s.ListPollingTargets(ctx, w.now.Add(-polling.ClaimTTL))
	if err != nil {
		t.Fatal(err)
	}
	for _, tg := range targets {
		if bytes.Equal(tg, pollKey(local)) {
			t.Fatal("a claimed observer is still a polling target")
		}
	}

	// The sweep retires the request with the observer reason, once.
	if n, err := s.DisableObserverRequests(ctx, w.now, polling.ReasonObserver); err != nil || n != 1 {
		t.Fatalf("disabled %d %v", n, err)
	}
	got := w.status(alice.ID, r1.ID)
	if code, _ := polling.ReasonCode(got.Reason); got.Status != polling.StatusDisabled || code != polling.CodeObserver {
		t.Fatalf("after the sweep: %+v", got)
	}
	if n, _ := s.DisableObserverRequests(ctx, w.now, polling.ReasonObserver); n != 0 {
		t.Fatalf("disabled again: %d", n)
	}
	if _, err := w.reqs.Update(ctx, alice.ID, r1.ID, polling.RequestUpdate{Enable: true}); !errors.Is(err, polling.ErrObserverNode) {
		t.Fatalf("enable on an observer: %v", err)
	}
	if got := w.status(alice.ID, r1.ID); got.Status != polling.StatusDisabled {
		t.Fatalf("refused enable changed it: %+v", got)
	}
}
