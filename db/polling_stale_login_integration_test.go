// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"log/slog"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

// staleLoginWorld: alice's password fails, bob's blank request becomes the option in use. The firmware keeps
// reporting login_failed for the target even while its blank polls work.
func staleLoginWorld(t *testing.T) (w *reqWorld, o reqDevice, bob func() polling.RequestRow, read func()) {
	t.Helper()
	w = newReqWorld(t, "")
	alice := newUser(t, w.ctx, w.s, "alice")
	bobUser := newUser(t, w.ctx, w.s, "bob")
	o = newReqDevice(1)
	node := w.node(0xA1)
	oid := pollObserver(t, w.ctx, w.s, o.pub)
	pollHearAdvert(t, w.ctx, w.pool, pollKey(0xA1), oid, w.now.Add(-time.Hour), 5)
	w.claim(o, 1)
	a, err := w.reqs.Create(w.ctx, alice.ID, node, []byte("bad"))
	if err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	b, err := w.reqs.Create(w.ctx, bobUser.ID, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := w.owners(o, 0xA1); !ok || e.PW == nil {
		t.Fatal("password A not offered")
	}
	w.now = w.now.Add(31 * time.Minute)
	w.claim(o, 0, reqNode(pollKey(0xA1), true, "login_failed"))
	if got := w.status(alice.ID, a.ID); got.Status != polling.StatusLoginFailed {
		t.Fatalf("password A: %+v", got)
	}
	if e, ok := w.owners(o, 0xA1); !ok || e.PW != nil {
		t.Fatalf("expected the blank option: %+v", e)
	}
	read = func() {
		t.Helper()
		r := polling.Reading{Version: 2, Observer: o.pub, Client: "fw", Target: pollKey(0xA1), Method: "status", SampledAt: w.now}
		if ok, err := w.s.InsertRepeaterReading(w.ctx, r, oid, w.now); err != nil || !ok {
			t.Fatalf("reading stored=%v err=%v", ok, err)
		}
	}
	return w, o, func() polling.RequestRow { return w.status(bobUser.ID, b.ID) }, read
}

func TestStaleLoginFailedAfterWorkingPollPostgres(t *testing.T) {
	t.Run("a working poll keeps the blank option", func(t *testing.T) {
		w, o, bob, read := staleLoginWorld(t)
		for range 3 {
			w.now = w.now.Add(20 * time.Minute)
			read()
			w.now = w.now.Add(time.Minute)
			w.claim(o, 0, reqNode(pollKey(0xA1), true, "login_failed"))
		}
		if got := bob(); got.Status != polling.StatusActive {
			t.Fatalf("working blank option stopped: status=%s reason=%s", got.Status, got.Reason)
		}
	})
	t.Run("no reading since the change still fails it", func(t *testing.T) {
		w, o, bob, _ := staleLoginWorld(t)
		w.now = w.now.Add(31 * time.Minute)
		w.claim(o, 0, reqNode(pollKey(0xA1), true, "login_failed"))
		if got := bob(); got.Status != polling.StatusLoginFailed {
			t.Fatalf("blank option kept with no working poll: status=%s", got.Status)
		}
	})
	t.Run("a reading that stopped long ago does not hold it", func(t *testing.T) {
		w, o, bob, read := staleLoginWorld(t)
		w.now = w.now.Add(5 * time.Minute)
		read()
		w.now = w.now.Add(polling.WorkingPollFresh(config.ResolvedPolling{}) + time.Minute)
		w.claim(o, 0, reqNode(pollKey(0xA1), true, "login_failed"))
		if got := bob(); got.Status != polling.StatusLoginFailed {
			t.Fatalf("an old reading kept the option: status=%s", got.Status)
		}
	})
}

// Cleanup with the shortest supported reading_retention must not delete the reading the guard needs.
func TestStaleLoginFailedSurvivesReadingCleanupPostgres(t *testing.T) {
	w, o, bob, read := staleLoginWorld(t)
	cfg := config.ResolvedPolling{Enabled: true, RequestObservers: config.RequestObserversAll, DirectWindow: 2 * time.Hour, ReadingRetention: time.Hour}
	w.svc = polling.NewService(w.s, cfg, func() time.Time { return w.now }, slog.New(slog.DiscardHandler))
	w.svc.SetRequests(w.s, w.sealer)
	w.now = w.now.Add(5 * time.Minute)
	read()
	w.now = w.now.Add(30 * time.Minute)
	w.claim(o, 0, reqNode(pollKey(0xA1), true, "login_failed"))
	if got := bob(); got.Status != polling.StatusActive {
		t.Fatalf("before cleanup: %+v", got)
	}
	w.now = w.now.Add(31 * time.Minute)
	if err := w.s.DeleteExpiredPolling(w.ctx, w.now.Add(-24*time.Hour), w.now.Add(-polling.ReadingKeep(cfg))); err != nil {
		t.Fatal(err)
	}
	w.claim(o, 0, reqNode(pollKey(0xA1), true, "login_failed"))
	if got := bob(); got.Status != polling.StatusActive {
		t.Fatalf("working blank request stopped after cleanup: status=%s reason=%s", got.Status, got.Reason)
	}
}
