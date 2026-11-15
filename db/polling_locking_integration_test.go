// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
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
