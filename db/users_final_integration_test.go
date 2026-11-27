// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// An operator delete runs through the same locked transaction as a self-delete: a request that lands in the gap
// before the user row goes cannot leave the target without its fallback password.
func TestOperatorDeleteNewRequestPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	a, b := newUser(t, ctx, s, "alpha"), newUser(t, ctx, s, "bravo")
	target := reqKey(77)
	now := time.Now()
	ar, br := newReq(a.ID, now, true), newReq(b.ID, now.Add(time.Second), true)
	var wg sync.WaitGroup
	create := func(u users.User, r polling.Request, at time.Time) {
		wg.Add(1)
		go func() { // waits behind the delete in a locked one, completes at once in an unlocked one
			defer wg.Done()
			_ = s.MutateRequestTarget(ctx, target, u.ID, func(st *polling.TargetState) error { return st.Create(r, at) })
		}()
		time.Sleep(300 * time.Millisecond)
	}
	deleteGapHook = func() {
		create(a, ar, now)
		create(b, br, now.Add(time.Second))
	}
	t.Cleanup(func() { deleteGapHook = nil })
	if err := s.DeleteUser(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, err := s.UserByID(ctx, a.ID); !errors.Is(err, users.ErrNotFound) {
		t.Fatal("user was not deleted", err)
	}
	var active pgtype.UUID
	var ver int64
	if err := pool.QueryRow(ctx, `SELECT active_request_id, ver FROM polling_targets WHERE target_public_key=$1`, target).Scan(&active, &ver); err != nil {
		t.Fatal(err)
	}
	if !active.Valid || uuid.UUID(active.Bytes) != br.ID {
		t.Fatalf("remaining user's request not selected: active=%v ver=%d", active, ver)
	}
}

// A reset in flight when the operator disables the user must not change the password.
func TestResetAfterDisablePostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "ResetCredentials")
	plain, hash := users.NewToken()
	if err := s.CreateToken(ctx, u.ID, users.TokenReset, hash, []byte(`{"email":"`+u.Email+`"}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.ConfirmReset(ctx, plain, resetPW) }()
	<-g.arrived
	g.armed.Store(false)
	now := time.Now()
	if err := s.SetUserDisabled(ctx, u.ID, &now); err != nil {
		t.Fatal(err)
	}
	if err := s.EndAllSessions(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := <-done
	got, readErr := s.UserByID(ctx, u.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil || got.PassHash != u.PassHash {
		t.Fatalf("disabled account's reset completed: err=%v passwordChanged=%v", err, got.PassHash != u.PassHash)
	}
}

// Only the newest email-change link works: a link replaced while it was being confirmed must fail.
func TestSupersededEmailVerificationPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "ApplyEmailChange")
	addr := "new@example.org"
	plain, hash := users.NewToken()
	if err := s.BeginEmailChange(ctx, u.ID, u.PassHash, addr, hash, []byte(`{"email":"`+addr+`"}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.Verify(ctx, plain) }()
	<-g.arrived
	g.armed.Store(false)
	if err := svc.ChangeEmail(ctx, u, serialPW, addr); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := <-done
	got, readErr := s.UserByID(ctx, u.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil || got.Email != u.Email {
		t.Fatalf("superseded email confirmation succeeded: err=%v newAddressApplied=%v", err, got.Email == addr)
	}
}

// An email-change link confirmed for a user the operator disabled meanwhile does not change the address.
func TestEmailChangeAfterDisablePostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "ApplyEmailChange")
	addr := "new@example.org"
	plain, hash := users.NewToken()
	if err := s.BeginEmailChange(ctx, u.ID, u.PassHash, addr, hash, []byte(`{"email":"`+addr+`"}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.Verify(ctx, plain) }()
	<-g.arrived
	g.armed.Store(false)
	now := time.Now()
	if err := s.SetUserDisabled(ctx, u.ID, &now); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := <-done
	got, readErr := s.UserByID(ctx, u.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil || got.Email != u.Email {
		t.Fatalf("disabled user's email changed: err=%v applied=%v", err, got.Email == addr)
	}
}
