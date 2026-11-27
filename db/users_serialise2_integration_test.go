// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

const resetPW = "another-long-passphrase"

func TestUserAppExchangeAfterAResetBetweenSpendAndIssuePostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "UserByID")
	sum := sha256.Sum256([]byte("verifier"))
	code, err := svc.IssueAppCode(ctx, u, base64.RawURLEncoding.EncodeToString(sum[:]), "phone")
	if err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, _, _, err := svc.ExchangeAppCode(ctx, code, "verifier", users.SessionMeta{})
		done <- err
	}()
	<-g.arrived // the code is spent; the user (and so the password hash) is not read yet
	g.armed.Store(false)
	resetPassword(t, ctx, s, svc, u, resetPW)
	close(g.release)
	if err := <-done; !errors.Is(err, users.ErrTokenInvalid) {
		t.Fatalf("exchange after a reset: %v", err)
	}
	if n := liveSessionCount(t, ctx, s, u); n != 0 {
		t.Fatalf("%d live sessions after the reset", n)
	}
}

// sessionFor stores a live web session for the user and returns its id.
func sessionFor(t *testing.T, s *Store, u users.User) users.Session {
	t.Helper()
	return newSession(t, t.Context(), s, u, "web", 0x7f)
}

func TestUserChangePasswordAfterAResetPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "ChangeCredentials")
	keep := sessionFor(t, s, u)
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.ChangePassword(ctx, u, keep.ID, serialPW, "mine-is-a-long-passphrase") }()
	<-g.arrived // the current password was checked
	g.armed.Store(false)
	resetPassword(t, ctx, s, svc, u, resetPW)
	close(g.release)
	if err := <-done; err == nil {
		t.Fatal("a password change that began before a reset went through")
	}
	if _, _, _, err := svc.Login(ctx, "ridge", resetPW, users.SessionMeta{}); err != nil {
		t.Fatalf("the reset password was overwritten: %v", err)
	}
}

func TestUserChangePasswordRefusesAnEndedSessionPostgres(t *testing.T) {
	ctx, s, svc, _, u := serialEnv(t)
	keep := sessionFor(t, s, u)
	if err := s.EndSession(ctx, u.ID, keep.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.ChangePassword(ctx, u, keep.ID, serialPW, "mine-is-a-long-passphrase"); err == nil {
		t.Fatal("a password change kept a session that was already ended")
	}
	if _, _, _, err := svc.Login(ctx, "ridge", serialPW, users.SessionMeta{}); err != nil {
		t.Fatalf("the password changed anyway: %v", err)
	}
}

func TestUserChangeEmailAfterAResetPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "BeginEmailChange")
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.ChangeEmail(ctx, u, serialPW, "new@example.org") }()
	<-g.arrived
	g.armed.Store(false)
	resetPassword(t, ctx, s, svc, u, resetPW)
	close(g.release)
	if err := <-done; err == nil {
		t.Fatal("an email change that began before a reset went through")
	}
	got, err := s.UserByID(ctx, u.ID)
	if err != nil || got.PendingEmail != nil {
		t.Fatalf("pending email %v %v", got.PendingEmail, err)
	}
}

func TestUserDeleteAccountAfterAResetPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "DeleteUserChecked")
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.DeleteAccount(ctx, u, serialPW) }()
	<-g.arrived
	g.armed.Store(false)
	resetPassword(t, ctx, s, svc, u, resetPW)
	close(g.release)
	if err := <-done; err == nil {
		t.Fatal("an account deletion that began before a reset went through")
	}
	if _, err := s.UserByID(ctx, u.ID); err != nil {
		t.Fatalf("account deleted: %v", err)
	}
	_ = time.Now
}

// A reset that lands after the delete's hash check must neither remove the account's requests nor leave the
// account behind without them: the check and the delete are one locked transaction.
func TestUserDeleteAccountKeepsRequestsWhenAResetLandsInTheGapPostgres(t *testing.T) {
	ctx, s, svc, _, u := serialEnv(t)
	target := reqKey(9)
	err := s.MutateRequestTarget(ctx, target, u.ID, func(st *polling.TargetState) error {
		return st.Create(newReq(u.ID, time.Now(), true), time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	// The token exists before the delete starts; the reset itself runs in the gap and may find the user gone.
	plain, hash := users.NewToken()
	if err := s.CreateToken(ctx, u.ID, users.TokenReset, hash, []byte(`{"email":"`+u.Email+`"}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	resetDone := make(chan struct{})
	deleteGapHook = func() {
		go func() {
			defer close(resetDone)
			_ = svc.ConfirmReset(ctx, plain, resetPW)
		}()
		select { // a locked delete makes the reset wait, so give up waiting
		case <-resetDone:
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Cleanup(func() { deleteGapHook = nil })
	delErr := svc.DeleteAccount(ctx, u, serialPW)
	<-resetDone
	_, userErr := s.UserByID(ctx, u.ID)
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM telemetry_requests WHERE user_id = $1`, u.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if userErr == nil && n == 0 {
		t.Fatalf("account survived (delete: %v) but its requests were removed", delErr)
	}
	if userErr == nil && delErr == nil {
		t.Fatal("delete reported success but the account is still there")
	}
}

// A reset must not outlive a credential change that completes while it is in flight.
func TestUserResetAfterAPasswordChangePostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "ResetCredentials")
	keep := sessionFor(t, s, u)
	plain, hash := users.NewToken()
	if err := s.CreateToken(ctx, u.ID, users.TokenReset, hash, []byte(`{"email":"`+u.Email+`"}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- svc.ConfirmReset(ctx, plain, resetPW) }()
	<-g.arrived
	g.armed.Store(false)
	if err := svc.ChangePassword(ctx, u, keep.ID, serialPW, "mine-is-a-long-passphrase"); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	if err := <-done; err == nil {
		t.Fatal("a reset in flight overwrote a completed password change")
	}
	if _, _, _, err := svc.Login(ctx, "ridge", "mine-is-a-long-passphrase", users.SessionMeta{}); err != nil {
		t.Fatalf("the changed password was overwritten: %v", err)
	}
}

// Two links issued before either was used: once one reset succeeded the other must fail.
func TestUserTwoResetLinksPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnvAt(t, "ResetCredentials")
	var plains [2]string
	for i := range plains {
		p, hash := users.NewToken()
		plains[i] = p
		if err := s.CreateToken(ctx, u.ID, users.TokenReset, hash, []byte(`{"email":"`+u.Email+`"}`), time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	g.armed.Store(true)
	done := make(chan error, 2)
	for i, pw := range []string{resetPW, "third-long-passphrase"} {
		go func() { done <- svc.ConfirmReset(ctx, plains[i], pw) }()
	}
	<-g.arrived
	<-g.arrived
	g.armed.Store(false)
	close(g.release)
	ok := 0
	for range plains {
		if err := <-done; err == nil {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("%d resets succeeded, want 1", ok)
	}
}
