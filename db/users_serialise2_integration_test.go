// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
	"time"

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
