// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// gate holds the calls that reach it until released, so a test can run another request in the gap.
type gate struct {
	armed   atomic.Bool
	arrived chan struct{}
	release chan struct{}
}

func newGate() *gate { return &gate{arrived: make(chan struct{}, 8), release: make(chan struct{})} }

func (g *gate) pause() {
	if g.armed.Load() {
		g.arrived <- struct{}{}
		<-g.release
	}
}

// gatedStore pauses just before a session is issued.
type gatedStore struct {
	*Store
	g *gate
}

// IssueSession is where the service stores a session: the gap is after the credential was checked and before the
// store takes its lock.
func (s gatedStore) IssueSession(ctx context.Context, sess users.Session, hash []byte, expect string, maxApp int) (users.Session, error) {
	s.g.pause()
	return s.Store.IssueSession(ctx, sess, hash, expect, maxApp)
}

const serialPW = "a-long-enough-passphrase"

func serialEnv(t *testing.T) (context.Context, *Store, *users.Service, *gate, users.User) {
	t.Helper()
	ctx, _, s := userStore(t)
	hash, err := users.HashPassword(serialPW)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	u, err := s.CreateUser(ctx, users.User{Username: "ridge", DisplayName: "ridge", Email: "ridge@example.org", PassHash: hash, Provider: "local", EmailVerifiedAt: &now})
	if err != nil {
		t.Fatal(err)
	}
	pu, _ := url.Parse("https://beacon.example.org")
	g := newGate()
	svc := users.NewService(gatedStore{s, g}, &mail.Recorder{}, config.ResolvedAccounts{Enabled: true, PublicURL: pu, RegistrationOpen: true}, time.Now, slog.New(slog.DiscardHandler))
	return ctx, s, svc, g, u
}

// resetPassword runs a full reset for the user through the service.
func resetPassword(t *testing.T, ctx context.Context, s *Store, svc *users.Service, u users.User, pw string) {
	t.Helper()
	plain, hash := users.NewToken()
	if err := s.CreateToken(ctx, u.ID, users.TokenReset, hash, []byte(`{"email":"`+u.Email+`"}`), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := svc.ConfirmReset(ctx, plain, pw); err != nil {
		t.Fatal(err)
	}
}

func TestUserLoginSerialisedWithResetPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnv(t)
	g.armed.Store(true)
	type result struct {
		tok string
		err error
	}
	done := make(chan result, 1)
	go func() {
		tok, _, _, err := svc.Login(ctx, "ridge", serialPW, users.SessionMeta{Label: "x"})
		done <- result{tok, err}
	}()
	<-g.arrived // the password was accepted; the session is not issued yet
	g.armed.Store(false)
	resetPassword(t, ctx, s, svc, u, "another-long-passphrase")
	close(g.release)
	r := <-done
	if n := liveSessionCount(t, ctx, s, u); n != 0 {
		t.Fatalf("a login that began before the reset left %d live sessions (login err %v)", n, r.err)
	}
	if r.tok != "" {
		if _, _, err := svc.Authenticate(ctx, r.tok); err == nil {
			t.Fatal("the old login's token authenticates after the reset")
		}
	}
}

func TestUserAppExchangeSerialisedWithResetPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnv(t)
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
	<-g.arrived // the code is spent and checked; the session is not stored yet
	g.armed.Store(false)
	resetPassword(t, ctx, s, svc, u, "another-long-passphrase")
	close(g.release)
	err = <-done
	if n := liveSessionCount(t, ctx, s, u); n != 0 {
		t.Fatalf("an exchange that began before the reset left %d live sessions (err %v)", n, err)
	}
}

func TestUserAppSessionCapSerialisedPostgres(t *testing.T) {
	ctx, s, svc, g, u := serialEnv(t)
	for i := 0; i < 9; i++ {
		newSession(t, ctx, s, u, "app", byte(i+1))
	}
	sum := sha256.Sum256([]byte("verifier"))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	g.armed.Store(true)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		code, err := svc.IssueAppCode(ctx, u, challenge, "phone")
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, _, err := svc.ExchangeAppCode(ctx, code, "verifier", users.SessionMeta{}); err != nil {
				t.Error(err)
			}
		}()
	}
	<-g.arrived
	<-g.arrived // both codes are spent and both exchanges wait to issue
	g.armed.Store(false)
	close(g.release)
	wg.Wait()
	if n := liveSessionCount(t, ctx, s, u); n > 10 {
		t.Fatalf("%d live app sessions, at most 10 allowed", n)
	}
}
