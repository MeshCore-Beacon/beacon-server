// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
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
	g     *gate
	where string // the method that pauses
}

func (s gatedStore) at(name string) {
	if s.where == name {
		s.g.pause()
	}
}

// UserByID is read by an app exchange right after its code is spent.
func (s gatedStore) UserByID(ctx context.Context, id uuid.UUID) (users.User, error) {
	s.at("UserByID")
	return s.Store.UserByID(ctx, id)
}

func (s gatedStore) BeginEmailChange(ctx context.Context, id uuid.UUID, expect, addr string, tokenHash, data []byte, expires time.Time) error {
	s.at("BeginEmailChange")
	return s.Store.BeginEmailChange(ctx, id, expect, addr, tokenHash, data, expires)
}

func (s gatedStore) ChangeCredentials(ctx context.Context, id uuid.UUID, hash string, verifiedAt *time.Time, keep uuid.UUID, expect string) error {
	if expect != "" { // a user-authorised change, not the reset that runs in the gap
		s.at("ChangeCredentials")
	}
	return s.Store.ChangeCredentials(ctx, id, hash, verifiedAt, keep, expect)
}

func (s gatedStore) ResetCredentials(ctx context.Context, tokenHash []byte, id uuid.UUID, hash string, verifiedAt *time.Time, now time.Time) error {
	s.at("ResetCredentials")
	return s.Store.ResetCredentials(ctx, tokenHash, id, hash, verifiedAt, now)
}

func (s gatedStore) ApplyEmailChange(ctx context.Context, tokenHash []byte, now time.Time) (uuid.UUID, string, string, error) {
	s.at("ApplyEmailChange")
	return s.Store.ApplyEmailChange(ctx, tokenHash, now)
}

func (s gatedStore) DeleteUserChecked(ctx context.Context, id uuid.UUID, expect string) error {
	s.at("DeleteUserChecked")
	return s.Store.DeleteUserChecked(ctx, id, expect)
}

// IssueSession is where the service stores a session: the gap is after the credential was checked and before the
// store takes its lock.
func (s gatedStore) IssueSession(ctx context.Context, sess users.Session, hash []byte, expect string, maxApp int) (users.Session, error) {
	s.at("IssueSession")
	return s.Store.IssueSession(ctx, sess, hash, expect, maxApp)
}

const serialPW = "a-long-enough-passphrase"

func serialEnv(t *testing.T) (context.Context, *Store, *users.Service, *gate, users.User) {
	t.Helper()
	return serialEnvAt(t, "IssueSession")
}

func serialEnvAt(t *testing.T, where string) (context.Context, *Store, *users.Service, *gate, users.User) {
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
	svc := users.NewService(gatedStore{s, g, where}, &mail.Recorder{}, config.ResolvedAccounts{Enabled: true, PublicURL: pu, RegistrationOpen: true}, time.Now, slog.New(slog.DiscardHandler))
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

// clockEnv is serialEnv with a clock the test moves, a mail recorder and the mail worker running.
func clockEnv(t *testing.T) (context.Context, *Store, *users.Service, *mail.Recorder, *time.Time) {
	t.Helper()
	ctx, _, s := userStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	rec := &mail.Recorder{}
	pu, _ := url.Parse("https://beacon.example.org")
	svc := users.NewService(s, rec, config.ResolvedAccounts{Enabled: true, PublicURL: pu, RegistrationOpen: true}, func() time.Time { return now }, slog.New(slog.DiscardHandler))
	wctx, cancel := context.WithCancel(ctx)
	done := svc.Start(wctx)
	t.Cleanup(func() { cancel(); <-done })
	return ctx, s, svc, rec, &now
}

var tokenInMail = regexp.MustCompile(`token=([A-Za-z0-9_%-]+)`)

func mailTokens(t *testing.T, ctx context.Context, svc *users.Service, rec *mail.Recorder) []mail.Message {
	t.Helper()
	if err := svc.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	return rec.Messages()
}

func lastToken(t *testing.T, m mail.Message) string {
	t.Helper()
	g := tokenInMail.FindStringSubmatch(m.Text)
	if g == nil {
		t.Fatalf("no link in %q", m.Text)
	}
	v, _ := url.QueryUnescape(g[1])
	return v
}

func TestUserResetTokenBoundToItsAddressPostgres(t *testing.T) {
	ctx, s, svc, rec, now := clockEnv(t)
	hash, _ := users.HashPassword(serialPW)
	verified := *now
	u, err := s.CreateUser(ctx, users.User{Username: "ridge", DisplayName: "ridge", Email: "old@example.org", PassHash: hash, Provider: "local", EmailVerifiedAt: &verified})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.RequestReset(ctx, "old@example.org"); err != nil {
		t.Fatal(err)
	}
	oldLink := lastToken(t, mailTokens(t, ctx, svc, rec)[0])
	// The account moves to a new address, confirmed from that mailbox.
	if err := svc.ChangeEmail(ctx, u, serialPW, "new@example.org"); err != nil {
		t.Fatal(err)
	}
	msgs := mailTokens(t, ctx, svc, rec)
	if err := svc.Verify(ctx, lastToken(t, msgs[len(msgs)-1])); err != nil {
		t.Fatal(err)
	}
	if err := svc.ConfirmReset(ctx, oldLink, "another-long-passphrase"); !errors.Is(err, users.ErrTokenInvalid) {
		t.Fatalf("a reset link mailed to the previous address still works: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM user_tokens WHERE user_id = $1 AND kind = 'reset' AND used_at IS NULL`, u.ID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d reset tokens left after the email change (%v)", n, err)
	}
	// A reset asked for the new address works, and the password does change.
	// The previous mailbox was told the address changed.
	msgs = mailTokens(t, ctx, svc, rec)
	var told bool
	for _, m := range msgs {
		told = told || (m.To == "old@example.org" && m.Subject == "The email address of your Beacon account was changed")
	}
	if !told {
		t.Fatalf("the old address was not told: %+v", msgs)
	}
	if err := svc.RequestReset(ctx, "new@example.org"); err != nil {
		t.Fatal(err)
	}
	msgs = mailTokens(t, ctx, svc, rec)
	if err := svc.ConfirmReset(ctx, lastToken(t, msgs[len(msgs)-1]), "another-long-passphrase"); err != nil {
		t.Fatal(err)
	}
	msgs = mailTokens(t, ctx, svc, rec)
	if m := msgs[len(msgs)-1]; m.To != "new@example.org" || m.Subject != "Your Beacon password was changed" {
		t.Fatalf("no password-changed mail: %+v", m)
	}
}

func TestUserResendRecoversAnExpiredLinkPostgres(t *testing.T) {
	ctx, s, svc, rec, now := clockEnv(t)
	if err := svc.Register(ctx, "newbie", "newbie@example.org", serialPW); err != nil {
		t.Fatal(err)
	}
	if n := len(mailTokens(t, ctx, svc, rec)); n != 1 {
		t.Fatalf("%d mails", n)
	}
	*now = now.Add(25 * time.Hour) // the link has expired
	if err := svc.ResendVerification(ctx, "newbie@example.org"); err != nil {
		t.Fatal(err)
	}
	msgs := mailTokens(t, ctx, svc, rec)
	if len(msgs) != 2 || msgs[1].To != "newbie@example.org" || !strings.Contains(msgs[1].Text, "newbie") {
		t.Fatalf("resend sent %d mails", len(msgs))
	}
	if err := svc.Verify(ctx, lastToken(t, msgs[1])); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := svc.Login(ctx, "newbie", serialPW, users.SessionMeta{}); err != nil {
		t.Fatalf("the recovered sign-up did not keep its username and password: %v", err)
	}
	_ = s
}

func TestUserPendingSignupKeptForSevenDaysPostgres(t *testing.T) {
	ctx, s, svc, rec, now := clockEnv(t)
	if err := svc.Register(ctx, "newbie", "newbie@example.org", serialPW); err != nil {
		t.Fatal(err)
	}
	mailTokens(t, ctx, svc, rec)
	*now = now.Add(6 * 24 * time.Hour) // link long expired, row inside its 7 days
	if err := s.DeleteExpiredAuth(ctx, *now); err != nil {
		t.Fatal(err)
	}
	if err := svc.ResendVerification(ctx, "newbie@example.org"); err != nil {
		t.Fatal(err)
	}
	if n := len(mailTokens(t, ctx, svc, rec)); n != 2 {
		t.Fatalf("a pending sign-up inside its 7 days could not resend: %d mails", n)
	}
	*now = now.Add(3 * 24 * time.Hour)
	if err := s.DeleteExpiredAuth(ctx, *now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UserByEmail(ctx, "newbie@example.org"); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("an abandoned sign-up outlived its link and its 7 days: %v", err)
	}
}
