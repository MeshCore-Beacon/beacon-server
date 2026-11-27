// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/google/uuid"
)

// ---- fakes ----

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type sessRec struct {
	Session
	hash  string
	ended bool
}

type tokRec struct {
	kind    TokenKind
	user    uuid.UUID
	data    []byte
	expires time.Time
	used    bool
	seq     int
}

type fakeStore struct {
	mu          sync.Mutex
	clk         *clock
	users       map[uuid.UUID]*User
	sessions    map[uuid.UUID]*sessRec
	tokens      map[string]*tokRec
	touchCalls  int
	tokSeq      int
	setPassHash int
}

func newFakeStore(c *clock) *fakeStore {
	return &fakeStore{clk: c, users: map[uuid.UUID]*User{}, sessions: map[uuid.UUID]*sessRec{}, tokens: map[string]*tokRec{}}
}

func (f *fakeStore) CreateUser(_ context.Context, u User) (User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.users {
		if strings.EqualFold(o.Username, u.Username) {
			return User{}, ErrUsernameTaken
		}
		if u.Email != "" && o.Email == strings.ToLower(u.Email) {
			return User{}, ErrEmailTaken
		}
	}
	u.ID = uuid.New()
	u.Email = strings.ToLower(u.Email)
	u.CreatedAt = f.clk.Now()
	cp := u
	f.users[u.ID] = &cp
	return u, nil
}
func (f *fakeStore) get(id uuid.UUID) (*User, error) {
	if u, ok := f.users[id]; ok {
		return u, nil
	}
	return nil, ErrNotFound
}
func (f *fakeStore) UserByID(_ context.Context, id uuid.UUID) (User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err != nil {
		return User{}, err
	}
	return *u, nil
}
func (f *fakeStore) UserByLogin(ctx context.Context, login string) (User, error) {
	if strings.Contains(login, "@") {
		return f.UserByEmail(ctx, login)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.users {
		if strings.EqualFold(u.Username, login) {
			return *u, nil
		}
	}
	return User{}, ErrNotFound
}
func (f *fakeStore) UserByEmail(_ context.Context, email string) (User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.users {
		if u.Email == strings.ToLower(email) {
			return *u, nil
		}
	}
	return User{}, ErrNotFound
}
func (f *fakeStore) UpdateUserDisplayName(_ context.Context, id uuid.UUID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err == nil {
		u.DisplayName = name
	}
	return err
}
func (f *fakeStore) SetUserPassword(_ context.Context, id uuid.UUID, hash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err == nil {
		u.PassHash = hash
		f.setPassHash++
	}
	return err
}
func (f *fakeStore) SetUserEmailVerified(_ context.Context, id uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err == nil {
		u.EmailVerifiedAt = &at
	}
	return err
}
func (f *fakeStore) SetUserPendingEmail(_ context.Context, id uuid.UUID, email *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err == nil {
		u.PendingEmail = nil
		if email != nil {
			v := strings.ToLower(*email)
			u.PendingEmail = &v
		}
	}
	return err
}
func (f *fakeStore) ApplyPendingEmail(_ context.Context, id uuid.UUID, expected string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err != nil {
		return err
	}
	if u.PendingEmail == nil || *u.PendingEmail != strings.ToLower(expected) {
		return ErrNotFound
	}
	for _, o := range f.users {
		if o.ID != id && o.Email == *u.PendingEmail {
			return ErrEmailTaken
		}
	}
	now := f.clk.Now()
	u.Email, u.PendingEmail, u.EmailVerifiedAt = *u.PendingEmail, nil, &now
	for h, t := range f.tokens {
		if t.user == id && t.kind == TokenReset {
			delete(f.tokens, h)
		}
	}
	return nil
}
func (f *fakeStore) ApplyEmailChange(ctx context.Context, hash []byte, now time.Time) (uuid.UUID, string, string, error) {
	id, raw, err := f.ConsumeToken(ctx, TokenEmailChange, hash, now)
	if err != nil {
		return uuid.Nil, "", "", err
	}
	var d tokenData
	if json.Unmarshal(raw, &d) != nil || d.Email == "" {
		return uuid.Nil, "", "", ErrNotFound
	}
	f.mu.Lock()
	u, gerr := f.get(id)
	old := ""
	if gerr == nil {
		old = u.Email
	}
	f.mu.Unlock()
	if gerr != nil {
		return uuid.Nil, "", "", gerr
	}
	if err := f.ApplyPendingEmail(ctx, id, d.Email); err != nil {
		return uuid.Nil, "", "", err
	}
	return id, old, strings.ToLower(d.Email), nil
}
func (f *fakeStore) ActivateUser(_ context.Context, id uuid.UUID, username, hash string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err != nil || u.EmailVerifiedAt != nil || u.DisabledAt != nil {
		return ErrNotFound
	}
	for _, o := range f.users {
		if o.ID != id && strings.EqualFold(o.Username, username) {
			return ErrUsernameTaken
		}
	}
	u.Username, u.DisplayName, u.PassHash, u.EmailVerifiedAt = username, username, hash, &at
	for h, t := range f.tokens {
		if t.user == id && t.kind == TokenVerify {
			delete(f.tokens, h)
		}
	}
	for _, s := range f.sessions {
		if s.UserID == id {
			s.ended = true
		}
	}
	return nil
}
func (f *fakeStore) ResetCredentials(ctx context.Context, hash []byte, id uuid.UUID, pass string, verifiedAt *time.Time, now time.Time) error {
	f.mu.Lock()
	t, ok := f.tokens[string(hash)]
	if !ok || t.kind != TokenReset || t.used || !now.Before(t.expires) || t.user != id {
		f.mu.Unlock()
		return ErrNotFound
	}
	var d tokenData
	u, err := f.get(t.user)
	if err != nil || json.Unmarshal(t.data, &d) != nil || d.Email != u.Email {
		f.mu.Unlock()
		return ErrNotFound
	}
	t.used = true
	f.mu.Unlock()
	return f.ChangeCredentials(ctx, id, pass, verifiedAt, uuid.Nil, "")
}
func (f *fakeStore) LatestToken(_ context.Context, userID uuid.UUID, kind TokenKind) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var best *tokRec
	for _, t := range f.tokens {
		if t.user == userID && t.kind == kind && !t.used && (best == nil || t.seq > best.seq) {
			best = t
		}
	}
	if best == nil {
		return nil, ErrNotFound
	}
	return best.data, nil
}
func (f *fakeStore) PeekToken(_ context.Context, kind TokenKind, hash []byte, now time.Time) (uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tokens[string(hash)]
	if !ok || t.kind != kind || t.used || !now.Before(t.expires) {
		return uuid.Nil, ErrNotFound
	}
	return t.user, nil
}
func (f *fakeStore) TouchUserLogin(_ context.Context, id uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err == nil {
		u.LastLoginAt = &at
	}
	return err
}
func (f *fakeStore) DeleteUser(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.get(id); err != nil {
		return err
	}
	delete(f.users, id)
	return nil
}
func (f *fakeStore) CreateSession(_ context.Context, sess Session, hash []byte) (Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sess.ID = uuid.New()
	sess.CreatedAt, sess.LastUsedAt = f.clk.Now(), f.clk.Now()
	f.sessions[sess.ID] = &sessRec{Session: sess, hash: string(hash)}
	return sess, nil
}
func (f *fakeStore) SessionByTokenHash(_ context.Context, hash []byte, now time.Time) (Session, User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.sessions {
		if s.hash != string(hash) || s.ended || !now.Before(s.ExpiresAt) || (s.HardExpiresAt != nil && !now.Before(*s.HardExpiresAt)) {
			continue
		}
		u, err := f.get(s.UserID)
		if err != nil || u.DisabledAt != nil {
			continue
		}
		return s.Session, *u, nil
	}
	return Session{}, User{}, ErrNotFound
}
func (f *fakeStore) TouchSession(_ context.Context, id uuid.UUID, lastUsed, expires time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sessions[id]
	if !ok {
		return ErrNotFound
	}
	s.LastUsedAt, s.ExpiresAt = lastUsed, expires
	f.touchCalls++
	return nil
}
func (f *fakeStore) live(userID uuid.UUID, now time.Time) []*sessRec {
	var out []*sessRec
	for _, s := range f.sessions {
		if s.UserID == userID && !s.ended && now.Before(s.ExpiresAt) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}
func (f *fakeStore) ListSessions(_ context.Context, userID uuid.UUID, now time.Time) ([]Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Session
	for _, s := range f.live(userID, now) {
		out = append(out, s.Session)
	}
	return out, nil
}
func (f *fakeStore) EndSession(_ context.Context, userID, sessionID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.sessions[sessionID]; ok && s.UserID == userID {
		s.ended = true
	}
	return nil
}
func (f *fakeStore) EndOtherSessions(_ context.Context, userID, keep uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.sessions {
		if s.UserID == userID && s.ID != keep {
			s.ended = true
		}
	}
	return nil
}
func (f *fakeStore) EndAllSessions(_ context.Context, userID uuid.UUID) error {
	return f.EndOtherSessions(context.Background(), userID, uuid.Nil)
}
func (f *fakeStore) EndOldestAppSessions(_ context.Context, userID uuid.UUID, keep int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var apps []*sessRec
	for _, s := range f.live(userID, f.clk.Now()) {
		if s.Kind == "app" {
			apps = append(apps, s)
		}
	}
	for i := 0; i < len(apps)-keep; i++ {
		apps[i].ended = true
	}
	return nil
}
func (f *fakeStore) IssueSession(ctx context.Context, sess Session, hash []byte, expect string, maxApp int) (Session, error) {
	f.mu.Lock()
	u, err := f.get(sess.UserID)
	if err != nil || u.DisabledAt != nil || u.PassHash != expect {
		f.mu.Unlock()
		return Session{}, ErrNotFound
	}
	f.mu.Unlock()
	if sess.Kind == "app" && maxApp > 0 {
		f.EndOldestAppSessions(ctx, sess.UserID, maxApp-1)
	}
	return f.CreateSession(ctx, sess, hash)
}
func (f *fakeStore) checked(id uuid.UUID, expect string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err != nil || (expect != "" && (u.DisabledAt != nil || u.PassHash != expect)) {
		return ErrNotFound
	}
	return nil
}
func (f *fakeStore) ChangeCredentials(ctx context.Context, id uuid.UUID, hash string, verifiedAt *time.Time, keep uuid.UUID, expect string) error {
	if err := f.checked(id, expect); err != nil {
		return err
	}
	if expect != "" && keep != uuid.Nil {
		f.mu.Lock()
		s, ok := f.sessions[keep]
		live := ok && s.UserID == id && !s.ended
		f.mu.Unlock()
		if !live {
			return ErrNotFound
		}
	}
	if err := f.SetUserPassword(ctx, id, hash); err != nil {
		return err
	}
	if verifiedAt != nil {
		if err := f.SetUserEmailVerified(ctx, id, *verifiedAt); err != nil {
			return err
		}
	}
	f.EndOtherSessions(ctx, id, keep)
	for _, k := range []TokenKind{TokenVerify, TokenReset, TokenEmailChange, TokenAppCode} {
		f.DeleteUserTokens(ctx, id, k)
	}
	return f.SetUserPendingEmail(ctx, id, nil)
}
func (f *fakeStore) BeginEmailChange(ctx context.Context, id uuid.UUID, expect, addr string, tokenHash, data []byte, expires time.Time) error {
	if err := f.checked(id, expect); err != nil {
		return err
	}
	f.DeleteUserTokens(ctx, id, TokenEmailChange)
	if err := f.SetUserPendingEmail(ctx, id, &addr); err != nil {
		return err
	}
	return f.CreateToken(ctx, id, TokenEmailChange, tokenHash, data, expires)
}
func (f *fakeStore) DeleteUserChecked(ctx context.Context, id uuid.UUID, expect string) error {
	if err := f.checked(id, expect); err != nil {
		return err
	}
	return f.DeleteUser(ctx, id)
}
func (f *fakeStore) RehashPassword(_ context.Context, id uuid.UUID, oldHash, newHash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, err := f.get(id)
	if err != nil || u.PassHash != oldHash {
		return false, err
	}
	u.PassHash = newHash
	f.setPassHash++
	return true, nil
}
func (f *fakeStore) CreateToken(_ context.Context, userID uuid.UUID, kind TokenKind, hash, data []byte, expires time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokSeq++
	f.tokens[string(hash)] = &tokRec{kind: kind, user: userID, data: data, expires: expires, seq: f.tokSeq}
	return nil
}
func (f *fakeStore) ConsumeToken(_ context.Context, kind TokenKind, hash []byte, now time.Time) (uuid.UUID, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tokens[string(hash)]
	if !ok || t.kind != kind || t.used || !now.Before(t.expires) {
		return uuid.Nil, nil, ErrNotFound
	}
	t.used = true
	return t.user, t.data, nil
}
func (f *fakeStore) DeleteUserTokens(_ context.Context, userID uuid.UUID, kind TokenKind) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for h, t := range f.tokens {
		if t.user == userID && t.kind == kind {
			delete(f.tokens, h)
		}
	}
	return nil
}

// ---- environment ----

type env struct {
	t     *testing.T
	svc   *Service
	store *fakeStore
	mail  *mail.Recorder
	clk   *clock
	logs  *bytes.Buffer
}

const goodPW = "a-long-enough-passphrase"

func newEnv(t *testing.T) *env {
	t.Helper()
	pu, _ := url.Parse("https://beacon.example.org")
	clk := &clock{t: time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)}
	e := &env{t: t, store: newFakeStore(clk), mail: &mail.Recorder{}, clk: clk, logs: &bytes.Buffer{}}
	log := slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e.svc = NewService(e.store, e.mail, config.ResolvedAccounts{Enabled: true, PublicURL: pu, RegistrationOpen: true}, clk.Now, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := e.svc.Start(ctx)
	t.Cleanup(func() { cancel(); <-done })
	return e
}

// msgs waits for the mail queue to drain, then returns what was sent.
func (e *env) msgs() []mail.Message {
	e.t.Helper()
	if err := e.svc.Flush(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	return e.mail.Messages()
}

var tokenRe = regexp.MustCompile(`token=([A-Za-z0-9_-]+)`)

func tokenIn(t *testing.T, m mail.Message) string {
	t.Helper()
	g := tokenRe.FindStringSubmatch(m.Text)
	if g == nil {
		t.Fatalf("no token in %q", m.Text)
	}
	return g[1]
}

func (e *env) last() mail.Message {
	e.t.Helper()
	m := e.msgs()
	if len(m) == 0 {
		e.t.Fatal("no mail sent")
	}
	return m[len(m)-1]
}

// user registers and verifies a user, then clears the mailbox view by remembering its length.
func (e *env) user(name string) User {
	e.t.Helper()
	ctx := context.Background()
	if err := e.svc.Register(ctx, name, name+"@example.org", goodPW); err != nil {
		e.t.Fatal(err)
	}
	if err := e.svc.Verify(ctx, tokenIn(e.t, e.last())); err != nil {
		e.t.Fatal(err)
	}
	u, _ := e.store.UserByLogin(ctx, name)
	return u
}

func (e *env) login(name string) (string, Session, User) {
	e.t.Helper()
	tok, s, u, err := e.svc.Login(context.Background(), name, goodPW, SessionMeta{Label: "Test"})
	if err != nil {
		e.t.Fatal(err)
	}
	return tok, s, u
}

var bg = context.Background()

// ---- register and verify ----

func TestRegisterCreatesUnverifiedUserAndMailsLink(t *testing.T) {
	e := newEnv(t)
	if err := e.svc.Register(bg, "Ridge", " Ridge@Example.org ", goodPW); err != nil {
		t.Fatal(err)
	}
	u, err := e.store.UserByLogin(bg, "ridge")
	if err != nil || u.EmailVerifiedAt != nil || u.Email != "ridge@example.org" || u.DisplayName != "Ridge" || u.Provider != "local" {
		t.Fatalf("user %+v %v", u, err)
	}
	if ok, _ := VerifyPassword(u.PassHash, goodPW); !ok {
		t.Fatal("stored hash does not verify")
	}
	msgs := e.msgs()
	if len(msgs) != 1 || msgs[0].To != "ridge@example.org" {
		t.Fatalf("mail: %+v", msgs)
	}
	line := ""
	for _, l := range strings.Split(msgs[0].Text, "\n") {
		if strings.HasPrefix(l, "https://beacon.example.org/?view=verify#token=") {
			line = l
		}
	}
	if line == "" || !strings.Contains(msgs[0].Text, "beacon.example.org") {
		t.Fatalf("link not on its own line: %q", msgs[0].Text)
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[0])); err != nil {
		t.Fatal(err)
	}
	if u, _ = e.store.UserByLogin(bg, "ridge"); u.EmailVerifiedAt == nil {
		t.Fatal("not verified")
	}
}

func TestRegisterValidation(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name, user, email, pw string
		want                  error
	}{
		{"short username", "ab", "a@example.org", goodPW, ErrInvalidInput},
		{"long username", strings.Repeat("a", 31), "a@example.org", goodPW, ErrInvalidInput},
		{"bad char", "bad name", "a@example.org", goodPW, ErrInvalidInput},
		{"at sign", "a@b.c", "a@example.org", goodPW, ErrInvalidInput},
		{"reserved", "Admin", "a@example.org", goodPW, ErrUsernameTaken},
		{"no email", "okname", "nope", goodPW, ErrInvalidInput},
		{"display name", "okname", "Bob <bob@example.org>", goodPW, ErrInvalidInput},
		{"comment", "okname", "bob@example.org (hi)", goodPW, ErrInvalidInput},
		{"two addresses", "okname", "a@example.org, b@example.org", goodPW, ErrInvalidInput},
		{"too long email", "okname", strings.Repeat("a", 250) + "@example.org", goodPW, ErrInvalidInput},
		{"weak", "okname", "a@example.org", "short", ErrWeakPassword},
		{"password has username", "okname", "a@example.org", "xx-okname-xx-xx", ErrWeakPassword},
	}
	for _, c := range cases {
		if err := e.svc.Register(bg, c.user, c.email, c.pw); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v", c.name, err)
		}
	}
	if len(e.msgs()) != 0 || len(e.store.users) != 0 {
		t.Fatal("rejected registrations must leave nothing behind")
	}
}

func TestRegisterClosedComesFirst(t *testing.T) {
	e := newEnv(t)
	e.svc.cfg.RegistrationOpen = false
	if err := e.svc.Register(bg, "x", "bad", "short"); !errors.Is(err, ErrRegistrationClosed) {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterDuplicateEmailDoesNotReveal(t *testing.T) {
	e := newEnv(t)
	if err := e.svc.Register(bg, "first", "dup@example.org", goodPW); err != nil {
		t.Fatal(err)
	}
	// Still unverified: a second try replaces the pending account and sends a new verify mail.
	if err := e.svc.Register(bg, "second", "dup@example.org", goodPW); err != nil {
		t.Fatal(err)
	}
	msgs := e.msgs()
	if len(msgs) != 2 || msgs[1].To != "dup@example.org" {
		t.Fatalf("mail: %+v", msgs)
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[1])); err != nil {
		t.Fatal(err)
	}
	// Verified: nothing is sent, nothing changes, still nil.
	if err := e.svc.Register(bg, "third", "dup@example.org", "some other long passphrase"); err != nil {
		t.Fatal(err)
	}
	if n := len(e.msgs()); n != 2 {
		t.Fatalf("%d mails", n)
	}
	if len(e.store.users) != 1 {
		t.Fatal("duplicate email created a user")
	}
	u, err := e.store.UserByEmail(bg, "dup@example.org")
	if err != nil || u.Username != "second" {
		t.Fatalf("verified account changed: %+v %v", u, err)
	}
	if ok, _ := VerifyPassword(u.PassHash, goodPW); !ok {
		t.Fatal("verified account's password changed")
	}
}

// Whichever link the owner of the address opens decides the account, so a stranger who registered the address
// first or second ends up with nothing.
func TestPreRegistrationAttackerFirstVictimSecond(t *testing.T) {
	e := newEnv(t)
	const p1, p2 = "attackers long passphrase", "the real owners passphrase"
	if err := e.svc.Register(bg, "mallory", "victim@example.org", p1); err != nil {
		t.Fatal(err)
	}
	pending, _ := e.store.UserByEmail(bg, "victim@example.org")
	sess, _ := e.store.CreateSession(bg, Session{UserID: pending.ID, Kind: "web", ExpiresAt: e.clk.Now().Add(time.Hour)}, []byte("old"))
	if err := e.svc.Register(bg, "victim", "victim@example.org", p2); err != nil {
		t.Fatal(err)
	}
	row, _ := e.store.UserByEmail(bg, "victim@example.org")
	if len(e.store.users) != 1 || row.Username != "mallory" || row.EmailVerifiedAt != nil {
		t.Fatalf("second sign-up changed the row: %+v", row)
	}
	// An unverified account cannot sign in, whichever password is tried.
	for _, pw := range []string{p1, p2} {
		if _, _, _, err := e.svc.Login(bg, "victim@example.org", pw, SessionMeta{}); err == nil {
			t.Fatal("unverified account signed in")
		}
	}
	msgs := e.msgs()
	if !strings.Contains(msgs[1].Text, "account named victim") || !strings.Contains(msgs[1].Text, "did not sign up") {
		t.Fatalf("mail: %q", msgs[1].Text)
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[1])); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.svc.Login(bg, "victim@example.org", p1, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("attacker's password still signs in: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "mallory", p1, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("attacker's username still signs in: %v", err)
	}
	if _, _, u, err := e.svc.Login(bg, "victim@example.org", p2, SessionMeta{}); err != nil || u.Username != "victim" {
		t.Fatalf("victim cannot sign in: %v", err)
	}
	if !e.store.sessions[sess.ID].ended {
		t.Fatal("old session not ended")
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[0])); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("attacker's link still works: %v", err)
	}
}

func TestPreRegistrationVictimFirstAttackerSecond(t *testing.T) {
	e := newEnv(t)
	const p1, p2 = "the real owners passphrase", "attackers long passphrase"
	_ = e.svc.Register(bg, "victim", "victim@example.org", p1)
	_ = e.svc.Register(bg, "mallory", "victim@example.org", p2)
	msgs := e.msgs()
	if len(msgs) != 2 {
		t.Fatalf("%d mails", len(msgs))
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[0])); err != nil {
		t.Fatal(err)
	}
	if _, _, u, err := e.svc.Login(bg, "victim", p1, SessionMeta{}); err != nil || u.Username != "victim" {
		t.Fatalf("victim cannot sign in: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "victim@example.org", p2, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("attacker's password signs in: %v", err)
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[1])); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("attacker's link still works: %v", err)
	}
}

func TestUsernameAnswerDoesNotDependOnTheAddress(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "a@example.org", goodPW)
	n := len(e.msgs())
	// "ridge" is held by the pending row for a@; the same answer for that address, another address and a verified one.
	e.user("other")
	for _, addr := range []string{"a@example.org", "b@example.org", "other@example.org"} {
		if err := e.svc.Register(bg, "Ridge", addr, "some distinct passphrase"); !errors.Is(err, ErrUsernameTaken) {
			t.Fatalf("%s: %v", addr, err)
		}
	}
	if len(e.msgs()) != n+1 { // only e.user's own verify mail
		t.Fatal("mail sent for a refused sign-up")
	}
	if _, err := e.store.UserByEmail(bg, "b@example.org"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a row was created")
	}
	// The owner of the pending sign-up asks again with resend.
	if err := e.svc.ResendVerification(bg, "a@example.org"); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, tokenIn(t, e.last())); err != nil {
		t.Fatal(err)
	}
	e.login("ridge")
}

func TestResendRepeatsTheNewestSignUpNotTheRow(t *testing.T) {
	e := newEnv(t)
	const p1, p2 = "attackers long passphrase", "the real owners passphrase"
	_ = e.svc.Register(bg, "mallory", "victim@example.org", p1)
	e.clk.Advance(time.Minute)
	_ = e.svc.Register(bg, "victim", "victim@example.org", p2)
	e.clk.Advance(time.Minute)
	if err := e.svc.ResendVerification(bg, "victim@example.org"); err != nil {
		t.Fatal(err)
	}
	msgs := e.msgs()
	if len(msgs) != 3 || !strings.Contains(msgs[2].Text, "account named victim") {
		t.Fatalf("resent mail: %+v", msgs)
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[2])); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.svc.Login(bg, "victim@example.org", p1, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("attacker's password signs in: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "victim@example.org", p2, SessionMeta{}); err != nil {
		t.Fatalf("victim: %v", err)
	}
}

func TestResendRebuildsFromTheRetainedSignupWhenItsLinkExpired(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	e.clk.Advance(25 * time.Hour)
	if err := e.svc.ResendVerification(bg, "ridge@example.org"); err != nil {
		t.Fatal(err)
	}
	msgs := e.msgs()
	if len(msgs) != 2 || !strings.Contains(msgs[1].Text, "account named ridge") {
		t.Fatalf("expired link not recovered: %+v", msgs)
	}
	if err := e.svc.Verify(bg, tokenIn(t, msgs[1])); err != nil {
		t.Fatal(err)
	}
}

func TestResendSendsNothingWithoutAnyRetainedSignup(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	e.store.mu.Lock()
	e.store.tokens = map[string]*tokRec{} // cleanup removed it
	e.store.mu.Unlock()
	if err := e.svc.ResendVerification(bg, "ridge@example.org"); err != nil {
		t.Fatal(err)
	}
	if len(e.msgs()) != 1 {
		t.Fatal("a link was minted from the row")
	}
}

func TestRegisterOverAnUnverifiedAddressStillHonoursUsernameUniqueness(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "pending", "p@example.org", goodPW)
	e.user("taken")
	n := len(e.msgs())
	if err := e.svc.Register(bg, "Taken", "p@example.org", "another long passphrase"); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("err = %v", err)
	}
	if len(e.msgs()) != n {
		t.Fatal("mail sent for a refused sign-up")
	}
}

func TestVerifyFailsWhenTheUsernameWasTakenMeanwhile(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "first", "p@example.org", goodPW)
	_ = e.svc.Register(bg, "bob", "p@example.org", "another long passphrase")
	link := tokenIn(t, e.last())
	e.user("bob")
	if err := e.svc.Verify(bg, link); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("err = %v", err)
	}
	if u, _ := e.store.UserByEmail(bg, "p@example.org"); u.EmailVerifiedAt != nil {
		t.Fatal("verified despite the failure")
	}
}

func TestRegisterDuplicateUsername(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "Taken", "a@example.org", goodPW)
	if err := e.svc.Register(bg, "taken", "b@example.org", goodPW); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterMailFailureIsSilentAndLogsNoToken(t *testing.T) {
	e := newEnv(t)
	e.mail.FailWith(errors.New("smtp down"))
	if err := e.svc.Register(bg, "ridge", "ridge@example.org", goodPW); err != nil {
		t.Fatalf("err = %v", err)
	}
	_ = e.msgs()
	if !strings.Contains(e.logs.String(), "smtp down") {
		t.Fatalf("failure not logged: %s", e.logs)
	}
	for _, tok := range e.store.tokens {
		_ = tok
	}
	if strings.Contains(e.logs.String(), goodPW) || strings.Contains(e.logs.String(), "ridge@example.org") {
		t.Fatalf("log leaks: %s", e.logs)
	}
}

func TestVerifyTokens(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	tok := tokenIn(t, e.last())
	if err := e.svc.Verify(bg, "made-up"); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("made up: %v", err)
	}
	if err := e.svc.Verify(bg, ""); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("empty: %v", err)
	}
	if err := e.svc.Verify(bg, tok); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("twice: %v", err)
	}
	// Expired.
	_ = e.svc.Register(bg, "other", "other@example.org", goodPW)
	tok2 := tokenIn(t, e.last())
	e.clk.Advance(24*time.Hour + time.Second)
	if err := e.svc.Verify(bg, tok2); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expired: %v", err)
	}
}

func TestVerifyTokenOfOtherKindIsRefused(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	if err := e.svc.RequestReset(bg, u.Email); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, tokenIn(t, e.last())); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("reset token verified: %v", err)
	}
}

func TestVerifyOnlyMarksTheAddressTheLinkWentTo(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	tok := tokenIn(t, e.last())
	u, _ := e.store.UserByLogin(bg, "ridge")
	e.store.users[u.ID].Email = "changed@example.org"
	if err := e.svc.Verify(bg, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("err = %v", err)
	}
	if e.store.users[u.ID].EmailVerifiedAt != nil {
		t.Fatal("verified an address the link did not go to")
	}
}

func TestResendVerification(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	first := tokenIn(t, e.last())
	if err := e.svc.ResendVerification(bg, "RIDGE@example.org"); err != nil {
		t.Fatal(err)
	}
	if len(e.msgs()) != 2 {
		t.Fatal("no resend")
	}
	// Links are not cancelled by a resend (a stranger could do that to the owner); using one retires the rest.
	if err := e.svc.Verify(bg, tokenIn(t, e.last())); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, first); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("other link still works after verifying: %v", err)
	}
	// Verified and unknown addresses: nil, nothing sent.
	for _, a := range []string{"ridge@example.org", "nobody@example.org"} {
		if err := e.svc.ResendVerification(bg, a); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.msgs()) != 2 {
		t.Fatal("mail sent for verified or unknown address")
	}
	if err := e.svc.ResendVerification(bg, "not an email"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v", err)
	}
}

// ---- login ----

func TestLoginByUsernameAndEmail(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	for _, l := range []string{"ridge", "RIDGE", "Ridge@Example.org"} {
		tok, sess, u, err := e.svc.Login(bg, l, goodPW, SessionMeta{IP: "1.2.3.4", UserAgent: "UA", Label: "Firefox"})
		if err != nil || tok == "" || sess.Kind != "web" || u.Username != "ridge" || sess.Label != "Firefox" || sess.IP != "1.2.3.4" {
			t.Fatalf("%s: %v %+v", l, err, sess)
		}
		if want := e.clk.Now().Add(30 * 24 * time.Hour); !sess.ExpiresAt.Equal(want) || sess.HardExpiresAt != nil {
			t.Fatalf("expiry %v hard %v", sess.ExpiresAt, sess.HardExpiresAt)
		}
		if _, _, err := e.svc.Authenticate(bg, tok); err != nil {
			t.Fatal(err)
		}
	}
	if u, _ := e.store.UserByLogin(bg, "ridge"); u.LastLoginAt == nil {
		t.Fatal("login time not recorded")
	}
}

func TestLoginFailures(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	if _, _, _, err := e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("wrong: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "nobody", goodPW, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("unknown: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "nobody@example.org", goodPW, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("unknown email: %v", err)
	}
	u, _ := e.store.UserByLogin(bg, "ridge")
	now := e.clk.Now()
	e.store.users[u.ID].DisabledAt = &now
	if _, _, _, err := e.svc.Login(bg, "ridge", goodPW, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("disabled: %v", err)
	}
}

func TestLoginUnverified(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	if _, _, _, err := e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("wrong password must not reveal the state: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", goodPW, SessionMeta{}); !errors.Is(err, ErrEmailUnverified) {
		t.Fatalf("right password: %v", err)
	}
	if len(e.store.sessions) != 0 {
		t.Fatal("session created for an unverified user")
	}
}

func TestLoginCostsOneComparisonForUnknownAndKnown(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	calls := 0
	real := e.svc.verifyPW
	e.svc.verifyPW = func(h, p string) (bool, bool) { calls++; return real(h, p) }
	_, _, _, _ = e.svc.Login(bg, "nobody", goodPW, SessionMeta{})
	if calls != 1 {
		t.Fatalf("unknown user: %d comparisons", calls)
	}
	calls = 0
	_, _, _, _ = e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{})
	if calls != 1 {
		t.Fatalf("known user: %d comparisons", calls)
	}
	// A disabled user also costs one.
	u, _ := e.store.UserByLogin(bg, "ridge")
	now := e.clk.Now()
	e.store.users[u.ID].DisabledAt = &now
	calls = 0
	_, _, _, _ = e.svc.Login(bg, "ridge", goodPW, SessionMeta{})
	if calls != 1 {
		t.Fatalf("disabled: %d", calls)
	}
}

func TestLoginRehashesOldParameters(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	old := phcFor(goodPW, 1, 8*1024)
	e.store.users[u.ID].PassHash = old
	e.login("ridge")
	if e.store.users[u.ID].PassHash == old {
		t.Fatal("hash not upgraded")
	}
	if ok, re := VerifyPassword(e.store.users[u.ID].PassHash, goodPW); !ok || re {
		t.Fatalf("new hash ok=%v rehash=%v", ok, re)
	}
}

func TestThrottle(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	e.user("other")
	calls := 0
	real := e.svc.verifyPW
	e.svc.verifyPW = func(h, p string) (bool, bool) { calls++; return real(h, p) }
	for i := 0; i < 10; i++ {
		if _, _, _, err := e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	calls = 0
	// The eleventh is refused, even with the right password, and costs no hash.
	_, _, _, err := e.svc.Login(bg, "ridge", goodPW, SessionMeta{})
	if !errors.Is(err, ErrTooManyAttempts) || calls != 0 {
		t.Fatalf("err = %v, comparisons = %d", err, calls)
	}
	var te *TooManyAttemptsError
	if !errors.As(err, &te) || te.RetryAfter <= 0 || te.RetryAfter > 15*time.Minute {
		t.Fatalf("retry after: %+v", te)
	}
	// Another account is not affected.
	e.login("other")
	e.clk.Advance(15*time.Minute + time.Second)
	e.login("ridge")
}

func TestThrottleClearedByReset(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	for i := 0; i < 10; i++ {
		_, _, _, _ = e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{})
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", goodPW, SessionMeta{}); !errors.Is(err, ErrTooManyAttempts) {
		t.Fatalf("not throttled: %v", err)
	}
	_ = e.svc.RequestReset(bg, u.Email)
	if err := e.svc.ConfirmReset(bg, tokenIn(t, e.last()), "another long passphrase"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", "another long passphrase", SessionMeta{}); err != nil {
		t.Fatalf("after reset: %v", err)
	}
}

func TestThrottleWindowAndPruning(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	// Failures spread over more than 15 minutes never add up.
	for i := 0; i < 20; i++ {
		_, _, _, err := e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{})
		if !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("attempt %d: %v", i, err)
		}
		e.clk.Advance(2 * time.Minute)
	}
	// Entries for many users expire and are removed.
	th := newThrottle()
	now := e.clk.Now()
	for i := 0; i < 500; i++ {
		th.fail(uuid.NewString(), now)
	}
	if th.size() != 500 {
		t.Fatalf("size %d", th.size())
	}
	later := now.Add(31 * time.Minute)
	th.locked(uuid.NewString(), later)
	if th.size() != 0 {
		t.Fatalf("not pruned: %d", th.size())
	}
}

// ---- sessions ----

func TestAuthenticate(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	tok, sess, u := e.login("ridge")
	gs, gu, err := e.svc.Authenticate(bg, tok)
	if err != nil || gs.ID != sess.ID || gu.ID != u.ID {
		t.Fatalf("good: %v", err)
	}
	for name, bad := range map[string]string{"unknown": "nope", "empty": ""} {
		if _, _, err := e.svc.Authenticate(bg, bad); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if err := e.svc.Logout(bg, u.ID, sess.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.svc.Authenticate(bg, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("ended: %v", err)
	}
	tok2, _, _ := e.login("ridge")
	e.clk.Advance(30*24*time.Hour + time.Second)
	if _, _, err := e.svc.Authenticate(bg, tok2); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expired: %v", err)
	}
}

func TestAuthenticateDisabledUser(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	tok, _, _ := e.login("ridge")
	now := e.clk.Now()
	e.store.users[u.ID].DisabledAt = &now
	if _, _, err := e.svc.Authenticate(bg, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("err = %v", err)
	}
}

func TestRollingTouchAtMostEveryFiveMinutes(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	tok, sess, _ := e.login("ridge")
	e.clk.Advance(4 * time.Minute)
	_, _, _ = e.svc.Authenticate(bg, tok)
	if e.store.touchCalls != 0 {
		t.Fatalf("touched after 4 minutes: %d", e.store.touchCalls)
	}
	e.clk.Advance(2 * time.Minute)
	got, _, err := e.svc.Authenticate(bg, tok)
	if err != nil || e.store.touchCalls != 1 {
		t.Fatalf("touches %d err %v", e.store.touchCalls, err)
	}
	if want := e.clk.Now().Add(30 * 24 * time.Hour); !got.ExpiresAt.Equal(want) || !got.LastUsedAt.Equal(e.clk.Now()) {
		t.Fatalf("returned session not updated: %+v", got)
	}
	_, _, _ = e.svc.Authenticate(bg, tok)
	e.clk.Advance(time.Minute)
	_, _, _ = e.svc.Authenticate(bg, tok)
	if e.store.touchCalls != 1 {
		t.Fatalf("touches %d", e.store.touchCalls)
	}
	if !e.store.sessions[sess.ID].ExpiresAt.After(sess.ExpiresAt) {
		t.Fatal("expiry not extended in the store")
	}
}

func appFlow(t *testing.T, e *env, u User, label string) (string, Session) {
	t.Helper()
	verifier := "a-verifier-that-is-long-enough-for-pkce-0123456789"
	sum := sha256.Sum256([]byte(verifier))
	code, err := e.svc.IssueAppCode(bg, u, base64.RawURLEncoding.EncodeToString(sum[:]), label)
	if err != nil {
		t.Fatal(err)
	}
	tok, sess, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{UserAgent: "app"})
	if err != nil {
		t.Fatal(err)
	}
	return tok, sess
}

func TestAppSessionHardEndAndRolling(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	tok, sess := appFlow(t, e, u, "Pixel")
	if sess.Kind != "app" || sess.Label != "Pixel" || sess.HardExpiresAt == nil {
		t.Fatalf("session %+v", sess)
	}
	start := e.clk.Now()
	if !sess.ExpiresAt.Equal(start.Add(180*24*time.Hour)) || !sess.HardExpiresAt.Equal(start.Add(365*24*time.Hour)) {
		t.Fatalf("expiry %v hard %v", sess.ExpiresAt, sess.HardExpiresAt)
	}
	// Use it every 100 days: rolling carries it past 180 days but never past the hard end.
	for i := 0; i < 3; i++ {
		e.clk.Advance(100 * 24 * time.Hour)
		got, _, err := e.svc.Authenticate(bg, tok)
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if got.ExpiresAt.After(*sess.HardExpiresAt) {
			t.Fatalf("extended past the hard end: %v", got.ExpiresAt)
		}
	}
	if !e.store.sessions[sess.ID].ExpiresAt.Equal(*sess.HardExpiresAt) {
		t.Fatalf("expiry %v, want hard end %v", e.store.sessions[sess.ID].ExpiresAt, *sess.HardExpiresAt)
	}
	e.clk.Advance(66 * 24 * time.Hour)
	if _, _, err := e.svc.Authenticate(bg, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("after the hard end: %v", err)
	}
}

func TestEleventhAppSessionEndsTheOldest(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	var toks []string
	for i := 0; i < 11; i++ {
		e.clk.Advance(time.Minute)
		tok, _ := appFlow(t, e, u, "Device")
		toks = append(toks, tok)
	}
	if _, _, err := e.svc.Authenticate(bg, toks[0]); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("oldest still live: %v", err)
	}
	for _, tok := range toks[1:] {
		if _, _, err := e.svc.Authenticate(bg, tok); err != nil {
			t.Fatal(err)
		}
	}
	list, _ := e.svc.Sessions(bg, u.ID)
	if len(list) != 10 {
		t.Fatalf("%d live", len(list))
	}
}

func TestSessionsAdministration(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_, s1, _ := e.login("ridge")
	_, s2, _ := e.login("ridge")
	_, s3, _ := e.login("ridge")
	if err := e.svc.EndSession(bg, u.ID, s1.ID); err != nil {
		t.Fatal(err)
	}
	if l, _ := e.svc.Sessions(bg, u.ID); len(l) != 2 {
		t.Fatalf("%d", len(l))
	}
	if err := e.svc.EndOtherSessions(bg, u.ID, s2.ID); err != nil {
		t.Fatal(err)
	}
	l, _ := e.svc.Sessions(bg, u.ID)
	if len(l) != 1 || l[0].ID != s2.ID || e.store.sessions[s3.ID].ended != true {
		t.Fatalf("%+v", l)
	}
}

// ---- reset ----

func TestResetUnknownDisabledAndMailFailureSendNothingAndReturnNil(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	before := len(e.msgs())
	if err := e.svc.RequestReset(bg, "nobody@example.org"); err != nil {
		t.Fatal(err)
	}
	now := e.clk.Now()
	e.store.users[u.ID].DisabledAt = &now
	if err := e.svc.RequestReset(bg, u.Email); err != nil {
		t.Fatal(err)
	}
	if len(e.msgs()) != before {
		t.Fatal("mail sent")
	}
	e.store.users[u.ID].DisabledAt = nil
	e.mail.FailWith(errors.New("smtp down"))
	if err := e.svc.RequestReset(bg, u.Email); err != nil {
		t.Fatalf("mail failure leaked: %v", err)
	}
	if err := e.svc.RequestReset(bg, "garbage"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v", err)
	}
}

func TestResetFlow(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	e.login("ridge")
	tokA, _, _ := e.login("ridge")
	if err := e.svc.RequestReset(bg, " RIDGE@example.org"); err != nil {
		t.Fatal(err)
	}
	m := e.last()
	if !strings.Contains(m.Text, "https://beacon.example.org/?view=reset#token=") {
		t.Fatalf("link: %q", m.Text)
	}
	tok := tokenIn(t, m)
	newPW := "a different long passphrase"
	if err := e.svc.ConfirmReset(bg, tok, "short"); !errors.Is(err, ErrWeakPassword) {
		t.Fatalf("weak: %v", err)
	}
	if err := e.svc.ConfirmReset(bg, tok, newPW); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.ConfirmReset(bg, tok, newPW); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("twice: %v", err)
	}
	if _, _, err := e.svc.Authenticate(bg, tokA); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("session survived the reset: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", goodPW, SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("old password: %v", err)
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", newPW, SessionMeta{}); err != nil {
		t.Fatalf("new password: %v", err)
	}
	_ = u
}

func TestResetPolicyAgainstUsernameAndExpiry(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_ = e.svc.RequestReset(bg, u.Email)
	tok := tokenIn(t, e.last())
	if err := e.svc.ConfirmReset(bg, tok, "my-ridge-pass-123"); !errors.Is(err, ErrWeakPassword) {
		t.Fatalf("contains username: %v", err)
	}
	_ = e.svc.RequestReset(bg, u.Email)
	tok2 := tokenIn(t, e.last())
	e.clk.Advance(time.Hour + time.Second)
	if err := e.svc.ConfirmReset(bg, tok2, "a different long passphrase"); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expired: %v", err)
	}
}

func TestResetVerifiesAnUnverifiedEmail(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	if err := e.svc.RequestReset(bg, "ridge@example.org"); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.ConfirmReset(bg, tokenIn(t, e.last()), "a different long passphrase"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", "a different long passphrase", SessionMeta{}); err != nil {
		t.Fatalf("login after reset: %v", err)
	}
}

func TestNewResetInvalidatesTheOldLink(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_ = e.svc.RequestReset(bg, u.Email)
	old := tokenIn(t, e.last())
	_ = e.svc.RequestReset(bg, u.Email)
	if err := e.svc.ConfirmReset(bg, old, "a different long passphrase"); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("old link: %v", err)
	}
}

// ---- profile ----

func TestChangePassword(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_, s1, _ := e.login("ridge")
	tok2, s2, _ := e.login("ridge")
	newPW := "a different long passphrase"
	if err := e.svc.ChangePassword(bg, u, s2.ID, "wrong current pass", newPW); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("wrong current: %v", err)
	}
	u, _ = e.store.UserByID(bg, u.ID)
	if err := e.svc.ChangePassword(bg, u, s2.ID, goodPW, "short"); !errors.Is(err, ErrWeakPassword) {
		t.Fatalf("weak: %v", err)
	}
	if err := e.svc.ChangePassword(bg, u, s2.ID, goodPW, newPW); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.svc.Authenticate(bg, tok2); err != nil {
		t.Fatalf("current session ended: %v", err)
	}
	if !e.store.sessions[s1.ID].ended {
		t.Fatal("other session kept")
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", newPW, SessionMeta{}); err != nil {
		t.Fatal(err)
	}
}

func TestPasswordGuessesThroughProfileAreThrottled(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	for i := 0; i < 10; i++ {
		if err := e.svc.DeleteAccount(bg, u, "wrong password here"); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("%d: %v", i, err)
		}
	}
	if err := e.svc.DeleteAccount(bg, u, goodPW); !errors.Is(err, ErrTooManyAttempts) {
		t.Fatalf("err = %v", err)
	}
	if len(e.store.users) != 1 {
		t.Fatal("deleted while throttled")
	}
}

func TestDeleteAccount(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	if err := e.svc.DeleteAccount(bg, u, "wrong password here"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("err = %v", err)
	}
	if len(e.store.users) != 1 {
		t.Fatal("deleted on wrong password")
	}
	if err := e.svc.DeleteAccount(bg, u, goodPW); err != nil {
		t.Fatal(err)
	}
	if len(e.store.users) != 0 {
		t.Fatal("not deleted")
	}
}

func TestSetDisplayName(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	got, err := e.svc.SetDisplayName(bg, u, "  Ridge Walker ")
	if err != nil || got.DisplayName != "Ridge Walker" || e.store.users[u.ID].DisplayName != "Ridge Walker" {
		t.Fatalf("%+v %v", got, err)
	}
	for _, bad := range []string{"", "   ", strings.Repeat("x", 41), "line\nbreak"} {
		if _, err := e.svc.SetDisplayName(bg, u, bad); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	if _, err := e.svc.SetDisplayName(bg, u, strings.Repeat("é", 40)); err != nil {
		t.Fatalf("40 characters: %v", err)
	}
}

// ---- email change ----

func TestChangeEmail(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	if err := e.svc.ChangeEmail(bg, u, "wrong password here", "new@example.org"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("wrong: %v", err)
	}
	if err := e.svc.ChangeEmail(bg, u, goodPW, "bad"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid: %v", err)
	}
	before := len(e.msgs())
	if err := e.svc.ChangeEmail(bg, u, goodPW, " New@Example.org "); err != nil {
		t.Fatal(err)
	}
	m := e.msgs()
	if len(m) != before+1 || m[before].To != "new@example.org" {
		t.Fatalf("mail %+v", m)
	}
	cur := e.store.users[u.ID]
	if cur.Email != "ridge@example.org" || cur.PendingEmail == nil || *cur.PendingEmail != "new@example.org" {
		t.Fatalf("user %+v", cur)
	}
	if !strings.Contains(m[before].Text, "https://beacon.example.org/?view=verify#token=") {
		t.Fatalf("email-change link: %q", m[before].Text)
	}
	if err := e.svc.Verify(bg, tokenIn(t, m[before])); err != nil {
		t.Fatal(err)
	}
	cur = e.store.users[u.ID]
	if cur.Email != "new@example.org" || cur.PendingEmail != nil {
		t.Fatalf("after verify %+v", cur)
	}
}

func TestChangeEmailToAnAddressAnotherUserOwnsLooksTheSame(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	e.user("bob")
	before := len(e.msgs())
	if err := e.svc.ChangeEmail(bg, u, goodPW, "bob@example.org"); err != nil {
		t.Fatal(err)
	}
	if len(e.msgs()) != before {
		t.Fatal("mail sent to the owner of the address")
	}
	if p := e.store.users[u.ID].PendingEmail; p == nil || *p != "bob@example.org" {
		t.Fatal("pending not stored, which would make the two cases differ")
	}
	n := 0
	for _, tk := range e.store.tokens {
		if tk.kind == TokenEmailChange && tk.user == u.ID {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d email_change tokens", n)
	}
}

func TestEmailTakenMeanwhileFailsTheLink(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_ = e.svc.ChangeEmail(bg, u, goodPW, "late@example.org")
	tok := tokenIn(t, e.last())
	e.user("late") // takes the address? the fake registers late@example.org
	if err := e.svc.Verify(bg, tok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("err = %v", err)
	}
	if e.store.users[u.ID].Email != "ridge@example.org" {
		t.Fatal("email changed")
	}
}

func TestChangeEmailTwiceOnlyTheNewestLinkWorks(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	e.user("victim")
	// The attack: prove address A, then point the pending address at someone else's B.
	if err := e.svc.ChangeEmail(bg, u, goodPW, "a@example.org"); err != nil {
		t.Fatal(err)
	}
	tokA := tokenIn(t, e.last())
	if err := e.svc.ChangeEmail(bg, u, goodPW, "b@example.org"); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, tokA); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("old link: %v", err)
	}
	cur := e.store.users[u.ID]
	if cur.Email != "ridge@example.org" || cur.PendingEmail == nil || *cur.PendingEmail != "b@example.org" {
		t.Fatalf("user changed: %+v", cur)
	}
}

func TestEmailChangeTokenForAnotherAddressIsRefusedEvenIfStillStored(t *testing.T) {
	// Defence in depth: even when the old token survives, the stored address must match.
	e := newEnv(t)
	u := e.user("ridge")
	_ = e.svc.ChangeEmail(bg, u, goodPW, "a@example.org")
	tokA := tokenIn(t, e.last())
	other := "b@example.org"
	e.store.users[u.ID].PendingEmail = &other
	if err := e.svc.Verify(bg, tokA); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("err = %v", err)
	}
	if e.store.users[u.ID].Email != "ridge@example.org" {
		t.Fatal("email changed")
	}
}

// ---- app code ----

func challengeFor(v string) string {
	sum := sha256.Sum256([]byte(v))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

const verifier = "a-verifier-that-is-long-enough-for-pkce-0123456789"

func TestAppCodeFlow(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	before := len(e.msgs())
	code, err := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel 9\nInjected: header")
	if err != nil || code == "" {
		t.Fatalf("%q %v", code, err)
	}
	tok, sess, got, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{IP: "1.2.3.4", UserAgent: "ua"})
	if err != nil || tok == "" || got.ID != u.ID || sess.Kind != "app" || sess.IP != "1.2.3.4" {
		t.Fatalf("%v %+v", err, sess)
	}
	if strings.ContainsAny(sess.Label, "\n\r") || !strings.Contains(sess.Label, "Pixel 9") {
		t.Fatalf("label %q", sess.Label)
	}
	if _, _, err := e.svc.Authenticate(bg, tok); err != nil {
		t.Fatal(err)
	}
	m := e.msgs()
	if len(m) != before+1 {
		t.Fatalf("expected one notification, got %d", len(m)-before)
	}
	n := m[before]
	if n.To != u.Email || n.Subject != "A new app signed in to your Beacon account" ||
		!strings.Contains(n.Text, "Pixel 9") || !strings.Contains(n.Text, "2026-11-14") || !strings.Contains(n.Text, "Profile") || !strings.Contains(n.Text, "Devices") {
		t.Fatalf("notification: %+v", n)
	}
	if strings.Contains(n.Text, tok) || strings.Contains(n.Text, code) {
		t.Fatal("notification leaks a secret")
	}
}

func TestAppCodeLabelFromTheApp(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	for _, c := range []struct{ sent, want string }{
		{"Beacon app on iPhone", "Beacon app on iPhone"},
		{"  ", "Firefox on iOS"},
		{"", "Firefox on iOS"},
	} {
		code, err := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Firefox on iOS")
		if err != nil {
			t.Fatal(err)
		}
		_, sess, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{Label: c.sent})
		if err != nil {
			t.Fatal(err)
		}
		if sess.Label != c.want {
			t.Fatalf("sent %q: label %q, want %q", c.sent, sess.Label, c.want)
		}
	}
	_ = e.msgs()
}

func TestAppCodeSignInMailFailureDoesNotFailSignIn(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	e.mail.FailWith(errors.New("smtp down"))
	code, _ := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{}); err != nil {
		t.Fatalf("err = %v", err)
	}
	_ = e.msgs()
	if !strings.Contains(e.logs.String(), "smtp down") {
		t.Fatal("failure not logged")
	}
}

func TestAppCodeWrongVerifierBurnsTheCode(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	code, _ := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, "the-wrong-verifier-0123456789-abcdefghijklmnop", SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("wrong verifier: %v", err)
	}
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("second try with the right one: %v", err)
	}
	if len(e.store.sessions) != 0 {
		t.Fatal("session created")
	}
}

func TestAppCodeExpiredUsedAndOtherKinds(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	code, _ := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	e.clk.Advance(5*time.Minute + time.Second)
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("expired: %v", err)
	}
	code, _ = e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("twice: %v", err)
	}
	// A reset token is not an app code.
	_ = e.svc.RequestReset(bg, u.Email)
	if _, _, _, err := e.svc.ExchangeAppCode(bg, tokenIn(t, e.last()), verifier, SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("reset token: %v", err)
	}
}

func TestIssueAppCodeRejectsBadChallenge(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	for _, c := range []string{"", "short", strings.Repeat("!", 43), challengeFor(verifier) + "A"} {
		if _, err := e.svc.IssueAppCode(bg, u, c, "x"); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%q: %v", c, err)
		}
	}
}

func TestAppSignInSkipsMailWhenUserHasNoEmail(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	e.store.users[u.ID].Email = ""
	before := len(e.msgs())
	code, _ := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	if _, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{}); err != nil {
		t.Fatal(err)
	}
	if len(e.msgs()) != before {
		t.Fatal("mailed a user with no address")
	}
}

func TestOnlyHashesOfTokensAreStored(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	tok, _, _ := e.login("ridge")
	code, _ := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	for _, secret := range []string{tok, code} {
		if _, ok := e.store.tokens[secret]; ok {
			t.Fatal("plain token used as key")
		}
		for _, s := range e.store.sessions {
			if s.hash == secret {
				t.Fatal("plain token stored")
			}
		}
	}
	if _, ok := e.store.tokens[string(HashToken(code))]; !ok {
		t.Fatal("code hash not stored")
	}
	var d struct{ CodeChallenge, DeviceLabel string }
	_ = json.Unmarshal(e.store.tokens[string(HashToken(code))].data, &d)
}

// ---- logging ----

func TestLogsNeverContainSecrets(t *testing.T) {
	e := newEnv(t)
	const secretPW = "very-secret-passphrase-xyz"
	const wrongPW = "wrong-secret-passphrase-abc"
	const resetPW = "brand-new-secret-passphrase-qrs"
	_ = e.svc.Register(bg, "ridge", "ridge@mail.example.org", secretPW)
	secrets := []string{secretPW, wrongPW, resetPW}
	verifyMail := e.last()
	secrets = append(secrets, tokenIn(t, verifyMail))
	_ = e.svc.Verify(bg, tokenIn(t, verifyMail))
	_, _, _, _ = e.svc.Login(bg, "ridge", wrongPW, SessionMeta{})
	tok, _, u, err := e.svc.Login(bg, "ridge", secretPW, SessionMeta{})
	if err != nil {
		t.Fatal(err)
	}
	secrets = append(secrets, tok)
	e.mail.FailWith(errors.New("smtp down"))
	_ = e.svc.RequestReset(bg, u.Email)
	e.mail.FailWith(nil)
	_ = e.svc.RequestReset(bg, u.Email)
	rm := e.last()
	secrets = append(secrets, tokenIn(t, rm))
	_ = e.svc.ConfirmReset(bg, tokenIn(t, rm), resetPW)
	u, _ = e.store.UserByID(bg, u.ID) // the reset changed the hash the code is bound to
	code, _ := e.svc.IssueAppCode(bg, u, challengeFor(verifier), "Pixel")
	secrets = append(secrets, code, verifier)
	atok, _, _, err := e.svc.ExchangeAppCode(bg, code, verifier, SessionMeta{})
	if err != nil {
		t.Fatal(err)
	}
	secrets = append(secrets, atok)
	u, _ = e.store.UserByID(bg, u.ID)
	_ = e.svc.ChangeEmail(bg, u, resetPW, "moved@mail.example.org")
	secrets = append(secrets, tokenIn(t, e.last()), "https://beacon.example.org/?view=")
	out := e.logs.String()
	if out == "" {
		t.Fatal("nothing logged, the test proves nothing")
	}
	for _, s := range secrets {
		if strings.Contains(out, s) {
			t.Errorf("log contains %q", s)
		}
	}
	for _, a := range []string{"ridge@mail.example.org", "moved@mail.example.org", "ridge@", "moved@"} {
		if strings.Contains(out, a) {
			t.Errorf("log contains the address %q", a)
		}
	}
}

func phcFor(pw string, t, m uint32) string {
	salt := []byte("0123456789abcdef")
	key := argon2IDKey(pw, salt, t, m)
	return phc(m, t, 1, salt, key)
}

// ---- mail queue ----

type gateSender struct {
	release chan struct{}
	entered chan struct{}
	mu      sync.Mutex
	n       int
}

func (g *gateSender) Send(ctx context.Context, m mail.Message) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	select {
	case <-g.release:
	case <-ctx.Done():
	}
	g.mu.Lock()
	g.n++
	g.mu.Unlock()
	return nil
}

func TestMailIsSentOffTheRequestPath(t *testing.T) {
	e := newEnv(t)
	g := &gateSender{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	e.svc.mailer = g
	done := make(chan error, 1)
	go func() { done <- e.svc.Register(bg, "ridge", "ridge@example.org", goodPW) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Register waited for the mail to be sent")
	}
	close(g.release)
	if err := e.svc.Flush(bg); err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.n != 1 {
		t.Fatalf("%d mails sent", g.n)
	}
}

func TestMailQueueIsBoundedAndNeverBlocksTheRequest(t *testing.T) {
	e := newEnv(t)
	g := &gateSender{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	e.svc.mailer = g
	done := make(chan struct{})
	go func() {
		for i := 0; i < mailQueueSize*3; i++ {
			e.svc.enqueue(uuid.New(), "verify", "a@example.org", "s", "t")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("enqueue blocked on a full queue")
	}
	if !strings.Contains(e.logs.String(), "mail queue full") {
		t.Fatalf("drop not logged: %s", e.logs)
	}
	close(g.release)
}

func TestMailFailureLogsNoAddress(t *testing.T) {
	e := newEnv(t)
	e.mail.FailWith(errors.New("smtp down"))
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	_ = e.msgs()
	if !strings.Contains(e.logs.String(), "smtp down") || strings.Contains(e.logs.String(), "ridge@example.org") {
		t.Fatalf("log: %s", e.logs)
	}
}

func TestVerifyMailSaysToIgnoreIfYouDidNotSignUp(t *testing.T) {
	e := newEnv(t)
	_ = e.svc.Register(bg, "ridge", "ridge@example.org", goodPW)
	if !strings.Contains(e.last().Text, "did not sign up") {
		t.Fatalf("text: %q", e.last().Text)
	}
}

// ---- reset and password change leave nothing usable behind ----

func TestRefusedResetPasswordDoesNotBurnTheLink(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_ = e.svc.RequestReset(bg, u.Email)
	tok := tokenIn(t, e.last())
	if err := e.svc.ConfirmReset(bg, tok, "my-ridge-pass-123"); !errors.Is(err, ErrWeakPassword) {
		t.Fatalf("err = %v", err)
	}
	if err := e.svc.ConfirmReset(bg, tok, "a different long passphrase"); err != nil {
		t.Fatalf("link was burned: %v", err)
	}
}

// outstanding gives a user a pending email change and an app code, as a stolen session might.
func outstanding(t *testing.T, e *env, u User) (changeTok, appCode string) {
	t.Helper()
	if err := e.svc.ChangeEmail(bg, u, goodPW, "attacker@example.org"); err != nil {
		t.Fatal(err)
	}
	changeTok = tokenIn(t, e.last())
	chal := sha256.Sum256([]byte("verifier"))
	code, err := e.svc.IssueAppCode(bg, u, base64.RawURLEncoding.EncodeToString(chal[:]), "x")
	if err != nil {
		t.Fatal(err)
	}
	return changeTok, code
}

func assertNothingOutstanding(t *testing.T, e *env, u User, changeTok, appCode string) {
	t.Helper()
	if err := e.svc.Verify(bg, changeTok); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("email change link still works: %v", err)
	}
	if _, _, _, err := e.svc.ExchangeAppCode(bg, appCode, "verifier", SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("app code still works: %v", err)
	}
	got, _ := e.store.UserByID(bg, u.ID)
	if got.PendingEmail != nil || got.Email != u.Email {
		t.Fatalf("pending email left: %+v", got)
	}
}

func TestChangePasswordRevokesEveryOneTimeToken(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	_, s, _ := e.login("ridge")
	ct, ac := outstanding(t, e, u)
	_ = e.svc.RequestReset(bg, u.Email)
	rt := tokenIn(t, e.last())
	if err := e.svc.ChangePassword(bg, u, s.ID, goodPW, "a different long passphrase"); err != nil {
		t.Fatal(err)
	}
	assertNothingOutstanding(t, e, u, ct, ac)
	if err := e.svc.ConfirmReset(bg, rt, "yet another long passphrase"); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("reset link still works: %v", err)
	}
}

func TestConfirmResetRevokesEveryOneTimeToken(t *testing.T) {
	e := newEnv(t)
	u := e.user("ridge")
	ct, ac := outstanding(t, e, u)
	_ = e.svc.RequestReset(bg, u.Email)
	if err := e.svc.ConfirmReset(bg, tokenIn(t, e.last()), "a different long passphrase"); err != nil {
		t.Fatal(err)
	}
	assertNothingOutstanding(t, e, u, ct, ac)
}

// ---- login throttle keyed on the name typed ----

func TestThrottleGivesKnownAndUnknownNamesTheSameAnswer(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	answer := func(name string) (error, time.Duration) {
		_, _, _, err := e.svc.Login(bg, name, "wrong password here", SessionMeta{})
		var te *TooManyAttemptsError
		if errors.As(err, &te) {
			return err, te.RetryAfter
		}
		return err, 0
	}
	for i := 0; i < 10; i++ {
		for _, n := range []string{"ridge", " GHOST "} {
			if err, _ := answer(n); !errors.Is(err, ErrInvalidCredentials) {
				t.Fatalf("%s attempt %d: %v", n, i, err)
			}
		}
	}
	ek, rk := answer("ridge")
	eu, ru := answer("ghost")
	if !errors.Is(ek, ErrTooManyAttempts) || !errors.Is(eu, ErrTooManyAttempts) || rk != ru || rk <= 0 {
		t.Fatalf("known: %v %v, unknown: %v %v", ek, rk, eu, ru)
	}
	// Spelling does not dodge it.
	if err, _ := answer("RIDGE "); !errors.Is(err, ErrTooManyAttempts) {
		t.Fatalf("case variant: %v", err)
	}
}

func fillNames(th *throttle, n int, now time.Time, lock bool) {
	for i := 0; i < n; i++ {
		k := loginKey(uuid.NewString())
		reps := 1
		if lock {
			reps = throttleMax
		}
		for j := 0; j < reps; j++ {
			th.fail(k, now)
		}
	}
}

func TestThrottleUnlockedJunkIsEvictedAndNothingReadsAsLocked(t *testing.T) {
	th := newThrottle()
	th.cap = 5
	now := time.Now()
	uk := userKey(uuid.New())
	for i := 0; i < throttleMax; i++ {
		th.fail(uk, now)
	}
	fillNames(th, 5, now, false)
	// A full table never makes an unknown key read as locked.
	if _, locked := th.locked(loginKey("new-name"), now); locked {
		t.Fatal("unknown name reads as locked")
	}
	// A real account's lock still works, and a new name is still counted (the oldest junk makes room).
	if _, locked := th.locked(uk, now); !locked {
		t.Fatal("user lock lost")
	}
	if _, full := th.fail(loginKey("new-name"), now); full {
		t.Fatal("refused with room to evict")
	}
	for i := 0; i < throttleMax-1; i++ {
		th.fail(loginKey("new-name"), now)
	}
	if _, locked := th.locked(loginKey("new-name"), now); !locked {
		t.Fatal("new name was not counted")
	}
	if len(th.names) > th.cap {
		t.Fatalf("%d names over the cap", len(th.names))
	}
}

func TestThrottleUserEntriesAreNeverRefusedOrEvicted(t *testing.T) {
	th := newThrottle()
	th.cap = 2
	now := time.Now()
	fillNames(th, 2, now, true)
	for i := 0; i < 50; i++ {
		id := userKey(uuid.New())
		if _, full := th.fail(id, now); full {
			t.Fatal("user key refused for lack of space")
		}
	}
	if len(th.users) != 50 {
		t.Fatalf("%d user entries", len(th.users))
	}
}

func TestThrottleLockedJunkMakesNewNamesFailClosedButNotKnownUsers(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	e.svc.throttle.cap = 3
	fillNames(e.svc.throttle, 3, e.clk.Now(), true)

	// A new unknown name: refused with the short retry, no matter the password.
	_, _, _, err := e.svc.Login(bg, "ghost", "wrong password here", SessionMeta{})
	var te *TooManyAttemptsError
	if !errors.As(err, &te) || te.RetryAfter != fullRetry {
		t.Fatalf("unknown name: %v", err)
	}
	// An existing user can still sign in with the right password.
	e.login("ridge")
	// With a wrong one the failure cannot be recorded under the typed name, so it is refused too.
	if _, _, _, err := e.svc.Login(bg, "ridge", "wrong password here", SessionMeta{}); !errors.Is(err, ErrTooManyAttempts) {
		t.Fatalf("known user, wrong password: %v", err)
	}
	// Their own key still locks them out after enough failures.
	for i := 0; i < throttleMax; i++ {
		e.svc.throttle.fail(userKey(e.store.userID("ridge")), e.clk.Now())
	}
	if _, _, _, err := e.svc.Login(bg, "ridge", goodPW, SessionMeta{}); !errors.Is(err, ErrTooManyAttempts) {
		t.Fatalf("own lock: %v", err)
	}
}

func TestLoginAliasesShareOneBudget(t *testing.T) {
	e := newEnv(t)
	e.user("ridge")
	names := []string{"ridge", "RIDGE@example.org"}
	for i := 0; i < 10; i++ {
		if _, _, _, err := e.svc.Login(bg, names[i%2], "wrong password here", SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	// 5 + 5 failures through two names exhaust the one account budget; both names are refused.
	for _, n := range names {
		if _, _, _, err := e.svc.Login(bg, n, goodPW, SessionMeta{}); !errors.Is(err, ErrTooManyAttempts) {
			t.Fatalf("%s: %v", n, err)
		}
	}
	e.clk.Advance(15*time.Minute + time.Second)
	e.login("ridge")
}

func (f *fakeStore) userID(name string) uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.users {
		if u.Username == name {
			return u.ID
		}
	}
	return uuid.Nil
}

// A password registered composed signs in typed decomposed, and the other way round.
func TestLoginAcceptsEitherFormOfAPassword(t *testing.T) {
	e := newEnv(t)
	if err := e.svc.Register(bg, "ridge", "ridge@example.org", composed); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, tokenIn(t, e.last())); err != nil {
		t.Fatal(err)
	}
	for _, pw := range []string{composed, decomposed} {
		if _, _, _, err := e.svc.Login(bg, "ridge", pw, SessionMeta{}); err != nil {
			t.Fatalf("%q: %v", pw, err)
		}
	}
}

// A wrong password costs one verification, for a known name and an unknown one alike.
func TestWrongPasswordCostsOneVerification(t *testing.T) {
	e := newEnv(t)
	if err := e.svc.Register(bg, "ridge", "ridge@example.org", composed); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Verify(bg, tokenIn(t, e.last())); err != nil {
		t.Fatal(err)
	}
	calls := 0
	real := e.svc.verifyPW
	e.svc.verifyPW = func(h, p string) (bool, bool) { calls++; return real(h, p) }
	for _, name := range []string{"ridge", "nobody"} {
		calls = 0
		if _, _, _, err := e.svc.Login(bg, name, decomposed+"x", SessionMeta{}); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("%s: %v", name, err)
		}
		if calls != 1 {
			t.Fatalf("%s: %d verifications, want 1", name, calls)
		}
	}
}

// At most four password computations run at once across the service, dummy verifications included.
func TestPasswordWorkIsCappedAtFour(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	inFlight, peak := 0, 0
	e.svc.verifyPW = func(string, string) (bool, bool) {
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return false, false
	}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := e.svc.Login(bg, fmt.Sprintf("nobody%d", i), "whatever-password", SessionMeta{})
			if !errors.Is(err, ErrInvalidCredentials) {
				t.Errorf("login %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if peak > 4 || peak == 0 {
		t.Fatalf("%d password computations at once", peak)
	}
}

// A request that waits too long for a slot fails with ErrBusy, and waiting respects the request context.
func TestPasswordWorkBusyAndContext(t *testing.T) {
	e := newEnv(t)
	e.svc.slotWait = 50 * time.Millisecond
	hold := make(chan struct{})
	started := make(chan struct{}, 4)
	e.svc.verifyPW = func(string, string) (bool, bool) {
		started <- struct{}{}
		<-hold
		return false, false
	}
	for i := range 4 {
		go e.svc.Login(bg, fmt.Sprintf("holder%d", i), "whatever-password", SessionMeta{})
	}
	for range 4 {
		<-started
	}
	if _, _, _, err := e.svc.Login(bg, "late", "whatever-password", SessionMeta{}); !errors.Is(err, ErrBusy) {
		t.Fatalf("waited out: %v", err)
	}
	if err := e.svc.Register(bg, "newbie", "newbie@example.org", goodPW); !errors.Is(err, ErrBusy) {
		t.Fatalf("hashing while busy: %v", err)
	}
	e.svc.slotWait = time.Minute
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if _, _, _, err := e.svc.Login(ctx, "cancelled", "whatever-password", SessionMeta{}); !errors.Is(err, context.DeadlineExceeded) || time.Since(begin) > 5*time.Second {
		t.Fatalf("context: %v after %v", err, time.Since(begin))
	}
	close(hold)
}
