// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func userStore(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return ctx, pool, New(pool, pool, 0, 0)
}

func newUser(t *testing.T, ctx context.Context, s *Store, name string) users.User {
	t.Helper()
	u, err := s.CreateUser(ctx, users.User{Username: name, DisplayName: name, Email: name + "@example.org", PassHash: "hash", Provider: "local"})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func newSession(t *testing.T, ctx context.Context, s *Store, u users.User, kind string, hash byte) users.Session {
	t.Helper()
	now := time.Now()
	hard := now.Add(24 * time.Hour)
	ss, err := s.CreateSession(ctx, users.Session{UserID: u.ID, Kind: kind, Label: "x", ExpiresAt: now.Add(time.Hour), HardExpiresAt: &hard}, []byte{hash})
	if err != nil {
		t.Fatal(err)
	}
	return ss
}

func liveSessionCount(t *testing.T, ctx context.Context, s *Store, u users.User) int {
	t.Helper()
	l, err := s.ListSessions(ctx, u.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return len(l)
}

func TestUserCreateAndLookupPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u, err := s.CreateUser(ctx, users.User{Username: "RidgeWalker", DisplayName: "Ridge", Email: "Ridge@Example.ORG", PassHash: "h", Provider: "local"})
	if err != nil {
		t.Fatal(err)
	}
	if u.ID == uuid.Nil || u.Email != "ridge@example.org" || u.Username != "RidgeWalker" || u.CreatedAt.IsZero() || u.EmailVerifiedAt != nil {
		t.Fatalf("created user %+v", u)
	}
	for _, login := range []string{"RidgeWalker", "ridgewalker", "RIDGEWALKER", "ridge@example.org", "RIDGE@EXAMPLE.ORG"} {
		got, err := s.UserByLogin(ctx, login)
		if err != nil || got.ID != u.ID {
			t.Fatalf("UserByLogin(%q) = %+v, %v", login, got, err)
		}
	}
	if got, err := s.UserByID(ctx, u.ID); err != nil || got.ID != u.ID {
		t.Fatalf("UserByID %+v %v", got, err)
	}
	if got, err := s.UserByEmail(ctx, "Ridge@example.org"); err != nil || got.ID != u.ID {
		t.Fatalf("UserByEmail %+v %v", got, err)
	}
	// An email never matches as a username, and a username never as an email.
	if _, err := s.UserByLogin(ctx, "ridge@example.org "); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("padded login: %v", err)
	}
	for _, f := range []func() error{
		func() error { _, err := s.UserByLogin(ctx, "nobody"); return err },
		func() error { _, err := s.UserByID(ctx, uuid.New()); return err },
		func() error { _, err := s.UserByEmail(ctx, "no@example.org"); return err },
		func() error { return s.UpdateUserDisplayName(ctx, uuid.New(), "x") },
		func() error { return s.SetUserPassword(ctx, uuid.New(), "x") },
		func() error { return s.DeleteUser(ctx, uuid.New()) },
	} {
		if err := f(); !errors.Is(err, users.ErrNotFound) {
			t.Fatalf("missing user: %v", err)
		}
	}

	at := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.UpdateUserDisplayName(ctx, u.ID, "New Name"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetUserPassword(ctx, u.ID, "newhash"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetUserEmailVerified(ctx, u.ID, at); err != nil {
		t.Fatal(err)
	}
	if err := s.TouchUserLogin(ctx, u.ID, at); err != nil {
		t.Fatal(err)
	}
	if err := s.SetUserDisabled(ctx, u.ID, &at); err != nil {
		t.Fatal(err)
	}
	got, _ := s.UserByID(ctx, u.ID)
	if got.DisplayName != "New Name" || got.PassHash != "newhash" || got.EmailVerifiedAt == nil || !got.EmailVerifiedAt.Equal(at) ||
		got.LastLoginAt == nil || got.DisabledAt == nil || !got.DisabledAt.Equal(at) {
		t.Fatalf("updated user %+v", got)
	}
	if err := s.SetUserDisabled(ctx, u.ID, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ = s.UserByID(ctx, u.ID); got.DisabledAt != nil {
		t.Fatal("still disabled")
	}
}

func TestUserDuplicatesPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	newUser(t, ctx, s, "Ridge")
	_, err := s.CreateUser(ctx, users.User{Username: "rIDGE", DisplayName: "x", Email: "other@example.org", PassHash: "h", Provider: "local"})
	if !errors.Is(err, users.ErrUsernameTaken) {
		t.Fatalf("username differing by case: %v", err)
	}
	_, err = s.CreateUser(ctx, users.User{Username: "other", DisplayName: "x", Email: "RIDGE@example.org", PassHash: "h", Provider: "local"})
	if !errors.Is(err, users.ErrEmailTaken) {
		t.Fatalf("duplicate email: %v", err)
	}
	// A failed insert leaves nothing behind.
	if _, err := s.UserByLogin(ctx, "other"); !errors.Is(err, users.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestUserTokenConsumePostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u := newUser(t, ctx, s, "ridge")
	now := time.Now()
	if err := s.CreateToken(ctx, u.ID, "verify", []byte("a"), []byte(`{"email":"x@example.org"}`), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateToken(ctx, u.ID, "reset", []byte("b"), nil, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateToken(ctx, u.ID, "verify", []byte("c"), nil, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ConsumeToken(ctx, "reset", []byte("a"), now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("wrong kind: %v", err)
	}
	if _, _, err := s.ConsumeToken(ctx, "verify", []byte("c"), now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("expired: %v", err)
	}
	id, data, err := s.ConsumeToken(ctx, "verify", []byte("a"), now)
	if err != nil || id != u.ID || !strings.Contains(string(data), "x@example.org") {
		t.Fatalf("first use: %v %s %v", id, data, err)
	}
	if _, _, err := s.ConsumeToken(ctx, "verify", []byte("a"), now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("second use: %v", err)
	}
	if _, data, err := s.ConsumeToken(ctx, "reset", []byte("b"), now); err != nil || len(data) != 0 && string(data) != "null" {
		t.Fatalf("token without data: %s %v", data, err)
	}
	if err := s.DeleteUserTokens(ctx, u.ID, "verify"); err != nil {
		t.Fatal(err)
	}
}

func TestUserTokenConsumeRacePostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u := newUser(t, ctx, s, "ridge")
	if err := s.CreateToken(ctx, u.ID, "reset", []byte("race"), nil, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var wins, misses atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := s.ConsumeToken(ctx, "reset", []byte("race"), time.Now())
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, users.ErrNotFound):
				misses.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 || misses.Load() != 19 {
		t.Fatalf("wins %d misses %d", wins.Load(), misses.Load())
	}
}

func TestUserSessionLookupPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "ridge")
	now := time.Now()
	live := newSession(t, ctx, s, u, "web", 1)
	if live.ID == uuid.Nil || live.UserID != u.ID || live.Kind != "web" || live.CreatedAt.IsZero() || live.HardExpiresAt == nil {
		t.Fatalf("created session %+v", live)
	}
	got, gu, err := s.SessionByTokenHash(ctx, []byte{1}, now)
	if err != nil || got.ID != live.ID || gu.ID != u.ID || gu.Username != "ridge" {
		t.Fatalf("live: %+v %+v %v", got, gu, err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{99}, now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	// No hard end: a session with a nil HardExpiresAt is live until expires_at.
	noHard, err := s.CreateSession(ctx, users.Session{UserID: u.ID, Kind: "web", ExpiresAt: now.Add(time.Hour)}, []byte{2})
	if err != nil || noHard.HardExpiresAt != nil {
		t.Fatalf("no hard end: %+v %v", noHard, err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{2}, now); err != nil {
		t.Fatal(err)
	}
	// Past expires_at, past hard_expires_at, exactly at either (<= now), ended.
	if _, _, err := s.SessionByTokenHash(ctx, []byte{1}, now.Add(2*time.Hour)); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("expired: %v", err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{1}, live.ExpiresAt); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("expires_at == now: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE user_sessions SET expires_at = now() + interval '5 days', hard_expires_at = now() - interval '1 second' WHERE token_hash = $1`, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{1}, now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("hard expired: %v", err)
	}
	if err := s.EndSession(ctx, u.ID, noHard.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{2}, now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("ended: %v", err)
	}
	// A disabled user's live session is refused, and works again once re-enabled.
	other := newSession(t, ctx, s, u, "app", 3)
	if err := s.SetUserDisabled(ctx, u.ID, &now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{3}, now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("disabled user: %v", err)
	}
	if err := s.SetUserDisabled(ctx, u.ID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{3}, now); err != nil {
		t.Fatal(err)
	}
	// TouchSession moves the rolling expiry and last use.
	later := now.Add(10 * time.Minute).UTC().Truncate(time.Millisecond)
	exp := now.Add(48 * time.Hour).UTC().Truncate(time.Millisecond)
	if err := s.TouchSession(ctx, other.ID, later, exp); err != nil {
		t.Fatal(err)
	}
	got, _, _ = s.SessionByTokenHash(ctx, []byte{3}, now)
	if !got.LastUsedAt.Equal(later) || !got.ExpiresAt.Equal(exp) {
		t.Fatalf("touched %+v", got)
	}
}

func TestUserSessionEndPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	a, b := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	a1, a2, a3 := newSession(t, ctx, s, a, "web", 1), newSession(t, ctx, s, a, "web", 2), newSession(t, ctx, s, a, "app", 3)
	b1 := newSession(t, ctx, s, b, "web", 4)
	if n := liveSessionCount(t, ctx, s, a); n != 3 {
		t.Fatalf("live %d", n)
	}
	// Another user's id ends nothing.
	if err := s.EndSession(ctx, b.ID, a1.ID); err != nil {
		t.Fatal(err)
	}
	if n := liveSessionCount(t, ctx, s, a); n != 3 {
		t.Fatalf("ended someone else's session: %d", n)
	}
	if err := s.EndSession(ctx, a.ID, a1.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.EndOtherSessions(ctx, a.ID, a2.ID); err != nil {
		t.Fatal(err)
	}
	l, _ := s.ListSessions(ctx, a.ID, time.Now())
	if len(l) != 1 || l[0].ID != a2.ID {
		t.Fatalf("after EndOtherSessions %+v", l)
	}
	if _, _, err := s.SessionByTokenHash(ctx, []byte{4}, time.Now()); err != nil {
		t.Fatalf("bob's session: %v", err)
	}
	if err := s.EndAllSessions(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if n := liveSessionCount(t, ctx, s, a); n != 0 {
		t.Fatalf("live after EndAll %d", n)
	}
	// Ending sets ended_at and keeps the rows.
	var rows, ended int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(ended_at) FROM user_sessions WHERE user_id = $1`, a.ID).Scan(&rows, &ended); err != nil || rows != 3 || ended != 3 {
		t.Fatalf("rows %d ended %d %v", rows, ended, err)
	}
	_, _ = a3, b1
	// ListSessions skips expired sessions.
	if _, err := pool.Exec(ctx, `UPDATE user_sessions SET ended_at = NULL WHERE id = $1`, a3.ID); err != nil {
		t.Fatal(err)
	}
	if n := liveSessionCount(t, ctx, s, a); n != 1 {
		t.Fatalf("live %d", n)
	}
	l, _ = s.ListSessions(ctx, a.ID, time.Now().Add(2*time.Hour))
	if len(l) != 0 {
		t.Fatalf("expired session listed: %+v", l)
	}
}

func TestUserEndOldestAppSessionsPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "ridge")
	web := newSession(t, ctx, s, u, "web", 100)
	var apps []users.Session
	for i := range 12 {
		apps = append(apps, newSession(t, ctx, s, u, "app", byte(i)))
		// Distinct creation times keep "oldest" unambiguous.
		if _, err := pool.Exec(ctx, `UPDATE user_sessions SET created_at = now() - make_interval(mins => $2) WHERE id = $1`, apps[i].ID, 100-i); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EndOldestAppSessions(ctx, u.ID, 10); err != nil {
		t.Fatal(err)
	}
	live := map[uuid.UUID]bool{}
	l, _ := s.ListSessions(ctx, u.ID, time.Now())
	for _, x := range l {
		live[x.ID] = true
	}
	if len(l) != 11 || !live[web.ID] || live[apps[0].ID] || live[apps[1].ID] || !live[apps[2].ID] || !live[apps[11].ID] {
		t.Fatalf("live after trim: %d", len(l))
	}
	// Running it again, or with room to spare, changes nothing.
	if err := s.EndOldestAppSessions(ctx, u.ID, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.EndOldestAppSessions(ctx, u.ID, 50); err != nil {
		t.Fatal(err)
	}
	if n := liveSessionCount(t, ctx, s, u); n != 11 {
		t.Fatalf("live %d", n)
	}
	// Another user's sessions are untouched.
	v := newUser(t, ctx, s, "other")
	newSession(t, ctx, s, v, "app", 200)
	if err := s.EndOldestAppSessions(ctx, u.ID, 0); err != nil {
		t.Fatal(err)
	}
	if n := liveSessionCount(t, ctx, s, v); n != 1 {
		t.Fatalf("other user %d", n)
	}
	if n := liveSessionCount(t, ctx, s, u); n != 1 {
		t.Fatalf("web session should remain, live %d", n)
	}
}

func TestUserPendingEmailPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	a, b := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	if err := s.ApplyPendingEmail(ctx, a.ID, "new@example.org"); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("nothing pending: %v", err)
	}
	free := "New@Example.org"
	if err := s.SetUserPendingEmail(ctx, a.ID, &free); err != nil {
		t.Fatal(err)
	}
	got, _ := s.UserByID(ctx, a.ID)
	if got.PendingEmail == nil || *got.PendingEmail != "new@example.org" || got.Email != "alice@example.org" {
		t.Fatalf("pending %+v", got)
	}
	// The pending address was changed after the link was issued: the old address must not apply.
	other := "someone-else@example.org"
	if err := s.SetUserPendingEmail(ctx, a.ID, &other); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyPendingEmail(ctx, a.ID, "new@example.org"); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("stale expected address: %v", err)
	}
	got, _ = s.UserByID(ctx, a.ID)
	if got.Email != "alice@example.org" || got.PendingEmail == nil || *got.PendingEmail != other {
		t.Fatalf("stale apply changed the user %+v", got)
	}
	if err := s.SetUserPendingEmail(ctx, a.ID, &free); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyPendingEmail(ctx, a.ID, "NEW@example.org"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.UserByID(ctx, a.ID)
	if got.Email != "new@example.org" || got.PendingEmail != nil || got.EmailVerifiedAt == nil {
		t.Fatalf("applied %+v", got)
	}
	if _, err := s.UserByEmail(ctx, "alice@example.org"); !errors.Is(err, users.ErrNotFound) {
		t.Fatal("old address still resolves")
	}
	// Someone else owns the address by the time the link is used.
	taken := "bob@example.org"
	if err := s.SetUserPendingEmail(ctx, a.ID, &taken); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyPendingEmail(ctx, a.ID, taken); !errors.Is(err, users.ErrEmailTaken) {
		t.Fatalf("taken: %v", err)
	}
	got, _ = s.UserByID(ctx, a.ID)
	if got.Email != "new@example.org" || got.PendingEmail == nil {
		t.Fatalf("failed apply changed the user %+v", got)
	}
	if err := s.SetUserPendingEmail(ctx, a.ID, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ = s.UserByID(ctx, a.ID); got.PendingEmail != nil {
		t.Fatal("pending not cleared")
	}
	_ = b
}

func TestUserDeleteCascadePostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	a, b := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	newSession(t, ctx, s, a, "web", 1)
	newSession(t, ctx, s, b, "web", 2)
	for i, u := range []users.User{a, b} {
		if err := s.CreateToken(ctx, u.ID, "reset", []byte{byte(i + 10)}, nil, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteUser(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	var u, ss, tk int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM user_sessions), (SELECT count(*) FROM user_tokens)`).Scan(&u, &ss, &tk); err != nil {
		t.Fatal(err)
	}
	if u != 1 || ss != 1 || tk != 1 {
		t.Fatalf("users %d sessions %d tokens %d", u, ss, tk)
	}
}

func TestUserListPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	base := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second)
	names := []string{"alpha", "bravo", "Charlie_x", "delta", "e%cho", "fox_trot"}
	for i, n := range names {
		u := newUser(t, ctx, s, n)
		// delta shares a created_at with charlie so the id tie-break is exercised.
		at := base.Add(time.Duration(i) * time.Minute)
		if n == "delta" {
			at = base.Add(2 * time.Minute)
		}
		if _, err := pool.Exec(ctx, `UPDATE users SET created_at = $2 WHERE id = $1`, u.ID, at); err != nil {
			t.Fatal(err)
		}
	}
	far := time.Now().Add(24 * time.Hour)
	maxID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	all, err := s.ListUsers(ctx, "", far, maxID, 100)
	if err != nil || len(all) != 6 {
		t.Fatalf("all: %d %v", len(all), err)
	}
	for i := 1; i < len(all); i++ {
		p, c := all[i-1], all[i]
		if c.CreatedAt.After(p.CreatedAt) || c.CreatedAt.Equal(p.CreatedAt) && c.ID.String() > p.ID.String() {
			t.Fatalf("order broken at %d", i)
		}
	}
	// Walk the pages two at a time: every user exactly once, in order.
	var walked []users.User
	cur, curID := far, maxID
	for {
		page, err := s.ListUsers(ctx, "", cur, curID, 2)
		if err != nil {
			t.Fatal(err)
		}
		walked = append(walked, page...)
		if len(page) < 2 {
			break
		}
		last := page[len(page)-1]
		cur, curID = last.CreatedAt, last.ID
	}
	if len(walked) != 6 {
		t.Fatalf("walked %d", len(walked))
	}
	for i := range walked {
		if walked[i].ID != all[i].ID {
			t.Fatalf("paged order differs at %d", i)
		}
	}
	count := func(search string) int {
		l, err := s.ListUsers(ctx, search, far, maxID, 100)
		if err != nil {
			t.Fatal(err)
		}
		return len(l)
	}
	checks := []struct {
		search string
		want   int
	}{
		{"CHARLIE", 1}, {"charlie_x", 1}, {"BRAVO@EXAMPLE", 1}, {"example.org", 6}, {"nomatch", 0},
		{"%", 1}, // the literal percent in e%cho, not a wildcard
		{"_", 2}, // Charlie_x and fox_trot, not every user
		{"e%c", 1}, {"x_t", 1}, {"fo_trot", 0}, {"fo_ _", 0}, {"fox_t", 1},
	}
	for _, c := range checks {
		if got := count(c.search); got != c.want {
			t.Errorf("search %q: %d, want %d", c.search, got, c.want)
		}
	}
}

func TestUserDeleteExpiredAuthPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "ridge")
	now := time.Now()
	old, recent := now.Add(-8*24*time.Hour), now.Add(-6*24*time.Hour)
	mk := func(h byte, ended, expires *time.Time, hard *time.Time) {
		t.Helper()
		exp := now.Add(time.Hour)
		if expires != nil {
			exp = *expires
		}
		if _, err := pool.Exec(ctx, `INSERT INTO user_sessions (user_id, token_hash, kind, expires_at, hard_expires_at, ended_at) VALUES ($1, $2, 'web', $3, $4, $5)`,
			u.ID, []byte{h}, exp, hard, ended); err != nil {
			t.Fatal(err)
		}
	}
	mk(1, &old, nil, nil)    // ended long ago: gone
	mk(2, &recent, nil, nil) // ended recently: kept
	mk(3, nil, &old, nil)    // expired long ago: gone
	mk(4, nil, &recent, nil) // expired recently: kept
	mk(5, nil, nil, nil)     // live: kept
	hardOld := old
	mk(6, nil, nil, &hardOld) // hard expired long ago: gone
	tok := func(h byte, used, expires *time.Time) {
		t.Helper()
		exp := now.Add(time.Hour)
		if expires != nil {
			exp = *expires
		}
		if _, err := pool.Exec(ctx, `INSERT INTO user_tokens (token_hash, user_id, kind, expires_at, used_at) VALUES ($1, $2, 'verify', $3, $4)`, []byte{h}, u.ID, exp, used); err != nil {
			t.Fatal(err)
		}
	}
	tok(1, &old, nil)    // used long ago: gone
	tok(2, &recent, nil) // used recently: kept
	tok(3, nil, &old)    // expired long ago: gone
	tok(4, nil, &recent) // expired recently: kept
	tok(5, nil, nil)     // live: kept
	if err := s.DeleteExpiredAuth(ctx, now); err != nil {
		t.Fatal(err)
	}
	keep := func(table string) []byte {
		rows, err := pool.Query(ctx, `SELECT token_hash FROM `+table+` ORDER BY token_hash`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []byte
		for rows.Next() {
			var h []byte
			if err := rows.Scan(&h); err != nil {
				t.Fatal(err)
			}
			out = append(out, h...)
		}
		return out
	}
	if got := keep("user_sessions"); string(got) != string([]byte{2, 4, 5}) {
		t.Fatalf("sessions kept %v", got)
	}
	if got := keep("user_tokens"); string(got) != string([]byte{2, 4, 5}) {
		t.Fatalf("tokens kept %v", got)
	}
}

func TestUserTextClippedPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u := newUser(t, ctx, s, "ridge")
	ss, err := s.CreateSession(ctx, users.Session{
		UserID: u.ID, Kind: "web", ExpiresAt: time.Now().Add(time.Hour),
		Label: strings.Repeat("é", 100), IP: strings.Repeat("1", 100), UserAgent: strings.Repeat("日", 100) + "\x00",
	}, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if len(ss.Label) != 60 || len(ss.IP) != 64 || len(ss.UserAgent) != 198 {
		t.Fatalf("lengths %d %d %d", len(ss.Label), len(ss.IP), len(ss.UserAgent))
	}
	got, _, _ := s.SessionByTokenHash(ctx, []byte{1}, time.Now())
	if got.Label != ss.Label || got.UserAgent != ss.UserAgent {
		t.Fatal("stored text differs from the returned text")
	}
}

func TestUserActivateAndPeekTokenPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	now := time.Now()
	pending := newUser(t, ctx, s, "pending")
	other := newUser(t, ctx, s, "other")
	newSession(t, ctx, s, pending, "web", 1)
	if err := s.CreateToken(ctx, pending.ID, users.TokenVerify, []byte("t1"), []byte(`{}`), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if id, err := s.PeekToken(ctx, users.TokenVerify, []byte("t1"), now); err != nil || id != pending.ID {
		t.Fatalf("peek = %v, %v", id, err)
	}
	// Peeking does not spend the token.
	if _, _, err := s.ConsumeToken(ctx, users.TokenVerify, []byte("t1"), now); err != nil {
		t.Fatalf("consume after peek: %v", err)
	}
	if _, err := s.PeekToken(ctx, users.TokenVerify, []byte("t1"), now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("peek of a used token: %v", err)
	}
	_ = s.CreateToken(ctx, pending.ID, users.TokenVerify, []byte("t2"), []byte(`{}`), now.Add(time.Hour))
	_ = s.CreateToken(ctx, pending.ID, users.TokenReset, []byte("t3"), []byte(`{}`), now.Add(time.Hour))

	if err := s.ActivateUser(ctx, pending.ID, "Other", "h2", now); !errors.Is(err, users.ErrUsernameTaken) {
		t.Fatalf("taken username: %v", err)
	}
	if got, _ := s.UserByID(ctx, pending.ID); got.EmailVerifiedAt != nil || got.Username != "pending" {
		t.Fatalf("failed activation changed the user: %+v", got)
	}
	if err := s.ActivateUser(ctx, pending.ID, "fresh", "h2", now); err != nil {
		t.Fatal(err)
	}
	got, _ := s.UserByID(ctx, pending.ID)
	if got.Username != "fresh" || got.DisplayName != "fresh" || got.PassHash != "h2" || got.Email != pending.Email || got.EmailVerifiedAt == nil {
		t.Fatalf("user %+v", got)
	}
	if liveSessionCount(t, ctx, s, got) != 0 {
		t.Fatal("sessions survived")
	}
	if _, err := s.PeekToken(ctx, users.TokenVerify, []byte("t2"), now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("verify token survived: %v", err)
	}
	if _, err := s.PeekToken(ctx, users.TokenReset, []byte("t3"), now); err != nil {
		t.Fatalf("other token kinds must stay: %v", err)
	}
	// A verified user is never changed, and nothing of theirs is deleted.
	newSession(t, ctx, s, other, "web", 2)
	if err := s.ActivateUser(ctx, pending.ID, "hijack", "h3", now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("verified: %v", err)
	}
	if err := s.SetUserEmailVerified(ctx, other.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateUser(ctx, other.ID, "hijack", "h3", now); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("verified: %v", err)
	}
	if o, _ := s.UserByID(ctx, other.ID); o.Username != "other" || o.PassHash != "hash" {
		t.Fatalf("verified user changed: %+v", o)
	}
	if liveSessionCount(t, ctx, s, other) != 1 {
		t.Fatal("verified user's session deleted")
	}
}

func TestUserLatestTokenPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	now := time.Now()
	u := newUser(t, ctx, s, "ridge")
	if _, err := s.LatestToken(ctx, u.ID, users.TokenVerify); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("none: %v", err)
	}
	_ = s.CreateToken(ctx, u.ID, users.TokenVerify, []byte("a"), []byte(`{"n":1}`), now.Add(time.Hour))
	time.Sleep(5 * time.Millisecond)
	_ = s.CreateToken(ctx, u.ID, users.TokenVerify, []byte("b"), []byte(`{"n":2}`), now.Add(time.Hour))
	_ = s.CreateToken(ctx, u.ID, users.TokenReset, []byte("c"), []byte(`{"n":3}`), now.Add(time.Hour))
	if d, err := s.LatestToken(ctx, u.ID, users.TokenVerify); err != nil || string(d) != `{"n": 2}` {
		t.Fatalf("latest = %s, %v", d, err)
	}
	if _, _, err := s.ConsumeToken(ctx, users.TokenVerify, []byte("b"), now); err != nil {
		t.Fatal(err)
	}
	if d, _ := s.LatestToken(ctx, u.ID, users.TokenVerify); string(d) != `{"n": 1}` {
		t.Fatalf("after use = %s", d)
	}
	// An expired token is still the newest sign-up the user can resend.
	if _, err := s.pool.Exec(ctx, `UPDATE user_tokens SET expires_at = $1`, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if d, err := s.LatestToken(ctx, u.ID, users.TokenVerify); err != nil || string(d) != `{"n": 1}` {
		t.Fatalf("expired: %s %v", d, err)
	}
}

func TestUserDeleteStaleUnverifiedPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	now := time.Now()
	old := now.Add(-10 * 24 * time.Hour)
	mk := func(name string, created time.Time, verified bool) users.User {
		u := newUser(t, ctx, s, name)
		if _, err := pool.Exec(ctx, `UPDATE users SET created_at = $2 WHERE id = $1`, u.ID, created); err != nil {
			t.Fatal(err)
		}
		if verified {
			if err := s.SetUserEmailVerified(ctx, u.ID, created); err != nil {
				t.Fatal(err)
			}
		}
		return u
	}
	stale := mk("stale", old, false)
	withLink := mk("withlink", old, false)
	fresh := mk("fresh", now.Add(-time.Hour), false)
	verified := mk("verified", old, true)
	disabled := mk("disabled", old, false)
	if _, err := pool.Exec(ctx, `UPDATE users SET disabled_at = now() WHERE id = $1`, disabled.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateToken(ctx, withLink.ID, users.TokenVerify, []byte("live"), []byte(`{}`), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteExpiredAuth(ctx, now); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		u    users.User
		kept bool
	}{"stale": {stale, false}, "withlink": {withLink, true}, "fresh": {fresh, true}, "verified": {verified, true}, "disabled": {disabled, true}} {
		_, err := s.UserByID(ctx, c.u.ID)
		if c.kept && err != nil || !c.kept && !errors.Is(err, users.ErrNotFound) {
			t.Errorf("%s: kept=%v err=%v", name, c.kept, err)
		}
	}
}
