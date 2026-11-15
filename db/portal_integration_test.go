// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

func TestPortalUsersPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	local := newUser(t, ctx, s, "ridge")

	// A name a local user holds gets a suffix; two portal users with no address do not collide.
	a, err := s.UpsertPortalUser(ctx, "42", "ridge", "Ridge Walker")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.UpsertPortalUser(ctx, "43", "ridge", "Other Ridge")
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "ridge2" || b.Username != "ridge3" || a.ID == local.ID || a.Email != "" || a.PassHash != users.NoPassword ||
		a.Provider != "portal" || *a.ProviderSubject != "42" || a.EmailVerifiedAt != nil {
		t.Fatalf("a=%+v b=%+v", a, b)
	}

	// The same subject finds the same user and refreshes the names.
	again, err := s.UpsertPortalUser(ctx, "42", "walker", "R. Walker")
	if err != nil || again.ID != a.ID || again.Username != "walker" || again.DisplayName != "R. Walker" {
		t.Fatalf("%+v %v", again, err)
	}
	// A refreshed name that is taken gets a suffix too.
	taken, err := s.UpsertPortalUser(ctx, "42", "ridge", "R. Walker")
	if err != nil || taken.ID != a.ID || taken.Username == "ridge" || !strings.HasPrefix(taken.Username, "ridge") {
		t.Fatalf("%+v %v", taken, err)
	}
	// Long names are cut to 30 characters, suffix included.
	long, err := s.UpsertPortalUser(ctx, "44", strings.Repeat("x", 40), "Long")
	if err != nil || len(long.Username) != 30 {
		t.Fatalf("%q %v", long.Username, err)
	}
	long2, err := s.UpsertPortalUser(ctx, "45", strings.Repeat("x", 40), "Long")
	if err != nil || len(long2.Username) != 30 || long2.Username == long.Username {
		t.Fatalf("%q %v", long2.Username, err)
	}
	// Local sign-up keeps a unique address.
	if _, err := s.CreateUser(ctx, users.User{Username: "dup", DisplayName: "d", Email: local.Email, PassHash: "h", Provider: "local"}); !errors.Is(err, users.ErrEmailTaken) {
		t.Fatalf("duplicate address: %v", err)
	}
	if _, err := s.UserByEmail(ctx, ""); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("empty address found a user: %v", err)
	}
	_ = pool
}

func TestPortalUsersConcurrentFirstSignInPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	var wg sync.WaitGroup
	ids := make([]string, 8)
	for i := range ids {
		wg.Go(func() {
			u, err := s.UpsertPortalUser(ctx, "77", "racer", "Racer")
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = u.ID.String()
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("one subject became several users: %v", ids)
		}
	}
}

func TestPortalStateIsConsumedOncePostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	exp := time.Now().Add(time.Minute)
	if err := s.CreatePortalState(ctx, []byte("c1"), []byte("s1"), "verifier", "/x", exp); err != nil {
		t.Fatal(err)
	}
	st, err := s.ConsumePortalState(ctx, []byte("c1"))
	if err != nil || string(st.StateHash) != "s1" || st.Verifier != "verifier" || st.ReturnPath != "/x" {
		t.Fatalf("%+v %v", st, err)
	}
	if _, err := s.ConsumePortalState(ctx, []byte("c1")); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("second use: %v", err)
	}
	// Cleanup drops expired states only.
	_ = s.CreatePortalState(ctx, []byte("old"), []byte("s"), "v", "/", time.Now().Add(-time.Minute))
	_ = s.CreatePortalState(ctx, []byte("new"), []byte("s"), "v", "/", time.Now().Add(time.Minute))
	if err := s.DeleteExpiredAuth(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM portal_states`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d states left, %v", n, err)
	}
}

// A portal user has no password hash; the code and session bindings use the marker and disabling still ends them.
func TestPortalUserAppCodeAndDeletePostgres(t *testing.T) {
	ctx, s, svc, _, _ := serialEnv(t)
	u, err := s.UpsertPortalUser(ctx, "9", "walker", "Walker")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("verifier"))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	code, err := svc.IssueAppCode(ctx, u, challenge, "phone")
	if err != nil {
		t.Fatal(err)
	}
	tok, sess, _, err := svc.ExchangeAppCode(ctx, code, "verifier", users.SessionMeta{})
	if err != nil || tok == "" || sess.Kind != "app" {
		t.Fatalf("exchange: %v", err)
	}
	code, _ = svc.IssueAppCode(ctx, u, challenge, "phone")
	now := time.Now()
	if err := s.SetUserDisabled(ctx, u.ID, &now); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := svc.ExchangeAppCode(ctx, code, "verifier", users.SessionMeta{}); !errors.Is(err, users.ErrTokenInvalid) {
		t.Fatalf("a disabled user's code worked: %v", err)
	}
	if err := s.SetUserDisabled(ctx, u.ID, nil); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeleteAccount(ctx, u, "walker"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UserByID(ctx, u.ID); !errors.Is(err, users.ErrNotFound) {
		t.Fatalf("not deleted: %v", err)
	}
}
