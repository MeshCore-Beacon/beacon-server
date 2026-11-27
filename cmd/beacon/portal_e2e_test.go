// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/router"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// TestPortalEndToEndPostgres boots the real router in portal mode against a stand-in portal and walks the sign-in,
// the session, the app code exchange and account deletion, plus Cancel and a reused state.
func TestPortalEndToEndPostgres(t *testing.T) {
	ctx, pool := e2eSchemaPool(t)
	store := db.New(pool, pool, 0, 0)

	var tokenCalls atomic.Int32
	portal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/portal_beacon_api.php" || r.URL.Query().Get("action") != "token" {
			http.NotFound(w, r)
			return
		}
		tokenCalls.Add(1)
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in["client_id"] != "beacon-test" || in["client_secret"] != "portal-secret" || in["code"] != "good-code" || in["code_verifier"] == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"user":{"id":4242,"username":"Hiker","display_name":"Hiker One"}}`)
	}))
	t.Cleanup(portal.Close)
	portalURL, _ := url.Parse(portal.URL)
	publicURL, _ := url.Parse("http://localhost:8080")

	cfg := config.ResolvedAccounts{Enabled: true, Portal: true, PortalURL: portalURL, PortalClientID: "beacon-test", PortalSecret: "portal-secret", PublicURL: publicURL}
	svc := users.NewService(store, &mail.Recorder{}, cfg, time.Now, slog.New(slog.DiscardHandler))
	mctx, stop := context.WithCancel(ctx)
	done := svc.Start(mctx)
	t.Cleanup(func() { stop(); <-done })
	h := router.New(nil, nil, nil, router.Options{
		AccountsMode: "portal", PortalURL: portal.URL, Accounts: svc,
		AuthOptions: handlers.AuthOptions{Enabled: true, PublicOrigin: "http://localhost:8080", Portal: users.NewPortal(svc, store, cfg, nil)},
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	// Redirects are read, not followed.
	newClient := func() *http.Client {
		jar, _ := cookiejar.New(nil)
		return &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	get := func(c *http.Client, path string) *http.Response {
		t.Helper()
		resp, err := c.Get(srv.URL + "/api/v1" + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	start := func(c *http.Client, ret string) (state string) {
		t.Helper()
		resp := get(c, "/auth/portal/start?return="+url.QueryEscape(ret))
		loc, err := resp.Location()
		if resp.StatusCode != http.StatusFound || err != nil || !strings.HasPrefix(loc.String(), portal.URL+"/portal.php?") {
			t.Fatalf("start: %d %v %v", resp.StatusCode, loc, err)
		}
		q := loc.Query()
		if q.Get("client_id") != "beacon-test" || q.Get("code_challenge_method") != "S256" || q.Get("redirect_uri") != "http://localhost:8080/api/v1/auth/portal/callback" || q.Get("state") == "" {
			t.Fatalf("start redirect: %v", q)
		}
		return q.Get("state")
	}
	hasSession := func(c *http.Client) bool {
		for _, ck := range c.Jar.Cookies(mustURL(srv.URL)) {
			if ck.Name == handlers.SessionCookie {
				return true
			}
		}
		return false
	}
	failed := func(resp *http.Response) {
		t.Helper()
		if loc, _ := resp.Location(); resp.StatusCode != http.StatusFound || loc == nil || loc.String() != "http://localhost:8080/?view=signin&error=portal" {
			t.Fatalf("expected the failure redirect: %d %v", resp.StatusCode, loc)
		}
	}
	api := func(c *http.Client, method, path, body, bearer string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+"/api/v1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Beacon-CSRF", "1")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}

	if status, out := api(newClient(), "GET", "/info", "", ""); status != 200 || out["features"].(map[string]any)["accounts"] != "portal" {
		t.Fatalf("info: %d %v", status, out)
	}

	// Start, callback, session cookie, /me.
	a := newClient()
	state := start(a, "/atlas?tab=Atlas")
	if hasSession(a) {
		t.Fatal("a session before the callback")
	}
	resp := get(a, "/auth/portal/callback?code=good-code&state="+url.QueryEscape(state))
	if loc, _ := resp.Location(); resp.StatusCode != http.StatusFound || loc == nil || loc.String() != "http://localhost:8080/atlas?tab=Atlas" {
		t.Fatalf("callback: %d %v", resp.StatusCode, loc)
	}
	if !hasSession(a) {
		t.Fatal("no session cookie after the callback")
	}
	status, out := api(a, "GET", "/me", "", "")
	me, _ := out["user"].(map[string]any)
	if status != 200 || me["username"] != "Hiker" || me["displayName"] != "Hiker One" || me["provider"] != "portal" {
		t.Fatalf("me: %d %v", status, out)
	}

	// App code exchange with PKCE.
	verifier := "a-long-random-code-verifier-for-the-test-1234567890"
	sum := sha256.Sum256([]byte(verifier))
	status, out = api(a, "POST", "/auth/app/code", `{"codeChallenge":"`+base64.RawURLEncoding.EncodeToString(sum[:])+`","state":"xyz","deviceLabel":"Pixel 8"}`, "")
	if status != 200 || out["code"] == nil {
		t.Fatalf("app code: %d %v", status, out)
	}
	app := newClient()
	status, out = api(app, "POST", "/auth/app/token", `{"code":"`+out["code"].(string)+`","codeVerifier":"`+verifier+`"}`, "")
	bearer, _ := out["token"].(string)
	if status != 200 || bearer == "" {
		t.Fatalf("app token: %d %v", status, out)
	}
	if status, out = api(app, "GET", "/me", "", bearer); status != 200 || out["user"].(map[string]any)["username"] != "Hiker" {
		t.Fatalf("bearer me: %d %v", status, out)
	}

	// A reused state: replaying the finished callback fails and signs nobody in.
	b := newClient()
	st2 := start(b, "/")
	cb := "/auth/portal/callback?code=good-code&state=" + url.QueryEscape(st2)
	cookies := b.Jar.Cookies(mustURL(srv.URL))
	if resp = get(b, cb); resp.StatusCode != http.StatusFound || !hasSession(b) {
		t.Fatalf("second sign-in: %d", resp.StatusCode)
	}
	replay := newClient()
	replay.Jar.SetCookies(mustURL(srv.URL), cookies)
	failed(get(replay, cb))
	if hasSession(replay) {
		t.Fatal("a reused state signed in")
	}
	calls := tokenCalls.Load()

	// Cancel at the portal: no code, error=access_denied. The state is used up and the portal is not called.
	c := newClient()
	st3 := start(c, "/")
	cancelPath := "/auth/portal/callback?error=access_denied&state=" + url.QueryEscape(st3)
	failed(get(c, cancelPath))
	if hasSession(c) || tokenCalls.Load() != calls {
		t.Fatalf("cancel signed in or called the portal: %v %d", hasSession(c), tokenCalls.Load()-calls)
	}
	failed(get(c, "/auth/portal/callback?code=good-code&state="+url.QueryEscape(st3)))
	if hasSession(c) {
		t.Fatal("the cancelled state still signed in")
	}

	// Delete the account: the username must be confirmed. Every session ends and the user is gone.
	if status, _ = api(a, "DELETE", "/me", `{"confirmUsername":"someone else"}`, ""); status == http.StatusNoContent || status < 400 {
		t.Fatalf("delete with the wrong username: %d", status)
	}
	if status, _ = api(a, "DELETE", "/me", `{"confirmUsername":"Hiker"}`, ""); status != http.StatusNoContent {
		t.Fatalf("delete: %d", status)
	}
	if status, _ = api(a, "GET", "/me", "", ""); status != 401 {
		t.Fatalf("me after delete: %d", status)
	}
	if status, _ = api(app, "GET", "/me", "", bearer); status != 401 {
		t.Fatalf("bearer after delete: %d", status)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM users) + (SELECT count(*) FROM user_sessions) + (SELECT count(*) FROM portal_states)`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rows left: %d %v", n, err)
	}
}
