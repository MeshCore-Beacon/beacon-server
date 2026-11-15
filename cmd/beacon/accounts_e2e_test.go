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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/router"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// browser is an HTTP client with its own cookie jar, standing in for one device.
type browser struct {
	t    *testing.T
	base string
	c    *http.Client
}

func newBrowser(t *testing.T, base string) *browser {
	jar, _ := cookiejar.New(nil)
	return &browser{t: t, base: base, c: &http.Client{Jar: jar}}
}

func (b *browser) do(method, path, body, bearer string) (int, map[string]any) {
	b.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, b.base+"/api/v1"+path, rd)
	if err != nil {
		b.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Beacon-CSRF", "1")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := b.c.Do(req)
	if err != nil {
		b.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func (b *browser) expect(want int, method, path, body string) map[string]any {
	b.t.Helper()
	code, out := b.do(method, path, body, "")
	if code != want {
		b.t.Fatalf("%s %s: %d, want %d: %v", method, path, code, want, out)
	}
	return out
}

var mailToken = regexp.MustCompile(`token=([A-Za-z0-9_-]+)`)

func TestAccountsEndToEndPostgres(t *testing.T) {
	ctx, pool := e2eSchemaPool(t)
	store := db.New(pool, pool, 0, 0)
	recorder := &mail.Recorder{}
	pu, _ := url.Parse("http://localhost:8080")
	svc := users.NewService(store, recorder, config.ResolvedAccounts{Enabled: true, PublicURL: pu, RegistrationOpen: true}, time.Now, slog.New(slog.DiscardHandler))
	mctx, stop := context.WithCancel(ctx)
	done := svc.Start(mctx)
	t.Cleanup(func() { stop(); <-done })

	h := router.New(nil, nil, nil, router.Options{
		AccountsMode: "local", RegistrationOpen: true, Accounts: svc,
		AuthOptions: handlers.AuthOptions{Enabled: true, CookieSecure: false, PublicOrigin: "http://localhost:8080"},
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	mailFor := func(subject string) string {
		t.Helper()
		if err := svc.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		msgs := recorder.Messages()
		for i := len(msgs) - 1; i >= 0; i-- {
			if strings.Contains(msgs[i].Subject, subject) {
				if g := mailToken.FindStringSubmatch(msgs[i].Text); g != nil {
					return g[1]
				}
				return "no-token"
			}
		}
		t.Fatalf("no mail with subject %q in %d messages", subject, len(msgs))
		return ""
	}

	const pw1, pw2, pw3 = "first long passphrase 1", "second long passphrase 2", "third long passphrase 3"
	a := newBrowser(t, srv.URL)

	// Features report the mode.
	if out := a.expect(200, "GET", "/info", ""); out["features"].(map[string]any)["accounts"] != "local" {
		t.Fatalf("info: %v", out)
	}

	// Register, read the link from the recorded mail, verify, sign in.
	a.expect(202, "POST", "/auth/register", `{"username":"ridge","email":"Ridge@Example.org","password":"`+pw1+`"}`)
	a.expect(403, "POST", "/auth/login", `{"login":"ridge","password":"`+pw1+`"}`) // right password, address not confirmed
	a.expect(200, "POST", "/auth/verify", `{"token":"`+mailFor("Confirm your email")+`"}`)
	user := a.expect(200, "POST", "/auth/login", `{"login":"ridge@example.org","password":"`+pw1+`"}`)["user"].(map[string]any)
	if user["username"] != "ridge" || user["emailVerified"] != true || user["provider"] != "local" {
		t.Fatalf("login user: %v", user)
	}
	if me := a.expect(200, "GET", "/me", "")["user"].(map[string]any); me["id"] != user["id"] {
		t.Fatalf("me: %v", me)
	}

	// Change the display name.
	if me := a.expect(200, "PATCH", "/me", `{"displayName":"Ridge Walker"}`)["user"].(map[string]any); me["displayName"] != "Ridge Walker" {
		t.Fatalf("patch: %v", me)
	}
	if me := a.expect(200, "GET", "/me", "")["user"].(map[string]any); me["displayName"] != "Ridge Walker" {
		t.Fatalf("not saved: %v", me)
	}

	// A second device signs in; both sessions are listed, one marked current.
	b := newBrowser(t, srv.URL)
	b.expect(200, "POST", "/auth/login", `{"login":"ridge","password":"`+pw1+`"}`)
	list := a.expect(200, "GET", "/me/sessions", "")["sessions"].([]any)
	if len(list) != 2 {
		t.Fatalf("sessions: %v", list)
	}
	var otherID string
	for _, s := range list {
		m := s.(map[string]any)
		if m["current"] != true {
			otherID = m["id"].(string)
		}
	}
	if otherID == "" {
		t.Fatalf("no other session in %v", list)
	}

	// End the other one; that device is signed out at once.
	a.expect(204, "DELETE", "/me/sessions/"+otherID, "")
	b.expect(401, "GET", "/me", "")
	if list = a.expect(200, "GET", "/me/sessions", "")["sessions"].([]any); len(list) != 1 {
		t.Fatalf("after ending: %v", list)
	}

	// Change the password: the second session is gone, the first stays.
	b.expect(200, "POST", "/auth/login", `{"login":"ridge","password":"`+pw1+`"}`)
	a.expect(204, "POST", "/me/password", `{"currentPassword":"`+pw1+`","newPassword":"`+pw2+`"}`)
	b.expect(401, "GET", "/me", "")
	a.expect(200, "GET", "/me", "")
	a.expect(401, "POST", "/me/password", `{"currentPassword":"wrong password here","newPassword":"`+pw3+`"}`)

	// Forgot password: request a reset, confirm it with the mailed token.
	c := newBrowser(t, srv.URL)
	c.expect(202, "POST", "/auth/reset/request", `{"email":"ridge@example.org"}`)
	c.expect(202, "POST", "/auth/reset/request", `{"email":"nobody@example.org"}`) // same answer for an unknown address
	resetTok := mailFor("Reset your Beacon password")
	c.expect(422, "POST", "/auth/reset/confirm", `{"token":"`+resetTok+`","password":"short"}`) // a refused password keeps the link
	c.expect(200, "POST", "/auth/reset/confirm", `{"token":"`+resetTok+`","password":"`+pw3+`"}`)
	c.expect(400, "POST", "/auth/reset/confirm", `{"token":"`+resetTok+`","password":"`+pw3+`"}`) // single use
	a.expect(401, "GET", "/me", "")                                                               // the reset ended every session
	c.expect(401, "POST", "/auth/login", `{"login":"ridge","password":"`+pw2+`"}`)                // the old password fails
	c.expect(200, "POST", "/auth/login", `{"login":"ridge","password":"`+pw3+`"}`)

	// App sign-in with PKCE: the web session issues a code, the app exchanges it and calls /me with the bearer.
	verifier := "a-long-random-code-verifier-for-the-test-1234567890"
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	code := c.expect(200, "POST", "/auth/app/code", `{"codeChallenge":"`+challenge+`","state":"xyz","deviceLabel":"Pixel 8"}`)
	if code["redirectUri"] != "meshcorebeacon://auth" {
		t.Fatalf("redirect: %v", code)
	}
	app := newBrowser(t, srv.URL)
	app.expect(400, "POST", "/auth/app/token", `{"code":"`+code["code"].(string)+`","codeVerifier":"wrong"}`) // burns the code
	code = c.expect(200, "POST", "/auth/app/code", `{"codeChallenge":"`+challenge+`","state":"xyz","deviceLabel":"Pixel 8"}`)
	tok := app.expect(200, "POST", "/auth/app/token", `{"code":"`+code["code"].(string)+`","codeVerifier":"`+verifier+`"}`)
	bearer := tok["token"].(string)
	if bearer == "" || tok["expiresAt"].(float64) <= float64(time.Now().UnixMilli()) {
		t.Fatalf("app token: %v", tok)
	}
	if status, out := app.do("GET", "/me", "", bearer); status != 200 || out["user"].(map[string]any)["username"] != "ridge" {
		t.Fatalf("bearer /me: %d %v", status, out)
	}
	mailFor("A new app signed in")
	// A bearer write needs no CSRF header; the cookie session of c still does.
	req, _ := http.NewRequest("PATCH", srv.URL+"/api/v1/me", strings.NewReader(`{"displayName":"Via App"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("bearer write: %v %v", resp, err)
	}
	req, _ = http.NewRequest("PATCH", srv.URL+"/api/v1/me", strings.NewReader(`{"displayName":"Forged"}`))
	req.Header.Set("Content-Type", "application/json")
	for _, ck := range c.c.Jar.Cookies(mustURL(srv.URL)) {
		req.AddCookie(ck)
	}
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 403 {
		t.Fatalf("cookie write without the header: %v %v", resp, err)
	}

	// Delete the account: every session answers 401 and the user is gone.
	c.expect(401, "DELETE", "/me", `{"currentPassword":"wrong password here"}`)
	c.expect(204, "DELETE", "/me", `{"currentPassword":"`+pw3+`"}`)
	c.expect(401, "GET", "/me", "")
	if status, _ := app.do("GET", "/me", "", bearer); status != 401 {
		t.Fatalf("bearer after delete: %d", status)
	}
	c.expect(401, "POST", "/auth/login", `{"login":"ridge","password":"`+pw3+`"}`)
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("users left: %d %v", n, err)
	}
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM user_sessions) + (SELECT count(*) FROM user_tokens)`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("sessions or tokens left behind: %d %v", n, err)
	}
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}
