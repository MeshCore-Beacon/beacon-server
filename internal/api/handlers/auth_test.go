// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

var (
	testUser = users.User{ID: uuid.New(), Username: "ridge", DisplayName: "Ridge Walker", Email: "a@example.org",
		EmailVerifiedAt: ptrTime(time.UnixMilli(1794700001000)), Provider: "local", CreatedAt: time.UnixMilli(1794700000000)}
	testSess = users.Session{ID: uuid.New(), UserID: testUser.ID, Kind: "web", Label: "Firefox on macOS",
		CreatedAt: time.UnixMilli(1794700000000), LastUsedAt: time.UnixMilli(1794703600000), ExpiresAt: time.UnixMilli(1797292000000)}
	otherSess = users.Session{ID: uuid.New(), UserID: testUser.ID, Kind: "app", Label: "Pixel",
		CreatedAt: time.UnixMilli(1794700000000), LastUsedAt: time.UnixMilli(1794703600000), ExpiresAt: time.UnixMilli(1797292000000)}
)

func ptrTime(t time.Time) *time.Time { return &t }

// fakeAccounts answers every call with err when set, otherwise with a canned success, and records what it saw.
type fakeAccounts struct {
	mu    sync.Mutex
	err   error
	calls []string
	meta  users.SessionMeta
	keep  uuid.UUID
	ended uuid.UUID
	pw    string
	name  string
}

func (f *fakeAccounts) rec(s string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
	return f.err
}

func (f *fakeAccounts) called() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.calls, ",")
}

func (f *fakeAccounts) Authenticate(_ context.Context, token string) (users.Session, users.User, error) {
	switch token {
	case "web-token":
		return testSess, testUser, nil
	case "app-token":
		return otherSess, testUser, nil
	case "portal-token":
		pu := testUser
		pu.Provider, pu.Email, pu.Username = users.ProviderPortal, "", "ridge"
		return testSess, pu, nil
	}
	return users.Session{}, users.User{}, users.ErrTokenInvalid
}
func (f *fakeAccounts) Register(_ context.Context, u, e, p string) error { return f.rec("register") }
func (f *fakeAccounts) Verify(context.Context, string) error             { return f.rec("verify") }
func (f *fakeAccounts) ResendVerification(context.Context, string) error { return f.rec("resend") }
func (f *fakeAccounts) Login(_ context.Context, login, pw string, m users.SessionMeta) (string, users.Session, users.User, error) {
	f.meta = m
	if err := f.rec("login"); err != nil {
		return "", users.Session{}, users.User{}, err
	}
	return "web-token", testSess, testUser, nil
}
func (f *fakeAccounts) Logout(_ context.Context, uid, sid uuid.UUID) error {
	f.ended = sid
	return f.rec("logout")
}
func (f *fakeAccounts) RequestReset(context.Context, string) error { return f.rec("reset-request") }
func (f *fakeAccounts) ConfirmReset(context.Context, string, string) error {
	return f.rec("reset-confirm")
}
func (f *fakeAccounts) IssueAppCode(context.Context, users.User, string, string) (string, error) {
	return "the-code", f.rec("app-code")
}
func (f *fakeAccounts) ExchangeAppCode(_ context.Context, c, v string, m users.SessionMeta) (string, users.Session, users.User, error) {
	f.meta = m
	if err := f.rec("app-token"); err != nil {
		return "", users.Session{}, users.User{}, err
	}
	return "app-token", otherSess, testUser, nil
}
func (f *fakeAccounts) Sessions(context.Context, uuid.UUID) ([]users.Session, error) {
	return []users.Session{testSess, otherSess}, f.rec("sessions")
}
func (f *fakeAccounts) EndSession(_ context.Context, _, id uuid.UUID) error {
	f.ended = id
	return f.rec("end-session")
}
func (f *fakeAccounts) EndOtherSessions(_ context.Context, _, keep uuid.UUID) error {
	f.keep = keep
	return f.rec("end-others")
}
func (f *fakeAccounts) ChangePassword(_ context.Context, _ users.User, keep uuid.UUID, cur, n string) error {
	f.keep, f.pw = keep, cur+"|"+n
	return f.rec("change-password")
}
func (f *fakeAccounts) ChangeEmail(context.Context, users.User, string, string) error {
	return f.rec("change-email")
}
func (f *fakeAccounts) SetDisplayName(_ context.Context, u users.User, name string) (users.User, error) {
	f.name = name
	u.DisplayName = name
	return u, f.rec("set-name")
}
func (f *fakeAccounts) DeleteAccount(_ context.Context, _ users.User, cur string) error {
	f.pw = cur
	return f.rec("delete")
}

const publicOrigin = "https://beacon.example.org"

func accountsOpts() AuthOptions {
	return AuthOptions{Enabled: true, CookieSecure: true, PublicOrigin: publicOrigin}
}

// accountsServer mounts both routers the way the main router does.
func accountsServer(svc AccountsService, opts AuthOptions) http.Handler {
	r := chi.NewRouter()
	r.Use(mw.UserAuth(svc, SessionCookie))
	r.Mount("/auth", AuthRouter(svc, opts))
	r.Mount("/me", MeRouter(svc, opts))
	return r
}

type reqOpt func(*http.Request)

func cookie(v string) reqOpt {
	return func(r *http.Request) { r.AddCookie(&http.Cookie{Name: SessionCookie, Value: v}) }
}
func bearer(v string) reqOpt {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+v) }
}
func header(k, v string) reqOpt {
	return func(r *http.Request) { r.Header.Set(k, v) }
}

var csrf = header("X-Beacon-CSRF", "1")

func call(h http.Handler, method, path, body string, opts ...reqOpt) *httptest.ResponseRecorder {
	var rd *bytes.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	} else {
		rd = bytes.NewReader(nil)
	}
	r := httptest.NewRequest(method, path, rd)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	for _, o := range opts {
		o(r)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func errCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var b map[string]APIError
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("not an error body: %q", w.Body)
	}
	return b["error"].Code
}

func jsonBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var b map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("not JSON: %q", w.Body)
	}
	return b
}

func TestAuthRoutesSuccessShapes(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, accountsOpts())
	check := func(name string, w *httptest.ResponseRecorder, status int, key, want string) {
		t.Helper()
		if w.Code != status {
			t.Fatalf("%s: %d %s", name, w.Code, w.Body)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: Cache-Control %q", name, w.Header().Get("Cache-Control"))
		}
		if key != "" {
			if got := jsonBody(t, w)[key]; got != want {
				t.Fatalf("%s: %s = %v, want %v (%s)", name, key, got, want, w.Body)
			}
		}
	}
	check("register", call(h, "POST", "/auth/register", `{"username":"ridge","email":"a@example.org","password":"x"}`), 202, "status", "check_email")
	check("verify", call(h, "POST", "/auth/verify", `{"token":"t"}`), 200, "status", "verified")
	check("resend", call(h, "POST", "/auth/verify/resend", `{"email":"a@example.org"}`), 202, "status", "check_email")
	check("reset request", call(h, "POST", "/auth/reset/request", `{"email":"a@example.org"}`), 202, "status", "check_email")
	check("reset confirm", call(h, "POST", "/auth/reset/confirm", `{"token":"t","password":"x"}`), 200, "status", "password_set")

	w := call(h, "POST", "/auth/login", `{"login":"ridge","password":"x"}`, header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:130.0) Gecko/20100101 Firefox/130.0"))
	check("login", w, 200, "", "")
	u := jsonBody(t, w)["user"].(map[string]any)
	for k, want := range map[string]any{"id": testUser.ID.String(), "username": "ridge", "displayName": "Ridge Walker", "email": "a@example.org",
		"emailVerified": true, "pendingEmail": nil, "provider": "local", "createdAt": float64(1794700000000)} {
		if got, ok := u[k]; !ok || got != want {
			t.Fatalf("user.%s = %v, want %v", k, got, want)
		}
	}
	if _, leaked := u["passHash"]; leaked {
		t.Fatal("password hash in the response")
	}
	if f.meta.Label != "Firefox on macOS" {
		t.Fatalf("label %q", f.meta.Label)
	}

	w = call(h, "POST", "/auth/logout", "", cookie("web-token"), csrf)
	if w.Code != 204 || f.ended != testSess.ID {
		t.Fatalf("logout: %d", w.Code)
	}

	w = call(h, "POST", "/auth/app/code", `{"codeChallenge":"c","state":"s","deviceLabel":"Pixel"}`, cookie("web-token"), csrf)
	check("app code", w, 200, "redirectUri", "meshcorebeacon://auth")
	if jsonBody(t, w)["code"] != "the-code" {
		t.Fatalf("code: %s", w.Body)
	}
	w = call(h, "POST", "/auth/app/token", `{"code":"c","codeVerifier":"v"}`)
	check("app token", w, 200, "token", "app-token")
	b := jsonBody(t, w)
	if b["expiresAt"] != float64(1797292000000) || b["user"].(map[string]any)["username"] != "ridge" {
		t.Fatalf("app token body %s", w.Body)
	}
}

func TestLoginSetsTheCookieWithItsAttributes(t *testing.T) {
	for _, secure := range []bool{true, false} {
		opts := accountsOpts()
		opts.CookieSecure = secure
		w := call(accountsServer(&fakeAccounts{}, opts), "POST", "/auth/login", `{"login":"ridge","password":"x"}`)
		var c *http.Cookie
		for _, k := range w.Result().Cookies() {
			if k.Name == "beacon_session" {
				c = k
			}
		}
		if c == nil || c.Value != "web-token" || !c.HttpOnly || c.Path != "/" || c.SameSite != http.SameSiteLaxMode || c.Secure != secure || c.MaxAge <= 0 {
			t.Fatalf("secure=%v cookie %+v", secure, c)
		}
	}
}

func TestLogoutAndDeleteClearTheCookie(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, accountsOpts())
	for name, w := range map[string]*httptest.ResponseRecorder{
		"logout": call(h, "POST", "/auth/logout", "", cookie("web-token"), csrf),
		"delete": call(h, "DELETE", "/me", `{"currentPassword":"x"}`, cookie("web-token"), csrf),
		"end me": call(h, "DELETE", "/me/sessions/"+testSess.ID.String(), "", cookie("web-token"), csrf),
	} {
		var c *http.Cookie
		for _, k := range w.Result().Cookies() {
			if k.Name == "beacon_session" {
				c = k
			}
		}
		if w.Code != 204 || c == nil || c.Value != "" || c.MaxAge >= 0 || !c.HttpOnly {
			t.Fatalf("%s: %d %+v", name, w.Code, c)
		}
	}
	// Ending another session leaves the cookie alone.
	w := call(h, "DELETE", "/me/sessions/"+otherSess.ID.String(), "", cookie("web-token"), csrf)
	if w.Code != 204 || len(w.Result().Cookies()) != 0 {
		t.Fatalf("other session: %d %v", w.Code, w.Result().Cookies())
	}
}

func TestEveryErrorCodeAndStatus(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{users.ErrInvalidCredentials, 401, "invalid_credentials"},
		{users.ErrEmailUnverified, 403, "email_unverified"},
		{users.ErrTokenInvalid, 400, "token_invalid"},
		{users.ErrUsernameTaken, 409, "username_taken"},
		{&users.PolicyError{Reason: "Password must be at least 12 characters."}, 422, "weak_password"},
		{users.ErrInvalidInput, 422, "invalid_input"},
		{users.ErrRegistrationClosed, 403, "registration_closed"},
		{&users.TooManyAttemptsError{RetryAfter: 90 * time.Second}, 429, "too_many_attempts"},
		{errors.New("boom"), 500, "internal_server_error"},
	}
	for _, c := range cases {
		h := accountsServer(&fakeAccounts{err: c.err}, accountsOpts())
		w := call(h, "POST", "/auth/login", `{"login":"a","password":"b"}`)
		if w.Code != c.status || errCode(t, w) != c.code {
			t.Errorf("%v: %d %s", c.err, w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), "boom") {
			t.Errorf("internal error text leaked: %s", w.Body)
		}
	}
	// The weak-password message says why; Retry-After rounds up.
	w := call(accountsServer(&fakeAccounts{err: &users.PolicyError{Reason: "Password must be at least 12 characters."}}, accountsOpts()), "POST", "/auth/register", `{}`)
	if !strings.Contains(w.Body.String(), "at least 12 characters") {
		t.Fatalf("message: %s", w.Body)
	}
	w = call(accountsServer(&fakeAccounts{err: &users.TooManyAttemptsError{RetryAfter: 90*time.Second + time.Millisecond}}, accountsOpts()), "POST", "/auth/login", `{}`)
	if w.Header().Get("Retry-After") != "91" {
		t.Fatalf("Retry-After %q", w.Header().Get("Retry-After"))
	}
}

func TestSessionRoutesNeedASession(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, accountsOpts())
	for _, rt := range [][2]string{{"POST", "/auth/logout"}, {"POST", "/auth/app/code"}, {"GET", "/me"}, {"PATCH", "/me"}, {"POST", "/me/password"},
		{"POST", "/me/email"}, {"DELETE", "/me"}, {"GET", "/me/sessions"}, {"DELETE", "/me/sessions"}, {"DELETE", "/me/sessions/" + uuid.NewString()}} {
		w := call(h, rt[0], rt[1], "", cookie("expired"), csrf)
		if w.Code != 401 || errCode(t, w) != "not_signed_in" {
			t.Errorf("%s %s: %d %s", rt[0], rt[1], w.Code, w.Body)
		}
		if w := call(h, rt[0], rt[1], ""); w.Code != 401 {
			t.Errorf("%s %s signed out: %d", rt[0], rt[1], w.Code)
		}
	}
}

func TestAppCodeNeedsTheCookieSession(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, accountsOpts())
	w := call(h, "POST", "/auth/app/code", `{"codeChallenge":"c","state":"s","deviceLabel":"x"}`, bearer("app-token"))
	if w.Code != 401 || errCode(t, w) != "not_signed_in" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestCookieWritesNeedCSRFButBearerDoesNot(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, accountsOpts())
	body := `{"currentPassword":"a","newPassword":"b"}`
	if w := call(h, "POST", "/me/password", body, cookie("web-token")); w.Code != 403 || errCode(t, w) != "csrf_rejected" {
		t.Fatalf("no header: %d %s", w.Code, w.Body)
	}
	if w := call(h, "POST", "/me/password", body, cookie("web-token"), csrf, header("Origin", "https://evil.example")); w.Code != 403 {
		t.Fatalf("foreign origin: %d", w.Code)
	}
	if w := call(h, "POST", "/me/password", body, cookie("web-token"), csrf, header("Origin", publicOrigin)); w.Code != 204 {
		t.Fatalf("public origin: %d", w.Code)
	}
	if w := call(h, "POST", "/me/password", body, bearer("app-token")); w.Code != 204 {
		t.Fatalf("bearer: %d", w.Code)
	}
	if f.keep != otherSess.ID {
		t.Fatal("the bearer session was not the one kept")
	}
	if f.called() != "change-password,change-password" {
		t.Fatalf("calls %q", f.called())
	}
}

func TestBodyRules(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, accountsOpts())
	if w := call(h, "POST", "/auth/login", `{"login":"a","password":"`+strings.Repeat("x", 5000)+`"}`); w.Code != 413 {
		t.Fatalf("big body: %d", w.Code)
	}
	if w := call(h, "POST", "/auth/login", `{"login":"a","password":"b","extra":1}`); w.Code != 400 {
		t.Fatalf("unknown field: %d", w.Code)
	}
	if w := call(h, "POST", "/auth/login", `{"login":"a","password":"b"} {}`); w.Code != 400 {
		t.Fatalf("two values: %d", w.Code)
	}
	if w := call(h, "POST", "/auth/login", `not json`); w.Code != 400 {
		t.Fatalf("not json: %d", w.Code)
	}
	r := httptest.NewRequest("POST", "/auth/login", strings.NewReader(`{"login":"a","password":"b"}`))
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatalf("content type: %d", w.Code)
	}
}

func TestPerIPLimitsTripAtTheirNumber(t *testing.T) {
	cases := []struct {
		name, path, body string
		limit            int
	}{
		{"register", "/auth/register", `{}`, 5},
		{"login", "/auth/login", `{}`, 10},
		{"verify", "/auth/verify", `{}`, 30},
		{"resend", "/auth/verify/resend", `{}`, 5},
		{"reset request", "/auth/reset/request", `{}`, 5},
		{"reset confirm", "/auth/reset/confirm", `{}`, 10},
		{"app token", "/auth/app/token", `{}`, 10},
	}
	for _, c := range cases {
		h := accountsServer(&fakeAccounts{}, accountsOpts())
		for i := 0; i < c.limit; i++ {
			if w := call(h, "POST", c.path, c.body); w.Code == 429 {
				t.Fatalf("%s: limited at request %d", c.name, i+1)
			}
		}
		w := call(h, "POST", c.path, c.body)
		if w.Code != 429 || errCode(t, w) != "rate_limited" || w.Header().Get("Retry-After") == "" {
			t.Errorf("%s: request %d: %d %s", c.name, c.limit+1, w.Code, w.Body)
		}
	}
}

func TestOtherWritesShareSixtyPerMinute(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, accountsOpts())
	for i := 0; i < 60; i++ {
		if w := call(h, "PATCH", "/me", `{"displayName":"x"}`, cookie("web-token"), csrf); w.Code != 200 {
			t.Fatalf("request %d: %d", i+1, w.Code)
		}
	}
	w := call(h, "PATCH", "/me", `{"displayName":"x"}`, cookie("web-token"), csrf)
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("61st: %d", w.Code)
	}
	// Reading is not part of that budget.
	if w := call(h, "GET", "/me", "", cookie("web-token")); w.Code != 200 {
		t.Fatalf("read: %d", w.Code)
	}
}

func TestDisabledAnswersAccountsDisabledEverywhere(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, AuthOptions{})
	for _, p := range []string{"/auth/login", "/auth/nothing", "/auth", "/me", "/me/sessions", "/me/sessions/x"} {
		for _, m := range []string{"GET", "POST", "PATCH", "DELETE"} {
			w := call(h, m, p, "")
			if w.Code != 404 || errCode(t, w) != "accounts_disabled" || w.Header().Get("Cache-Control") != "no-store" || len(w.Result().Cookies()) != 0 {
				t.Fatalf("%s %s: %d %s", m, p, w.Code, w.Body)
			}
		}
	}
	if w := call(AuthRouter(nil, AuthOptions{Enabled: true}), "POST", "/login", ""); w.Code != 404 {
		t.Fatalf("nil service: %d", w.Code)
	}
}

func TestReadOnlyRefusesEveryWrite(t *testing.T) {
	opts := accountsOpts()
	opts.ReadOnly = true
	f := &fakeAccounts{}
	h := accountsServer(f, opts)
	for _, rt := range [][2]string{{"POST", "/auth/register"}, {"POST", "/auth/verify"}, {"POST", "/auth/verify/resend"}, {"POST", "/auth/login"},
		{"POST", "/auth/logout"}, {"POST", "/auth/reset/request"}, {"POST", "/auth/reset/confirm"}, {"POST", "/auth/app/code"}, {"POST", "/auth/app/token"},
		{"PATCH", "/me"}, {"POST", "/me/password"}, {"POST", "/me/email"}, {"DELETE", "/me"}, {"DELETE", "/me/sessions"}, {"DELETE", "/me/sessions/" + uuid.NewString()}} {
		w := call(h, rt[0], rt[1], `{}`, cookie("web-token"), csrf)
		if w.Code != 503 {
			t.Errorf("%s %s: %d", rt[0], rt[1], w.Code)
		}
	}
	if f.called() != "" {
		t.Fatalf("the service was called: %s", f.called())
	}
	if w := call(h, "GET", "/me", "", cookie("web-token")); w.Code != 200 {
		t.Fatalf("read: %d", w.Code)
	}
}

func TestBrowserLabel(t *testing.T) {
	cases := map[string]string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:130.0) Gecko/20100101 Firefox/130.0":                                                    "Firefox on macOS",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36":                             "Chrome on Windows",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36 Edg/129.0":                   "Edge on Windows",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1": "Safari on iOS",
		"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Mobile Safari/537.36":                       "Chrome on Android",
		"Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0":                                                                  "Firefox on Linux",
		"curl/8.0": "Web browser",
		"":         "Web browser",
	}
	for ua, want := range cases {
		if got := browserLabel(ua); got != want {
			t.Errorf("%q: %q, want %q", ua, got, want)
		}
	}
	if got := browserLabel("Firefox/1 " + strings.Repeat("Linux", 40)); len(got) > 60 {
		t.Fatalf("not cut: %d", len(got))
	}
}
