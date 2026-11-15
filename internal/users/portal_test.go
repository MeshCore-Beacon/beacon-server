// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

// portalStore keeps sign-in states in memory and creates users in the fake user store.
type portalStore struct {
	f      *fakeStore
	mu     sync.Mutex
	states map[string]PortalState
}

func (p *portalStore) CreatePortalState(_ context.Context, cookieHash, stateHash []byte, verifier, ret string, exp time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.states[string(cookieHash)] = PortalState{StateHash: stateHash, Verifier: verifier, ReturnPath: ret, ExpiresAt: exp}
	return nil
}

func (p *portalStore) ConsumePortalState(_ context.Context, cookieHash []byte) (PortalState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.states[string(cookieHash)]
	if !ok {
		return PortalState{}, ErrNotFound
	}
	delete(p.states, string(cookieHash))
	return st, nil
}

func (p *portalStore) UpsertPortalUser(ctx context.Context, subject, username, display string) (User, error) {
	p.f.mu.Lock()
	for _, u := range p.f.users {
		if u.Provider == ProviderPortal && u.ProviderSubject != nil && *u.ProviderSubject == subject {
			u.Username, u.DisplayName = username, display
			cp := *u
			p.f.mu.Unlock()
			return cp, nil
		}
	}
	p.f.mu.Unlock()
	return p.f.CreateUser(ctx, User{Username: username, DisplayName: display, PassHash: NoPassword, Provider: ProviderPortal, ProviderSubject: &subject})
}

type portalEnv struct {
	agent string
	*env
	p      *Portal
	ps     *portalStore
	srv    *httptest.Server
	calls  []map[string]string
	handle func(w http.ResponseWriter, body map[string]string)
}

func newPortalEnv(t *testing.T) *portalEnv {
	t.Helper()
	e := &portalEnv{env: newEnv(t)}
	e.ps = &portalStore{f: e.store, states: map[string]PortalState{}}
	e.handle = func(w http.ResponseWriter, _ map[string]string) {
		_, _ = w.Write([]byte(`{"user":{"id":42,"username":"ridge","display_name":"Ridge Walker"}}`))
	}
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/portal_beacon_api.php" || r.URL.Query().Get("action") != "token" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		e.calls = append(e.calls, body)
		e.agent = r.Header.Get("User-Agent")
		e.handle(w, body)
	}))
	t.Cleanup(e.srv.Close)
	pu, _ := url.Parse("https://beacon.example.org")
	portal, _ := url.Parse(e.srv.URL)
	cfg := config.ResolvedAccounts{Enabled: true, Portal: true, PublicURL: pu, PortalURL: portal, PortalClientID: "beacon-analyzer", PortalSecret: "s3cret-value"}
	e.p = NewPortal(e.svc, e.ps, cfg, nil)
	return e
}

// begin runs Start and returns the cookie and the state the portal would send back.
func (e *portalEnv) begin(ret string) (cookie, state string, redirect *url.URL) {
	e.t.Helper()
	cookie, loc, err := e.p.Start(context.Background(), ret)
	if err != nil {
		e.t.Fatal(err)
	}
	u, err := url.Parse(loc)
	if err != nil {
		e.t.Fatal(err)
	}
	return cookie, u.Query().Get("state"), u
}

func (e *portalEnv) finish(cookie, state, code string) (string, string, error) {
	return e.p.Callback(context.Background(), cookie, state, code, SessionMeta{Label: "Firefox on macOS"})
}

func TestPortalStartRedirect(t *testing.T) {
	e := newPortalEnv(t)
	_, state, u := e.begin("/?view=profile")
	q := u.Query()
	if u.Path != "/portal.php" || q.Get("beacon_authorize") != "1" || q.Get("client_id") != "beacon-analyzer" ||
		q.Get("redirect_uri") != "https://beacon.example.org/api/v1/auth/portal/callback" ||
		q.Get("code_challenge_method") != "S256" || state == "" || len(q.Get("code_challenge")) != 43 {
		t.Fatalf("redirect %s", u)
	}
	if strings.Contains(u.String(), "s3cret") {
		t.Fatal("secret in the redirect")
	}
}

func TestPortalSignIn(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, u := e.begin("/?view=profile")
	tok, ret, err := e.finish(cookie, state, "the-code")
	if err != nil || tok == "" || ret != "/?view=profile" {
		t.Fatalf("%q %q %v", tok, ret, err)
	}
	if len(e.calls) != 1 {
		t.Fatalf("%d portal calls", len(e.calls))
	}
	c := e.calls[0]
	sum := sha256.Sum256([]byte(c["code_verifier"]))
	if c["client_id"] != "beacon-analyzer" || c["client_secret"] != "s3cret-value" || c["code"] != "the-code" ||
		c["redirect_uri"] != "https://beacon.example.org/api/v1/auth/portal/callback" ||
		base64.RawURLEncoding.EncodeToString(sum[:]) != u.Query().Get("code_challenge") {
		t.Fatalf("token request %+v", c)
	}
	sess, user, err := e.svc.Authenticate(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	if sess.Kind != "web" || user.Provider != ProviderPortal || user.Username != "ridge" || user.DisplayName != "Ridge Walker" ||
		user.Email != "" || user.PassHash != NoPassword || *user.ProviderSubject != "42" {
		t.Fatalf("session %+v user %+v", sess, user)
	}
	for _, secret := range []string{"s3cret-value", "the-code", c["code_verifier"], tok} {
		if strings.Contains(e.logs.String(), secret) {
			t.Fatalf("log holds %q", secret)
		}
	}
}

func TestPortalFailures(t *testing.T) {
	cases := map[string]func(e *portalEnv) (cookie, state, code string){
		"bad state": func(e *portalEnv) (string, string, string) {
			c, _, _ := e.begin("/")
			return c, "not-the-state", "code"
		},
		"no cookie": func(e *portalEnv) (string, string, string) {
			_, s, _ := e.begin("/")
			return "", s, "code"
		},
		"unknown cookie": func(e *portalEnv) (string, string, string) {
			_, s, _ := e.begin("/")
			return "forged", s, "code"
		},
		"expired": func(e *portalEnv) (string, string, string) {
			c, s, _ := e.begin("/")
			e.clk.Advance(portalStateTTL + time.Second)
			return c, s, "code"
		},
		"portal 4xx": func(e *portalEnv) (string, string, string) {
			e.handle = func(w http.ResponseWriter, _ map[string]string) {
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
			}
			c, s, _ := e.begin("/")
			return c, s, "code"
		},
		"portal 500": func(e *portalEnv) (string, string, string) {
			e.handle = func(w http.ResponseWriter, _ map[string]string) { w.WriteHeader(500) }
			c, s, _ := e.begin("/")
			return c, s, "code"
		},
		"timeout": func(e *portalEnv) (string, string, string) {
			e.p.client.Timeout = 50 * time.Millisecond
			e.handle = func(w http.ResponseWriter, _ map[string]string) { time.Sleep(300 * time.Millisecond) }
			c, s, _ := e.begin("/")
			return c, s, "code"
		},
		"not json":    portalAnswer(`<html>`),
		"no user":     portalAnswer(`{}`),
		"null user":   portalAnswer(`{"user":null}`),
		"no id":       portalAnswer(`{"user":{"username":"x"}}`),
		"bool id":     portalAnswer(`{"user":{"id":true,"username":"x"}}`),
		"float id":    portalAnswer(`{"user":{"id":1.5,"username":"x"}}`),
		"empty id":    portalAnswer(`{"user":{"id":"  ","username":"x"}}`),
		"object id":   portalAnswer(`{"user":{"id":{"a":1},"username":"x"}}`),
		"huge answer": portalAnswer(`{"user":{"id":1,"username":"` + strings.Repeat("a", 70<<10) + `"}}`),
		"cancel": func(e *portalEnv) (string, string, string) {
			c, s, _ := e.begin("/")
			return c, s, ""
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			e := newPortalEnv(t)
			cookie, state, code := setup(e)
			tok, _, err := e.finish(cookie, state, code)
			if !errors.Is(err, ErrPortalFailed) || tok != "" {
				t.Fatalf("%q %v", tok, err)
			}
			if n := len(e.store.users); n != 0 {
				t.Fatalf("%d users created", n)
			}
			for _, secret := range []string{"s3cret-value", "invalid_grant"} {
				if strings.Contains(e.logs.String(), secret) {
					t.Fatalf("log holds %q", secret)
				}
			}
		})
	}
}

func portalAnswer(body string) func(e *portalEnv) (string, string, string) {
	return func(e *portalEnv) (string, string, string) {
		e.handle = func(w http.ResponseWriter, _ map[string]string) { _, _ = w.Write([]byte(body)) }
		c, s, _ := e.begin("/")
		return c, s, "code"
	}
}

func TestPortalStateIsSingleUse(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	if _, _, err := e.finish(cookie, state, "code"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.finish(cookie, state, "code"); !errors.Is(err, ErrPortalFailed) {
		t.Fatalf("reused state: %v", err)
	}
	if len(e.calls) != 1 {
		t.Fatalf("%d portal calls", len(e.calls))
	}
}

func TestPortalCancelConsumesState(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	if _, _, err := e.finish(cookie, state, ""); !errors.Is(err, ErrPortalFailed) {
		t.Fatal(err)
	}
	if _, _, err := e.finish(cookie, state, "late-code"); !errors.Is(err, ErrPortalFailed) || len(e.calls) != 0 {
		t.Fatalf("state survived the cancel: %v, %d calls", err, len(e.calls))
	}
}

func TestPortalRedirectIsNotFollowed(t *testing.T) {
	e := newPortalEnv(t)
	hit := false
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit = true }))
	defer other.Close()
	e.handle = func(w http.ResponseWriter, _ map[string]string) {
		w.Header().Set("Location", other.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}
	cookie, state, _ := e.begin("/")
	if _, _, err := e.finish(cookie, state, "code"); !errors.Is(err, ErrPortalFailed) || hit {
		t.Fatalf("redirect followed (%v, hit=%v)", err, hit)
	}
}

func TestPortalUserRefreshAndTypes(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	if _, _, err := e.finish(cookie, state, "code"); err != nil {
		t.Fatal(err)
	}
	e.handle = func(w http.ResponseWriter, _ map[string]string) {
		_, _ = w.Write([]byte(`{"user":{"id":"42","username":"ridge2","display_name":"R. W."}}`))
	}
	cookie, state, _ = e.begin("/")
	if _, _, err := e.finish(cookie, state, "code"); err != nil {
		t.Fatal(err)
	}
	if len(e.store.users) != 1 {
		t.Fatalf("%d users for one subject", len(e.store.users))
	}
	for _, u := range e.store.users {
		if u.Username != "ridge2" || u.DisplayName != "R. W." {
			t.Fatalf("not refreshed: %+v", u)
		}
	}
}

func TestPortalDisabledUserRefused(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	if _, _, err := e.finish(cookie, state, "code"); err != nil {
		t.Fatal(err)
	}
	now := e.clk.Now()
	for _, u := range e.store.users {
		u.DisabledAt = &now
	}
	cookie, state, _ = e.begin("/")
	if tok, _, err := e.finish(cookie, state, "code"); !errors.Is(err, ErrPortalFailed) || tok != "" {
		t.Fatalf("disabled user signed in: %v", err)
	}
}

func TestPortalUserAppCodeAndDisable(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	tok, _, err := e.finish(cookie, state, "code")
	if err != nil {
		t.Fatal(err)
	}
	_, u, err := e.svc.Authenticate(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("verifier"))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	code, err := e.svc.IssueAppCode(context.Background(), u, challenge, "phone")
	if err != nil {
		t.Fatal(err)
	}
	appTok, sess, _, err := e.svc.ExchangeAppCode(context.Background(), code, "verifier", SessionMeta{})
	if err != nil || appTok == "" || sess.Kind != "app" {
		t.Fatalf("app sign-in: %v", err)
	}
	// A second code dies when the user is disabled before it is exchanged.
	code, _ = e.svc.IssueAppCode(context.Background(), u, challenge, "phone")
	now := e.clk.Now()
	e.store.users[u.ID].DisabledAt = &now
	if _, _, _, err := e.svc.ExchangeAppCode(context.Background(), code, "verifier", SessionMeta{}); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("disabled user's code worked: %v", err)
	}
	if n := len(e.msgs()); n != 0 {
		t.Fatalf("%d mails for a user with no address", n)
	}
}

func TestPortalDeleteConfirmsByUsername(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	tok, _, _ := e.finish(cookie, state, "code")
	_, u, _ := e.svc.Authenticate(context.Background(), tok)
	for _, bad := range []string{"", "someone", goodPW} {
		if err := e.svc.DeleteAccount(context.Background(), u, bad); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
	if err := e.svc.DeleteAccount(context.Background(), u, " RIDGE "); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.store.users[u.ID]; ok {
		t.Fatal("not deleted")
	}
}

func TestSafeReturn(t *testing.T) {
	for in, want := range map[string]string{
		"":                              "/",
		"/":                             "/",
		"/?view=profile":                "/?view=profile",
		"/a/b?c=d&e=f":                  "/a/b?c=d&e=f",
		"//evil.example":                "/",
		"/\\evil.example":               "/",
		"https://evil.example/":         "/",
		"evil.example":                  "/",
		"?view=profile":                 "/",
		"/%0d%0aSet-Cookie:x":           "/%0d%0aSet-Cookie:x",
		"/a\r\nSet-Cookie: x=1":         "/",
		"/ x":                           "/",
		"/" + strings.Repeat("a", 2000): "/",
		"javascript:alert(1)":           "/",
	} {
		if got := SafeReturn(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// Distinct portal subjects stay distinct accounts, whatever they look like.
func TestPortalSubjectIsExact(t *testing.T) {
	long := strings.Repeat("x", 127)
	subjects := []string{long + "A", long + "B", " pad ", "pad", "a  b", "a b"}
	e := newPortalEnv(t)
	for i, sub := range subjects {
		e.handle = func(w http.ResponseWriter, _ map[string]string) {
			b, _ := json.Marshal(map[string]any{"user": map[string]any{"id": sub, "username": fmt.Sprintf("u%d", i)}})
			_, _ = w.Write(b)
		}
		cookie, state, _ := e.begin("/")
		if _, _, err := e.finish(cookie, state, "code"); err != nil {
			t.Fatalf("%q: %v", sub, err)
		}
	}
	if n := len(e.store.users); n != len(subjects) {
		t.Fatalf("%d users for %d distinct subjects", n, len(subjects))
	}
	for _, u := range e.store.users {
		found := false
		for _, sub := range subjects {
			found = found || *u.ProviderSubject == sub
		}
		if !found {
			t.Fatalf("subject rewritten to %q", *u.ProviderSubject)
		}
	}
}

func TestPortalSubjectRefused(t *testing.T) {
	for name, id := range map[string]string{
		"too long":     `"` + strings.Repeat("x", 129) + `"`,
		"control":      `"a\u0000b"`,
		"newline":      `"a\nb"`,
		"empty":        `""`,
		"blank":        `"   "`,
		"bad utf8 esc": `"\ud800"`,
	} {
		t.Run(name, func(t *testing.T) {
			e := newPortalEnv(t)
			cookie, state, code := portalAnswer(`{"user":{"id":` + id + `,"username":"x"}}`)(e)
			if tok, _, err := e.finish(cookie, state, code); !errors.Is(err, ErrPortalFailed) || tok != "" {
				t.Fatalf("%q %v", tok, err)
			}
			if len(e.store.users) != 0 {
				t.Fatal("user created")
			}
		})
	}
}

// The portal's scrape blocking allows callers by the first product token of their User-Agent.
func TestPortalTokenCallNamesItself(t *testing.T) {
	e := newPortalEnv(t)
	cookie, state, _ := e.begin("/")
	if _, _, err := e.p.Callback(context.Background(), cookie, state, "code-1", SessionMeta{}); err != nil {
		t.Fatalf("callback: %v", err)
	}
	if e.agent != "Beacon-Portal-SignIn/1" {
		t.Fatalf("User-Agent = %q", e.agent)
	}
}
