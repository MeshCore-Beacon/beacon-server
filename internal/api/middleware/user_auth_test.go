// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

type fakeAuth struct{ tokens map[string]users.User }

func (f fakeAuth) Authenticate(_ context.Context, token string) (users.Session, users.User, error) {
	if u, ok := f.tokens[token]; ok {
		return users.Session{ID: uuid.New(), UserID: u.ID}, u, nil
	}
	return users.Session{}, users.User{}, users.ErrTokenInvalid
}

type brokenAuth struct{}

func (brokenAuth) Authenticate(context.Context, string) (users.Session, users.User, error) {
	return users.Session{}, users.User{}, errors.New("db down")
}

func probe(auth Authenticator, wrap ...func(http.Handler) http.Handler) http.Handler {
	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, _, ok := UserFrom(r.Context())
		if ok {
			w.Header().Set("X-User", u.Username)
		}
		if SessionViaCookie(r.Context()) {
			w.Header().Set("X-Via", "cookie")
		}
		w.WriteHeader(204)
	})
	for i := len(wrap) - 1; i >= 0; i-- {
		h = wrap[i](h)
	}
	return UserAuth(auth, "beacon_session")(h)
}

var ridge = users.User{ID: uuid.New(), Username: "ridge"}

func do(h http.Handler, method string, hdr map[string]string, cookie string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/x", nil)
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	if cookie != "" {
		r.AddCookie(&http.Cookie{Name: "beacon_session", Value: cookie})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestUserAuthAttachesFromBearerAndCookieAndNeverRejects(t *testing.T) {
	h := probe(fakeAuth{tokens: map[string]users.User{"good": ridge}})
	if w := do(h, "GET", map[string]string{"Authorization": "Bearer good"}, ""); w.Code != 204 || w.Header().Get("X-User") != "ridge" || w.Header().Get("X-Via") != "" {
		t.Fatalf("bearer: %d %v", w.Code, w.Header())
	}
	if w := do(h, "GET", nil, "good"); w.Header().Get("X-User") != "ridge" || w.Header().Get("X-Via") != "cookie" {
		t.Fatalf("cookie: %v", w.Header())
	}
	for name, w := range map[string]*httptest.ResponseRecorder{
		"none":        do(h, "GET", nil, ""),
		"bad cookie":  do(h, "GET", nil, "bad"),
		"bad bearer":  do(h, "GET", map[string]string{"Authorization": "Bearer bad"}, ""),
		"basic":       do(h, "GET", map[string]string{"Authorization": "Basic Zm9v"}, ""),
		"bad+cookie":  do(h, "GET", map[string]string{"Authorization": "Bearer bad"}, "good"),
		"store error": do(probe(brokenAuth{}), "GET", nil, "good"),
	} {
		if w.Code != 204 || w.Header().Get("X-User") != "" {
			t.Fatalf("%s: %d %v", name, w.Code, w.Header())
		}
	}
}

func TestRequireUser(t *testing.T) {
	h := probe(fakeAuth{tokens: map[string]users.User{"good": ridge}}, RequireUser)
	w := do(h, "GET", nil, "")
	if w.Code != 401 || !strings.Contains(w.Body.String(), `"code":"not_signed_in"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := do(h, "GET", nil, "good"); w.Code != 204 {
		t.Fatalf("signed in: %d", w.Code)
	}
}

func TestRequireCSRF(t *testing.T) {
	h := probe(fakeAuth{tokens: map[string]users.User{"good": ridge}}, RequireCSRF("https://beacon.example.org", []string{"https://app.example.org/", "*"}))
	csrf := map[string]string{"X-Beacon-CSRF": "1"}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range csrf {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	cases := []struct {
		name, method string
		hdr          map[string]string
		cookie       string
		want         int
	}{
		{"cookie POST without header", "POST", nil, "good", 403},
		{"cookie POST wrong value", "POST", map[string]string{"X-Beacon-CSRF": "yes"}, "good", 403},
		{"cookie POST with header", "POST", csrf, "good", 204},
		{"cookie PUT without header", "PUT", nil, "good", 403},
		{"cookie PATCH without header", "PATCH", nil, "good", 403},
		{"cookie DELETE without header", "DELETE", nil, "good", 403},
		{"cookie GET without header", "GET", nil, "good", 204},
		{"foreign origin", "POST", with(map[string]string{"Origin": "https://evil.example"}), "good", 403},
		{"null origin", "POST", with(map[string]string{"Origin": "null"}), "good", 403},
		{"public origin", "POST", with(map[string]string{"Origin": "https://beacon.example.org"}), "good", 204},
		{"allowed origin", "POST", with(map[string]string{"Origin": "https://app.example.org"}), "good", 204},
		{"wildcard is not an origin", "POST", with(map[string]string{"Origin": "*"}), "good", 403},
		{"bearer needs no header", "POST", map[string]string{"Authorization": "Bearer good"}, "", 204},
		{"bearer with foreign origin", "POST", map[string]string{"Authorization": "Bearer good", "Origin": "https://evil.example"}, "", 204},
		{"signed out", "POST", nil, "", 204},
	}
	for _, c := range cases {
		if w := do(h, c.method, c.hdr, c.cookie); w.Code != c.want {
			t.Errorf("%s: %d, want %d (%s)", c.name, w.Code, c.want, w.Body)
		}
	}
	if w := do(h, "POST", nil, "good"); !strings.Contains(w.Body.String(), `"code":"csrf_rejected"`) {
		t.Fatalf("body %s", w.Body)
	}
}

func TestRequireUserAnswers503WhenTheSessionLookupFails(t *testing.T) {
	// A lookup failure is not an invalid session: a client that sees 401 drops a token that is still good.
	w := do(probe(brokenAuth{}, RequireUser), "GET", nil, "good")
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), `"service_unavailable"`) || strings.Contains(w.Body.String(), "db down") {
		t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body)
	}
	// A route that does not need a user still serves the visitor signed out.
	if w := do(probe(brokenAuth{}), "GET", nil, "good"); w.Code != 204 || w.Header().Get("X-User") != "" {
		t.Fatalf("public route: %d %v", w.Code, w.Header())
	}
	// A token that is simply invalid stays 401.
	if w := do(probe(fakeAuth{}, RequireUser), "GET", nil, "bad"); w.Code != 401 {
		t.Fatalf("invalid token: %d", w.Code)
	}
}
