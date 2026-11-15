// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

// routedAccounts implements only what these tests reach; the embedded nil interface panics on anything else.
type routedAccounts struct {
	handlers.AccountsService
	user users.User
}

func (a routedAccounts) Authenticate(_ context.Context, token string) (users.Session, users.User, error) {
	if token == "live" {
		return users.Session{ID: uuid.New(), UserID: a.user.ID, Kind: "web", ExpiresAt: time.Now().Add(time.Hour)}, a.user, nil
	}
	// A disabled user's session is refused by the service the same way as an unknown one.
	return users.Session{}, users.User{}, users.ErrTokenInvalid
}

func (a routedAccounts) Login(_ context.Context, login, pw string, m users.SessionMeta) (string, users.Session, users.User, error) {
	return "live", users.Session{ID: uuid.New(), ExpiresAt: time.Now().Add(time.Hour)}, a.user, nil
}

func accountsRouter(on bool) http.Handler {
	opts := Options{AccountsMode: "off"}
	if on {
		opts.AccountsMode = "local"
		opts.Accounts = routedAccounts{user: users.User{ID: uuid.New(), Username: "ridge", Email: "ridge@example.org", CreatedAt: time.Now()}}
		opts.AuthOptions = handlers.AuthOptions{Enabled: true, CookieSecure: true, PublicOrigin: "https://beacon.example.org"}
	}
	return New(nil, nil, nil, opts)
}

func TestAccountsOffAnswers404OnEveryAccountRoute(t *testing.T) {
	h := accountsRouter(false)
	for _, p := range []string{"/api/v1/auth/login", "/api/v1/auth/register", "/api/v1/me", "/api/v1/me/sessions", "/api/v1/me/atlas"} {
		for _, m := range []string{"GET", "POST", "DELETE"} {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(m, p, strings.NewReader(`{}`)))
			if w.Code != 404 || !strings.Contains(w.Body.String(), `"accounts_disabled"`) || len(w.Result().Cookies()) != 0 {
				t.Fatalf("%s %s: %d %s", m, p, w.Code, w.Body)
			}
		}
	}
	// Nothing that works signed out changes.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/info", nil))
	var info struct{ Features struct{ Accounts string } }
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil || w.Code != 200 || info.Features.Accounts != "off" {
		t.Fatalf("info: %d %s", w.Code, w.Body)
	}
}

func TestAccountsOnReportsItsModeAndServesTheRoutes(t *testing.T) {
	h := accountsRouter(true)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/info", nil))
	if !strings.Contains(w.Body.String(), `"accounts":"local"`) {
		t.Fatalf("info: %s", w.Body)
	}
	for name, c := range map[string]struct {
		cookie string
		want   int
	}{"live session": {"live", 200}, "disabled or ended user": {"gone", 401}, "none": {"", 401}} {
		r := httptest.NewRequest("GET", "/api/v1/me", nil)
		if c.cookie != "" {
			r.AddCookie(&http.Cookie{Name: "beacon_session", Value: c.cookie})
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != c.want || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: %d %s", name, w.Code, w.Body)
		}
	}
}

func TestLoginRequestLogCarriesNoCredentials(t *testing.T) {
	var out bytes.Buffer
	prev, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev); log.SetOutput(writer); log.SetFlags(flags) })

	h := accountsRouter(true)
	r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"login":"secret-login-name@example.org","password":"secret-pass-phrase-123"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("login: %d %s", w.Code, w.Body)
	}
	logged := out.String()
	if !strings.Contains(logged, "request complete") {
		t.Fatalf("no request line: %q", logged)
	}
	for _, secret := range []string{"secret-login-name", "secret-pass-phrase", "live"} {
		if strings.Contains(logged, secret) {
			t.Fatalf("%q in the log: %s", secret, logged)
		}
	}
}

type routedRequests struct {
	handlers.RequestsService
}

func (routedRequests) Create(_ context.Context, _, _ uuid.UUID, _ []byte) (polling.RequestRow, error) {
	return polling.RequestRow{ID: uuid.New(), Target: make([]byte, 32), Status: polling.StatusWaiting}, nil
}

func requestsRouter(svc handlers.RequestsService) http.Handler {
	opts := Options{AccountsMode: "local", TelemetryRequests: svc != nil,
		Accounts:    routedAccounts{user: users.User{ID: uuid.New(), Username: "ridge", Email: "ridge@example.org", CreatedAt: time.Now()}},
		AuthOptions: handlers.AuthOptions{Enabled: true, CookieSecure: true, PublicOrigin: "https://beacon.example.org"},
		MeMounts:    []handlers.MeMount{handlers.RequestRoutes(svc)}}
	return New(nil, nil, nil, opts)
}

func TestTelemetryRequestsFlagAndRoutes(t *testing.T) {
	for _, on := range []bool{false, true} {
		var svc handlers.RequestsService
		if on {
			svc = routedRequests{}
		}
		h := requestsRouter(svc)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/info", nil))
		if want := `"telemetryRequests":` + map[bool]string{false: "false", true: "true"}[on]; !strings.Contains(w.Body.String(), want) {
			t.Errorf("on=%v info: %s", on, w.Body)
		}
		r := httptest.NewRequest("POST", "/api/v1/me/telemetry-requests", strings.NewReader(`{"nodeId":"`+uuid.NewString()+`"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Beacon-CSRF", "1")
		r.AddCookie(&http.Cookie{Name: "beacon_session", Value: "live"})
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if on && w.Code != 201 || !on && (w.Code != 404 || !strings.Contains(w.Body.String(), "requests_disabled")) {
			t.Errorf("on=%v create: %d %s", on, w.Code, w.Body)
		}
	}
}

func TestRequestPasswordIsNotLogged(t *testing.T) {
	var out bytes.Buffer
	prev, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev); log.SetOutput(writer); log.SetFlags(flags) })
	h := requestsRouter(routedRequests{})
	r := httptest.NewRequest("POST", "/api/v1/me/telemetry-requests", strings.NewReader(`{"nodeId":"`+uuid.NewString()+`","password":"guestpw-123"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Beacon-CSRF", "1")
	r.AddCookie(&http.Cookie{Name: "beacon_session", Value: "live"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 201 || !strings.Contains(out.String(), "request complete") || strings.Contains(out.String(), "guestpw-123") || strings.Contains(w.Body.String(), "guestpw-123") {
		t.Fatalf("%d log=%q body=%s", w.Code, out.String(), w.Body)
	}
}

func TestPortalModeReportsItsURL(t *testing.T) {
	h := New(nil, nil, nil, Options{AccountsMode: "portal", PortalURL: "https://portal.example.org",
		Accounts: routedAccounts{}, AuthOptions: handlers.AuthOptions{Enabled: true, CookieSecure: true, PublicOrigin: "https://beacon.example.org"}})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/info", nil))
	if !strings.Contains(w.Body.String(), `"accounts":"portal"`) || !strings.Contains(w.Body.String(), `"portalUrl":"https://portal.example.org"`) ||
		!strings.Contains(w.Body.String(), `"registration":false`) {
		t.Fatalf("info: %s", w.Body)
	}
}

func TestAlertFeatures(t *testing.T) {
	for delivery, want := range map[string]string{"": `"alerts":false`, "push": `"alertDelivery":"push"`, "email": `"alertDelivery":"email"`} {
		h := New(nil, nil, nil, Options{AlertDelivery: delivery})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/info", nil))
		if !strings.Contains(w.Body.String(), want) || (delivery != "" && !strings.Contains(w.Body.String(), `"alerts":true`)) {
			t.Errorf("%q: %s", delivery, w.Body)
		}
		if delivery == "" && strings.Contains(w.Body.String(), "alertDelivery") {
			t.Errorf("alertDelivery present while off: %s", w.Body)
		}
	}
}
