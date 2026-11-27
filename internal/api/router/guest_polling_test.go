// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

func firmwareRequest(h http.Handler, method, path, peer string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = peer
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRoutesMountedWithoutPolling(t *testing.T) {
	// A plain nil, not a typed nil, which is what main must pass when polling is off.
	h := New(nil, nil, nil, Options{})
	want, _ := json.Marshal(polling.DisabledPolicy())
	if rec := firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy", "198.51.100.1:1"); rec.Code != 200 || rec.Body.String() != string(want) {
		t.Fatalf("policy: %d %s", rec.Code, rec.Body)
	}
	if rec := firmwareRequest(h, http.MethodGet, "/api/guest-polling/owners", "198.51.100.1:1"); rec.Code != 200 || !strings.HasPrefix(rec.Body.String(), `{"nodes":{},"policy":{`) {
		t.Fatalf("owners: %d %s", rec.Code, rec.Body)
	}
	for _, c := range [][2]string{{http.MethodPost, "/api/guest-keys"}, {http.MethodGet, "/api/shared-guest/key"}} {
		rec := firmwareRequest(h, c[0], c[1], "198.51.100.1:1")
		if rec.Code != 404 || !strings.Contains(rec.Body.String(), `"code":"not_found"`) {
			t.Fatalf("%s %s: %d %s", c[0], c[1], rec.Code, rec.Body)
		}
	}
}

func TestRoutesRejectOtherMethods(t *testing.T) {
	h := New(nil, nil, nil, Options{})
	for _, c := range [][2]string{
		{http.MethodPost, "/api/guest-polling/policy"},
		{http.MethodPost, "/api/guest-polling/owners"},
		{http.MethodGet, "/api/guest-keys"},
		{http.MethodPost, "/api/shared-guest/key"},
	} {
		if rec := firmwareRequest(h, c[0], c[1], "198.51.100.1:1"); rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s: got %d", c[0], c[1], rec.Code)
		}
	}
}

func TestRoutesRateLimited(t *testing.T) {
	h := New(nil, nil, nil, Options{})
	for i := 1; i <= 241; i++ {
		rec := firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy", "198.51.100.7:1")
		want := http.StatusOK
		if i == 241 {
			want = http.StatusTooManyRequests
		}
		if rec.Code != want {
			t.Fatalf("request %d: got %d, want %d", i, rec.Code, want)
		}
	}
	// The limiter is per route group, not shared with /api/v1 or other callers.
	if rec := firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy", "198.51.100.8:1"); rec.Code != 200 {
		t.Fatalf("other IP: %d", rec.Code)
	}
}

func TestV1UnaffectedByFirmwareLimiter(t *testing.T) {
	h := New(nil, nil, nil, Options{RateLimit: config.ResolvedRateLimitConfig{}})
	for i := 0; i < 250; i++ {
		firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy", "198.51.100.9:1")
	}
	if rec := firmwareRequest(h, http.MethodGet, "/api/v1/brokers", "198.51.100.9:1"); rec.Code != 200 {
		t.Fatalf("v1 after firmware flood: %d", rec.Code)
	}
}

func TestQueryStringNotLogged(t *testing.T) {
	var logs bytes.Buffer
	previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags) })
	obs := strings.Repeat("ab", 32)
	h := New(nil, nil, nil, Options{})
	firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy?observer="+obs+"&client=x", "198.51.100.1:1")
	if logs.Len() == 0 {
		t.Fatal("expected a request log line")
	}
	if strings.Contains(logs.String(), obs) || strings.Contains(logs.String(), "observer=") {
		t.Fatalf("query string leaked into logs: %s", logs.String())
	}
}

func TestKeyRoutesAlways404(t *testing.T) {
	h := New(nil, nil, nil, Options{})
	for range 241 {
		firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy", "198.51.100.9:1")
	}
	if rec := firmwareRequest(h, http.MethodGet, "/api/guest-polling/policy", "198.51.100.9:1"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("limiter not exhausted: %d", rec.Code)
	}
	for _, c := range [][2]string{{http.MethodPost, "/api/guest-keys"}, {http.MethodGet, "/api/shared-guest/key"}} {
		for range 3 {
			if rec := firmwareRequest(h, c[0], c[1], "198.51.100.9:1"); rec.Code != 404 {
				t.Fatalf("%s %s: %d", c[0], c[1], rec.Code)
			}
		}
	}
}
