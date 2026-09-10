// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestDefaultAPIRateLimit(t *testing.T) {
	handler := New(nil, nil, nil, 5, config.CORSConfig{}, config.ServerConfig{}, config.Resolve(&config.Config{}).RateLimit)
	for i := 0; i <= 300; i++ {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/brokers", nil)
		request.RemoteAddr = "198.51.100.1:1234"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		want := http.StatusOK
		if i == 300 {
			want = http.StatusTooManyRequests
		}
		if response.Code != want {
			t.Fatalf("request %d: got %d, want %d", i+1, response.Code, want)
		}
	}
}

func rateRequest(handler http.Handler, path, peer, headerIP string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.RemoteAddr = peer
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("X-Real-IP", headerIP)
	request.Header.Set("X-Forwarded-For", headerIP)
	request.Header.Set("True-Client-IP", headerIP)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestAPIRateLimitContractAndLogging(t *testing.T) {
	var logs bytes.Buffer
	previous := middleware.DefaultLogger
	middleware.DefaultLogger = middleware.RequestLogger(&middleware.DefaultLogFormatter{Logger: log.New(&logs, "", 0), NoColor: true})
	t.Cleanup(func() { middleware.DefaultLogger = previous })
	proxy := config.ServerConfig{TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}
	handler := New(nil, nil, nil, 5, config.CORSConfig{}, proxy, config.ResolvedRateLimitConfig{Enabled: true, RequestsPerMinute: 2, Burst: 10})
	for _, path := range []string{"/api/v1/brokers", "/api/v1/packets?limit=0"} {
		response := rateRequest(handler, path, "127.0.0.1:1234", "198.51.100.25")
		if response.Code != http.StatusOK && response.Code != http.StatusBadRequest {
			t.Fatalf("initial request: %d", response.Code)
		}
	}
	response := rateRequest(handler, "/api/v1/brokers", "127.0.0.1:1234", "198.51.100.25")
	var body struct {
		Error struct{ Code, Message string }
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusTooManyRequests || body.Error.Code != "rate_limited" || body.Error.Message == "" {
		t.Fatalf("wrong rate-limit response: %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Retry-After") != "60" || !strings.EqualFold(response.Header().Get("Access-Control-Expose-Headers"), "Retry-After") || response.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatalf("missing JSON/backoff/CORS headers: %v", response.Header())
	}
	if strings.Count(logs.String(), " - 429 ") != 1 || !strings.Contains(logs.String(), "from 198.51.100.25 - 429 ") || !strings.Contains(logs.String(), "/api/v1/brokers") {
		t.Fatalf("expected one rejection log with resolved client and path: %s", logs.String())
	}
}

func TestAPIRateLimitClientIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, firstPeer, secondPeer, firstHeader, secondHeader string
		trusted, shared                                        bool
	}{
		{"untrusted header rotation", "198.51.100.1:1", "198.51.100.1:2", "192.0.2.1", "192.0.2.2", false, true},
		{"independent peers", "198.51.100.1:1", "198.51.100.2:1", "192.0.2.1", "192.0.2.1", false, false},
		{"trusted independent clients", "127.0.0.1:1", "127.0.0.1:2", "198.51.100.1", "198.51.100.2", true, false},
		{"trusted shared client", "127.0.0.1:1", "127.0.0.1:2", "198.51.100.1", "198.51.100.1", true, true},
		{"IPv6 shared prefix", "[2001:db8:1::1]:1", "[2001:db8:1::2]:2", "", "", false, true},
		{"IPv6 independent prefixes", "[2001:db8:1::1]:1", "[2001:db8:2::1]:2", "", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxy := config.ServerConfig{}
			if tc.trusted {
				proxy.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
			}
			handler := New(nil, nil, nil, 5, config.CORSConfig{}, proxy, config.ResolvedRateLimitConfig{Enabled: true, RequestsPerMinute: 1, Burst: 1})
			if response := rateRequest(handler, "/api/v1/brokers", tc.firstPeer, tc.firstHeader); response.Code != http.StatusOK {
				t.Fatalf("first client: %d", response.Code)
			}
			want := http.StatusOK
			if tc.shared {
				want = http.StatusTooManyRequests
			}
			if response := rateRequest(handler, "/api/v1/brokers", tc.secondPeer, tc.secondHeader); response.Code != want {
				t.Fatalf("second client: %d, want %d", response.Code, want)
			}
		})
	}
}

func TestAPIRateLimitWindowsAndExclusions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := New(nil, nil, nil, 5, config.CORSConfig{}, config.ServerConfig{}, config.ResolvedRateLimitConfig{Enabled: true, RequestsPerMinute: 4, Burst: 2})
		request := func() *httptest.ResponseRecorder {
			return rateRequest(handler, "/api/v1/brokers", "198.51.100.1:1", "")
		}
		preflight := httptest.NewRequest(http.MethodOptions, "/api/v1/brokers", nil)
		preflight.RemoteAddr = "198.51.100.1:1"
		preflight.Header.Set("Origin", "https://example.test")
		preflight.Header.Set("Access-Control-Request-Method", "GET")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, preflight)
		if response.Code != http.StatusOK || request().Code != http.StatusOK || request().Code != http.StatusOK {
			t.Fatal("preflight consumed the API budget")
		}
		if response := request(); response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "1" {
			t.Fatalf("burst limit: %d %v", response.Code, response.Header())
		}
		time.Sleep(2 * time.Second)
		if request().Code != http.StatusOK {
			t.Fatal("burst did not recover")
		}
		if response := request(); response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "60" {
			t.Fatalf("minute limit: %d %v", response.Code, response.Header())
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, preflight)
		if response.Code != http.StatusOK {
			t.Fatal("exhausted API budget blocked preflight")
		}
		if response := rateRequest(handler, "/swagger", "198.51.100.1:1", ""); response.Code != http.StatusMovedPermanently {
			t.Fatalf("Swagger shares the API budget: %d", response.Code)
		}
		time.Sleep(2 * time.Minute)
		if request().Code != http.StatusOK {
			t.Fatal("minute budget did not recover")
		}
	})
	handler := New(nil, nil, nil, 5, config.CORSConfig{}, config.ServerConfig{}, config.ResolvedRateLimitConfig{Enabled: false, RequestsPerMinute: 1, Burst: 1})
	for range 5 {
		if response := rateRequest(handler, "/api/v1/brokers", "198.51.100.1:1", ""); response.Code != http.StatusOK || response.Header().Get("Retry-After") != "" {
			t.Fatal("disabled limiter still applied")
		}
	}
}

func TestAPIRateLimitConcurrentRequests(t *testing.T) {
	handler := New(nil, nil, nil, 5, config.CORSConfig{}, config.ServerConfig{}, config.ResolvedRateLimitConfig{Enabled: true, RequestsPerMinute: 5, Burst: 5})
	var accepted, rejected atomic.Int32
	var group sync.WaitGroup
	for range 20 {
		group.Go(func() {
			switch rateRequest(handler, "/api/v1/brokers", "198.51.100.1:1", "").Code {
			case http.StatusOK:
				accepted.Add(1)
			case http.StatusTooManyRequests:
				rejected.Add(1)
			}
		})
	}
	group.Wait()
	if accepted.Load() != 5 || rejected.Load() != 15 {
		t.Fatalf("concurrent budget: accepted=%d rejected=%d", accepted.Load(), rejected.Load())
	}
}
