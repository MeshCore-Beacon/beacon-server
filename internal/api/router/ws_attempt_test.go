// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestWebSocketAttemptClientIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, firstPeer, secondPeer, firstHeader, secondHeader string
		trusted, shared                                        bool
	}{
		{"untrusted rotation", "198.51.100.1:1", "198.51.100.1:2", "192.0.2.1", "192.0.2.2", false, true},
		{"independent peers", "198.51.100.1:1", "198.51.100.2:1", "192.0.2.1", "192.0.2.1", false, false},
		{"trusted independent clients", "127.0.0.1:1", "127.0.0.1:2", "198.51.100.1", "198.51.100.2", true, false},
		{"trusted shared client", "127.0.0.1:1", "127.0.0.1:2", "198.51.100.1", "198.51.100.1", true, true},
		{"IPv6 shared prefix", "[2001:db8:1::1]:1", "[2001:db8:1::2]:2", "", "", false, true},
		{"IPv6 independent prefixes", "[2001:db8:1::1]:1", "[2001:db8:2::1]:2", "", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.ServerConfig{}
			if tc.trusted {
				cfg.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
			}
			handler := New(nil, nil, nil, 5, 1, config.CORSConfig{}, cfg)
			attempt := func(peer, header string) int {
				request := httptest.NewRequest(http.MethodGet, "/ws", nil)
				request.RemoteAddr = peer
				request.Header.Set("X-Real-IP", header)
				request.Header.Set("X-Forwarded-For", header)
				request.Header.Set("True-Client-IP", header)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				return response.Code
			}
			first := attempt(tc.firstPeer, tc.firstHeader)
			if first < 400 || first == http.StatusTooManyRequests {
				t.Fatalf("first failed handshake: %d", first)
			}
			second := attempt(tc.secondPeer, tc.secondHeader)
			if (second == http.StatusTooManyRequests) != tc.shared || second < 400 {
				t.Fatalf("second handshake status %d; shared budget=%v", second, tc.shared)
			}
		})
	}
}
