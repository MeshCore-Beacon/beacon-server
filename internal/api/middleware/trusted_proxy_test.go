// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestTrustedProxyIP(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("2001:db8:1::/48")}
	for _, tc := range []struct {
		name, peer string
		proxies    []netip.Prefix
		realIP     []string
		want       string
	}{
		{"empty allowlist", "192.0.2.10:1234", nil, []string{"198.51.100.1"}, "192.0.2.10:1234"},
		{"untrusted peer", "192.0.3.10:1234", trusted, []string{"198.51.100.1"}, "192.0.3.10:1234"},
		{"trusted IPv4", "192.0.2.10:1234", trusted, []string{"198.51.100.1"}, "198.51.100.1"},
		{"trusted IPv6", "[2001:db8:1::10]:1234", trusted, []string{"2001:db8:2::1"}, "2001:db8:2::1"},
		{"mapped peer", "[::ffff:192.0.2.10]:1234", trusted, []string{"198.51.100.1"}, "198.51.100.1"},
		{"mapped client", "192.0.2.10:1234", trusted, []string{"::ffff:198.51.100.1"}, "198.51.100.1"},
		{"missing header", "192.0.2.10:1234", trusted, nil, "192.0.2.10:1234"},
		{"empty header", "192.0.2.10:1234", trusted, []string{""}, "192.0.2.10:1234"},
		{"invalid IP", "192.0.2.10:1234", trusted, []string{"not-an-ip"}, "192.0.2.10:1234"},
		{"IP with port", "192.0.2.10:1234", trusted, []string{"198.51.100.1:8080"}, "192.0.2.10:1234"},
		{"IP with zone", "192.0.2.10:1234", trusted, []string{"fe80::1%eth0"}, "192.0.2.10:1234"},
		{"comma list", "192.0.2.10:1234", trusted, []string{"198.51.100.1, 198.51.100.2"}, "192.0.2.10:1234"},
		{"repeated header", "192.0.2.10:1234", trusted, []string{"198.51.100.1", "198.51.100.2"}, "192.0.2.10:1234"},
		{"invalid peer", "unknown", trusted, []string{"198.51.100.1"}, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.peer
			for _, ip := range tc.realIP {
				r.Header.Add("X-Real-IP", ip)
			}
			// These must never override the selected address, even for trusted peers.
			r.Header.Set("True-Client-IP", "203.0.113.1")
			r.Header.Set("X-Forwarded-For", "203.0.113.2")
			TrustedProxyIP(tc.proxies)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.RemoteAddr != tc.want {
					t.Errorf("RemoteAddr = %q, want %q", r.RemoteAddr, tc.want)
				}
			})).ServeHTTP(httptest.NewRecorder(), r)
		})
	}
}
