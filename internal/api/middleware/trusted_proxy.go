// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"net/netip"
)

// TrustedProxyIP accepts X-Real-IP only from a configured direct proxy peer.
// Run it before the access logger and handlers that use RemoteAddr as a key.
// All other forwarding headers are ignored, as are invalid/ambiguous values.
func TrustedProxyIP(proxies []netip.Prefix) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			peer, err := netip.ParseAddrPort(r.RemoteAddr)
			if err == nil {
				for _, proxy := range proxies {
					if !proxy.Contains(peer.Addr().Unmap()) {
						continue
					}
					if values := r.Header.Values("X-Real-IP"); len(values) == 1 {
						if ip, err := netip.ParseAddr(values[0]); err == nil && ip.Zone() == "" {
							r.RemoteAddr = ip.Unmap().String()
						}
					}
					break
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
