// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"log/slog"
	"net/http"
	"net/netip"
	"sync"
)

// TrustedProxyIP accepts X-Real-IP only from a configured direct proxy peer.
// Run it before the access logger and handlers that use RemoteAddr as a key.
// All other forwarding headers are ignored, as are invalid/ambiguous values.
func TrustedProxyIP(proxies []netip.Prefix) func(http.Handler) http.Handler {
	// Misconfigured proxies key every visitor to one IP; say so once, not per request.
	var untrustedOnce, noRealIPOnce sync.Once
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			peer, err := netip.ParseAddrPort(r.RemoteAddr)
			if err == nil {
				trusted := false
				for _, proxy := range proxies {
					if !proxy.Contains(peer.Addr().Unmap()) {
						continue
					}
					trusted = true
					if values := r.Header.Values("X-Real-IP"); len(values) == 1 {
						if ip, err := netip.ParseAddr(values[0]); err == nil && ip.Zone() == "" {
							r.RemoteAddr = ip.Unmap().String()
						}
					} else if len(values) == 0 && r.Header.Get("X-Forwarded-For") != "" {
						noRealIPOnce.Do(func() {
							slog.Warn("trusted proxy sent X-Forwarded-For without X-Real-IP; clients share its rate limits until the proxy sets X-Real-IP (see server.trusted_proxies)",
								"component", "http", "peer", peer.Addr().Unmap().String())
						})
					}
					break
				}
				if !trusted && hasForwardingHeader(r.Header) {
					untrustedOnce.Do(func() {
						slog.Warn("forwarding headers from an untrusted peer were ignored; if it is your reverse proxy, add it to server.trusted_proxies and have it set X-Real-IP, or all clients share its rate limits",
							"component", "http", "peer", peer.Addr().Unmap().String())
					})
				}
			}
			// Downstream client-IP keys must use the resolved RemoteAddr, not
			// reinterpret forwarding headers (including those from trusted peers).
			r.Header.Del("True-Client-IP")
			r.Header.Del("X-Forwarded-For")
			r.Header.Del("X-Real-IP")
			next.ServeHTTP(w, r)
		})
	}
}

func hasForwardingHeader(h http.Header) bool {
	return len(h.Values("X-Forwarded-For")) > 0 || len(h.Values("X-Real-IP")) > 0 || len(h.Values("True-Client-IP")) > 0
}
