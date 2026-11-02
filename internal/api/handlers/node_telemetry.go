// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

var telemetryKeyPath = regexp.MustCompile(`^/[0-9a-f]{64}$`)

// NewNodeTelemetryGateway exposes only the companion intake contract. Validation,
// signature/session checks, quotas and durable receipts belong to the separate
// intake service; Beacon never interprets a submitted report as observed RF.
// Empty configuration keeps the feature off without contacting another service.
func NewNodeTelemetryGateway(endpoint string) (http.Handler, error) {
	target, err := config.ParseNodeTelemetryURL(endpoint)
	if err != nil || target == nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxConnsPerHost = 32
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		method := ""
		switch r.URL.Path {
		case "/challenge", "/enroll", "/reports":
			method = http.MethodPost
		case "/availability":
			method = http.MethodGet
		default:
			if telemetryKeyPath.MatchString(r.URL.Path) {
				method = http.MethodGet
			}
		}
		if method == "" {
			respond(w, 404, map[string]string{"error": "Telemetry endpoint not found"})
			return
		}
		if method != r.Method {
			w.Header().Set("Allow", method)
			respond(w, 405, map[string]string{"error": "Method not allowed"})
			return
		}
		// No caller-controlled target, query, credentials, cookies or forwarding
		// headers. Only the exact signed JSON envelope is relayed on writes.
		if r.URL.RawQuery != "" || r.URL.RawPath != "" {
			respond(w, 400, map[string]string{"error": "Telemetry query parameters are not supported"})
			return
		}
		var body []byte
		if method == http.MethodPost {
			mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if mediaErr != nil || mediaType != "application/json" {
				respond(w, 415, map[string]string{"error": "Use application/json"})
				return
			}
			var readErr error
			body, readErr = io.ReadAll(http.MaxBytesReader(w, r.Body, 12000))
			if readErr != nil {
				respond(w, 413, map[string]string{"error": "Telemetry envelope too large"})
				return
			}
			if !json.Valid(body) {
				respond(w, 400, map[string]string{"error": "Invalid JSON envelope"})
				return
			}
		}
		destination := *target
		destination.Path = strings.TrimRight(target.Path, "/") + r.URL.Path
		request, requestErr := http.NewRequestWithContext(r.Context(), method, destination.String(), bytes.NewReader(body))
		if requestErr != nil {
			respond(w, 502, map[string]string{"error": "Telemetry intake unavailable"})
			return
		}
		request.Header.Set("Accept", "application/json")
		// TrustedProxyIP has already resolved RemoteAddr. Do not copy a caller's
		// forwarding headers; the gateway must trust only Beacon's service peer.
		host, _, splitErr := net.SplitHostPort(r.RemoteAddr)
		if splitErr != nil {
			host = r.RemoteAddr
		}
		if address, parseErr := netip.ParseAddr(host); parseErr == nil && address.Zone() == "" {
			request.Header.Set("X-Real-IP", address.Unmap().String())
		}
		if method == http.MethodPost {
			request.Header.Set("Content-Type", "application/json")
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			w.Header().Set("Retry-After", "60")
			respond(w, 503, map[string]string{"error": "Telemetry intake unavailable"})
			return
		}
		defer response.Body.Close()
		if response.StatusCode >= 400 {
			if retry, parseErr := strconv.Atoi(response.Header.Get("Retry-After")); parseErr == nil && retry > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(min(retry, 86400)))
			}
			respond(w, response.StatusCode, map[string]string{"error": "Telemetry intake rejected the request or is unavailable"})
			return
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			respond(w, 502, map[string]string{"error": "Invalid telemetry intake response"})
			return
		}
		const maxResponse = 8 << 20 // at most 500 bounded samples, or the availability index
		reply, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
		if readErr != nil || len(reply) > maxResponse || !json.Valid(reply) {
			respond(w, 502, map[string]string{"error": "Invalid telemetry intake response"})
			return
		}
		if method == http.MethodGet {
			w.Header().Set("Cache-Control", "public, max-age=60")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(reply)
	}), nil
}
