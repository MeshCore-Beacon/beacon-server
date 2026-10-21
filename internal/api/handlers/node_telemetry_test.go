// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNodeTelemetryGatewayPreservesSignedBytesAndStripsCredentials(t *testing.T) {
	payload := `{ "authorization":"session", "payload":"exact-base64", "signature":"signed" }`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != payload || r.URL.Path != "/intake/reports" {
			t.Errorf("altered request: %s %s", r.URL.Path, body)
		}
		for _, header := range []string{"Authorization", "Cookie", "X-Forwarded-For"} {
			if r.Header.Get(header) != "" {
				t.Errorf("forwarded %s", header)
			}
		}
		if r.Header.Get("X-Real-IP") != "192.0.2.1" {
			t.Errorf("client limiter identity was not derived from RemoteAddr")
		}
		w.Header().Set("Set-Cookie", "secret=internal")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"accepted":true}`)
	}))
	defer upstream.Close()
	handler, err := NewNodeTelemetryGateway(upstream.URL + "/intake")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/reports", strings.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer unrelated")
	request.Header.Set("Cookie", "operator=secret")
	request.Header.Set("X-Forwarded-For", "spoofed")
	request.Header.Set("X-Real-IP", "spoofed")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 202 || response.Body.String() != `{"accepted":true}` || response.Header().Get("Set-Cookie") != "" {
		t.Fatalf("unexpected receipt: %d %s", response.Code, response.Body)
	}
}

func TestNodeTelemetryGatewayRejectsPathsAndOversizedReportsBeforeForwarding(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; _, _ = io.WriteString(w, `{}`) }))
	defer upstream.Close()
	handler, _ := NewNodeTelemetryGateway(upstream.URL)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", "/admin", `{}`, 404}, {"GET", "/challenge", "", 405},
		{"GET", "/abc", "", 404}, {"GET", "/availability?url=https://other.test", "", 400},
		{"POST", "/reports", strings.Repeat("x", 12001), 413},
		{"POST", "/enroll", "{broken", 400},
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		request.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(response, request)
		if response.Code != tc.status {
			t.Errorf("%s %s: %d, want %d", tc.method, tc.path, response.Code, tc.status)
		}
	}
	if calls != 0 {
		t.Fatalf("forwarded %d rejected requests", calls)
	}
}

func TestNodeTelemetryGatewayFailsClosedAndKeepsRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name               string
		upstream, expected int
		body               string
	}{
		{"quota", 429, 429, "private gateway error"},
		{"unavailable", 503, 503, "private connection details"},
		{"redirect", 302, 502, ""},
		{"invalid JSON", 200, 502, "not json"},
		{"read availability", 200, 200, `{"items":[],"retentionDays":30,"truncated":false}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Retry-After", "3600")
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(tc.upstream)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer upstream.Close()
			handler, _ := NewNodeTelemetryGateway(upstream.URL)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/availability", nil))
			if response.Code != tc.expected || calls != 1 {
				t.Fatalf("status %d; calls %d", response.Code, calls)
			}
			if tc.upstream >= 400 && (strings.Contains(response.Body.String(), "private") || response.Header().Get("Retry-After") != "3600") {
				t.Fatal("leaked error or lost retry window")
			}
		})
	}
}

func TestNodeTelemetryGatewayConfiguration(t *testing.T) {
	handler, err := NewNodeTelemetryGateway("")
	if handler != nil || err != nil {
		t.Fatal("must be off by default")
	}
	for _, url := range []string{"file:///tmp/data", "https://user:password@host", "https://host?token=private", "https://host/#fragment", "http://", "https://host/path%2fescape"} {
		if _, err = NewNodeTelemetryGateway(url); err == nil {
			t.Errorf("accepted unsafe URL %s", url)
		}
	}
}
