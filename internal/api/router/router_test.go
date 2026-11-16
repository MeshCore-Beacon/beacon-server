// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

func TestWebSocketLimitIgnoresUntrustedHeaders(t *testing.T) {
	checkWebSocketLimit(t, config.ServerConfig{}, "198.51.100.2", true)
}

func TestWebSocketLimitUsesTrustedClientIP(t *testing.T) {
	cfg := config.ServerConfig{TrustedProxies: []netip.Prefix{
		netip.MustParsePrefix("127.0.0.1/8"), netip.MustParsePrefix("::1/128"),
	}}
	t.Run("different clients", func(t *testing.T) {
		checkWebSocketLimit(t, cfg, "198.51.100.2", false)
	})
	t.Run("same client", func(t *testing.T) {
		checkWebSocketLimit(t, cfg, "198.51.100.1", true)
	})
}

func checkWebSocketLimit(t *testing.T, cfg config.ServerConfig, secondIP string, wantShed bool) {
	t.Helper()
	server := httptest.NewServer(New(hub.New(), nil, nil, Options{MaxConnsPerIP: 1, MaxConnectsPerMinute: 10, Server: cfg, RateLimit: config.ResolvedRateLimitConfig{Enabled: true, RequestsPerMinute: 1, Burst: 1}}))
	defer server.Close()
	// Exhaust this client's REST budget before checking its independent WS cap.
	for _, want := range []int{http.StatusOK, http.StatusTooManyRequests} {
		request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/brokers", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Real-IP", "198.51.100.1")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("REST budget setup: %d, want %d", response.StatusCode, want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	dial := func(ip string) (*websocket.Conn, *http.Response, error) {
		return websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{
			"X-Real-Ip": {ip}, "True-Client-Ip": {ip}, "X-Forwarded-For": {ip},
		}})
	}
	first, _, err := dial("198.51.100.1")
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseNow()
	if _, _, err := first.Read(ctx); err != nil {
		t.Fatalf("read hello: %v", err)
	}
	second, response, err := dial(secondIP)
	if err != nil || response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("second handshake: response=%v err=%v", response, err)
	}
	defer second.CloseNow()
	_, _, err = second.Read(ctx)
	if wantShed && websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
		t.Fatalf("expected close 1013, got %v", err)
	}
	if !wantShed && err != nil {
		t.Fatalf("independent client did not receive hello: %v", err)
	}
	if err := first.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"ping","id":"still-active"}`)); err != nil {
		t.Fatal(err)
	}
	_, pong, err := first.Read(ctx)
	if err != nil || !strings.Contains(string(pong), `"type":"pong"`) {
		t.Fatalf("established client stopped responding: %s %v", pong, err)
	}
}

// Old servers 404 here and the app reads that as "no requirement", so the route must stay public.
func TestInfoRoute(t *testing.T) {
	handler := New(nil, nil, nil, Options{MinAppVersion: "0.1.1", MinWebVersion: "2.0.3", ServerVersion: "2.0.2"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/info", nil))
	if body := response.Body.String(); response.Code != http.StatusOK || !strings.Contains(body, `"minAppVersion":"0.1.1"`) ||
		!strings.Contains(body, `"minWebVersion":"2.0.3"`) {
		t.Fatalf("info: %d %s", response.Code, response.Body.String())
	}
}

func TestInfoAccountsFeatures(t *testing.T) {
	for name, tc := range map[string]struct {
		opts Options
		want string
	}{
		"off":                      {Options{}, `"accounts":"off","registration":false`},
		"local open":               {Options{AccountsMode: "local", RegistrationOpen: true}, `"accounts":"local","registration":true`},
		"local closed":             {Options{AccountsMode: "local"}, `"accounts":"local","registration":false`},
		"off ignores registration": {Options{RegistrationOpen: true}, `"accounts":"off","registration":false`},
	} {
		handler := New(nil, nil, nil, tc.opts)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/info", nil))
		if body := response.Body.String(); response.Code != http.StatusOK || !strings.Contains(body, tc.want) {
			t.Errorf("%s: %d %s", name, response.Code, body)
		}
	}
}

func TestInfoFeatures(t *testing.T) {
	for polling, want := range map[bool]string{
		true:  `"features":{"polling":true,"accounts":"off","registration":false,"telemetryRequests":false,"alerts":false,"atlasTelemetry":true,"mapTelemetry":true}`,
		false: `"features":{"polling":false,"accounts":"off","registration":false,"telemetryRequests":false,"alerts":false,"atlasTelemetry":true,"mapTelemetry":true}`,
	} {
		handler := New(nil, nil, nil, Options{ServerVersion: "2.0.2", PollingEnabled: polling})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/info", nil))
		if body := response.Body.String(); response.Code != http.StatusOK || !strings.Contains(body, want) ||
			!strings.Contains(body, `"minAppVersion":null`) || !strings.Contains(body, `"serverVersion":"2.0.2"`) {
			t.Errorf("polling=%v: %d %s", polling, response.Code, body)
		}
	}
}

func TestInfoDisplay(t *testing.T) {
	for name, tc := range map[string]struct {
		opts Options
		want string
	}{
		"default":  {Options{}, `"display":{"temperatureUnit":"c","distanceUnit":"metric"}`},
		"imperial": {Options{TemperatureUnit: "f", DistanceUnit: "imperial"}, `"display":{"temperatureUnit":"f","distanceUnit":"imperial"}`},
	} {
		handler := New(nil, nil, nil, tc.opts)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/info", nil))
		if body := response.Body.String(); response.Code != http.StatusOK || !strings.Contains(body, tc.want) || !strings.Contains(body, `"mapTelemetry":true`) {
			t.Errorf("%s: %d %s", name, response.Code, body)
		}
	}
}
