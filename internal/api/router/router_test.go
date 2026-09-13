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
	checkWebSocketLimit(t, config.ServerConfig{}, "198.51.100.2", http.StatusTooManyRequests)
}

func TestWebSocketLimitUsesTrustedClientIP(t *testing.T) {
	cfg := config.ServerConfig{TrustedProxies: []netip.Prefix{
		netip.MustParsePrefix("127.0.0.1/8"), netip.MustParsePrefix("::1/128"),
	}}
	t.Run("different clients", func(t *testing.T) {
		checkWebSocketLimit(t, cfg, "198.51.100.2", http.StatusSwitchingProtocols)
	})
	t.Run("same client", func(t *testing.T) {
		checkWebSocketLimit(t, cfg, "198.51.100.1", http.StatusTooManyRequests)
	})
}

func checkWebSocketLimit(t *testing.T, cfg config.ServerConfig, secondIP string, wantStatus int) {
	t.Helper()
	server := httptest.NewServer(New(hub.New(), nil, nil, 1, config.CORSConfig{}, cfg, config.AuthConfig{}))
	defer server.Close()
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
	if second != nil {
		second.CloseNow()
	}
	if response == nil || response.StatusCode != wantStatus || (err == nil) != (wantStatus == http.StatusSwitchingProtocols) {
		t.Fatalf("second connection: response=%v err=%v, want status %d", response, err, wantStatus)
	}
}
