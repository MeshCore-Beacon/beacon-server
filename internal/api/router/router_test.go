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
	server := httptest.NewServer(New(hub.New(), nil, nil, 1, 10, config.CORSConfig{}, cfg))
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
