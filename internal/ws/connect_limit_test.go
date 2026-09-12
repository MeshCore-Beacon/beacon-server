// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/coder/websocket"
)

func connectTestServer(t *testing.T, maxConns, maxConnects int) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	handler := Handler(hub.New(), nil, maxConns, maxConnects)
	done := make(chan struct{}, 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
		done <- struct{}{}
	}))
	t.Cleanup(server.Close)
	return server, done
}

func TestFailedHandshakeDoesNotUseConnectionSlot(t *testing.T) {
	server, done := connectTestServer(t, 1, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode < 400 || response.StatusCode == http.StatusTooManyRequests {
		t.Fatalf("expected a failed handshake: %d", response.StatusCode)
	}
	waitConnectionEnd(t, ctx, done)
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("failed handshake consumed a connection slot: %v", err)
	}
	conn.CloseNow()
	waitConnectionEnd(t, ctx, done)
}

func TestConnectAttemptBudgetAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := Handler(nil, nil, 1, 2)
		attempt := func() *httptest.ResponseRecorder {
			request := httptest.NewRequest(http.MethodGet, "/ws", nil)
			request.RemoteAddr = "198.51.100.1:1234"
			response := httptest.NewRecorder()
			handler(response, request)
			return response
		}
		for range 2 {
			if response := attempt(); response.Code < 400 || response.Code == http.StatusTooManyRequests {
				t.Fatalf("expected failed handshake within budget: %d", response.Code)
			}
		}
		if response := attempt(); response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "60" {
			t.Fatalf("failed handshakes bypassed attempt budget: %d %v", response.Code, response.Header())
		}
		time.Sleep(2 * time.Minute)
		if response := attempt(); response.Code < 400 || response.Code == http.StatusTooManyRequests {
			t.Fatalf("attempt budget did not recover: %d", response.Code)
		}
	})
}

func TestConcurrentConnectsRespectBothLimits(t *testing.T) {
	for _, tc := range []struct {
		name               string
		conns, attempts    int
		wantOpen, wantShed int32
		wantRejected       int32
	}{
		{"concurrent cap", 2, 20, 2, 6, 0},
		{"attempt cap", 20, 2, 2, 0, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, done := connectTestServer(t, tc.conns, tc.attempts)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var opened, shed, rejected atomic.Int32
			connections := make(chan *websocket.Conn, 8)
			defer func() {
				close(connections)
				for conn := range connections {
					conn.CloseNow()
				}
				for range 8 {
					waitConnectionEnd(t, ctx, done)
				}
			}()
			var group sync.WaitGroup
			for range 8 {
				group.Go(func() {
					conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
					if err != nil {
						if response != nil && response.StatusCode == http.StatusTooManyRequests {
							rejected.Add(1)
						} else {
							t.Errorf("unexpected handshake failure: %v", err)
						}
						return
					}
					connections <- conn // Keep accepted connections open until every attempt completes.
					if _, _, err := conn.Read(ctx); err == nil {
						opened.Add(1)
					} else if websocket.CloseStatus(err) == websocket.StatusTryAgainLater {
						shed.Add(1)
					} else {
						t.Errorf("unexpected WebSocket read error: %v", err)
					}
				})
			}
			group.Wait()
			if opened.Load() != tc.wantOpen || shed.Load() != tc.wantShed || rejected.Load() != tc.wantRejected {
				t.Fatalf("opened=%d shed=%d rejected=%d", opened.Load(), shed.Load(), rejected.Load())
			}
		})
	}
}

func waitConnectionEnd(t *testing.T, ctx context.Context, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("connection handler did not release its slot")
	}
}

func TestConnectChurnLimited(t *testing.T) {
	server, done := connectTestServer(t, 1, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	for range 10 {
		conn, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := conn.Read(ctx); err != nil {
			conn.CloseNow()
			t.Fatalf("read hello: %v", err)
		}
		conn.CloseNow()
		waitConnectionEnd(t, ctx, done)
	}
	conn, response, err := websocket.Dial(ctx, url, nil)
	if conn != nil {
		conn.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusTooManyRequests {
		t.Fatal("eleventh connection attempt was not rejected with HTTP 429")
	}
	if response.Header.Get("Retry-After") != "60" {
		t.Fatal("missing upgrade retry guidance")
	}
}
