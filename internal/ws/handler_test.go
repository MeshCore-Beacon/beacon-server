// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/coder/websocket"
)

func TestAllowedOrigins(t *testing.T) {
	wildcard := []string{"https://*.example.com"}
	apexAndWildcard := []string{"https://example.com", "https://*.example.com"}
	for _, tc := range []struct {
		name    string
		allowed []string
		origin  string
		wantOK  bool
	}{
		{"no origin header", nil, "", true},
		{"foreign origin by default", nil, "https://example.com", false},
		{"listed origin", []string{"https://example.com"}, "https://example.com", true},
		{"listed origin, other case", []string{"https://example.com"}, "https://Example.com", true},
		{"scheme mismatch", []string{"https://example.com"}, "http://example.com", false},
		{"port mismatch", []string{"https://example.com"}, "https://example.com:8443", false},
		{"unlisted origin", []string{"https://example.com"}, "https://other.example", false},
		{"wildcard subdomain", wildcard, "https://sub.example.com", true},
		{"wildcard deeper subdomain", wildcard, "https://a.b.example.com", true},
		{"wildcard other case", wildcard, "https://SUB.Example.com", true},
		{"wildcard apex", wildcard, "https://example.com", false},
		{"wildcard lookalike", wildcard, "https://evilexample.com", false},
		{"wildcard suffix host", wildcard, "https://example.com.evil.net", false},
		{"wildcard scheme mismatch", wildcard, "http://sub.example.com", false},
		{"wildcard port mismatch", wildcard, "https://sub.example.com:8443", false},
		{"apex and wildcard, apex", apexAndWildcard, "https://example.com", true},
		{"apex and wildcard, subdomain", apexAndWildcard, "https://sub.example.com", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(Handler(hub.New(), nil, 5, 100, tc.allowed))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			header := http.Header{}
			if tc.origin != "" {
				header.Set("Origin", tc.origin)
			}
			conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{HTTPHeader: header})
			if tc.wantOK {
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				conn.CloseNow()
				return
			}
			if err == nil {
				conn.CloseNow()
				t.Fatal("expected handshake to be refused")
			}
			if resp == nil || resp.StatusCode != http.StatusForbidden {
				t.Fatalf("expected 403, got %v (%v)", resp, err)
			}
		})
	}
}

func TestConfigureIncludeObserverKey(t *testing.T) {
	h := hub.New()
	go h.Run()
	server := httptest.NewServer(Handler(h, nil, 5, 100, nil))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	read := func(conn *websocket.Conn) map[string]json.RawMessage {
		t.Helper()
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var msg map[string]json.RawMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatal(err)
		}
		return msg
	}
	send := func(conn *websocket.Conn, msg string) {
		t.Helper()
		if err := conn.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
			t.Fatal(err)
		}
	}
	connect := func(configure string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.CloseNow() })
		read(conn) // hello
		if configure != "" {
			send(conn, configure)
			reply := read(conn)
			if string(reply["type"]) != `"configured"` || string(reply["includeObserverKey"]) != "true" || string(reply["resolvePath"]) != "false" {
				t.Fatalf("unexpected configured reply: %v", reply)
			}
		}
		send(conn, `{"v":1,"type":"subscribe","id":"s","scope":{}}`)
		read(conn) // subscribed
		return conn
	}

	plain := connect("")
	keyed := connect(`{"v":1,"type":"configure","id":"c","includeObserverKey":true}`)
	time.Sleep(20 * time.Millisecond) // let the hub apply both subscriptions
	h.Broadcast(hub.Event{
		Type:           hub.EventPacketObservation,
		Payload:        json.RawMessage(`{"variant":"base"}`),
		PayloadWithKey: json.RawMessage(`{"variant":"key"}`),
	})
	if got := string(read(plain)["data"]); got != `{"variant":"base"}` {
		t.Errorf("plain client got %s", got)
	}
	if got := string(read(keyed)["data"]); got != `{"variant":"key"}` {
		t.Errorf("opted-in client got %s", got)
	}
}

func TestConfigureIncludeRepeats(t *testing.T) {
	h := hub.New()
	go h.Run()
	server := httptest.NewServer(Handler(h, nil, 5, 100, nil))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	read := func() map[string]json.RawMessage {
		t.Helper()
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var msg map[string]json.RawMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatal(err)
		}
		return msg
	}
	send := func(msg string) {
		t.Helper()
		if err := conn.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
			t.Fatal(err)
		}
	}
	configure := func(msg, want string) {
		t.Helper()
		send(msg)
		if reply := read(); string(reply["type"]) != `"configured"` || string(reply["includeRepeats"]) != want {
			t.Fatalf("configured reply: %v", reply)
		}
		time.Sleep(20 * time.Millisecond) // let the hub apply it
	}
	// Each repeat is followed by a normal marker, so a skipped repeat shows up as the marker.
	next := func() string {
		t.Helper()
		h.BroadcastRepeat(hub.Event{Type: hub.EventPacketObservation, Payload: json.RawMessage(`"repeat"`)})
		h.Broadcast(hub.Event{Type: hub.EventPacketObservation, Payload: json.RawMessage(`"marker"`)})
		got := string(read()["data"])
		if got == `"repeat"` {
			read() // marker
		}
		return got
	}

	read() // hello
	configure(`{"v":1,"type":"configure","id":"c","includeRepeats":true}`, "true")
	send(`{"v":1,"type":"subscribe","id":"s","scope":{}}`)
	read() // subscribed
	time.Sleep(20 * time.Millisecond)
	if got := next(); got != `"repeat"` {
		t.Fatalf("opted-in client got %s", got)
	}
	configure(`{"v":1,"type":"configure","id":"c","resolvePath":true}`, "false")
	if got := next(); got != `"marker"` {
		t.Fatalf("client still received a repeat after opting out: %s", got)
	}
}
