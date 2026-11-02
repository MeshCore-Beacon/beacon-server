// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/wsticket"
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
			server := httptest.NewServer(Handler(hub.New(), nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100}, Access{AllowedOrigins: tc.allowed}))
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
	server := httptest.NewServer(Handler(h, nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100}, Access{}))
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
	server := httptest.NewServer(Handler(h, nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100}, Access{}))
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

func TestCompressionNegotiated(t *testing.T) {
	const chrome = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
	const iPhone = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1"
	for _, tc := range []struct {
		name      string
		allowed   []string
		userAgent string
		mode      websocket.CompressionMode
		wantExt   string
	}{
		{"context takeover", nil, chrome, websocket.CompressionContextTakeover, "permessage-deflate"},
		{"context takeover with origins configured", []string{"https://example.com"}, chrome, websocket.CompressionContextTakeover, "permessage-deflate"},
		{"client asks for no context takeover", nil, chrome, websocket.CompressionNoContextTakeover, "permessage-deflate; client_no_context_takeover; server_no_context_takeover"},
		{"client without compression", nil, chrome, websocket.CompressionDisabled, ""},
		{"iPhone offering deflate gets plain frames", nil, iPhone, websocket.CompressionContextTakeover, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := hub.New()
			go h.Run()
			server := httptest.NewServer(Handler(h, nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100}, Access{AllowedOrigins: tc.allowed}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{CompressionMode: tc.mode, HTTPHeader: http.Header{"User-Agent": {tc.userAgent}}})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			t.Cleanup(func() { conn.CloseNow() })
			if got := resp.Header.Get("Sec-WebSocket-Extensions"); got != tc.wantExt {
				t.Fatalf("Sec-WebSocket-Extensions = %q, want %q", got, tc.wantExt)
			}
			if _, data, err := conn.Read(ctx); err != nil || !strings.Contains(string(data), `"hello"`) {
				t.Fatalf("hello: %q, %v", data, err)
			}
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"subscribe","id":"s","scope":{}}`)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := conn.Read(ctx); err != nil { // subscribed
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond) // let the hub apply the subscription
			payload := `{"packetHash":"` + strings.Repeat("ab", 400) + `"}`
			h.Broadcast(hub.Event{Type: hub.EventPacketObservation, Payload: json.RawMessage(payload)})
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var msg map[string]json.RawMessage
			if err := json.Unmarshal(data, &msg); err != nil || string(msg["data"]) != payload {
				t.Fatalf("event data = %s, %v", msg["data"], err)
			}
		})
	}
}

func TestSendBudgetDropsEventsAndNoticesThem(t *testing.T) {
	old := budgetNoticeInterval
	budgetNoticeInterval = 200 * time.Millisecond
	t.Cleanup(func() { budgetNoticeInterval = old })
	h := hub.New()
	go h.Run()
	server := httptest.NewServer(Handler(h, nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100, SendBytesPerSecond: 1, SendBurstBytes: 1}, Access{}))
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
	read() // hello
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"subscribe","id":"s","scope":{}}`)); err != nil {
		t.Fatal(err)
	}
	read() // subscribed
	time.Sleep(20 * time.Millisecond)
	for i := range 5 {
		h.Broadcast(hub.Event{Type: hub.EventPacketObservation, Payload: json.RawMessage(strconv.Itoa(i))})
	}

	var events, dropped int
	var notices []time.Time
	for dropped < 4 {
		msg := read()
		switch string(msg["type"]) {
		case `"event"`:
			events++
		case `"lagged"`:
			n, _ := strconv.Atoi(string(msg["droppedCount"]))
			dropped += n
			notices = append(notices, time.Now())
		}
	}
	if events != 1 || dropped != 4 {
		t.Fatalf("events=%d dropped=%d, want the first event through and four dropped", events, dropped)
	}
	if len(notices) > 2 || (len(notices) == 2 && notices[1].Sub(notices[0]) < 150*time.Millisecond) {
		t.Fatalf("lagged notices not spaced out: %v", notices)
	}
}

func TestRequireOriginAcceptsBrowsersAndTickets(t *testing.T) {
	h := hub.New()
	go h.Run()
	tickets := wsticket.New()
	server := httptest.NewServer(Handler(h, nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100}, Access{RequireOrigin: true, Tickets: tickets}))
	t.Cleanup(server.Close)
	own, _ := tickets.Issue("127.0.0.1")
	other, _ := tickets.Issue("198.51.100.7")
	for _, tc := range []struct {
		name   string
		header http.Header
		ok     bool
	}{
		{"no origin, no ticket", http.Header{}, false},
		{"no origin, ticket for this address", http.Header{wsticket.Header: {own}}, true},
		{"no origin, ticket for another address", http.Header{wsticket.Header: {other}}, false},
		{"no origin, forged ticket", http.Header{wsticket.Header: {"forged"}}, false},
		{"same-host browser origin", http.Header{"Origin": {server.URL}}, true},
		{"foreign origin with a ticket", http.Header{"Origin": {"https://example.org"}, wsticket.Header: {own}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{HTTPHeader: tc.header})
			if !tc.ok {
				if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
					t.Fatalf("want 403, got err=%v resp=%v", err, resp)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			if _, _, err := conn.Read(ctx); err != nil {
				t.Fatalf("no hello: %v", err)
			}
		})
	}
}

func TestOriginNotRequiredByDefault(t *testing.T) {
	h := hub.New()
	go h.Run()
	server := httptest.NewServer(Handler(h, nil, Limits{MaxConnsPerIP: 5, MaxConnectsPerMinute: 100}, Access{}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if _, _, err := conn.Read(ctx); err != nil {
		t.Fatalf("no hello: %v", err)
	}
}
