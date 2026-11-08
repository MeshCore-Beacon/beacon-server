package ingest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTelemetryRelayOnlyCopiesPendingPrefixesAndBoundsWork(t *testing.T) {
	token := strings.Repeat("ab", 32)
	received := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("relay authentication missing")
		}
		switch r.URL.Path {
		case "/intake/internal/interests":
			io.WriteString(w, `{"prefixes":["0102"],"expiresInSeconds":5}`)
		case "/intake/internal/replies":
			var body struct {
				Hearings []relayHearing `json:"hearings"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("bad batch")
			}
			received += len(body.Hearings)
			if len(body.Hearings) > 8 {
				t.Error("oversized batch")
			}
			io.WriteString(w, `{"accepted":1}`)
		default:
			t.Error("unexpected request", r.URL.Path)
		}
	}))
	defer server.Close()
	relay, err := NewTelemetryRelay(server.URL+"/intake", token)
	if err != nil {
		t.Fatal(err)
	}
	observer := strings.Repeat("cd", 32)
	raw := []byte{6, 0, 1, 2}
	payload := []byte{1, 2, 3, 4}
	relay.Observe(observer, raw, payload)
	if len(relay.queue) != 0 {
		t.Fatal("forwarded without interest")
	}
	relay.refresh(context.Background())
	relay.Observe(observer, raw, []byte{2, 1, 3, 4})
	if len(relay.queue) != 0 {
		t.Fatal("unrelated response forwarded")
	}
	for range 130 {
		relay.Observe(observer, raw, payload)
	}
	if len(relay.queue) != 128 || relay.dropped.Load() != 2 {
		t.Fatal("queue not bounded")
	}
	relay.flush(context.Background())
	if received != 8 {
		t.Fatal(received)
	}
	relay.interests.Store(&relayInterests{prefixes: map[uint16]bool{0x0102: true}, until: time.Now().Add(-time.Second)})
	n := len(relay.queue)
	relay.Observe(observer, raw, payload)
	if len(relay.queue) != n {
		t.Fatal("stale interest forwarded")
	}
}

func TestTelemetryRelayFailsClosedOnInvalidInterests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"prefixes":["01"],"expiresInSeconds":999}`)
	}))
	defer server.Close()
	relay, _ := NewTelemetryRelay(server.URL, strings.Repeat("ab", 32))
	relay.refresh(context.Background())
	if relay.interests.Load() != nil {
		t.Fatal("invalid interest accepted")
	}
	if _, err := NewTelemetryRelay(server.URL, ""); err == nil {
		t.Fatal("missing private token accepted")
	}
}
