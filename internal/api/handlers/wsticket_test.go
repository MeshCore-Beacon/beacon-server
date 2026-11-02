// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/wsticket"
)

func TestGetWSTicket(t *testing.T) {
	tickets := wsticket.New()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "198.51.100.7:1234"
	w := httptest.NewRecorder()
	WSTicketRouter(tickets).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var body api.WSTicket
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !tickets.Valid(body.Ticket, "198.51.100.7") || tickets.Valid(body.Ticket, "198.51.100.8") {
		t.Fatal("ticket not bound to the caller's address")
	}
	if left := time.Until(time.UnixMilli(body.ExpiresAt)); left <= 0 || left > wsticket.TTL {
		t.Fatalf("expiresAt %d is %v away", body.ExpiresAt, left)
	}
}
