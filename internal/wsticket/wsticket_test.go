// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package wsticket

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTicketValidForItsClientUntilExpiry(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	i := New()
	i.now = func() time.Time { return now }
	ticket, expires := i.Issue("198.51.100.7")
	if !expires.Equal(now.Add(TTL)) {
		t.Fatalf("expires %v, want %v", expires, now.Add(TTL))
	}
	if !i.Valid(ticket, "198.51.100.7") {
		t.Fatal("fresh ticket refused")
	}
	if i.Valid(ticket, "198.51.100.8") {
		t.Fatal("ticket accepted from another client")
	}
	now = now.Add(TTL - time.Millisecond)
	if !i.Valid(ticket, "198.51.100.7") {
		t.Fatal("ticket refused before expiry")
	}
	now = now.Add(time.Millisecond)
	if i.Valid(ticket, "198.51.100.7") {
		t.Fatal("expired ticket accepted")
	}
}

func TestTicketRejectsForgeries(t *testing.T) {
	i := New()
	ticket, _ := i.Issue("198.51.100.7")
	tampered := []byte(ticket)
	tampered[len(tampered)-1] ^= 1
	for name, bad := range map[string]string{
		"empty":         "",
		"garbage":       "not a ticket",
		"truncated":     ticket[:len(ticket)-2],
		"tampered":      string(tampered),
		"padded":        ticket + "AA",
		"other issuer":  func() string { s, _ := New().Issue("198.51.100.7"); return s }(),
		"wrong charset": strings.ReplaceAll(ticket, "-", "+"),
	} {
		if bad != ticket && i.Valid(bad, "198.51.100.7") {
			t.Errorf("%s ticket accepted", name)
		}
	}
	var none *Issuer
	if none.Valid(ticket, "198.51.100.7") {
		t.Fatal("a nil issuer accepted a ticket")
	}
}

func TestClientKeySharesAnIPv6Slash64(t *testing.T) {
	key := func(remote string) string {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = remote
		return ClientKey(r)
	}
	if got := key("198.51.100.7:1234"); got != "198.51.100.7" {
		t.Fatalf("IPv4 key %q", got)
	}
	if key("[2001:db8::1]:1") != key("[2001:db8::2]:2") {
		t.Fatal("one /64 got two keys")
	}
	if key("[2001:db8::1]:1") == key("[2001:db8:0:1::1]:1") {
		t.Fatal("two /64s share a key")
	}
}
