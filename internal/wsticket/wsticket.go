// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package wsticket issues short-lived tickets that let a client sending no Origin, such as the
// mobile app, open /ws when websocket.require_origin is on.
package wsticket

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/httprate"
)

// TTL is how long a ticket opens /ws for. Clients fetch a fresh one before each connect.
const TTL = 60 * time.Second

// Header carries the ticket on the /ws upgrade request.
const Header = "X-Beacon-Ticket"

const macBytes = 16

// Issuer signs tickets with a key made at startup, so a restart retires every outstanding one.
type Issuer struct {
	key []byte
	now func() time.Time
}

func New() *Issuer {
	key := make([]byte, 32)
	_, _ = rand.Read(key) // crypto/rand never fails on supported platforms
	return &Issuer{key: key, now: time.Now}
}

// Issue returns a ticket for the client behind clientKey, and when it expires.
func (i *Issuer) Issue(clientKey string) (string, time.Time) {
	expires := i.now().Add(TTL)
	buf := binary.BigEndian.AppendUint64(nil, uint64(expires.UnixMilli()))
	buf = append(buf, i.mac(buf, clientKey)...)
	return base64.RawURLEncoding.EncodeToString(buf), expires
}

// Valid reports whether ticket was issued here for clientKey and has not expired.
func (i *Issuer) Valid(ticket, clientKey string) bool {
	if i == nil {
		return false
	}
	buf, err := base64.RawURLEncoding.DecodeString(ticket)
	if err != nil || len(buf) != 8+macBytes {
		return false
	}
	if !hmac.Equal(buf[8:], i.mac(buf[:8], clientKey)) {
		return false
	}
	return i.now().UnixMilli() < int64(binary.BigEndian.Uint64(buf[:8]))
}

func (i *Issuer) mac(expiry []byte, clientKey string) []byte {
	h := hmac.New(sha256.New, i.key)
	h.Write(expiry)
	h.Write([]byte(clientKey))
	return h.Sum(nil)[:macBytes]
}

// ClientKey names the client behind r the way the /ws limits do: its address, IPv6 per /64.
// RemoteAddr must already be resolved by the trusted-proxy middleware.
func ClientKey(r *http.Request) string {
	ip := r.RemoteAddr
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	return httprate.CanonicalizeIP(ip)
}
