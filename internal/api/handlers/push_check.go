// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"unicode/utf8"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

const maxPushChallenge = 128

// PushDomainCheck answers GET /.well-known/beacon-push?challenge=<c>, which the push server calls to check that the
// host at this server's registered address holds the Push secret: the answer is the Push ID and
// hex(HMAC-SHA256(SHA-256(secret), "beacon-push-challenge:" + pushId + ":" + c)). It is served outside /api/v1, only
// while push is on. The secret itself is never sent.
func PushDomainCheck(pushID, secret string) http.HandlerFunc {
	key := sha256.Sum256([]byte(secret))
	return func(w http.ResponseWriter, r *http.Request) {
		vals := r.URL.Query()["challenge"]
		if len(vals) != 1 || vals[0] == "" || !utf8.ValidString(vals[0]) || utf8.RuneCountInString(vals[0]) > maxPushChallenge {
			respondCode(w, http.StatusBadRequest, "invalid_input", "challenge must be 1 to 128 characters.")
			return
		}
		m := hmac.New(sha256.New, key[:])
		m.Write([]byte("beacon-push-challenge:" + pushID + ":" + vals[0]))
		w.Header().Set("Cache-Control", "no-store")
		respond(w, http.StatusOK, api.PushDomainCheck{PushID: pushID, MAC: hex.EncodeToString(m.Sum(nil))})
	}
}
