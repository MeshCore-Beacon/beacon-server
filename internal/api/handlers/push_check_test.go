// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestPushDomainCheck(t *testing.T) {
	h := PushDomainCheck("inst_abc", "s3cret")
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest("GET", "/.well-known/beacon-push?challenge=r4nd0m", nil))
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var got map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	key := sha256.Sum256([]byte("s3cret"))
	m := hmac.New(sha256.New, key[:])
	m.Write([]byte("beacon-push-challenge:inst_abc:r4nd0m"))
	if got["pushId"] != "inst_abc" || got["mac"] != hex.EncodeToString(m.Sum(nil)) || len(got) != 2 {
		t.Fatalf("%s", w.Body)
	}
	if strings.Contains(w.Body.String(), "s3cret") {
		t.Fatal("secret in the answer")
	}
	for name, q := range map[string]string{"missing": "", "empty": "?challenge=", "long": "?challenge=" + strings.Repeat("a", 129),
		"twice": "?challenge=a&challenge=b"} {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest("GET", "/.well-known/beacon-push"+q, nil))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
	}
	w = httptest.NewRecorder()
	h(w, httptest.NewRequest("GET", "/.well-known/beacon-push?challenge="+url.QueryEscape(strings.Repeat("é", 64)), nil))
	if w.Code != 200 {
		t.Errorf("128 characters refused: %d", w.Code)
	}
}
