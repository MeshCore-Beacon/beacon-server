// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/google/uuid"
)

type accountStub struct {
	account api.Account
	items   []api.Account
	err     error
	calls   int
	name    string
	id      uuid.UUID
}

func (s *accountStub) CreateAccount(_ context.Context, name string) (api.Account, error) {
	s.calls++
	s.name = name
	return s.account, s.err
}
func (s *accountStub) ListAccounts(context.Context) ([]api.Account, error) {
	s.calls++
	return s.items, s.err
}
func (s *accountStub) GetAccount(_ context.Context, id uuid.UUID) (api.Account, error) {
	s.calls++
	s.id = id
	return s.account, s.err
}
func (s *accountStub) DeactivateAccount(_ context.Context, id uuid.UUID) error {
	s.calls++
	s.id = id
	return s.err
}

func accountRequest(handler http.Handler, method, path, body, contentType, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func TestAccountInputAndErrors(t *testing.T) {
	id := uuid.New()
	for _, tc := range []struct {
		name, method, path, body, media string
		err                             error
		status, calls                   int
	}{
		{"create", "POST", "/", `{"name":"  Alice  "}`, "application/json", nil, 201, 1},
		{"media parameters", "POST", "/", `{"name":"Alice"}`, "application/json; charset=utf-8", nil, 201, 1},
		{"empty", "POST", "/", `{}`, "application/json", nil, 400, 0},
		{"null", "POST", "/", `null`, "application/json", nil, 400, 0},
		{"wrong field", "POST", "/", `{"name":"Alice","admin":true}`, "application/json", nil, 400, 0},
		{"trailing JSON", "POST", "/", `{"name":"Alice"} {}`, "application/json", nil, 400, 0},
		{"malformed", "POST", "/", `{"name":`, "application/json", nil, 400, 0},
		{"wrong name type", "POST", "/", `{"name":3}`, "application/json", nil, 400, 0},
		{"control", "POST", "/", `{"name":"x\u0000y"}`, "application/json", nil, 400, 0},
		{"oversized", "POST", "/", `{"name":"` + strings.Repeat("a", 5000) + `"}`, "application/json", nil, 413, 0},
		{"oversized trailing space", "POST", "/", `{"name":"Alice"}` + strings.Repeat(" ", 5000), "application/json", nil, 413, 0},
		{"missing media", "POST", "/", `{"name":"Alice"}`, "", nil, 415, 0},
		{"wrong media", "POST", "/", `{"name":"Alice"}`, "text/plain", nil, 415, 0},
		{"conflict", "POST", "/", `{"name":"Alice"}`, "application/json", fmt.Errorf("private-error: %w", api.ErrAccountNameConflict), 409, 1},
		{"get", "GET", "/" + id.String(), "", "", nil, 200, 1},
		{"bad ID", "GET", "/invalid", "", "", nil, 400, 0},
		{"bad delete ID", "DELETE", "/invalid", "", "", nil, 400, 0},
		{"not found", "GET", "/" + id.String(), "", "", api.ErrAccountNotFound, 404, 1},
		{"deactivate", "DELETE", "/" + id.String(), "", "", nil, 204, 1},
		{"inactive", "DELETE", "/" + id.String(), "", "", api.ErrAccountInactive, 409, 1},
		{"delete absent", "DELETE", "/" + id.String(), "", "", api.ErrAccountNotFound, 404, 1},
		{"list database failure", "GET", "/", "", "", errors.New("private-error"), 500, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &accountStub{account: api.Account{ID: id, Name: "Alice", CreatedAt: time.Unix(1, 0).UTC(), Active: true}, err: tc.err}
			handler := mw.BearerAuth("test-key", AccountsRouter(store))
			w := accountRequest(handler, tc.method, tc.path, tc.body, tc.media, "test-key")
			if w.Code != tc.status || store.calls != tc.calls {
				t.Fatalf("status=%d calls=%d", w.Code, store.calls)
			}
			if w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "private-error") {
				t.Fatal("cache policy or error disclosure")
			}
			if tc.status == 204 {
				if w.Body.Len() != 0 {
					t.Fatal("204 body")
				}
				return
			}
			if w.Header().Get("Content-Type") != "application/json" || !json.Valid(w.Body.Bytes()) {
				t.Fatal("invalid JSON response")
			}
			if tc.status == 201 && store.name != "Alice" {
				t.Fatal("untrimmed name reached storage")
			}
			if (tc.method == "DELETE" || tc.method == "GET") && tc.calls > 0 && tc.path != "/" && store.id != id {
				t.Fatal("wrong account ID")
			}
		})
	}
}

func TestAccountListAndAuthorization(t *testing.T) {
	for _, items := range [][]api.Account{nil, {{ID: uuid.New(), Name: "inactive", Active: false}}} {
		store := &accountStub{items: items}
		handler := mw.BearerAuth("test-key", AccountsRouter(store))
		w := accountRequest(handler, "GET", "/", "", "", "test-key")
		var body api.AccountList
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Items == nil || len(body.Items) != len(items) {
			t.Fatal("account list shape")
		}
	}
	for _, key := range []string{"", "test-key"} {
		store := &accountStub{}
		handler := mw.BearerAuth(key, AccountsRouter(store))
		for _, method := range []string{"GET", "POST", "DELETE"} {
			w := accountRequest(handler, method, "/", "{}", "application/json", "")
			want := 401
			if key == "" {
				want = 503
			}
			if w.Code != want || store.calls != 0 {
				t.Fatal("unauthorized store access")
			}
		}
	}
}
