// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/google/uuid"
)

type routedAccountStore struct {
	api.AccountStore
	calls int
}

func (s *routedAccountStore) CreateAccount(_ context.Context, name string) (api.Account, error) {
	s.calls++
	return api.Account{ID: uuid.New(), Name: name, Active: true}, nil
}
func TestAccountRouterUsesProtectedStore(t *testing.T) {
	store := &routedAccountStore{}
	r := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: "test-key"}, AdminRoutes: map[string]http.Handler{"/accounts": handlers.AccountsRouter(store)}})
	for _, token := range []string{"", "wrong", "test-key"} {
		req := httptest.NewRequest("POST", "/api/v1/admin/accounts", strings.NewReader(`{"name":"test"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		want := 401
		if token == "test-key" {
			want = 201
		}
		if w.Code != want {
			t.Fatalf("route status=%d want=%d", w.Code, want)
		}
	}
	if store.calls != 1 {
		t.Fatal("auth boundary was bypassed")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/accounts", nil))
	if w.Code != 404 || store.calls != 1 {
		t.Fatal("accounts mounted publicly")
	}
}
