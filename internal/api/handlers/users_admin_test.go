// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

type fakeUsersStore struct {
	rows     []users.User // newest first
	ended    []uuid.UUID
	deleted  []uuid.UUID
	search   string
	lastSize int
}

func (f *fakeUsersStore) ListUsers(_ context.Context, search string, afterCreated time.Time, afterID uuid.UUID, limit int) ([]users.User, error) {
	f.search, f.lastSize = search, limit
	var out []users.User
	for _, u := range f.rows {
		if search != "" && !strings.Contains(strings.ToLower(u.Username+u.Email), strings.ToLower(search)) {
			continue
		}
		if u.CreatedAt.After(afterCreated) || (u.CreatedAt.Equal(afterCreated) && u.ID.String() >= afterID.String()) {
			continue
		}
		out = append(out, u)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (f *fakeUsersStore) find(id uuid.UUID) *users.User {
	for i := range f.rows {
		if f.rows[i].ID == id {
			return &f.rows[i]
		}
	}
	return nil
}
func (f *fakeUsersStore) UserByID(_ context.Context, id uuid.UUID) (users.User, error) {
	if u := f.find(id); u != nil {
		return *u, nil
	}
	return users.User{}, users.ErrNotFound
}
func (f *fakeUsersStore) SetUserDisabled(_ context.Context, id uuid.UUID, at *time.Time) error {
	if u := f.find(id); u != nil {
		u.DisabledAt = at
		return nil
	}
	return users.ErrNotFound
}
func (f *fakeUsersStore) EndAllSessions(_ context.Context, id uuid.UUID) error {
	f.ended = append(f.ended, id)
	return nil
}
func (f *fakeUsersStore) DeleteUser(_ context.Context, id uuid.UUID) error {
	if f.find(id) == nil {
		return users.ErrNotFound
	}
	f.deleted = append(f.deleted, id)
	return nil
}

func seededUsers(n int) *fakeUsersStore {
	f := &fakeUsersStore{}
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		f.rows = append(f.rows, users.User{ID: uuid.New(), Username: fmt.Sprintf("user%02d", n-i), Email: fmt.Sprintf("user%02d@example.org", n-i),
			PassHash: "$argon2id$secret-hash", Provider: "local", CreatedAt: base.Add(time.Duration(n-i) * time.Hour)})
	}
	return f
}

func adminCall(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAdminUsersListPagesAndNeverShowsTheHash(t *testing.T) {
	f := seededUsers(5)
	h := UsersAdminRouter(f, false)
	w := adminCall(h, "GET", "/?limit=2", "")
	body := jsonBody(t, w)
	items := body["items"].([]any)
	if w.Code != 200 || len(items) != 2 || body["nextCursor"] == nil {
		t.Fatalf("page 1: %d %s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "argon2") || strings.Contains(strings.ToLower(w.Body.String()), "passhash") {
		t.Fatalf("hash in the response: %s", w.Body)
	}
	if items[0].(map[string]any)["username"] != "user05" {
		t.Fatalf("not newest first: %v", items[0])
	}
	seen := []string{items[0].(map[string]any)["username"].(string), items[1].(map[string]any)["username"].(string)}
	cursor := body["nextCursor"].(string)
	for cursor != "" {
		w = adminCall(h, "GET", "/?limit=2&cursor="+cursor, "")
		body = jsonBody(t, w)
		for _, it := range body["items"].([]any) {
			seen = append(seen, it.(map[string]any)["username"].(string))
		}
		cursor, _ = body["nextCursor"].(string)
	}
	if strings.Join(seen, ",") != "user05,user04,user03,user02,user01" {
		t.Fatalf("paging: %v", seen)
	}
}

func TestAdminUsersSearchAndBadParams(t *testing.T) {
	f := seededUsers(5)
	h := UsersAdminRouter(f, false)
	w := adminCall(h, "GET", "/?search=USER03", "")
	items := jsonBody(t, w)["items"].([]any)
	if len(items) != 1 || f.search != "USER03" {
		t.Fatalf("search: %s", w.Body)
	}
	for _, q := range []string{"limit=0", "limit=201", "limit=x", "cursor=garbage", "search=" + strings.Repeat("a", 101)} {
		if w := adminCall(h, "GET", "/?"+q, ""); w.Code != 400 {
			t.Errorf("%s: %d", q, w.Code)
		}
	}
}

func TestAdminUsersDisableEndsSessionsAndEnableLiftsIt(t *testing.T) {
	f := seededUsers(2)
	h := UsersAdminRouter(f, false)
	id := f.rows[0].ID
	w := adminCall(h, "PUT", "/"+id.String(), `{"disabled":true}`)
	u := jsonBody(t, w)["user"].(map[string]any)
	if w.Code != 200 || u["disabled"] != true || len(f.ended) != 1 || f.ended[0] != id {
		t.Fatalf("disable: %d %s ended=%v", w.Code, w.Body, f.ended)
	}
	w = adminCall(h, "PUT", "/"+id.String(), `{"disabled":false}`)
	if jsonBody(t, w)["user"].(map[string]any)["disabled"] != false || len(f.ended) != 1 {
		t.Fatalf("enable: %s", w.Body)
	}
	if w := adminCall(h, "PUT", "/"+uuid.NewString(), `{"disabled":true}`); w.Code != 404 {
		t.Fatalf("unknown: %d", w.Code)
	}
	if w := adminCall(h, "PUT", "/not-a-uuid", `{"disabled":true}`); w.Code != 400 {
		t.Fatalf("bad id: %d", w.Code)
	}
	if w := adminCall(h, "PUT", "/"+id.String(), `{"disabled":true,"x":1}`); w.Code != 400 {
		t.Fatalf("unknown field: %d", w.Code)
	}
}

func TestAdminUsersDelete(t *testing.T) {
	f := seededUsers(2)
	h := UsersAdminRouter(f, false)
	id := f.rows[1].ID
	if w := adminCall(h, "DELETE", "/"+id.String(), ""); w.Code != 204 || len(f.deleted) != 1 || f.deleted[0] != id {
		t.Fatalf("delete: %d", w.Code)
	}
	if w := adminCall(h, "DELETE", "/"+uuid.NewString(), ""); w.Code != 404 {
		t.Fatalf("unknown: %d", w.Code)
	}
}

func TestAdminUsersUnavailableWhenOffOrReadOnly(t *testing.T) {
	for name, h := range map[string]http.Handler{"off": UsersAdminRouter(nil, false), "read-only": UsersAdminRouter(seededUsers(1), true)} {
		for _, m := range []string{"GET", "PUT", "DELETE"} {
			if w := adminCall(h, m, "/"+uuid.NewString(), `{"disabled":true}`); w.Code != 503 {
				t.Errorf("%s %s: %d", name, m, w.Code)
			}
		}
	}
}
