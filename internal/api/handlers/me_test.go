// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import "testing"

func TestMeRoutes(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, accountsOpts())
	c := []reqOpt{cookie("web-token"), csrf}

	w := call(h, "GET", "/me", "", c...)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || jsonBody(t, w)["user"].(map[string]any)["username"] != "ridge" {
		t.Fatalf("get: %d %s", w.Code, w.Body)
	}

	w = call(h, "PATCH", "/me", `{"displayName":"New Name"}`, c...)
	if w.Code != 200 || jsonBody(t, w)["user"].(map[string]any)["displayName"] != "New Name" || f.name != "New Name" {
		t.Fatalf("patch: %d %s", w.Code, w.Body)
	}

	w = call(h, "POST", "/me/password", `{"currentPassword":"old","newPassword":"new"}`, c...)
	if w.Code != 204 || f.pw != "old|new" || f.keep != testSess.ID || w.Body.Len() != 0 {
		t.Fatalf("password: %d %q keep=%v", w.Code, f.pw, f.keep)
	}

	w = call(h, "POST", "/me/email", `{"email":"n@example.org","currentPassword":"pw"}`, c...)
	if w.Code != 202 || jsonBody(t, w)["status"] != "check_email" {
		t.Fatalf("email: %d %s", w.Code, w.Body)
	}

	w = call(h, "DELETE", "/me", `{"currentPassword":"pw"}`, c...)
	if w.Code != 204 || f.pw != "pw" {
		t.Fatalf("delete: %d %q", w.Code, f.pw)
	}
	// A local user's confirmUsername is ignored; other unknown fields are refused.
	if w := call(h, "DELETE", "/me", `{"currentPassword":"pw","other":"ridge"}`, c...); w.Code != 400 {
		t.Fatalf("unknown field: %d", w.Code)
	}
}

func TestMeSessions(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, accountsOpts())
	c := []reqOpt{cookie("web-token"), csrf}
	w := call(h, "GET", "/me/sessions", "", c...)
	list := jsonBody(t, w)["sessions"].([]any)
	if w.Code != 200 || len(list) != 2 {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}
	first, second := list[0].(map[string]any), list[1].(map[string]any)
	if first["id"] != testSess.ID.String() || first["current"] != true || first["kind"] != "web" || first["label"] != "Firefox on macOS" ||
		first["createdAt"] != float64(1794700000000) || first["lastUsedAt"] != float64(1794703600000) || first["expiresAt"] != float64(1797292000000) {
		t.Fatalf("first %v", first)
	}
	if second["current"] != false || second["kind"] != "app" {
		t.Fatalf("second %v", second)
	}

	if w := call(h, "DELETE", "/me/sessions/"+otherSess.ID.String(), "", c...); w.Code != 204 || f.ended != otherSess.ID {
		t.Fatalf("end one: %d", w.Code)
	}
	if w := call(h, "DELETE", "/me/sessions/not-a-uuid", "", c...); w.Code != 422 || errCode(t, w) != "invalid_input" {
		t.Fatalf("bad id: %d %s", w.Code, w.Body)
	}
	if w := call(h, "DELETE", "/me/sessions", "", c...); w.Code != 204 || f.keep != testSess.ID {
		t.Fatalf("end others: %d keep=%v", w.Code, f.keep)
	}
}
