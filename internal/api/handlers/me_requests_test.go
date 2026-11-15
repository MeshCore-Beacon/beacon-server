// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

type fakeRequests struct {
	rows      []polling.RequestRow
	err       error
	gotNode   uuid.UUID
	gotPw     []byte
	gotUpdate polling.RequestUpdate
	deleted   uuid.UUID
}

func (f *fakeRequests) Limit() int { return 3 }
func (f *fakeRequests) List(context.Context, uuid.UUID) ([]polling.RequestRow, error) {
	return f.rows, f.err
}
func (f *fakeRequests) Create(_ context.Context, _, node uuid.UUID, pw []byte) (polling.RequestRow, error) {
	f.gotNode, f.gotPw = node, pw
	if f.err != nil {
		return polling.RequestRow{}, f.err
	}
	return f.rows[0], nil
}
func (f *fakeRequests) Update(_ context.Context, _, _ uuid.UUID, u polling.RequestUpdate) (polling.RequestRow, error) {
	f.gotUpdate = u
	if f.err != nil {
		return polling.RequestRow{}, f.err
	}
	return f.rows[0], nil
}
func (f *fakeRequests) Delete(_ context.Context, _, id uuid.UUID) error {
	f.deleted = id
	return f.err
}

func requestsServer(svc AccountsService, rs RequestsService, opts AuthOptions) chi.Router {
	r := chi.NewRouter()
	r.Use(mw.UserAuth(svc, SessionCookie))
	r.Mount("/me", MeRouter(svc, opts, RequestRoutes(rs)))
	return r
}

func sampleRow() polling.RequestRow {
	node, poller := uuid.New(), uuid.New()
	at := time.UnixMilli(1794703600000)
	return polling.RequestRow{
		ID: uuid.New(), Target: bytes.Repeat([]byte{0xab}, 32), Status: polling.StatusActive, HasPassword: true,
		CreatedAt: time.UnixMilli(1794700000000), UpdatedAt: time.UnixMilli(1794701000000),
		NodeID: &node, NodeName: "Hilltop", PollerID: &poller, PollerName: "North Ridge", LatestAt: &at,
	}
}

func TestMeTelemetryRequestShapes(t *testing.T) {
	row := sampleRow()
	f := &fakeRequests{rows: []polling.RequestRow{row}}
	h := requestsServer(&fakeAccounts{}, f, accountsOpts())
	c := []reqOpt{cookie("web-token"), csrf}

	if w := call(h, "GET", "/me/telemetry-requests", ""); w.Code != 401 || errCode(t, w) != "not_signed_in" {
		t.Fatalf("signed out: %d %s", w.Code, w.Body)
	}
	w := call(h, "GET", "/me/telemetry-requests", "", c...)
	b := jsonBody(t, w)
	list := b["requests"].([]any)
	got := list[0].(map[string]any)
	node := got["node"].(map[string]any)
	poller := got["poller"].(map[string]any)
	if w.Code != 200 || b["limit"] != float64(3) || got["id"] != row.ID.String() || got["status"] != "active" || got["statusReason"] != "" ||
		got["hasPassword"] != true || got["createdAt"] != float64(1794700000000) || got["updatedAt"] != float64(1794701000000) ||
		got["latestReadingAt"] != float64(1794703600000) || node["id"] != row.NodeID.String() || node["publicKey"] != strings.Repeat("ab", 32) ||
		node["name"] != "Hilltop" || poller["observerId"] != row.PollerID.String() || poller["name"] != "North Ridge" {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}
	if strings.Contains(strings.ToLower(w.Body.String()), "password\"") && !strings.Contains(w.Body.String(), "hasPassword") {
		t.Fatalf("password field in the answer: %s", w.Body)
	}

	// Empty list, unknown node and nobody polling give nulls.
	f.rows = []polling.RequestRow{{ID: uuid.New(), Target: bytes.Repeat([]byte{1}, 32), Status: polling.StatusWaiting, Reason: polling.ReasonNoObserver}}
	b = jsonBody(t, call(h, "GET", "/me/telemetry-requests", "", c...))
	got = b["requests"].([]any)[0].(map[string]any)
	if got["poller"] != nil || got["latestReadingAt"] != nil || got["node"].(map[string]any)["id"] != nil || got["statusReason"] != polling.ReasonNoObserver {
		t.Fatalf("nulls: %v", got)
	}
	f.rows = nil
	if w := call(h, "GET", "/me/telemetry-requests", "", c...); w.Code != 200 || !strings.Contains(w.Body.String(), `"requests":[]`) {
		t.Fatalf("empty: %s", w.Body)
	}

	// POST answers 201 with the request object itself.
	f.rows = []polling.RequestRow{row}
	node1 := uuid.New()
	w = call(h, "POST", "/me/telemetry-requests", fmt.Sprintf(`{"nodeId":%q,"password":"guest"}`, node1), c...)
	got = jsonBody(t, w)
	if w.Code != 201 || got["id"] != row.ID.String() || got["status"] != "active" || f.gotNode != node1 || string(f.gotPw) != "guest" {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	if _, wrapped := got["request"]; wrapped {
		t.Fatalf("wrapped: %s", w.Body)
	}
	for _, body := range []string{fmt.Sprintf(`{"nodeId":%q}`, node1), fmt.Sprintf(`{"nodeId":%q,"password":null}`, node1), fmt.Sprintf(`{"nodeId":%q,"password":""}`, node1)} {
		f.gotPw = []byte("x")
		if w := call(h, "POST", "/me/telemetry-requests", body, c...); w.Code != 201 || f.gotPw != nil {
			t.Errorf("no password %s: %d %q", body, w.Code, f.gotPw)
		}
	}
	if w := call(h, "POST", "/me/telemetry-requests", `{"nodeId":"nope"}`, c...); w.Code != 422 || errCode(t, w) != "invalid_input" {
		t.Errorf("bad node id: %d %s", w.Code, w.Body)
	}
	if w := call(h, "POST", "/me/telemetry-requests", `{"nodeId":"`+node1.String()+`","extra":1}`, c...); w.Code != 400 {
		t.Errorf("unknown field: %d", w.Code)
	}

	// PATCH: absent, null and a value are three different things.
	id := row.ID.String()
	w = call(h, "PATCH", "/me/telemetry-requests/"+id, `{"password":"new"}`, c...)
	if w.Code != 200 || jsonBody(t, w)["id"] != id || !f.gotUpdate.SetPassword || string(f.gotUpdate.Password) != "new" || f.gotUpdate.Enable {
		t.Fatalf("patch password: %d %s %+v", w.Code, w.Body, f.gotUpdate)
	}
	call(h, "PATCH", "/me/telemetry-requests/"+id, `{"password":null}`, c...)
	if !f.gotUpdate.SetPassword || f.gotUpdate.Password != nil {
		t.Errorf("patch null: %+v", f.gotUpdate)
	}
	call(h, "PATCH", "/me/telemetry-requests/"+id, `{"enabled":true}`, c...)
	if f.gotUpdate.SetPassword || !f.gotUpdate.Enable {
		t.Errorf("patch enabled: %+v", f.gotUpdate)
	}
	for name, body := range map[string]string{"empty": `{}`, "enabled false": `{"enabled":false}`} {
		if w := call(h, "PATCH", "/me/telemetry-requests/"+id, body, c...); w.Code != 422 || errCode(t, w) != "invalid_input" {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
	}
	if w := call(h, "PATCH", "/me/telemetry-requests/nope", `{"enabled":true}`, c...); w.Code != 422 {
		t.Errorf("bad id: %d", w.Code)
	}

	if w := call(h, "DELETE", "/me/telemetry-requests/"+id, "", c...); w.Code != 204 || w.Body.Len() != 0 || f.deleted != row.ID {
		t.Errorf("delete: %d %s", w.Code, w.Body)
	}
}

func TestMeTelemetryRequestErrors(t *testing.T) {
	f := &fakeRequests{rows: []polling.RequestRow{sampleRow()}}
	h := requestsServer(&fakeAccounts{}, f, accountsOpts())
	c := []reqOpt{cookie("web-token"), csrf}
	body := fmt.Sprintf(`{"nodeId":%q}`, uuid.New())
	for _, k := range []struct {
		err    error
		status int
		code   string
	}{
		{polling.ErrRequestLimit, 409, "request_limit"},
		{polling.ErrAlreadyRequested, 409, "already_requested"},
		{polling.ErrNoPasswordBlocked, 409, "no_password_blocked"},
		{polling.ErrNotARepeater, 422, "not_a_repeater"},
		{polling.ErrPasswordInvalid, 422, "password_invalid"},
		{polling.ErrRequestNotFound, 404, "not_found"},
		{fmt.Errorf("wrapped: %w", polling.ErrRequestLimit), 409, "request_limit"},
	} {
		f.err = k.err
		if w := call(h, "POST", "/me/telemetry-requests", body, c...); w.Code != k.status || errCode(t, w) != k.code {
			t.Errorf("%v: %d %s", k.err, w.Code, w.Body)
		}
	}
	f.err = errors.New("db down: password=hunter2")
	w := call(h, "POST", "/me/telemetry-requests", body, c...)
	if w.Code != 500 || strings.Contains(w.Body.String(), "hunter2") || strings.Contains(w.Body.String(), "db down") {
		t.Errorf("internal: %d %s", w.Code, w.Body)
	}
}

func TestMeTelemetryRequestGuards(t *testing.T) {
	f := &fakeRequests{rows: []polling.RequestRow{sampleRow()}}
	body := fmt.Sprintf(`{"nodeId":%q}`, uuid.New())
	h := requestsServer(&fakeAccounts{}, f, accountsOpts())
	if w := call(h, "POST", "/me/telemetry-requests", body, cookie("web-token")); w.Code != 403 {
		t.Errorf("no csrf header: %d", w.Code)
	}
	if w := call(h, "GET", "/me/telemetry-requests", "", cookie("web-token")); w.Header().Get("Cache-Control") != "no-store" {
		t.Error("cacheable")
	}
	ro := accountsOpts()
	ro.ReadOnly = true
	h = requestsServer(&fakeAccounts{}, f, ro)
	if w := call(h, "POST", "/me/telemetry-requests", body, cookie("web-token"), csrf); w.Code != 503 {
		t.Errorf("read-only write: %d", w.Code)
	}
	if w := call(h, "DELETE", "/me/telemetry-requests/"+uuid.NewString(), "", cookie("web-token"), csrf); w.Code != 503 {
		t.Errorf("read-only delete: %d", w.Code)
	}
	if w := call(h, "GET", "/me/telemetry-requests", "", cookie("web-token")); w.Code != 200 {
		t.Errorf("read-only read: %d", w.Code)
	}
	// Requests off: every path is 404 requests_disabled for a signed-in user.
	h = requestsServer(&fakeAccounts{}, nil, accountsOpts())
	for _, p := range [][2]string{{"GET", "/me/telemetry-requests"}, {"POST", "/me/telemetry-requests"}, {"PATCH", "/me/telemetry-requests/" + uuid.NewString()}, {"DELETE", "/me/telemetry-requests/" + uuid.NewString()}} {
		if w := call(h, p[0], p[1], "", cookie("web-token"), csrf); w.Code != 404 || errCode(t, w) != "requests_disabled" {
			t.Errorf("%s %s off: %d %s", p[0], p[1], w.Code, w.Body)
		}
	}
}
