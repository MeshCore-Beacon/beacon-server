// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

type fakeAlerts struct {
	err      error
	put      alerts.RuleInput
	deleted  uuid.UUID
	sessions []uuid.UUID
	platform string
	token    string
	key      string
	removed  bool
	channels [2]*bool
}

var alertNodeID = uuid.MustParse("22222222-2222-2222-2222-222222222222")

var alertKey = strings.Repeat("ab", 32)

func (f *fakeAlerts) Overview(context.Context, uuid.UUID) (alerts.Overview, error) {
	if f.err != nil {
		return alerts.Overview{}, f.err
	}
	th := 3500
	sent := time.UnixMilli(1794703600000)
	return alerts.Overview{Channels: alerts.Channels{Email: alerts.EmailChannel{Enabled: true, Reason: "unverified_email"},
		Push: alerts.PushChannel{Available: true, Enabled: true, HasDevice: true}}, Targets: []alerts.TargetView{{
		TargetInfo: alerts.TargetInfo{ID: alertNodeID, Kind: "node", PublicKey: alertKey, Name: "Hilltop", Caps: alerts.Caps{Polled: true}, Rules: []alerts.StoredRule{{
			ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Kind: "battery_low", Threshold: &th, Enabled: true,
			State: "ok", StateChangedAt: time.UnixMilli(1794700000000), LastSentAt: &sent}}},
		Available: []string{"gone_quiet", "battery_low"}, Unavailable: map[string]string{"temperature": "This repeater does not report a temperature."},
		UnavailableCodes: map[string]string{"temperature": "no_data"},
	}, {TargetInfo: alerts.TargetInfo{Kind: "observer", PublicKey: strings.Repeat("cd", 32), Name: "North", Rules: []alerts.StoredRule{}}}}}, nil
}

func (f *fakeAlerts) PutRule(_ context.Context, _ uuid.UUID, in alerts.RuleInput) (alerts.StoredRule, error) {
	f.put = in
	if f.err != nil {
		return alerts.StoredRule{}, f.err
	}
	return alerts.StoredRule{ID: uuid.New(), Kind: in.Kind, Threshold: in.Threshold, High: in.High, Enabled: in.Enabled, State: "unknown",
		StateChangedAt: time.UnixMilli(1794700000000)}, nil
}

func (f *fakeAlerts) DeleteRule(_ context.Context, _, id uuid.UUID) error {
	f.deleted = id
	return f.err
}

func (f *fakeAlerts) RegisterDevice(_ context.Context, _, session uuid.UUID, platform, token, key string) error {
	f.sessions, f.platform, f.token, f.key = append(f.sessions, session), platform, token, key
	return f.err
}

func (f *fakeAlerts) SetChannels(_ context.Context, _ uuid.UUID, email, push *bool) (alerts.Channels, error) {
	f.channels = [2]*bool{email, push}
	if f.err != nil {
		return alerts.Channels{}, f.err
	}
	return alerts.Channels{Email: alerts.EmailChannel{Available: true, Enabled: email != nil && *email},
		Push: alerts.PushChannel{Available: true, Enabled: push != nil && *push}}, nil
}

func (f *fakeAlerts) RemoveDevice(_ context.Context, _, _ uuid.UUID) error {
	f.removed = true
	return f.err
}

func alertsServer(svc AlertsService) chi.Router {
	r := chi.NewRouter()
	f := &fakeAccounts{}
	r.Use(mw.UserAuth(f, SessionCookie))
	r.Mount("/me", MeRouter(f, accountsOpts(), AlertRoutes(svc)))
	return r
}

func TestMeAlertsShapes(t *testing.T) {
	f := &fakeAlerts{}
	h := alertsServer(f)
	c := []reqOpt{cookie("web-token"), csrf}

	if w := call(h, "GET", "/me/alerts", ""); w.Code != 401 || errCode(t, w) != "not_signed_in" {
		t.Fatalf("signed out: %d %s", w.Code, w.Body)
	}
	w := call(h, "GET", "/me/alerts", "", c...)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	targets := got["targets"].([]any)
	t0, t1 := targets[0].(map[string]any), targets[1].(map[string]any)
	rule := t0["rules"].([]any)[0].(map[string]any)
	ch := got["channels"].(map[string]any)
	email, push := ch["email"].(map[string]any), ch["push"].(map[string]any)
	if email["available"] != false || email["enabled"] != true || email["reason"] != "unverified_email" ||
		push["available"] != true || push["enabled"] != true || push["hasDevice"] != true || len(push) != 3 {
		t.Fatalf("channels: %s", w.Body)
	}
	if _, ok := got["delivery"]; ok {
		t.Fatalf("delivery is gone: %s", w.Body)
	}
	if t0["kind"] != "node" || t0["publicKey"] != alertKey || t0["name"] != "Hilltop" ||
		len(t0["available"].([]any)) != 2 || t0["unavailable"].(map[string]any)["temperature"] == nil ||
		rule["id"] != "11111111-1111-1111-1111-111111111111" || rule["kind"] != "battery_low" || rule["threshold"] != float64(3500) ||
		rule["thresholdHigh"] != nil || rule["enabled"] != true || rule["state"] != "ok" || rule["stateChangedAt"] != float64(1794700000000) ||
		rule["lastSentAt"] != float64(1794703600000) {
		t.Fatalf("shape: %s", w.Body)
	}
	if t0["id"] != alertNodeID.String() || t0["polled"] != true || t0["unavailableCodes"].(map[string]any)["temperature"] != "no_data" {
		t.Fatalf("id, polled, codes: %s", w.Body)
	}
	// A radio that no longer exists has a null id and is not polled.
	if id, ok := t1["id"]; !ok || id != nil || t1["polled"] != false {
		t.Fatalf("unknown radio: %v", t1)
	}
	// Empty collections are arrays and objects, never null.
	if _, ok := t1["available"].([]any); !ok || t1["rules"] == nil || t1["unavailable"] == nil || t1["unavailableCodes"] == nil {
		t.Fatalf("empty target: %v", t1)
	}
}

func TestMeAlertRulePut(t *testing.T) {
	f := &fakeAlerts{}
	h := alertsServer(f)
	c := []reqOpt{cookie("web-token"), csrf}
	body := `{"target":{"kind":"node","publicKey":"` + alertKey + `"},"kind":"battery_low","threshold":3500,"thresholdHigh":null,"enabled":true}`
	w := call(h, "PUT", "/me/alerts/rules", body, c...)
	var rule map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &rule)
	if w.Code != 200 || rule["kind"] != "battery_low" || rule["threshold"] != float64(3500) || rule["thresholdHigh"] != nil || rule["state"] != "unknown" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if f.put.TargetKind != "node" || len(f.put.PublicKey) != 32 || f.put.Kind != "battery_low" || *f.put.Threshold != 3500 || f.put.High != nil || !f.put.Enabled {
		t.Fatalf("%+v", f.put)
	}
	// enabled defaults to true.
	call(h, "PUT", "/me/alerts/rules", `{"target":{"kind":"observer","publicKey":"`+alertKey+`"},"kind":"rebooted"}`, c...)
	if !f.put.Enabled || f.put.TargetKind != "observer" {
		t.Fatalf("%+v", f.put)
	}
	for name, bad := range map[string]string{
		"short key": `{"target":{"kind":"node","publicKey":"abcd"},"kind":"rebooted"}`,
		"not hex":   `{"target":{"kind":"node","publicKey":"` + strings.Repeat("zz", 32) + `"},"kind":"rebooted"}`,
	} {
		if w := call(h, "PUT", "/me/alerts/rules", bad, c...); w.Code != 422 || errCode(t, w) != "invalid_input" {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
	}
	if w := call(h, "PUT", "/me/alerts/rules", body, cookie("web-token")); w.Code != 403 {
		t.Errorf("no forgery header: %d", w.Code)
	}
	if w := call(h, "PUT", "/me/alerts/rules", `{"target":{},"kind":"x","extra":1}`, c...); w.Code != 400 {
		t.Errorf("unknown field: %d", w.Code)
	}
}

func TestMeAlertErrors(t *testing.T) {
	c := []reqOpt{cookie("web-token"), csrf}
	body := `{"target":{"kind":"node","publicKey":"` + alertKey + `"},"kind":"rebooted"}`
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{alerts.ErrNotOnAtlas, 409, "not_on_atlas"},
		{&alerts.KindUnavailable{Reason: "Nope."}, 422, "kind_unavailable"},
		{&alerts.InvalidThreshold{Message: "Bad."}, 422, "invalid_threshold"},
		{users.ErrNotFound, 401, "not_signed_in"},
		{errors.New("boom"), 500, ""},
	} {
		w := call(alertsServer(&fakeAlerts{err: tc.err}), "PUT", "/me/alerts/rules", body, c...)
		if w.Code != tc.status || (tc.code != "" && errCode(t, w) != tc.code) {
			t.Errorf("%v: %d %s", tc.err, w.Code, w.Body)
		}
		if strings.Contains(w.Body.String(), "boom") {
			t.Error("internal error leaked")
		}
	}
	w := call(alertsServer(&fakeAlerts{err: &alerts.KindUnavailable{Reason: "Nope.", Code: "not_polled"}}), "PUT", "/me/alerts/rules", body, c...)
	if !strings.Contains(w.Body.String(), "Nope.") {
		t.Errorf("reason not in the message: %s", w.Body)
	}
	// The code rides next to the usual code and message; other errors carry no reasonCode.
	var e struct{ Error map[string]string }
	_ = json.Unmarshal(w.Body.Bytes(), &e)
	if e.Error["code"] != "kind_unavailable" || e.Error["message"] != "Nope." || e.Error["reasonCode"] != "not_polled" || len(e.Error) != 3 {
		t.Errorf("error body: %s", w.Body)
	}
	w = call(alertsServer(&fakeAlerts{err: alerts.ErrNotOnAtlas}), "PUT", "/me/alerts/rules", body, c...)
	if strings.Contains(w.Body.String(), "reasonCode") {
		t.Errorf("reasonCode on another error: %s", w.Body)
	}
}

func TestMeAlertRuleDelete(t *testing.T) {
	f := &fakeAlerts{}
	h := alertsServer(f)
	c := []reqOpt{cookie("web-token"), csrf}
	id := uuid.New()
	if w := call(h, "DELETE", "/me/alerts/rules/"+id.String(), "", c...); w.Code != 204 || f.deleted != id {
		t.Fatalf("%d", w.Code)
	}
	if w := call(h, "DELETE", "/me/alerts/rules/nope", "", c...); w.Code != 422 {
		t.Fatalf("%d", w.Code)
	}
	f.err = alerts.ErrRuleMissing
	if w := call(h, "DELETE", "/me/alerts/rules/"+id.String(), "", c...); w.Code != 404 || errCode(t, w) != "not_found" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestMeDevices(t *testing.T) {
	f := &fakeAlerts{}
	h := alertsServer(f)
	app := []reqOpt{bearer("app-token"), csrf}
	body := `{"platform":"android","pushToken":"h_123","payloadKey":"AAAA"}`
	if w := call(h, "POST", "/me/devices", body, app...); w.Code != 204 || f.platform != "android" || f.token != "h_123" || f.key != "AAAA" ||
		f.sessions[0] != otherSess.ID {
		t.Fatalf("%d %+v", w.Code, f)
	}
	if w := call(h, "DELETE", "/me/devices", "", app...); w.Code != 204 || !f.removed {
		t.Fatalf("%d", w.Code)
	}
	// A cookie session is not an app.
	web := []reqOpt{cookie("web-token"), csrf}
	for _, m := range []string{"POST", "DELETE"} {
		if w := call(h, m, "/me/devices", body, web...); w.Code != 403 || errCode(t, w) != "app_session_required" {
			t.Errorf("%s with a cookie: %d %s", m, w.Code, w.Body)
		}
	}
	f.err = alerts.ErrBadDevice
	if w := call(h, "POST", "/me/devices", body, app...); w.Code != 422 || errCode(t, w) != "invalid_input" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	f.err = alerts.ErrPushOff
	if w := call(h, "POST", "/me/devices", body, app...); w.Code != 404 || errCode(t, w) != "push_disabled" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := call(h, "POST", "/me/devices", body, bearer("app-token")); w.Code != 204 && w.Code != 404 {
		// bearer sessions need no forgery header
		t.Fatalf("%d", w.Code)
	}
}

func TestAlertsOffAnswers404(t *testing.T) {
	h := alertsServer(nil)
	c := []reqOpt{cookie("web-token"), csrf}
	for _, rc := range []struct{ m, p string }{{"GET", "/me/alerts"}, {"PUT", "/me/alerts/rules"}, {"DELETE", "/me/alerts/rules/x"}, {"PUT", "/me/alerts/channels"}, {"POST", "/me/devices"}} {
		w := call(h, rc.m, rc.p, `{}`, c...)
		if w.Code != http.StatusNotFound || errCode(t, w) != "alerts_disabled" {
			t.Errorf("%s %s: %d %s", rc.m, rc.p, w.Code, w.Body)
		}
	}
	if w := call(h, "GET", "/me/alerts", ""); w.Code != 401 {
		t.Errorf("signed out should still be 401: %d", w.Code)
	}
}

func TestDeviceRegistrationIsRateLimitedPerUser(t *testing.T) {
	h := alertsServer(&fakeAlerts{})
	app := []reqOpt{bearer("app-token"), csrf}
	body := `{"platform":"ios","pushToken":"tok"}`
	limited := 0
	for i := 0; i < 12; i++ {
		// A different address each time: the limit follows the user.
		w := call(h, "POST", "/me/devices", body, append(app, func(r *http.Request) { r.RemoteAddr = "10.0.0." + string(rune('1'+i)) + ":1" })...)
		if w.Code == http.StatusTooManyRequests {
			limited++
		}
	}
	if limited != 2 {
		t.Fatalf("%d of 12 registrations limited, want 2", limited)
	}
}

func TestMeAlertChannelsPut(t *testing.T) {
	f := &fakeAlerts{}
	h := alertsServer(f)
	c := []reqOpt{cookie("web-token"), csrf}
	w := call(h, "PUT", "/me/alerts/channels", `{"email":false,"push":true}`, c...)
	if w.Code != 200 || f.channels[0] == nil || *f.channels[0] || f.channels[1] == nil || !*f.channels[1] {
		t.Fatalf("%d %s %v", w.Code, w.Body, f.channels)
	}
	var got map[string]map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["email"]["enabled"] != false || got["push"]["enabled"] != true || got["email"]["reason"] != "" {
		t.Fatalf("%s", w.Body)
	}
	// One switch alone leaves the other.
	call(h, "PUT", "/me/alerts/channels", `{"push":false}`, c...)
	if f.channels[0] != nil || f.channels[1] == nil || *f.channels[1] {
		t.Fatalf("%v", f.channels)
	}
	f.err = alerts.ErrNoChannel
	if w := call(h, "PUT", "/me/alerts/channels", `{}`, c...); w.Code != 422 || errCode(t, w) != "invalid_input" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := call(h, "PUT", "/me/alerts/channels", `{"email":true}`, cookie("web-token")); w.Code != 403 {
		t.Fatalf("no forgery header: %d", w.Code)
	}
}
