// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

type fakePortal struct {
	startRet     string
	cookie, code string
	state        string
	err          error
}

func (f *fakePortal) Start(_ context.Context, ret string) (string, string, error) {
	f.startRet = ret
	return "cookie-value", "https://portal.example.org/portal.php?beacon_authorize=1", f.err
}

func (f *fakePortal) Callback(_ context.Context, cookie, state, code string, _ users.SessionMeta) (string, string, error) {
	f.cookie, f.state, f.code = cookie, state, code
	if f.err != nil {
		return "", "", f.err
	}
	return "web-token", "/?view=profile", nil
}

func portalOpts(p PortalAuth) AuthOptions {
	o := accountsOpts()
	o.Portal = p
	return o
}

func TestPortalStartRedirects(t *testing.T) {
	p := &fakePortal{}
	h := accountsServer(&fakeAccounts{}, portalOpts(p))
	w := call(h, "GET", "/auth/portal/start?return=%2F%3Fview%3Dprofile", "")
	if w.Code != http.StatusFound || w.Header().Get("Location") != "https://portal.example.org/portal.php?beacon_authorize=1" || p.startRet != "/?view=profile" {
		t.Fatalf("%d %v %q", w.Code, w.Header(), p.startRet)
	}
	c := w.Result().Cookies()
	if len(c) != 1 || c[0].Name != PortalCookie || c[0].Value != "cookie-value" || !c[0].HttpOnly || !c[0].Secure || c[0].MaxAge != 600 || c[0].Path != "/api/v1/auth/portal" {
		t.Fatalf("cookie %+v", c)
	}
}

func TestPortalCallbackSignsIn(t *testing.T) {
	p := &fakePortal{}
	h := accountsServer(&fakeAccounts{}, portalOpts(p))
	w := call(h, "GET", "/auth/portal/callback?code=abc&state=st", "", func(r *http.Request) {
		r.AddCookie(&http.Cookie{Name: PortalCookie, Value: "cookie-value"})
	})
	if w.Code != http.StatusFound || w.Header().Get("Location") != publicOrigin+"/?view=profile" {
		t.Fatalf("%d %v", w.Code, w.Header())
	}
	if p.cookie != "cookie-value" || p.state != "st" || p.code != "abc" {
		t.Fatalf("%+v", p)
	}
	var session, portal *http.Cookie
	for _, c := range w.Result().Cookies() {
		switch c.Name {
		case SessionCookie:
			session = c
		case PortalCookie:
			portal = c
		}
	}
	if session == nil || session.Value != "web-token" || !session.HttpOnly || portal == nil || portal.MaxAge >= 0 {
		t.Fatalf("cookies %+v", w.Result().Cookies())
	}
}

func TestPortalCallbackFailureAndCancel(t *testing.T) {
	for name, path := range map[string]string{
		"failed": "/auth/portal/callback?code=abc&state=st",
		"cancel": "/auth/portal/callback?error=access_denied&state=st",
	} {
		t.Run(name, func(t *testing.T) {
			p := &fakePortal{err: users.ErrPortalFailed}
			w := call(accountsServer(&fakeAccounts{}, portalOpts(p)), "GET", path, "")
			if w.Code != http.StatusFound || w.Header().Get("Location") != publicOrigin+"/?view=signin&error=portal" {
				t.Fatalf("%d %v", w.Code, w.Header())
			}
			for _, c := range w.Result().Cookies() {
				if c.Name == SessionCookie {
					t.Fatal("session cookie on a failed sign-in")
				}
			}
			if name == "cancel" && p.code != "" {
				t.Fatalf("code %q", p.code)
			}
		})
	}
}

func TestPortalModeRefusesLocalRoutes(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, portalOpts(&fakePortal{}))
	for _, c := range []struct{ path, body string }{
		{"/auth/register", `{}`}, {"/auth/verify", `{}`}, {"/auth/verify/resend", `{}`}, {"/auth/login", `{}`},
		{"/auth/reset/request", `{}`}, {"/auth/reset/confirm", `{}`}, {"/me/password", `{}`}, {"/me/email", `{}`},
	} {
		w := call(h, "POST", c.path, c.body, csrf, cookie("web-token"))
		if w.Code != http.StatusNotFound || errCode(t, w) != "portal_managed" {
			t.Errorf("%s: %d %s", c.path, w.Code, w.Body)
		}
	}
	if f.called() != "" {
		t.Fatalf("service called: %s", f.called())
	}
}

func TestPortalRoutesAbsentInLocalMode(t *testing.T) {
	h := accountsServer(&fakeAccounts{}, accountsOpts())
	if w := call(h, "GET", "/auth/portal/start", ""); w.Code != http.StatusNotFound {
		t.Fatalf("%d", w.Code)
	}
}

func TestPortalReadOnly(t *testing.T) {
	o := portalOpts(&fakePortal{})
	o.ReadOnly = true
	h := accountsServer(&fakeAccounts{}, o)
	for _, p := range []string{"/auth/portal/start", "/auth/portal/callback?code=a&state=b"} {
		if w := call(h, "GET", p, ""); w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: %d", p, w.Code)
		}
	}
}

func TestDeleteMeUsesTheUsernameForPortalUsers(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, accountsOpts())
	call(h, "DELETE", "/me", `{"currentPassword":"pw","confirmUsername":"ridge"}`, csrf, cookie("web-token"))
	if f.pw != "pw" {
		t.Fatalf("local user confirm %q", f.pw)
	}
}

func TestDeleteMeConfirmsPortalUsersByUsername(t *testing.T) {
	f := &fakeAccounts{}
	h := accountsServer(f, portalOpts(&fakePortal{}))
	w := call(h, "DELETE", "/me", `{"currentPassword":"pw","confirmUsername":"ridge"}`, csrf, cookie("portal-token"))
	if w.Code != http.StatusNoContent || f.pw != "ridge" {
		t.Fatalf("%d confirm %q", w.Code, f.pw)
	}
}
