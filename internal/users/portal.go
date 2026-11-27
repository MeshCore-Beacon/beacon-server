// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

const (
	portalStateTTL    = 10 * time.Minute
	portalCallTimeout = 10 * time.Second
	portalMaxBody     = 64 << 10
	maxPortalCode     = 512
	maxPortalSubject  = 128
	maxPortalUsername = 30
	maxPortalReturn   = 1024
)

// ErrPortalFailed is every portal sign-in failure; the reason is logged, never returned.
var ErrPortalFailed = errors.New("users: portal sign-in failed")

// PortalState is a sign-in under way, stored against the hash of the browser's cookie.
type PortalState struct {
	StateHash  []byte
	Verifier   string
	ReturnPath string
	ExpiresAt  time.Time
}

// PortalStore is the storage behind the portal sign-in; *db.Store satisfies it.
type PortalStore interface {
	CreatePortalState(ctx context.Context, cookieHash, stateHash []byte, verifier, returnPath string, expires time.Time) error
	// ConsumePortalState deletes and returns the state; ErrNotFound when there is none.
	ConsumePortalState(ctx context.Context, cookieHash []byte) (PortalState, error)
	// UpsertPortalUser finds the portal user with the subject or creates one, and refreshes its username (made
	// unique with a numeric suffix) and display name.
	UpsertPortalUser(ctx context.Context, subject, username, displayName string) (User, error)
}

const portalUserAgent = "Beacon-Portal-SignIn/1"

// Portal signs users in through the configured portal. Beacon is the client of the portal's token endpoint.
type Portal struct {
	svc    *Service
	store  PortalStore
	cfg    config.ResolvedAccounts
	client *http.Client
}

// NewPortal builds the portal sign-in. A nil client gets one with a 10 second timeout that follows no redirect.
func NewPortal(svc *Service, store PortalStore, cfg config.ResolvedAccounts, client *http.Client) *Portal {
	if client == nil {
		client = &http.Client{Timeout: portalCallTimeout}
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if c.Timeout == 0 || c.Timeout > portalCallTimeout {
		c.Timeout = portalCallTimeout
	}
	return &Portal{svc: svc, store: store, cfg: cfg, client: &c}
}

func (p *Portal) fail(reason string, attrs ...any) error {
	p.svc.log.Warn("portal sign-in failed", append([]any{"reason", reason}, attrs...)...)
	return ErrPortalFailed
}

func (p *Portal) base() string { return strings.TrimRight(p.cfg.PortalURL.String(), "/") }

// SafeReturn accepts only a same-origin relative path; anything else becomes "/".
func SafeReturn(r string) string {
	if r == "" || len(r) > maxPortalReturn || r[0] != '/' || strings.HasPrefix(r, "//") || strings.ContainsAny(r, "\\") {
		return "/"
	}
	for _, c := range r {
		if c < 0x20 || c == 0x7f || c == ' ' {
			return "/"
		}
	}
	u, err := url.Parse(r)
	if err != nil || u.Scheme != "" || u.Host != "" || u.User != nil || !strings.HasPrefix(u.Path, "/") {
		return "/"
	}
	return r
}

// Start stores a new state and returns the value for the beacon_portal cookie and the portal URL to send the
// browser to.
func (p *Portal) Start(ctx context.Context, returnPath string) (cookie, redirect string, err error) {
	cookie, cookieHash := NewToken()
	state, stateHash := NewToken()
	vb := make([]byte, 32)
	if _, err := rand.Read(vb); err != nil {
		return "", "", err
	}
	verifier := base64.RawURLEncoding.EncodeToString(vb)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	if err := p.store.CreatePortalState(ctx, cookieHash, stateHash, verifier, SafeReturn(returnPath), p.svc.now().Add(portalStateTTL)); err != nil {
		return "", "", err
	}
	q := "beacon_authorize=1&client_id=" + url.QueryEscape(p.cfg.PortalClientID) +
		"&redirect_uri=" + url.QueryEscape(p.cfg.PublicURL.String()+"/api/v1/auth/portal/callback") +
		"&state=" + url.QueryEscape(state) + "&code_challenge=" + challenge + "&code_challenge_method=S256"
	return cookie, p.base() + "/portal.php?" + q, nil
}

// Callback finishes a sign-in. The state is used up whatever the outcome. code is empty when the portal answered
// without one (the user chose Cancel). It returns the web session token and the path to send the browser to.
func (p *Portal) Callback(ctx context.Context, cookie, state, code string, meta SessionMeta) (string, string, error) {
	if cookie == "" {
		return "", "", p.fail("no state cookie")
	}
	st, err := p.store.ConsumePortalState(ctx, HashToken(cookie))
	if errors.Is(err, ErrNotFound) {
		return "", "", p.fail("unknown or reused state")
	}
	if err != nil {
		return "", "", err
	}
	if !p.svc.now().Before(st.ExpiresAt) {
		return "", "", p.fail("state expired")
	}
	if subtle.ConstantTimeCompare(HashToken(state), st.StateHash) != 1 {
		return "", "", p.fail("state mismatch")
	}
	if code == "" || len(code) > maxPortalCode {
		return "", "", p.fail("no code from the portal")
	}
	pu, err := p.exchange(ctx, code, st.Verifier)
	if err != nil {
		return "", "", err
	}
	u, err := p.store.UpsertPortalUser(ctx, pu.subject, pu.username, pu.displayName)
	if err != nil {
		return "", "", err
	}
	if u.DisabledAt != nil {
		return "", "", p.fail("account disabled", "user_id", u.ID)
	}
	tok, sess, err := p.svc.createSession(ctx, u, u.PassHash, kindWeb, meta.Label, meta)
	if errors.Is(err, ErrNotFound) {
		return "", "", p.fail("account changed during sign-in", "user_id", u.ID)
	}
	if err != nil {
		return "", "", err
	}
	if err := p.svc.store.TouchUserLogin(ctx, u.ID, p.svc.now()); err != nil {
		p.svc.log.Warn("login time not saved", "user_id", u.ID, "err", err)
	}
	p.svc.log.Info("portal sign-in", "user_id", u.ID, "session_id", sess.ID)
	return tok, st.ReturnPath, nil
}

type portalUser struct{ subject, username, displayName string }

var digitsOnly = regexp.MustCompile(`^[0-9]{1,20}$`)

// exchange trades the code for the portal's user. Nothing from the request or the answer is logged.
func (p *Portal) exchange(ctx context.Context, code, verifier string) (portalUser, error) {
	body, err := json.Marshal(map[string]string{
		"client_id": p.cfg.PortalClientID, "client_secret": p.cfg.PortalSecret, "code": code,
		"code_verifier": verifier, "redirect_uri": p.cfg.PublicURL.String() + "/api/v1/auth/portal/callback",
	})
	if err != nil {
		return portalUser{}, p.fail("request not built")
	}
	ctx, cancel := context.WithTimeout(ctx, portalCallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base()+"/portal_beacon_api.php?action=token", bytes.NewReader(body))
	if err != nil {
		return portalUser{}, p.fail("request not built")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// A stable name the portal can allow; Go's default agent would be refused as a generic script.
	req.Header.Set("User-Agent", portalUserAgent)
	resp, err := p.client.Do(req)
	if err != nil {
		// The error text can carry the URL; only its kind is logged.
		if errors.Is(err, context.DeadlineExceeded) {
			return portalUser{}, p.fail("portal timed out")
		}
		return portalUser{}, p.fail("portal unreachable")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, portalMaxBody+1))
	if err != nil {
		return portalUser{}, p.fail("portal answer not read")
	}
	if len(raw) > portalMaxBody {
		return portalUser{}, p.fail("portal answer too large")
	}
	if resp.StatusCode != http.StatusOK {
		return portalUser{}, p.fail("portal refused the code", "status", resp.StatusCode)
	}
	var out struct {
		User *struct {
			ID          json.RawMessage `json:"id"`
			Username    string          `json:"username"`
			DisplayName string          `json:"display_name"`
		} `json:"user"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.User == nil {
		return portalUser{}, p.fail("portal answer malformed")
	}
	subject := ""
	switch id := bytes.TrimSpace(out.User.ID); {
	case len(id) > 0 && id[0] == '"':
		if json.Unmarshal(id, &subject) != nil {
			return portalUser{}, p.fail("portal answer malformed")
		}
	case digitsOnly.Match(id):
		subject = string(id)
	}
	// The subject is the account key, so it is kept exactly or refused, never tidied into another account's key.
	if !validSubject(subject) {
		return portalUser{}, p.fail("portal answer has no usable user id")
	}
	username := cleanText(out.User.Username, maxPortalUsername)
	if username == "" {
		username = "member"
	}
	display := cleanText(out.User.DisplayName, maxDisplayName)
	if display == "" {
		display = username
	}
	return portalUser{subject: subject, username: username, displayName: display}, nil
}

// validSubject accepts a non-blank string of at most maxPortalSubject bytes with no control characters or invalid UTF-8.
func validSubject(s string) bool {
	if strings.TrimSpace(s) == "" || len(s) > maxPortalSubject || !utf8.ValidString(s) {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool { return unicode.IsControl(r) || r == utf8.RuneError }) < 0
}

// cleanText drops control characters, collapses whitespace and cuts to max runes.
func cleanText(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == utf8.RuneError {
			return -1
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > max {
		s = string([]rune(s)[:max])
	}
	return s
}
