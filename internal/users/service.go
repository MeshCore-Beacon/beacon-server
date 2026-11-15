// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/mail"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	mailer "github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/google/uuid"
)

// Store is what the service needs from the database; *db.Store satisfies it.
type Store interface {
	CreateUser(ctx context.Context, u User) (User, error)
	UserByID(ctx context.Context, id uuid.UUID) (User, error)
	UserByLogin(ctx context.Context, login string) (User, error)
	UserByEmail(ctx context.Context, email string) (User, error)
	UpdateUserDisplayName(ctx context.Context, id uuid.UUID, name string) error
	SetUserPassword(ctx context.Context, id uuid.UUID, hash string) error
	SetUserEmailVerified(ctx context.Context, id uuid.UUID, at time.Time) error
	SetUserPendingEmail(ctx context.Context, id uuid.UUID, email *string) error
	ApplyPendingEmail(ctx context.Context, id uuid.UUID, expected string) error
	TouchUserLogin(ctx context.Context, id uuid.UUID, at time.Time) error
	DeleteUser(ctx context.Context, id uuid.UUID) error
	CreateSession(ctx context.Context, sess Session, tokenHash []byte) (Session, error)
	SessionByTokenHash(ctx context.Context, hash []byte, now time.Time) (Session, User, error)
	TouchSession(ctx context.Context, id uuid.UUID, lastUsed, expires time.Time) error
	ListSessions(ctx context.Context, userID uuid.UUID, now time.Time) ([]Session, error)
	EndSession(ctx context.Context, userID, sessionID uuid.UUID) error
	EndOtherSessions(ctx context.Context, userID, keep uuid.UUID) error
	EndAllSessions(ctx context.Context, userID uuid.UUID) error
	EndOldestAppSessions(ctx context.Context, userID uuid.UUID, keep int) error
	CreateToken(ctx context.Context, userID uuid.UUID, kind TokenKind, hash []byte, data []byte, expires time.Time) error
	ConsumeToken(ctx context.Context, kind TokenKind, hash []byte, now time.Time) (uuid.UUID, []byte, error)
	DeleteUserTokens(ctx context.Context, userID uuid.UUID, kind TokenKind) error
}

var (
	ErrInvalidCredentials = errors.New("users: invalid credentials")
	ErrEmailUnverified    = errors.New("users: email not verified")
	ErrTokenInvalid       = errors.New("users: token invalid")
	ErrTooManyAttempts    = errors.New("users: too many attempts")
	ErrRegistrationClosed = errors.New("users: registration closed")
	ErrWeakPassword       = errors.New("users: weak password")
	ErrInvalidInput       = errors.New("users: invalid input")
)

// TooManyAttemptsError matches ErrTooManyAttempts and says how long until the account may be tried again.
type TooManyAttemptsError struct{ RetryAfter time.Duration }

func (e *TooManyAttemptsError) Error() string        { return ErrTooManyAttempts.Error() }
func (e *TooManyAttemptsError) Is(target error) bool { return target == ErrTooManyAttempts }

// SessionMeta is what the HTTP layer knows about the client; the service stores it on the session.
type SessionMeta struct{ IP, UserAgent, Label string }

const (
	verifyTTL     = 24 * time.Hour
	resetTTL      = time.Hour
	emailTTL      = 24 * time.Hour
	appCodeTTL    = 5 * time.Minute
	webTTL        = 30 * 24 * time.Hour
	appTTL        = 180 * 24 * time.Hour
	appHardTTL    = 365 * 24 * time.Hour
	touchInterval = 5 * time.Minute
	maxAppSession = 10

	kindWeb = "web"
	kindApp = "app"

	maxEmailBytes   = 254
	maxDisplayName  = 40
	maxDeviceLabel  = 60
	defaultDevLabel = "Beacon app"
)

var reservedUsernames = map[string]bool{
	"admin": true, "root": true, "beacon": true, "system": true, "support": true, "moderator": true, "operator": true,
}

// Service is the account logic: registration, login, sessions, reset and app sign-in. Its methods return the
// package's error values; anything else is an infrastructure failure.
type Service struct {
	store    Store
	mailer   mailer.Sender
	cfg      config.ResolvedAccounts
	now      func() time.Time
	log      *slog.Logger
	throttle *throttle
	// verifyPW is injectable so a test can count password comparisons.
	verifyPW func(hash, password string) (bool, bool)
	// dummyHash is compared against when the user is unknown, so both paths cost one argon2 run.
	dummyHash string
}

func NewService(store Store, m mailer.Sender, cfg config.ResolvedAccounts, now func() time.Time, log *slog.Logger) *Service {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	seed, _ := NewToken()
	dummy, err := HashPassword(seed)
	if err != nil {
		panic("users: dummy hash: " + err.Error())
	}
	return &Service{store: store, mailer: m, cfg: cfg, now: now, log: log, throttle: newThrottle(), verifyPW: VerifyPassword, dummyHash: dummy}
}

// ---- input rules ----

func validUsername(u string) bool {
	if len(u) < 3 || len(u) > 30 {
		return false
	}
	for _, r := range u {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '.' || r == '-') {
			return false
		}
	}
	return true
}

// normalizeEmail lower-cases and trims, then accepts exactly one bare address.
func normalizeEmail(s string) (string, error) {
	e := strings.ToLower(strings.TrimSpace(s))
	if e == "" || len(e) > maxEmailBytes || strings.ContainsAny(e, "\"<>,;()[] \t\r\n") {
		return "", ErrInvalidInput
	}
	a, err := mail.ParseAddress(e)
	if err != nil || a.Name != "" || a.Address != e {
		return "", ErrInvalidInput
	}
	local, domain, ok := strings.Cut(e, "@")
	if !ok || local == "" || !strings.Contains(domain, ".") || strings.Contains(domain, "@") {
		return "", ErrInvalidInput
	}
	return e, nil
}

func emailDomain(e string) string {
	if _, d, ok := strings.Cut(e, "@"); ok {
		return d
	}
	return ""
}

func cleanLabel(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > maxDeviceLabel {
		s = string([]rune(s)[:maxDeviceLabel])
	}
	if s == "" {
		return defaultDevLabel
	}
	return s
}

// ---- mail ----

func (s *Service) link(view, token string) string {
	u := *s.cfg.PublicURL
	u.Path = "/"
	u.RawQuery = "view=" + view + "&token=" + url.QueryEscape(token)
	return u.String()
}

func (s *Service) host() string { return s.cfg.PublicURL.Host }

// send delivers a mail; a failure is logged (user id and kind only) and returned for callers that care.
func (s *Service) send(ctx context.Context, userID uuid.UUID, kind, to, subject, text string) error {
	if err := s.mailer.Send(ctx, mailer.Message{To: to, Subject: subject, Text: text}); err != nil {
		s.log.Warn("mail not sent", "user_id", userID, "mail", kind, "to_domain", emailDomain(to), "err", err)
		return err
	}
	return nil
}

type tokenData struct {
	Email string `json:"email,omitempty"`
}

func (s *Service) newToken(ctx context.Context, userID uuid.UUID, kind TokenKind, data any, ttl time.Duration) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	plain, hash := NewToken()
	if err := s.store.CreateToken(ctx, userID, kind, hash, raw, s.now().Add(ttl)); err != nil {
		return "", err
	}
	return plain, nil
}

// sendVerify replaces the user's verify links with a fresh one for their current address.
func (s *Service) sendVerify(ctx context.Context, u User) error {
	if err := s.store.DeleteUserTokens(ctx, u.ID, TokenVerify); err != nil {
		return err
	}
	tok, err := s.newToken(ctx, u.ID, TokenVerify, tokenData{Email: u.Email}, verifyTTL)
	if err != nil {
		return err
	}
	subject, text := verifyMailText(s.host(), s.link("verify", tok))
	_ = s.send(ctx, u.ID, "verify", u.Email, subject, text)
	return nil
}

// ---- registration and verification ----

// Register never says whether the email is already registered: a taken address returns nil like a new one.
func (s *Service) Register(ctx context.Context, username, email, password string) error {
	if !s.cfg.RegistrationOpen {
		return ErrRegistrationClosed
	}
	username = strings.TrimSpace(username)
	if !validUsername(username) {
		return ErrInvalidInput
	}
	addr, err := normalizeEmail(email)
	if err != nil {
		return err
	}
	if reservedUsernames[strings.ToLower(username)] {
		return ErrUsernameTaken
	}
	if err := CheckPasswordPolicy(password, username, addr); err != nil {
		return err
	}
	// Hashed before touching the database so a taken address costs the same as a new one.
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	u, err := s.store.CreateUser(ctx, User{Username: username, DisplayName: username, Email: addr, PassHash: hash, Provider: "local"})
	switch {
	case errors.Is(err, ErrUsernameTaken):
		return err
	case errors.Is(err, ErrEmailTaken):
		existing, gerr := s.store.UserByEmail(ctx, addr)
		if errors.Is(gerr, ErrNotFound) {
			return nil
		}
		if gerr != nil {
			return gerr
		}
		if existing.EmailVerifiedAt == nil && existing.DisabledAt == nil {
			return s.sendVerify(ctx, existing)
		}
		return nil
	case err != nil:
		return err
	}
	s.log.Info("user registered", "user_id", u.ID)
	return s.sendVerify(ctx, u)
}

func (s *Service) Verify(ctx context.Context, token string) error {
	if token == "" {
		return ErrTokenInvalid
	}
	hash := HashToken(token)
	now := s.now()
	uid, raw, err := s.store.ConsumeToken(ctx, TokenVerify, hash, now)
	if err == nil {
		var d tokenData
		if json.Unmarshal(raw, &d) != nil || d.Email == "" {
			return ErrTokenInvalid
		}
		u, err := s.store.UserByID(ctx, uid)
		if errors.Is(err, ErrNotFound) || (err == nil && u.Email != d.Email) {
			return ErrTokenInvalid
		}
		if err != nil {
			return err
		}
		if u.EmailVerifiedAt == nil {
			if err := s.store.SetUserEmailVerified(ctx, uid, now); err != nil {
				return err
			}
		}
		s.log.Info("email verified", "user_id", uid)
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	uid, raw, err = s.store.ConsumeToken(ctx, TokenEmailChange, hash, now)
	if errors.Is(err, ErrNotFound) {
		return ErrTokenInvalid
	}
	if err != nil {
		return err
	}
	var d tokenData
	if json.Unmarshal(raw, &d) != nil || d.Email == "" {
		return ErrTokenInvalid
	}
	// The store applies the address only if it is still the one this link was sent to.
	switch err := s.store.ApplyPendingEmail(ctx, uid, d.Email); {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrEmailTaken):
		return ErrTokenInvalid
	case err != nil:
		return err
	}
	s.log.Info("email changed", "user_id", uid)
	return nil
}

func (s *Service) ResendVerification(ctx context.Context, email string) error {
	addr, err := normalizeEmail(email)
	if err != nil {
		return err
	}
	u, err := s.store.UserByEmail(ctx, addr)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if u.EmailVerifiedAt != nil || u.DisabledAt != nil {
		return nil
	}
	return s.sendVerify(ctx, u)
}

// ---- password checks ----

// checkPassword verifies pw against u behind the per-account throttle. The throttle is read before any hashing, so
// a throttled account costs nothing to hit. Only a wrong password counts.
func (s *Service) checkPassword(u User, pw string) (needsRehash bool, err error) {
	now := s.now()
	if wait, locked := s.throttle.locked(u.ID, now); locked {
		return false, &TooManyAttemptsError{RetryAfter: wait}
	}
	ok, re := s.verifyPW(u.PassHash, pw)
	if !ok {
		if s.throttle.fail(u.ID, now) {
			s.log.Warn("account throttled after repeated wrong passwords", "user_id", u.ID)
		}
		return false, ErrInvalidCredentials
	}
	s.throttle.clear(u.ID)
	return re, nil
}

// ---- login and sessions ----

func (s *Service) createSession(ctx context.Context, u User, kind, label string, meta SessionMeta) (string, Session, error) {
	now := s.now()
	sess := Session{UserID: u.ID, Kind: kind, Label: label, IP: meta.IP, UserAgent: meta.UserAgent}
	if kind == kindApp {
		hard := now.Add(appHardTTL)
		sess.ExpiresAt, sess.HardExpiresAt = now.Add(appTTL), &hard
	} else {
		sess.ExpiresAt = now.Add(webTTL)
	}
	plain, hash := NewToken()
	created, err := s.store.CreateSession(ctx, sess, hash)
	if err != nil {
		return "", Session{}, err
	}
	return plain, created, nil
}

func (s *Service) Login(ctx context.Context, login, password string, meta SessionMeta) (string, Session, User, error) {
	login = strings.TrimSpace(login)
	if strings.Contains(login, "@") {
		login = strings.ToLower(login)
	}
	var u User
	var err error
	if login == "" || len(login) > maxEmailBytes {
		err = ErrNotFound
	} else {
		u, err = s.store.UserByLogin(ctx, login)
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return "", Session{}, User{}, err
	}
	if err != nil || u.DisabledAt != nil {
		s.verifyPW(s.dummyHash, password)
		return "", Session{}, User{}, ErrInvalidCredentials
	}
	needsRehash, err := s.checkPassword(u, password)
	if err != nil {
		s.log.Info("login refused", "user_id", u.ID, "reason", loginReason(err))
		return "", Session{}, User{}, err
	}
	if u.EmailVerifiedAt == nil {
		return "", Session{}, User{}, ErrEmailUnverified
	}
	if needsRehash {
		if h, herr := HashPassword(password); herr == nil {
			if werr := s.store.SetUserPassword(ctx, u.ID, h); werr != nil {
				s.log.Warn("password rehash not saved", "user_id", u.ID, "err", werr)
			}
		}
	}
	if err := s.store.TouchUserLogin(ctx, u.ID, s.now()); err != nil {
		s.log.Warn("login time not saved", "user_id", u.ID, "err", err)
	}
	tok, sess, err := s.createSession(ctx, u, kindWeb, meta.Label, meta)
	if err != nil {
		return "", Session{}, User{}, err
	}
	s.log.Info("login", "user_id", u.ID, "session_id", sess.ID)
	return tok, sess, u, nil
}

func loginReason(err error) string {
	if errors.Is(err, ErrTooManyAttempts) {
		return "throttled"
	}
	return "wrong_password"
}

// Authenticate resolves a session token and, at most every 5 minutes, slides its expiry (never past the hard end).
func (s *Service) Authenticate(ctx context.Context, token string) (Session, User, error) {
	if token == "" {
		return Session{}, User{}, ErrTokenInvalid
	}
	now := s.now()
	sess, u, err := s.store.SessionByTokenHash(ctx, HashToken(token), now)
	if errors.Is(err, ErrNotFound) {
		return Session{}, User{}, ErrTokenInvalid
	}
	if err != nil {
		return Session{}, User{}, err
	}
	// The store already filters these; checked again so a store bug cannot let an expired or disabled one in.
	if !now.Before(sess.ExpiresAt) || (sess.HardExpiresAt != nil && !now.Before(*sess.HardExpiresAt)) || u.DisabledAt != nil {
		return Session{}, User{}, ErrTokenInvalid
	}
	if now.Sub(sess.LastUsedAt) >= touchInterval {
		ttl := webTTL
		if sess.Kind == kindApp {
			ttl = appTTL
		}
		exp := now.Add(ttl)
		if sess.HardExpiresAt != nil && exp.After(*sess.HardExpiresAt) {
			exp = *sess.HardExpiresAt
		}
		if err := s.store.TouchSession(ctx, sess.ID, now, exp); err != nil {
			s.log.Warn("session touch not saved", "user_id", u.ID, "err", err)
		} else {
			sess.LastUsedAt, sess.ExpiresAt = now, exp
		}
	}
	return sess, u, nil
}

func (s *Service) Logout(ctx context.Context, userID, sessionID uuid.UUID) error {
	return s.store.EndSession(ctx, userID, sessionID)
}

func (s *Service) Sessions(ctx context.Context, userID uuid.UUID) ([]Session, error) {
	return s.store.ListSessions(ctx, userID, s.now())
}

func (s *Service) EndSession(ctx context.Context, userID, sessionID uuid.UUID) error {
	return s.store.EndSession(ctx, userID, sessionID)
}

func (s *Service) EndOtherSessions(ctx context.Context, userID, keep uuid.UUID) error {
	return s.store.EndOtherSessions(ctx, userID, keep)
}

// ---- reset ----

// RequestReset sends nothing and returns nil for an unknown or disabled user, and when the mail cannot be sent.
func (s *Service) RequestReset(ctx context.Context, email string) error {
	addr, err := normalizeEmail(email)
	if err != nil {
		return err
	}
	u, err := s.store.UserByEmail(ctx, addr)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if u.DisabledAt != nil {
		return nil
	}
	if err := s.store.DeleteUserTokens(ctx, u.ID, TokenReset); err != nil {
		return err
	}
	tok, err := s.newToken(ctx, u.ID, TokenReset, tokenData{}, resetTTL)
	if err != nil {
		return err
	}
	s.log.Info("password reset requested", "user_id", u.ID)
	subject, text := resetMailText(s.host(), s.link("reset", tok))
	_ = s.send(ctx, u.ID, "reset", u.Email, subject, text)
	return nil
}

// ConfirmReset sets the password, verifies the address (the link went to it), ends every session and clears the
// throttle. The checks that need no user run before the token is spent; one that needs the username runs after,
// so that rare refusal costs the link.
func (s *Service) ConfirmReset(ctx context.Context, token, newPassword string) error {
	if err := CheckPasswordPolicy(newPassword, "", ""); err != nil {
		return err
	}
	if token == "" {
		return ErrTokenInvalid
	}
	uid, _, err := s.store.ConsumeToken(ctx, TokenReset, HashToken(token), s.now())
	if errors.Is(err, ErrNotFound) {
		return ErrTokenInvalid
	}
	if err != nil {
		return err
	}
	u, err := s.store.UserByID(ctx, uid)
	if errors.Is(err, ErrNotFound) || (err == nil && u.DisabledAt != nil) {
		return ErrTokenInvalid
	}
	if err != nil {
		return err
	}
	if err := CheckPasswordPolicy(newPassword, u.Username, u.Email); err != nil {
		return err
	}
	hash, err := HashPassword(newPassword)
	if err != nil {
		return err
	}
	if err := s.store.SetUserPassword(ctx, uid, hash); err != nil {
		return err
	}
	if u.EmailVerifiedAt == nil {
		if err := s.store.SetUserEmailVerified(ctx, uid, s.now()); err != nil {
			return err
		}
	}
	if err := s.store.EndAllSessions(ctx, uid); err != nil {
		return err
	}
	for _, k := range []TokenKind{TokenReset, TokenAppCode} {
		if err := s.store.DeleteUserTokens(ctx, uid, k); err != nil {
			return err
		}
	}
	s.throttle.clear(uid)
	s.log.Info("password reset completed", "user_id", uid)
	return nil
}

// ---- profile ----

func (s *Service) ChangePassword(ctx context.Context, u User, keepSession uuid.UUID, current, newPassword string) error {
	if _, err := s.checkPassword(u, current); err != nil {
		return err
	}
	if err := CheckPasswordPolicy(newPassword, u.Username, u.Email); err != nil {
		return err
	}
	hash, err := HashPassword(newPassword)
	if err != nil {
		return err
	}
	if err := s.store.SetUserPassword(ctx, u.ID, hash); err != nil {
		return err
	}
	if err := s.store.EndOtherSessions(ctx, u.ID, keepSession); err != nil {
		return err
	}
	if err := s.store.DeleteUserTokens(ctx, u.ID, TokenReset); err != nil {
		return err
	}
	s.log.Info("password changed", "user_id", u.ID)
	return nil
}

// ChangeEmail holds the new address as pending and mails a link to it. An address another user owns gets the same
// treatment minus the mail, so the answer does not reveal who is registered.
func (s *Service) ChangeEmail(ctx context.Context, u User, current, newEmail string) error {
	if _, err := s.checkPassword(u, current); err != nil {
		return err
	}
	addr, err := normalizeEmail(newEmail)
	if err != nil {
		return err
	}
	if addr == u.Email {
		return ErrInvalidInput
	}
	other, err := s.store.UserByEmail(ctx, addr)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	owned := err == nil && other.ID != u.ID
	// Only the newest link may work: drop the older ones before the pending address changes.
	if err := s.store.DeleteUserTokens(ctx, u.ID, TokenEmailChange); err != nil {
		return err
	}
	if err := s.store.SetUserPendingEmail(ctx, u.ID, &addr); err != nil {
		return err
	}
	tok, err := s.newToken(ctx, u.ID, TokenEmailChange, tokenData{Email: addr}, emailTTL)
	if err != nil {
		return err
	}
	s.log.Info("email change requested", "user_id", u.ID)
	if !owned {
		subject, text := emailChangeMailText(s.host(), s.link("verify", tok))
		_ = s.send(ctx, u.ID, "email_change", addr, subject, text)
	}
	return nil
}

func (s *Service) SetDisplayName(ctx context.Context, u User, name string) (User, error) {
	name = strings.TrimSpace(name)
	n := utf8.RuneCountInString(name)
	if n < 1 || n > maxDisplayName || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return User{}, ErrInvalidInput
	}
	if err := s.store.UpdateUserDisplayName(ctx, u.ID, name); err != nil {
		return User{}, err
	}
	u.DisplayName = name
	return u, nil
}

func (s *Service) DeleteAccount(ctx context.Context, u User, current string) error {
	if _, err := s.checkPassword(u, current); err != nil {
		return err
	}
	if err := s.store.DeleteUser(ctx, u.ID); err != nil {
		return err
	}
	s.log.Info("account deleted", "user_id", u.ID)
	return nil
}

// ---- app sign-in ----

type appCodeData struct {
	CodeChallenge string `json:"codeChallenge"`
	DeviceLabel   string `json:"deviceLabel"`
}

// IssueAppCode stores a one-time code bound to the PKCE challenge (base64url SHA-256, 43 characters).
func (s *Service) IssueAppCode(ctx context.Context, u User, codeChallenge, deviceLabel string) (string, error) {
	if raw, err := base64.RawURLEncoding.DecodeString(codeChallenge); err != nil || len(raw) != sha256.Size {
		return "", ErrInvalidInput
	}
	return s.newToken(ctx, u.ID, TokenAppCode, appCodeData{CodeChallenge: codeChallenge, DeviceLabel: cleanLabel(deviceLabel)}, appCodeTTL)
}

// ExchangeAppCode burns the code first, then checks the verifier, so a wrong guess cannot be retried.
func (s *Service) ExchangeAppCode(ctx context.Context, code, codeVerifier string, meta SessionMeta) (string, Session, User, error) {
	fail := func(err error) (string, Session, User, error) { return "", Session{}, User{}, err }
	if code == "" {
		return fail(ErrTokenInvalid)
	}
	uid, raw, err := s.store.ConsumeToken(ctx, TokenAppCode, HashToken(code), s.now())
	if errors.Is(err, ErrNotFound) {
		return fail(ErrTokenInvalid)
	}
	if err != nil {
		return fail(err)
	}
	var d appCodeData
	if json.Unmarshal(raw, &d) != nil {
		return fail(ErrTokenInvalid)
	}
	sum := sha256.Sum256([]byte(codeVerifier))
	got := base64.RawURLEncoding.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(got), []byte(d.CodeChallenge)) != 1 {
		s.log.Warn("app sign-in with a wrong code verifier", "user_id", uid)
		return fail(ErrTokenInvalid)
	}
	u, err := s.store.UserByID(ctx, uid)
	if errors.Is(err, ErrNotFound) || (err == nil && u.DisabledAt != nil) {
		return fail(ErrTokenInvalid)
	}
	if err != nil {
		return fail(err)
	}
	// Keep one slot free for the session about to be created.
	if err := s.store.EndOldestAppSessions(ctx, uid, maxAppSession-1); err != nil {
		return fail(err)
	}
	label := cleanLabel(d.DeviceLabel)
	tok, sess, err := s.createSession(ctx, u, kindApp, label, meta)
	if err != nil {
		return fail(err)
	}
	if err := s.store.TouchUserLogin(ctx, uid, s.now()); err != nil {
		s.log.Warn("login time not saved", "user_id", uid, "err", err)
	}
	s.log.Info("app sign-in", "user_id", uid, "session_id", sess.ID)
	if u.Email != "" {
		subject, text := appSignInMailText(s.host(), label, s.now())
		_ = s.send(ctx, uid, "app_sign_in", u.Email, subject, text)
	}
	return tok, sess, u, nil
}
