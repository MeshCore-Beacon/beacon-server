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
	SetUserPendingEmail(ctx context.Context, id uuid.UUID, email *string) error
	ActivateUser(ctx context.Context, id uuid.UUID, username, hash string, at time.Time) error
	// LatestToken is the newest unused token of the kind, expired or not (cleanup keeps it a week).
	LatestToken(ctx context.Context, userID uuid.UUID, kind TokenKind) ([]byte, error)
	// ResetCredentials spends a live reset token (only while its address is still the account's) and does what
	// ChangeCredentials does with no expected hash, in one transaction under the user's credential lock.
	ResetCredentials(ctx context.Context, tokenHash []byte, id uuid.UUID, hash string, verifiedAt *time.Time, now time.Time) error
	PeekToken(ctx context.Context, kind TokenKind, hash []byte, now time.Time) (uuid.UUID, error)
	// ApplyEmailChange spends the email-change token and applies its address in one transaction under the user's
	// credential lock; it returns the user, the address replaced and the new one.
	ApplyEmailChange(ctx context.Context, tokenHash []byte, now time.Time) (uuid.UUID, string, string, error)
	TouchUserLogin(ctx context.Context, id uuid.UUID, at time.Time) error
	// IssueSession stores a session only while the user is live and still has the password hash the caller verified,
	// and for an app session first ends the oldest beyond maxApp-1; ErrNotFound when the user or the hash changed.
	IssueSession(ctx context.Context, sess Session, tokenHash []byte, expectPassHash string, maxApp int) (Session, error)
	// ChangeCredentials sets the hash, ends the user's sessions (all but keep; all when keep is the nil uuid), deletes
	// every outstanding one-time token and the pending email, and marks the address verified when verifiedAt is set,
	// all serialised with IssueSession.
	// A non-empty expectPassHash (the hash the user's password was checked against) makes it refuse, with ErrNotFound,
	// when the stored hash differs, the user is disabled or keep is no longer a live session.
	ChangeCredentials(ctx context.Context, id uuid.UUID, hash string, verifiedAt *time.Time, keep uuid.UUID, expectPassHash string) error
	// BeginEmailChange stores the pending address and its token, and DeleteUserChecked deletes the user, each only
	// while the stored hash is still expectPassHash (ErrNotFound otherwise).
	BeginEmailChange(ctx context.Context, id uuid.UUID, expectPassHash, addr string, tokenHash, data []byte, expires time.Time) error
	DeleteUserChecked(ctx context.Context, id uuid.UUID, expectPassHash string) error
	// RehashPassword stores a newer hash of the same password while the stored one is still oldHash.
	RehashPassword(ctx context.Context, id uuid.UUID, oldHash, newHash string) (bool, error)
	SessionByTokenHash(ctx context.Context, hash []byte, now time.Time) (Session, User, error)
	TouchSession(ctx context.Context, id uuid.UUID, lastUsed, expires time.Time) error
	ListSessions(ctx context.Context, userID uuid.UUID, now time.Time) ([]Session, error)
	EndSession(ctx context.Context, userID, sessionID uuid.UUID) error
	EndOtherSessions(ctx context.Context, userID, keep uuid.UUID) error
	EndAllSessions(ctx context.Context, userID uuid.UUID) error
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
	// ErrBusy: every password-hashing slot stayed taken for too long; the caller may retry.
	ErrBusy = errors.New("users: busy")
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

	// maxHashing argon2 runs at once, each holding 19 MiB; a request waits at most hashWait for its turn.
	maxHashing = 4
	hashWait   = 5 * time.Second

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
	// mailq feeds the one mail worker, see Start.
	mailq chan queuedMail
	// dummyHash is compared against when the user is unknown, so both paths cost one argon2 run.
	dummyHash string
	// slots caps concurrent argon2 runs (hashing and verifying, the dummy comparison included); slotWait is how
	// long a request waits for one before ErrBusy.
	slots    chan struct{}
	slotWait time.Duration
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
	return &Service{store: store, mailer: m, cfg: cfg, now: now, log: log, throttle: newThrottle(), mailq: make(chan queuedMail, mailQueueSize), verifyPW: VerifyPassword, dummyHash: dummy,
		slots: make(chan struct{}, maxHashing), slotWait: hashWait}
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

// ---- password work ----

// slot waits for one of the argon2 slots; the caller defers the returned release. It fails with ErrBusy after
// slotWait and with the context's error when the request goes away first.
func (s *Service) slot(ctx context.Context) (func(), error) {
	release := func() { <-s.slots }
	select {
	case s.slots <- struct{}{}:
		return release, nil
	default:
	}
	timer := time.NewTimer(s.slotWait)
	defer timer.Stop()
	select {
	case s.slots <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrBusy
	}
}

// hash is HashPassword under a slot.
func (s *Service) hash(ctx context.Context, password string) (string, error) {
	release, err := s.slot(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return HashPassword(password)
}

// verify is the password comparison under a slot; ok and rehash are as VerifyPassword's.
func (s *Service) verify(ctx context.Context, hash, password string) (ok, rehash bool, err error) {
	release, err := s.slot(ctx)
	if err != nil {
		return false, false, err
	}
	defer release()
	ok, rehash = s.verifyPW(hash, password)
	return ok, rehash, nil
}

// ---- mail ----

func (s *Service) link(view, token string) string {
	u := *s.cfg.PublicURL
	u.Path = "/"
	// The token rides in the fragment, which a browser never sends, so no proxy or access log sees a live token.
	u.RawQuery = "view=" + view
	u.Fragment = "token=" + token
	return u.String()
}

func (s *Service) host() string { return s.cfg.PublicURL.Host }

// name is the instance name mail carries; "Beacon" when none was set.
func (s *Service) name() string {
	if s.cfg.InstanceName == "" {
		return "Beacon"
	}
	return s.cfg.InstanceName
}

const (
	mailQueueSize   = 64
	mailSendTimeout = 30 * time.Second
	mailDrainTime   = 10 * time.Second
)

type queuedMail struct {
	userID  uuid.UUID
	kind    string
	msg     mailer.Message
	barrier chan struct{} // set only by Flush
}

// enqueue hands a mail to the worker without ever waiting, so a request costs the same whether or not it sends one.
// A full queue drops the mail and logs it.
func (s *Service) enqueue(userID uuid.UUID, kind, to, subject, text string) {
	q := queuedMail{userID: userID, kind: kind, msg: mailer.Message{To: to, Subject: subject, Text: text}}
	select {
	case s.mailq <- q:
	default:
		s.log.Warn("mail queue full, mail dropped", "user_id", userID, "mail", kind)
	}
}

// deliver sends one mail; a failure is logged with the user id, kind and recipient domain only.
func (s *Service) deliver(q queuedMail) {
	ctx, cancel := context.WithTimeout(context.Background(), mailSendTimeout)
	defer cancel()
	if err := s.mailer.Send(ctx, q.msg); err != nil {
		s.log.Warn("mail not sent", "user_id", q.userID, "mail", q.kind, "to_domain", emailDomain(q.msg.To), "err", err)
	}
}

// Start runs the mail worker until ctx ends, then sends what is still queued (for a few seconds at most). The
// returned channel closes when the worker has stopped.
func (s *Service) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				deadline := time.After(mailDrainTime)
				for {
					select {
					case q := <-s.mailq:
						if q.barrier != nil {
							close(q.barrier)
						} else {
							s.deliver(q)
						}
					case <-deadline:
						return
					default:
						return
					}
				}
			case q := <-s.mailq:
				if q.barrier != nil {
					close(q.barrier)
					continue
				}
				s.deliver(q)
			}
		}
	}()
	return done
}

// Flush returns once every mail queued before the call has been handled. Tests and shutdown use it.
func (s *Service) Flush(ctx context.Context) error {
	b := make(chan struct{})
	select {
	case s.mailq <- queuedMail{barrier: b}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-b:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tokenData is what a one-time token carries. A verify token holds its own sign-up (username and password hash,
// never the password), so what a link activates is fixed when the link is made.
type tokenData struct {
	Email    string `json:"email,omitempty"`
	Username string `json:"username,omitempty"`
	PassHash string `json:"passHash,omitempty"`
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

// sendVerify mails a link that activates the sign-up (username, hash) for u's address. Older links stay valid
// until they expire: each belongs to its own sign-up, and deleting them would let a stranger who knows the
// address cancel the owner's link.
func (s *Service) sendVerify(ctx context.Context, u User, username, hash string) error {
	tok, err := s.newToken(ctx, u.ID, TokenVerify, tokenData{Email: u.Email, Username: username, PassHash: hash}, verifyTTL)
	if err != nil {
		return err
	}
	subject, text := verifyMailText(s.name(), s.host(), username, s.link("verify", tok))
	s.enqueue(u.ID, "verify", u.Email, subject, text)
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
	hash, err := s.hash(ctx, password)
	if err != nil {
		return err
	}
	// The username answer never depends on the address: any row holding it, verified or pending, refuses.
	if _, uerr := s.store.UserByLogin(ctx, username); uerr == nil {
		return ErrUsernameTaken
	} else if !errors.Is(uerr, ErrNotFound) {
		return uerr
	}
	u, err := s.store.CreateUser(ctx, User{Username: username, DisplayName: username, Email: addr, PassHash: hash, Provider: "local"})
	switch {
	case errors.Is(err, ErrUsernameTaken):
		return err // taken between the check and the insert
	case errors.Is(err, ErrEmailTaken):
		existing, gerr := s.store.UserByEmail(ctx, addr)
		if errors.Is(gerr, ErrNotFound) {
			return nil
		}
		if gerr != nil {
			return gerr
		}
		if existing.EmailVerifiedAt != nil || existing.DisabledAt != nil {
			return nil
		}
		// Nobody has proved they own this address yet. The row stays as the first sign-up left it; this sign-up
		// gets its own link, and whichever link the owner opens decides the username and password.
		s.log.Info("pending registration added", "user_id", existing.ID)
		return s.sendVerify(ctx, existing, username, hash)
	case err != nil:
		return err
	}
	s.log.Info("user registered", "user_id", u.ID)
	return s.sendVerify(ctx, u, username, hash)
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
		if json.Unmarshal(raw, &d) != nil || d.Email == "" || d.Username == "" || d.PassHash == "" {
			return ErrTokenInvalid
		}
		u, err := s.store.UserByID(ctx, uid)
		if errors.Is(err, ErrNotFound) || (err == nil && (u.Email != d.Email || u.EmailVerifiedAt != nil || u.DisabledAt != nil)) {
			return ErrTokenInvalid
		}
		if err != nil {
			return err
		}
		// The sign-up in this link wins: its username and hash replace the row's, and every other verify link and
		// session of the account goes. A username taken meanwhile fails the link (it is spent either way).
		switch aerr := s.store.ActivateUser(ctx, uid, d.Username, d.PassHash, now); {
		case errors.Is(aerr, ErrNotFound):
			return ErrTokenInvalid
		case aerr != nil:
			return aerr
		}
		s.log.Info("email verified", "user_id", uid)
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	uid, oldEmail, newEmail, err := s.store.ApplyEmailChange(ctx, hash, now)
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrEmailTaken):
		return ErrTokenInvalid
	case err != nil:
		return err
	}
	s.log.Info("email changed", "user_id", uid)
	// The previous mailbox is told, so a takeover cannot go unnoticed there.
	if oldEmail != "" && oldEmail != newEmail {
		subject, text := emailChangedMailText(s.name(), s.host())
		s.enqueue(uid, "email_changed", oldEmail, subject, text)
	}
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
	// Never mint a link from the row: its username and hash may be a stranger's. Repeat the newest retained sign-up,
	// even when its link has expired: that is how its owner recovers it.
	raw, err := s.store.LatestToken(ctx, u.ID, TokenVerify)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var d tokenData
	if json.Unmarshal(raw, &d) != nil || d.Email != u.Email || d.Username == "" || d.PassHash == "" {
		return nil
	}
	return s.sendVerify(ctx, u, d.Username, d.PassHash)
}

// ---- password checks ----

// checkKeys verifies pw against hash behind the throttle entries for keys. Every key is read before any hashing,
// so a throttled one costs nothing to hit, and any locked key refuses the attempt. A wrong password counts against
// every key, a right one clears them all.
func (s *Service) checkKeys(ctx context.Context, keys []string, hash, pw string) (needsRehash bool, err error) {
	now := s.now()
	var wait time.Duration
	locked := false
	for _, k := range keys {
		if w, l := s.throttle.locked(k, now); l {
			locked = true
			if w > wait {
				wait = w
			}
		}
	}
	if locked {
		return false, &TooManyAttemptsError{RetryAfter: wait}
	}
	ok, re, verr := s.verify(ctx, hash, pw)
	if verr != nil {
		return false, verr // no verdict on the password, so it counts for nothing against the budget
	}
	if !ok {
		full := false
		for _, k := range keys {
			l, f := s.throttle.fail(k, now)
			if l {
				s.log.Warn("password attempts throttled", "key_kind", strings.SplitN(k, ":", 2)[0])
			}
			full = full || f
		}
		if full {
			return false, &TooManyAttemptsError{RetryAfter: fullRetry}
		}
		return false, ErrInvalidCredentials
	}
	for _, k := range keys {
		s.throttle.clear(k)
	}
	return re, nil
}

// checkPassword is the check for a signed-in user's own actions, throttled per user id.
func (s *Service) checkPassword(ctx context.Context, u User, pw string) (needsRehash bool, err error) {
	return s.checkKeys(ctx, []string{userKey(u.ID)}, u.PassHash, pw)
}

// ---- login and sessions ----

// createSession stores a session for u under the store's per-user lock, which refuses it when u's password hash is no
// longer expectHash (a reset or password change got in after the credential was checked): ErrNotFound.
func (s *Service) createSession(ctx context.Context, u User, expectHash, kind, label string, meta SessionMeta) (string, Session, error) {
	now := s.now()
	sess := Session{UserID: u.ID, Kind: kind, Label: label, IP: meta.IP, UserAgent: meta.UserAgent}
	if kind == kindApp {
		hard := now.Add(appHardTTL)
		sess.ExpiresAt, sess.HardExpiresAt = now.Add(appTTL), &hard
	} else {
		sess.ExpiresAt = now.Add(webTTL)
	}
	plain, hash := NewToken()
	maxApp := 0
	if kind == kindApp {
		maxApp = maxAppSession
	}
	created, err := s.store.IssueSession(ctx, sess, hash, expectHash, maxApp)
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
	// The throttle is keyed on the typed name, so a known and an unknown name look the same from outside.
	key := loginKey(login)
	if wait, locked := s.throttle.locked(key, s.now()); locked {
		return "", Session{}, User{}, &TooManyAttemptsError{RetryAfter: wait}
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
		if _, _, verr := s.verify(ctx, s.dummyHash, password); verr != nil {
			return "", Session{}, User{}, verr
		}
		if _, full := s.throttle.fail(key, s.now()); full {
			return "", Session{}, User{}, &TooManyAttemptsError{RetryAfter: fullRetry}
		}
		return "", Session{}, User{}, ErrInvalidCredentials
	}
	// Aliases of one account (username, email) share one budget through the user key.
	needsRehash, err := s.checkKeys(ctx, []string{key, userKey(u.ID)}, u.PassHash, password)
	if err != nil {
		s.log.Info("login refused", "user_id", u.ID, "reason", loginReason(err))
		return "", Session{}, User{}, err
	}
	if u.EmailVerifiedAt == nil {
		return "", Session{}, User{}, ErrEmailUnverified
	}
	// The session is only issued while the stored hash is the one just verified, so a reset that completes in
	// between wins. A rehash is that hash's replacement, so it moves the expectation with it.
	expect := u.PassHash
	if needsRehash {
		if h, herr := s.hash(ctx, password); herr == nil {
			switch done, werr := s.store.RehashPassword(ctx, u.ID, u.PassHash, h); {
			case werr != nil:
				s.log.Warn("password rehash not saved", "user_id", u.ID, "err", werr)
			case done:
				expect = h
			}
		}
	}
	if err := s.store.TouchUserLogin(ctx, u.ID, s.now()); err != nil {
		s.log.Warn("login time not saved", "user_id", u.ID, "err", err)
	}
	tok, sess, err := s.createSession(ctx, u, expect, kindWeb, meta.Label, meta)
	if errors.Is(err, ErrNotFound) {
		return "", Session{}, User{}, ErrInvalidCredentials
	}
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
	// Bound to the address it is mailed to, so it dies with that address.
	tok, err := s.newToken(ctx, u.ID, TokenReset, tokenData{Email: u.Email}, resetTTL)
	if err != nil {
		return err
	}
	s.log.Info("password reset requested", "user_id", u.ID)
	subject, text := resetMailText(s.name(), s.host(), s.link("reset", tok))
	s.enqueue(u.ID, "reset", u.Email, subject, text)
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
	// Peek first so a password the policy refuses does not spend the link.
	hashTok := HashToken(token)
	uid, err := s.store.PeekToken(ctx, TokenReset, hashTok, s.now())
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
	hash, err := s.hash(ctx, newPassword)
	if err != nil {
		return err
	}
	var verifiedAt *time.Time
	if u.EmailVerifiedAt == nil {
		now := s.now()
		verifiedAt = &now
	}
	if err := s.store.ResetCredentials(ctx, hashTok, uid, hash, verifiedAt, s.now()); errors.Is(err, ErrNotFound) {
		return ErrTokenInvalid
	} else if err != nil {
		return err
	}
	s.throttle.clear(userKey(uid))
	s.throttle.clear(loginKey(u.Username))
	s.throttle.clear(loginKey(u.Email))
	s.log.Info("password reset completed", "user_id", uid)
	subject, text := passwordChangedMailText(s.name(), s.host())
	s.enqueue(uid, "password_changed", u.Email, subject, text)
	return nil
}

// ---- profile ----

func (s *Service) ChangePassword(ctx context.Context, u User, keepSession uuid.UUID, current, newPassword string) error {
	if _, err := s.checkPassword(ctx, u, current); err != nil {
		return err
	}
	if err := CheckPasswordPolicy(newPassword, u.Username, u.Email); err != nil {
		return err
	}
	hash, err := s.hash(ctx, newPassword)
	if err != nil {
		return err
	}
	// Under the lock the store refuses when the password changed since it was checked (a reset got in), or the
	// session being kept has ended.
	if err := s.store.ChangeCredentials(ctx, u.ID, hash, nil, keepSession, u.PassHash); errors.Is(err, ErrNotFound) {
		return ErrInvalidCredentials
	} else if err != nil {
		return err
	}
	s.log.Info("password changed", "user_id", u.ID)
	subject, text := passwordChangedMailText(s.name(), s.host())
	s.enqueue(u.ID, "password_changed", u.Email, subject, text)
	return nil
}

// ChangeEmail holds the new address as pending and mails a link to it. An address another user owns gets the same
// treatment minus the mail, so the answer does not reveal who is registered.
func (s *Service) ChangeEmail(ctx context.Context, u User, current, newEmail string) error {
	if _, err := s.checkPassword(ctx, u, current); err != nil {
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
	// Only the newest link may work: the store drops the older ones as it sets the pending address, and refuses when
	// the password changed since it was checked.
	raw, err := json.Marshal(tokenData{Email: addr})
	if err != nil {
		return err
	}
	tok, tokHash := NewToken()
	if err := s.store.BeginEmailChange(ctx, u.ID, u.PassHash, addr, tokHash, raw, s.now().Add(emailTTL)); errors.Is(err, ErrNotFound) {
		return ErrInvalidCredentials
	} else if err != nil {
		return err
	}
	s.log.Info("email change requested", "user_id", u.ID)
	if !owned {
		subject, text := emailChangeMailText(s.name(), s.host(), s.link("verify", tok))
		s.enqueue(u.ID, "email_change", addr, subject, text)
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

// DeleteAccount removes the account. A portal user has no password, so confirm is their own username instead.
func (s *Service) DeleteAccount(ctx context.Context, u User, confirm string) error {
	if u.Provider == ProviderPortal {
		if c := strings.TrimSpace(confirm); c == "" || !strings.EqualFold(c, u.Username) {
			return ErrInvalidInput
		}
	} else if _, err := s.checkPassword(ctx, u, confirm); err != nil {
		return err
	}
	if err := s.store.DeleteUserChecked(ctx, u.ID, u.PassHash); errors.Is(err, ErrNotFound) {
		return ErrInvalidCredentials
	} else if err != nil {
		return err
	}
	s.log.Info("account deleted", "user_id", u.ID)
	return nil
}

// ---- app sign-in ----

type appCodeData struct {
	CodeChallenge string `json:"codeChallenge"`
	DeviceLabel   string `json:"deviceLabel"`
	// PassHash is the user's password hash when the code was issued: a reset or password change after that makes the
	// exchange fail, even when it had already spent the code.
	PassHash string `json:"passHash"`
}

// IssueAppCode stores a one-time code bound to the PKCE challenge (base64url SHA-256, 43 characters).
func (s *Service) IssueAppCode(ctx context.Context, u User, codeChallenge, deviceLabel string) (string, error) {
	if raw, err := base64.RawURLEncoding.DecodeString(codeChallenge); err != nil || len(raw) != sha256.Size {
		return "", ErrInvalidInput
	}
	return s.newToken(ctx, u.ID, TokenAppCode, appCodeData{CodeChallenge: codeChallenge, DeviceLabel: cleanLabel(deviceLabel), PassHash: u.PassHash}, appCodeTTL)
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
	// The app names its own device; the label the browser sent with the code is the fallback for apps that do not.
	label := cleanLabel(d.DeviceLabel)
	if strings.TrimSpace(meta.Label) != "" {
		label = cleanLabel(meta.Label)
	}
	// The store keeps a slot free and issues the session under the user's lock, refusing it when a reset or password
	// change completed after the user was read.
	if d.PassHash == "" {
		return fail(ErrTokenInvalid)
	}
	tok, sess, err := s.createSession(ctx, u, d.PassHash, kindApp, label, meta)
	if errors.Is(err, ErrNotFound) {
		return fail(ErrTokenInvalid)
	}
	if err != nil {
		return fail(err)
	}
	if err := s.store.TouchUserLogin(ctx, uid, s.now()); err != nil {
		s.log.Warn("login time not saved", "user_id", uid, "err", err)
	}
	s.log.Info("app sign-in", "user_id", uid, "session_id", sess.ID)
	if u.Email != "" {
		subject, text := appSignInMailText(s.name(), s.host(), label, s.now())
		s.enqueue(uid, "app_sign_in", u.Email, subject, text)
	}
	return tok, sess, u, nil
}
