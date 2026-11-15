// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Longest text kept from a request; the rest is dropped on a rune boundary.
const (
	maxSessionLabel     = 60
	maxSessionIP        = 64
	maxSessionUserAgent = 200
	// Sessions and tokens that ended or expired longer ago than this are deleted.
	authRetention = 7 * 24 * time.Hour
)

const (
	uniqueViolation = "23505"
	usernameIndex   = "idx_users_username"
	emailIndex      = "idx_users_email"
	subjectIndex    = "idx_users_provider_subject"
)

// errSubjectTaken is a second user for one provider subject, a lost race between two first sign-ins.
var errSubjectTaken = errors.New("db: provider subject taken")

func userFromRow(r sqlc.User) users.User {
	return users.User{
		ID: r.ID, Username: r.Username, DisplayName: r.DisplayName, Email: r.Email, PendingEmail: r.PendingEmail,
		PassHash: r.PassHash, EmailVerifiedAt: timePtr(r.EmailVerifiedAt), Provider: r.Provider, ProviderSubject: r.ProviderSubject,
		CreatedAt: r.CreatedAt.Time, LastLoginAt: timePtr(r.LastLoginAt), DisabledAt: timePtr(r.DisabledAt),
	}
}

func timePtr(t pgtype.Timestamptz) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}

// userWriteErr maps a unique violation to the users error for the index it hit, by constraint name.
func userWriteErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		switch pgErr.ConstraintName {
		case usernameIndex:
			return users.ErrUsernameTaken
		case emailIndex:
			return users.ErrEmailTaken
		case subjectIndex:
			return errSubjectTaken
		}
	}
	return err
}

// oneUser turns an empty result into users.ErrNotFound.
func oneUser(r sqlc.User, err error) (users.User, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return users.User{}, users.ErrNotFound
	}
	if err != nil {
		return users.User{}, err
	}
	return userFromRow(r), nil
}

// affected reports users.ErrNotFound when an update matched no row.
func affected(n int64, err error) error {
	if err != nil {
		return userWriteErr(err)
	}
	if n == 0 {
		return users.ErrNotFound
	}
	return nil
}

func (s *Store) CreateUser(ctx context.Context, u users.User) (users.User, error) {
	provider := u.Provider
	if provider == "" {
		provider = "local"
	}
	row, err := s.q.CreateUser(ctx, sqlc.CreateUserParams{
		Username: u.Username, DisplayName: u.DisplayName, Email: strings.ToLower(u.Email), PassHash: u.PassHash,
		Provider: provider, ProviderSubject: u.ProviderSubject, EmailVerifiedAt: optTime(u.EmailVerifiedAt),
	})
	if err != nil {
		return users.User{}, userWriteErr(err)
	}
	return userFromRow(row), nil
}

func (s *Store) UserByID(ctx context.Context, id uuid.UUID) (users.User, error) {
	return oneUser(s.q.UserByID(ctx, id))
}

// UserByLogin takes an email when login contains "@", otherwise a username compared ignoring case.
func (s *Store) UserByLogin(ctx context.Context, login string) (users.User, error) {
	if strings.Contains(login, "@") {
		return s.UserByEmail(ctx, login)
	}
	return oneUser(s.q.UserByUsername(ctx, login))
}

func (s *Store) UserByEmail(ctx context.Context, email string) (users.User, error) {
	return oneUser(s.q.UserByEmail(ctx, strings.ToLower(email)))
}

func (s *Store) UpdateUserDisplayName(ctx context.Context, id uuid.UUID, name string) error {
	return affected(s.q.UpdateUserDisplayName(ctx, sqlc.UpdateUserDisplayNameParams{ID: id, DisplayName: name}))
}

func (s *Store) SetUserPassword(ctx context.Context, id uuid.UUID, hash string) error {
	return affected(s.q.SetUserPassword(ctx, sqlc.SetUserPasswordParams{ID: id, PassHash: hash}))
}

func (s *Store) SetUserEmailVerified(ctx context.Context, id uuid.UUID, at time.Time) error {
	return affected(s.q.SetUserEmailVerified(ctx, sqlc.SetUserEmailVerifiedParams{ID: id, EmailVerifiedAt: ts(at)}))
}

// SetUserPendingEmail holds an unverified new address, or clears it with nil. It is not checked against other users
// until ApplyPendingEmail.
func (s *Store) SetUserPendingEmail(ctx context.Context, id uuid.UUID, email *string) error {
	var p *string
	if email != nil {
		v := strings.ToLower(*email)
		p = &v
	}
	return affected(s.q.SetUserPendingEmail(ctx, sqlc.SetUserPendingEmailParams{ID: id, PendingEmail: p}))
}

// ApplyPendingEmail makes the pending address the email and marks it verified. It reports users.ErrEmailTaken when
// another user owns the address now, and users.ErrNotFound when the pending address is not expected (or absent).
func (s *Store) ApplyPendingEmail(ctx context.Context, id uuid.UUID, expected string) error {
	e := strings.ToLower(expected)
	// Reset links mailed to the previous address go with it, in the same transaction.
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := affected(q.ApplyPendingEmail(ctx, sqlc.ApplyPendingEmailParams{ID: id, PendingEmail: &e})); err != nil {
			return err
		}
		return q.DeleteUserTokens(ctx, sqlc.DeleteUserTokensParams{UserID: id, Kind: string(users.TokenReset)})
	})
}

// ApplyEmailChange spends the email-change token and applies its address in one transaction under the user's
// credential lock, serialised with BeginEmailChange, so a link replaced meanwhile fails. It returns the user, the
// address it replaced and the new one, and reports users.ErrNotFound for a token that is unknown, used, expired,
// replaced, or whose address is no longer the pending one, and users.ErrEmailTaken when another user owns it now.
func (s *Store) ApplyEmailChange(ctx context.Context, tokenHash []byte, now time.Time) (uuid.UUID, string, string, error) {
	var (
		id       uuid.UUID
		old, add string
	)
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		uid, err := q.PeekUserToken(ctx, sqlc.PeekUserTokenParams{TokenHash: tokenHash, Kind: string(users.TokenEmailChange), Now: ts(now)})
		if errors.Is(err, pgx.ErrNoRows) {
			return users.ErrNotFound
		}
		if err != nil {
			return err
		}
		if err := lockChecked(ctx, q, uid, ""); err != nil {
			return err
		}
		// The lock is held, so a replacement that committed first has deleted the token and this finds nothing.
		r, err := q.ConsumeUserToken(ctx, sqlc.ConsumeUserTokenParams{TokenHash: tokenHash, Kind: string(users.TokenEmailChange), Now: ts(now)})
		if errors.Is(err, pgx.ErrNoRows) {
			return users.ErrNotFound
		}
		if err != nil {
			return err
		}
		var d struct {
			Email string `json:"email"`
		}
		if json.Unmarshal(r.Data, &d) != nil || d.Email == "" {
			return users.ErrNotFound
		}
		cur, err := q.UserByID(ctx, uid)
		if err != nil {
			return err
		}
		e := strings.ToLower(d.Email)
		if err := affected(q.ApplyPendingEmail(ctx, sqlc.ApplyPendingEmailParams{ID: uid, PendingEmail: &e})); err != nil {
			return err
		}
		if err := q.DeleteUserTokens(ctx, sqlc.DeleteUserTokensParams{UserID: uid, Kind: string(users.TokenReset)}); err != nil {
			return err
		}
		id, old, add = uid, cur.Email, e
		return nil
	})
	return id, old, add, err
}

// ActivateUser finishes a sign-up: it sets the username and password hash, marks the address verified and deletes
// the account's verify tokens and sessions. It reports users.ErrNotFound when the user is already verified,
// disabled or missing, and users.ErrUsernameTaken when another account has the username.
func (s *Store) ActivateUser(ctx context.Context, id uuid.UUID, username, hash string, at time.Time) error {
	_, err := s.q.ActivateUser(ctx, sqlc.ActivateUserParams{UserID: id, Username: username, PassHash: hash, VerifiedAt: ts(at)})
	if errors.Is(err, pgx.ErrNoRows) {
		return users.ErrNotFound
	}
	return userWriteErr(err)
}

func (s *Store) TouchUserLogin(ctx context.Context, id uuid.UUID, at time.Time) error {
	return affected(s.q.TouchUserLogin(ctx, sqlc.TouchUserLoginParams{ID: id, LastLoginAt: ts(at)}))
}

func (s *Store) SetUserDisabled(ctx context.Context, id uuid.UUID, at *time.Time) error {
	return affected(s.q.SetUserDisabled(ctx, sqlc.SetUserDisabledParams{ID: id, DisabledAt: optTime(at)}))
}

// DeleteUser removes the user for an operator. It runs the same locked transaction as DeleteUserChecked, so a request
// created meanwhile goes through the state rules too; only the authorisation differs. It relies on the ON DELETE
// CASCADE of every table that belongs to a user.
func (s *Store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	if s.pool == nil {
		return affected(s.q.DeleteUser(ctx, id))
	}
	return s.deleteUser(ctx, id, "")
}

// maxDeleteRestarts bounds how often a delete starts over because the user requested another target meanwhile.
const maxDeleteRestarts = 5

// deleteUser runs deleteLocked, starting over with every target it has seen when a new one appeared.
func (s *Store) deleteUser(ctx context.Context, id uuid.UUID, expect string) error {
	var seen [][]byte
	for attempt := 0; ; attempt++ {
		err := s.pollTx(ctx, func(q *sqlc.Queries) error {
			var err error
			seen, err = deleteLocked(ctx, q, id, expect, seen)
			return err
		})
		if errors.Is(err, errNewUserTarget) && attempt < maxDeleteRestarts {
			continue
		}
		return err
	}
}

// deleteLocked is the body of both deletes: target locks, then the user lock (with the hash check when expect is
// set), then the requests through the state rules, then the user. When the user holds a request for a target it did
// not lock, it returns errNewUserTarget and every target it knows of, for the next attempt to lock first.
func deleteLocked(ctx context.Context, q *sqlc.Queries, id uuid.UUID, expect string, extra [][]byte) ([][]byte, error) {
	locked, err := lockUserTargets(ctx, q, id, extra)
	if err != nil {
		return nil, err
	}
	lock := func() error { return lockChecked(ctx, q, id, expect) }
	if expect == "" {
		lock = func() error { return lockUser(ctx, q, id) } // an operator may delete a disabled user
	}
	if err := lock(); err != nil {
		return nil, err
	}
	if deleteGapHook != nil {
		deleteGapHook()
	}
	// No request can be created for the user now, so this list is final.
	targets, err := q.UserRequestTargets(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, t := range targets {
		if !locked[string(t)] {
			all := append([][]byte(nil), targets...)
			for k := range locked {
				all = append(all, []byte(k))
			}
			return all, errNewUserTarget
		}
	}
	if err := removeUserRequestsTx(ctx, q, id, targets); err != nil {
		return nil, err
	}
	if expect == "" {
		return nil, affected(q.DeleteUser(ctx, id))
	}
	return nil, affected(q.DeleteUserIfHash(ctx, sqlc.DeleteUserIfHashParams{ID: id, PassHash: expect}))
}

// escapeLike makes s match literally inside a LIKE pattern.
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// ListUsers returns users newest first. The first page passes a time after every user and the max uuid; the next
// page passes the last row's CreatedAt and ID. search matches a substring of username or email, ignoring case.
func (s *Store) ListUsers(ctx context.Context, search string, afterCreated time.Time, afterID uuid.UUID, limit int) ([]users.User, error) {
	pattern := ""
	if search != "" {
		pattern = "%" + escapeLike(strings.ToLower(search)) + "%"
	}
	rows, err := s.q.ListUsers(ctx, sqlc.ListUsersParams{Search: pattern, AfterCreated: ts(afterCreated), AfterID: afterID, RowLimit: int32(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]users.User, 0, len(rows))
	for _, r := range rows {
		out = append(out, userFromRow(r))
	}
	return out, nil
}

func (s *Store) CreateSession(ctx context.Context, sess users.Session, tokenHash []byte) (users.Session, error) {
	return createSession(ctx, s.q, sess, tokenHash)
}

func createSession(ctx context.Context, q sqlc.Querier, sess users.Session, tokenHash []byte) (users.Session, error) {
	row, err := q.CreateUserSession(ctx, sqlc.CreateUserSessionParams{
		UserID: sess.UserID, TokenHash: tokenHash, Kind: sess.Kind, Label: clipText(sess.Label, maxSessionLabel),
		ExpiresAt: ts(sess.ExpiresAt), HardExpiresAt: optTime(sess.HardExpiresAt),
		Ip: clipText(sess.IP, maxSessionIP), UserAgent: clipText(sess.UserAgent, maxSessionUserAgent),
	})
	if err != nil {
		return users.Session{}, err
	}
	return users.Session{
		ID: row.ID, UserID: row.UserID, Kind: row.Kind, Label: row.Label, CreatedAt: row.CreatedAt.Time,
		LastUsedAt: row.LastUsedAt.Time, ExpiresAt: row.ExpiresAt.Time, HardExpiresAt: timePtr(row.HardExpiresAt),
		IP: row.Ip, UserAgent: row.UserAgent,
	}, nil
}

// IssueSession stores a session for the user only while the user is live and still has the password hash the caller
// verified (expectPassHash), under the user's credential lock, so a session cannot slip in after a reset or password
// change has ended the others. For an app session it first ends the oldest app sessions beyond maxApp-1, in the same
// transaction, so concurrent exchanges cannot pass the cap. It reports users.ErrNotFound when the user is gone,
// disabled, or the hash changed.
func (s *Store) IssueSession(ctx context.Context, sess users.Session, tokenHash []byte, expectPassHash string, maxApp int) (users.Session, error) {
	var out users.Session
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		cur, err := q.LockUserCredentials(ctx, sess.UserID)
		if errors.Is(err, pgx.ErrNoRows) {
			return users.ErrNotFound
		}
		if err != nil {
			return err
		}
		if cur.DisabledAt.Valid || cur.PassHash != expectPassHash {
			return users.ErrNotFound
		}
		if sess.Kind == "app" && maxApp > 0 {
			if err := q.EndOldestAppSessions(ctx, sqlc.EndOldestAppSessionsParams{UserID: sess.UserID, Keep: int32(maxApp - 1)}); err != nil {
				return err
			}
		}
		out, err = createSession(ctx, q, sess, tokenHash)
		return err
	})
	return out, err
}

// lockChecked takes the user's credential lock and refuses (users.ErrNotFound) a user that is gone or disabled and,
// when expectPassHash is set, one whose hash is no longer the one the caller verified.
func lockChecked(ctx context.Context, q *sqlc.Queries, id uuid.UUID, expectPassHash string) error {
	cur, err := q.LockUserCredentials(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return users.ErrNotFound
	}
	if err != nil {
		return err
	}
	if cur.DisabledAt.Valid || (expectPassHash != "" && cur.PassHash != expectPassHash) {
		return users.ErrNotFound
	}
	return nil
}

// lockUser takes the lock for an operator action, which may act on a disabled user.
func lockUser(ctx context.Context, q *sqlc.Queries, id uuid.UUID) error {
	if _, err := q.LockUserCredentials(ctx, id); errors.Is(err, pgx.ErrNoRows) {
		return users.ErrNotFound
	} else {
		return err
	}
}

// ChangeCredentials sets a new password hash and, in the same transaction under the user's credential lock, ends
// the user's sessions (all of them, or all but keep), deletes every outstanding one-time token and the pending
// email, and marks the address verified when verifiedAt is set. With expectPassHash set (a change the user
// authorised with their password) it refuses, with users.ErrNotFound, when the stored hash differs, the user is
// disabled, or keep is no longer a live session; a reset passes "" because its token authorises it.
func (s *Store) ChangeCredentials(ctx context.Context, id uuid.UUID, hash string, verifiedAt *time.Time, keep uuid.UUID, expectPassHash string) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := lockChecked(ctx, q, id, expectPassHash); err != nil {
			return err
		}
		if expectPassHash != "" && keep != uuid.Nil {
			live, err := q.UserSessionLive(ctx, sqlc.UserSessionLiveParams{ID: keep, UserID: id})
			if err != nil {
				return err
			}
			if !live {
				return users.ErrNotFound
			}
		}
		return applyCredentials(ctx, q, id, hash, verifiedAt, keep)
	})
}

// ResetCredentials spends the reset token, still bound to the account's address, and replaces the credentials in
// one transaction under the user's credential lock, so a change that completes first revokes the token (its
// delete is seen once the lock is held) and two links cannot both win. It reports users.ErrNotFound for a token
// that is unknown, used, expired, rebound or not the user's.
func (s *Store) ResetCredentials(ctx context.Context, tokenHash []byte, id uuid.UUID, hash string, verifiedAt *time.Time, now time.Time) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		// A disabled user keeps the credentials it had, whatever an earlier check saw.
		if err := lockChecked(ctx, q, id, ""); err != nil {
			return err
		}
		got, err := q.ConsumeResetToken(ctx, sqlc.ConsumeResetTokenParams{TokenHash: tokenHash, Now: ts(now)})
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && got != id) {
			return users.ErrNotFound
		}
		if err != nil {
			return err
		}
		return applyCredentials(ctx, q, id, hash, verifiedAt, uuid.Nil)
	})
}

// applyCredentials is the body shared by the credential changes; the caller holds the user's lock.
func applyCredentials(ctx context.Context, q *sqlc.Queries, id uuid.UUID, hash string, verifiedAt *time.Time, keep uuid.UUID) error {
	if err := affected(q.SetUserPassword(ctx, sqlc.SetUserPasswordParams{ID: id, PassHash: hash})); err != nil {
		return err
	}
	if verifiedAt != nil {
		if err := affected(q.SetUserEmailVerified(ctx, sqlc.SetUserEmailVerifiedParams{ID: id, EmailVerifiedAt: ts(*verifiedAt)})); err != nil {
			return err
		}
	}
	if keep == uuid.Nil {
		if err := q.EndAllUserSessions(ctx, id); err != nil {
			return err
		}
	} else if err := q.EndOtherUserSessions(ctx, sqlc.EndOtherUserSessionsParams{UserID: id, ID: keep}); err != nil {
		return err
	}
	for _, k := range []users.TokenKind{users.TokenVerify, users.TokenReset, users.TokenEmailChange, users.TokenAppCode} {
		if err := q.DeleteUserTokens(ctx, sqlc.DeleteUserTokensParams{UserID: id, Kind: string(k)}); err != nil {
			return err
		}
	}
	return affected(q.SetUserPendingEmail(ctx, sqlc.SetUserPendingEmailParams{ID: id}))
}

// BeginEmailChange holds addr as the pending address and stores its confirmation token, replacing older ones, under
// the user's credential lock and only while the stored hash is still expectPassHash (users.ErrNotFound otherwise).
func (s *Store) BeginEmailChange(ctx context.Context, id uuid.UUID, expectPassHash, addr string, tokenHash, data []byte, expires time.Time) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := lockChecked(ctx, q, id, expectPassHash); err != nil {
			return err
		}
		if err := q.DeleteUserTokens(ctx, sqlc.DeleteUserTokensParams{UserID: id, Kind: string(users.TokenEmailChange)}); err != nil {
			return err
		}
		a := strings.ToLower(addr)
		if err := affected(q.SetUserPendingEmail(ctx, sqlc.SetUserPendingEmailParams{ID: id, PendingEmail: &a})); err != nil {
			return err
		}
		return q.CreateUserToken(ctx, sqlc.CreateUserTokenParams{
			TokenHash: tokenHash, UserID: id, Kind: string(users.TokenEmailChange), Data: json.RawMessage(data), ExpiresAt: ts(expires)})
	})
}

// DeleteUserChecked deletes the user only while the stored hash is still expectPassHash (users.ErrNotFound
// otherwise). The hash check, the removal of the user's requests through the state rules and the delete all run in
// one transaction under the user's credential lock, so a reset cannot land between them.
func (s *Store) DeleteUserChecked(ctx context.Context, id uuid.UUID, expectPassHash string) error {
	return s.deleteUser(ctx, id, expectPassHash)
}

// RehashPassword stores newHash for the same password when the stored hash is still oldHash; false means it was
// not, and nothing changed.
func (s *Store) RehashPassword(ctx context.Context, id uuid.UUID, oldHash, newHash string) (bool, error) {
	n, err := s.q.RehashUserPassword(ctx, sqlc.RehashUserPasswordParams{ID: id, OldHash: oldHash, NewHash: newHash})
	return n > 0, err
}

// SessionByTokenHash returns the live session and its user, or users.ErrNotFound when the session is missing,
// ended, past either expiry at now, or its user is disabled.
func (s *Store) SessionByTokenHash(ctx context.Context, hash []byte, now time.Time) (users.Session, users.User, error) {
	r, err := s.q.UserSessionByTokenHash(ctx, sqlc.UserSessionByTokenHashParams{TokenHash: hash, Now: ts(now)})
	if errors.Is(err, pgx.ErrNoRows) {
		return users.Session{}, users.User{}, users.ErrNotFound
	}
	if err != nil {
		return users.Session{}, users.User{}, err
	}
	sess := users.Session{
		ID: r.SessionID, UserID: r.SessionUserID, Kind: r.Kind, Label: r.Label, CreatedAt: r.SessionCreatedAt.Time,
		LastUsedAt: r.LastUsedAt.Time, ExpiresAt: r.ExpiresAt.Time, HardExpiresAt: timePtr(r.HardExpiresAt),
		IP: r.Ip, UserAgent: r.UserAgent,
	}
	u := userFromRow(sqlc.User{
		ID: r.ID, Username: r.Username, DisplayName: r.DisplayName, Email: r.Email, PendingEmail: r.PendingEmail,
		PassHash: r.PassHash, EmailVerifiedAt: r.EmailVerifiedAt, Provider: r.Provider, ProviderSubject: r.ProviderSubject,
		CreatedAt: r.CreatedAt, LastLoginAt: r.LastLoginAt, DisabledAt: r.DisabledAt,
	})
	return sess, u, nil
}

func (s *Store) TouchSession(ctx context.Context, id uuid.UUID, lastUsed, expires time.Time) error {
	return affected(s.q.TouchUserSession(ctx, sqlc.TouchUserSessionParams{ID: id, LastUsedAt: ts(lastUsed), ExpiresAt: ts(expires)}))
}

// ListSessions returns the user's live sessions, newest first.
func (s *Store) ListSessions(ctx context.Context, userID uuid.UUID, now time.Time) ([]users.Session, error) {
	rows, err := s.q.ListUserSessions(ctx, sqlc.ListUserSessionsParams{UserID: userID, Now: ts(now)})
	if err != nil {
		return nil, err
	}
	out := make([]users.Session, 0, len(rows))
	for _, r := range rows {
		out = append(out, users.Session{
			ID: r.ID, UserID: r.UserID, Kind: r.Kind, Label: r.Label, CreatedAt: r.CreatedAt.Time,
			LastUsedAt: r.LastUsedAt.Time, ExpiresAt: r.ExpiresAt.Time, HardExpiresAt: timePtr(r.HardExpiresAt),
			IP: r.Ip, UserAgent: r.UserAgent,
		})
	}
	return out, nil
}

// The End* methods set ended_at and keep the row; DeleteExpiredAuth removes it later.

func (s *Store) EndSession(ctx context.Context, userID, sessionID uuid.UUID) error {
	return s.q.EndUserSession(ctx, sqlc.EndUserSessionParams{UserID: userID, ID: sessionID})
}

func (s *Store) EndOtherSessions(ctx context.Context, userID, keep uuid.UUID) error {
	return s.q.EndOtherUserSessions(ctx, sqlc.EndOtherUserSessionsParams{UserID: userID, ID: keep})
}

func (s *Store) EndAllSessions(ctx context.Context, userID uuid.UUID) error {
	return s.q.EndAllUserSessions(ctx, userID)
}

// EndOldestAppSessions ends the oldest live app sessions so that at most keep remain.
func (s *Store) EndOldestAppSessions(ctx context.Context, userID uuid.UUID, keep int) error {
	return s.q.EndOldestAppSessions(ctx, sqlc.EndOldestAppSessionsParams{UserID: userID, Keep: int32(keep)})
}

func (s *Store) CreateToken(ctx context.Context, userID uuid.UUID, kind users.TokenKind, hash []byte, data []byte, expires time.Time) error {
	return s.q.CreateUserToken(ctx, sqlc.CreateUserTokenParams{
		TokenHash: hash, UserID: userID, Kind: string(kind), Data: json.RawMessage(data), ExpiresAt: ts(expires),
	})
}

// ConsumeToken marks a live token of the kind used and returns its user and data. It reports users.ErrNotFound for
// an unknown, expired, already used or wrong-kind token; two concurrent calls cannot both succeed.
func (s *Store) ConsumeToken(ctx context.Context, kind users.TokenKind, hash []byte, now time.Time) (uuid.UUID, []byte, error) {
	r, err := s.q.ConsumeUserToken(ctx, sqlc.ConsumeUserTokenParams{TokenHash: hash, Kind: string(kind), Now: ts(now)})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil, users.ErrNotFound
	}
	if err != nil {
		return uuid.Nil, nil, err
	}
	return r.UserID, r.Data, nil
}

// PeekToken returns the user a live token of the kind belongs to without using it, or users.ErrNotFound.
func (s *Store) PeekToken(ctx context.Context, kind users.TokenKind, hash []byte, now time.Time) (uuid.UUID, error) {
	id, err := s.q.PeekUserToken(ctx, sqlc.PeekUserTokenParams{TokenHash: hash, Kind: string(kind), Now: ts(now)})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, users.ErrNotFound
	}
	return id, err
}

// LatestToken returns the data of the user's newest unused token of the kind, expired or not, or users.ErrNotFound.
func (s *Store) LatestToken(ctx context.Context, userID uuid.UUID, kind users.TokenKind) ([]byte, error) {
	d, err := s.q.LatestUserToken(ctx, sqlc.LatestUserTokenParams{UserID: userID, Kind: string(kind)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, users.ErrNotFound
	}
	return d, err
}

func (s *Store) DeleteUserTokens(ctx context.Context, userID uuid.UUID, kind users.TokenKind) error {
	return s.q.DeleteUserTokens(ctx, sqlc.DeleteUserTokensParams{UserID: userID, Kind: string(kind)})
}

// DeleteExpiredAuth deletes sessions and tokens that ended, expired or were used more than a week before now, and
// sign-ups older than that which nobody confirmed and which have no live verify link.
func (s *Store) DeleteExpiredAuth(ctx context.Context, now time.Time) error {
	cutoff := ts(now.Add(-authRetention))
	if err := s.q.DeleteExpiredUserSessions(ctx, cutoff); err != nil {
		return err
	}
	if err := s.q.DeleteExpiredUserTokens(ctx, cutoff); err != nil {
		return err
	}
	if err := s.q.DeleteExpiredPortalStates(ctx, ts(now)); err != nil {
		return err
	}
	return s.q.DeleteStaleUnverifiedUsers(ctx, sqlc.DeleteStaleUnverifiedUsersParams{Cutoff: cutoff, Now: ts(now)})
}

// deleteGapHook runs in both deletes after the user lock; tests use it to land a change in the gap.
var deleteGapHook func()
