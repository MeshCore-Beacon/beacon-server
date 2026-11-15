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
)

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
	return affected(s.q.ApplyPendingEmail(ctx, sqlc.ApplyPendingEmailParams{ID: id, PendingEmail: &e}))
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

// DeleteUser relies on the ON DELETE CASCADE of every table that belongs to a user.
func (s *Store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	return affected(s.q.DeleteUser(ctx, id))
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
	row, err := s.q.CreateUserSession(ctx, sqlc.CreateUserSessionParams{
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

// LatestToken returns the data of the user's newest live token of the kind, or users.ErrNotFound.
func (s *Store) LatestToken(ctx context.Context, userID uuid.UUID, kind users.TokenKind, now time.Time) ([]byte, error) {
	d, err := s.q.LatestUserToken(ctx, sqlc.LatestUserTokenParams{UserID: userID, Kind: string(kind), Now: ts(now)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, users.ErrNotFound
	}
	return d, err
}

func (s *Store) DeleteUserTokens(ctx context.Context, userID uuid.UUID, kind users.TokenKind) error {
	return s.q.DeleteUserTokens(ctx, sqlc.DeleteUserTokensParams{UserID: userID, Kind: string(kind)})
}

// DeleteExpiredAuth deletes sessions and tokens that ended, expired or were used more than a week before now.
func (s *Store) DeleteExpiredAuth(ctx context.Context, now time.Time) error {
	cutoff := ts(now.Add(-authRetention))
	if err := s.q.DeleteExpiredUserSessions(ctx, cutoff); err != nil {
		return err
	}
	return s.q.DeleteExpiredUserTokens(ctx, cutoff)
}
