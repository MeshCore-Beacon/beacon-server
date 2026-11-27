// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/jackc/pgx/v5"
)

const (
	maxPortalUsername = 30
	portalNameTries   = 50
)

var _ users.PortalStore = (*Store)(nil)

func (s *Store) CreatePortalState(ctx context.Context, cookieHash, stateHash []byte, verifier, returnPath string, expires time.Time) error {
	return s.q.CreatePortalState(ctx, sqlc.CreatePortalStateParams{
		CookieHash: cookieHash, StateHash: stateHash, Verifier: verifier, ReturnPath: returnPath, ExpiresAt: ts(expires)})
}

// ConsumePortalState deletes the state and returns it; users.ErrNotFound when there is none.
func (s *Store) ConsumePortalState(ctx context.Context, cookieHash []byte) (users.PortalState, error) {
	r, err := s.q.ConsumePortalState(ctx, cookieHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return users.PortalState{}, users.ErrNotFound
	}
	if err != nil {
		return users.PortalState{}, err
	}
	return users.PortalState{StateHash: r.StateHash, Verifier: r.Verifier, ReturnPath: r.ReturnPath, ExpiresAt: r.ExpiresAt.Time}, nil
}

// portalName is base made unique by attempt: base, base2, base3, ..., kept within the length limit.
func portalName(base string, attempt int) string {
	if attempt == 0 {
		return base
	}
	suffix := strconv.Itoa(attempt + 1)
	r := []rune(base)
	if max := maxPortalUsername - len(suffix); len(r) > max {
		r = r[:max]
	}
	return string(r) + suffix
}

// UpsertPortalUser finds the user of the portal subject, or creates one with no email and no password, and sets its
// username and display name. A username another user holds gets a numeric suffix.
func (s *Store) UpsertPortalUser(ctx context.Context, subject, username, displayName string) (users.User, error) {
	if utf8.RuneCountInString(username) > maxPortalUsername {
		username = string([]rune(username)[:maxPortalUsername])
	}
	for attempt := 0; attempt < portalNameTries; attempt++ {
		name := portalName(username, attempt)
		cur, err := s.q.UserByProviderSubject(ctx, sqlc.UserByProviderSubjectParams{Provider: users.ProviderPortal, ProviderSubject: &subject})
		switch {
		case err == nil:
			if cur.Username == name && cur.DisplayName == displayName {
				return userFromRow(cur), nil
			}
			row, err := s.q.RefreshPortalUser(ctx, sqlc.RefreshPortalUserParams{ID: cur.ID, Username: name, DisplayName: displayName})
			if errors.Is(err, pgx.ErrNoRows) {
				continue // deleted meanwhile
			}
			if err = userWriteErr(err); errors.Is(err, users.ErrUsernameTaken) {
				continue
			} else if err != nil {
				return users.User{}, err
			}
			return userFromRow(row), nil
		case errors.Is(err, pgx.ErrNoRows):
			u, err := s.CreateUser(ctx, users.User{Username: name, DisplayName: displayName, PassHash: users.NoPassword,
				Provider: users.ProviderPortal, ProviderSubject: &subject})
			if errors.Is(err, users.ErrUsernameTaken) {
				continue
			}
			if errors.Is(err, errSubjectTaken) {
				attempt-- // another sign-in created it; read it again
				continue
			}
			return u, err
		default:
			return users.User{}, err
		}
	}
	return users.User{}, errors.New("db: no free username for the portal user")
}
