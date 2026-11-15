// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func requestFromRow(r sqlc.ListTargetRequestsRow) polling.Request {
	return polling.Request{
		ID: r.ID, UserID: r.UserID, Status: polling.RequestStatus(r.Status), Reason: r.StatusReason,
		HasPassword: r.PasswordCt != nil, PasswordCT: r.PasswordCt, PasswordNonce: r.PasswordNonce,
		PasswordFailed: r.PasswordFailed, CreatedAt: r.CreatedAt.Time, UpdatedAt: r.UpdatedAt.Time, EnabledAt: r.EnabledAt.Time,
	}
}

func sameRequest(a, b polling.Request) bool {
	return a.Status == b.Status && a.Reason == b.Reason && a.PasswordFailed == b.PasswordFailed &&
		bytes.Equal(a.PasswordCT, b.PasswordCT) && bytes.Equal(a.PasswordNonce, b.PasswordNonce) &&
		a.UpdatedAt.Equal(b.UpdatedAt) && a.EnabledAt.Equal(b.EnabledAt)
}

// MutateRequestTarget loads one repeater's requests and counters under its election lock, runs fn and stores what
// fn changed, in one transaction. With a userID it also locks that user and fills UserRequests, so the per-user
// limit cannot be passed by two requests at once; a user that no longer exists is polling.ErrRequestNotFound.
func (s *Store) MutateRequestTarget(ctx context.Context, target []byte, userID uuid.UUID, fn func(*polling.TargetState) error) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		return mutateRequestTarget(ctx, q, target, userID, fn)
	})
}

func mutateRequestTarget(ctx context.Context, q *sqlc.Queries, target []byte, userID uuid.UUID, fn func(*polling.TargetState) error) error {
	if err := q.LockPollingTarget(ctx, target); err != nil {
		return err
	}
	st := &polling.TargetState{Target: target}
	if userID != uuid.Nil {
		if _, err := q.LockUserForRequests(ctx, userID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return polling.ErrRequestNotFound
			}
			return err
		}
		n, err := q.CountUserRequests(ctx, userID)
		if err != nil {
			return err
		}
		st.UserRequests = int(n)
	}
	if err := q.EnsurePollingTarget(ctx, target); err != nil {
		return err
	}
	tr, err := q.GetPollingTarget(ctx, target)
	if err != nil {
		return err
	}
	if tr.ActiveRequestID.Valid {
		st.ActiveID = tr.ActiveRequestID.Bytes
	}
	st.Ver, st.NoPasswordBlocked = tr.Ver, tr.NoPasswordBlocked
	if tr.VerChangedAt.Valid {
		st.VerChangedAt = tr.VerChangedAt.Time
	}
	rows, err := q.ListTargetRequests(ctx, target)
	if err != nil {
		return err
	}
	orig := make(map[uuid.UUID]polling.Request, len(rows))
	for _, r := range rows {
		req := requestFromRow(r)
		orig[req.ID] = req
		st.Requests = append(st.Requests, req)
	}
	before := *st

	if err := fn(st); err != nil {
		return err
	}

	for _, id := range st.Deleted {
		if err := q.DeleteTelemetryRequest(ctx, id); err != nil {
			return err
		}
	}
	for _, r := range st.Requests {
		o, existed := orig[r.ID]
		switch {
		case !existed:
			err = q.InsertTelemetryRequest(ctx, sqlc.InsertTelemetryRequestParams{
				ID: r.ID, UserID: r.UserID, TargetPublicKey: target, Status: string(r.Status), StatusReason: r.Reason,
				PasswordCt: r.PasswordCT, PasswordNonce: r.PasswordNonce, PasswordFailed: r.PasswordFailed,
				CreatedAt: ts(r.CreatedAt), UpdatedAt: ts(r.UpdatedAt), EnabledAt: ts(r.EnabledAt),
			})
		case !sameRequest(o, r):
			err = q.UpdateTelemetryRequest(ctx, sqlc.UpdateTelemetryRequestParams{
				ID: r.ID, Status: string(r.Status), StatusReason: r.Reason, PasswordCt: r.PasswordCT, PasswordNonce: r.PasswordNonce,
				PasswordFailed: r.PasswordFailed, UpdatedAt: ts(r.UpdatedAt), EnabledAt: ts(r.EnabledAt),
			})
		}
		if err != nil {
			return err
		}
	}
	if st.ActiveID != before.ActiveID || st.Ver != before.Ver || st.NoPasswordBlocked != before.NoPasswordBlocked ||
		!st.VerChangedAt.Equal(before.VerChangedAt) {
		active := pgtype.UUID{}
		if st.ActiveID != uuid.Nil {
			active = pgtype.UUID{Bytes: st.ActiveID, Valid: true}
		}
		verAt := pgtype.Timestamptz{}
		if !st.VerChangedAt.IsZero() {
			verAt = ts(st.VerChangedAt)
		}
		return q.UpdatePollingTarget(ctx, sqlc.UpdatePollingTargetParams{
			TargetPublicKey: target, ActiveRequestID: active, Ver: st.Ver, VerChangedAt: verAt, NoPasswordBlocked: st.NoPasswordBlocked,
		})
	}
	return nil
}

// RequestNode returns the node a request names; an unknown id is polling.ErrRequestNotFound.
func (s *Store) RequestNode(ctx context.Context, nodeID uuid.UUID) (polling.RequestNode, error) {
	r, err := s.q.RequestNode(ctx, nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return polling.RequestNode{}, polling.ErrRequestNotFound
	}
	if err != nil {
		return polling.RequestNode{}, err
	}
	return polling.RequestNode{PublicKey: r.PublicKey, NodeType: r.NodeType, Name: r.Name}, nil
}

// UserRequestTarget returns the repeater of the user's request; anyone else's is polling.ErrRequestNotFound.
func (s *Store) UserRequestTarget(ctx context.Context, userID, requestID uuid.UUID) ([]byte, error) {
	t, err := s.q.TelemetryRequestTarget(ctx, sqlc.TelemetryRequestTargetParams{ID: requestID, UserID: userID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, polling.ErrRequestNotFound
	}
	return t, err
}

// UserTelemetryRequests lists the user's requests, oldest first. No password is read.
func (s *Store) UserTelemetryRequests(ctx context.Context, userID uuid.UUID) ([]polling.RequestRow, error) {
	rows, err := s.q.ListUserTelemetryRequests(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]polling.RequestRow, 0, len(rows))
	for _, r := range rows {
		row := polling.RequestRow{
			ID: r.ID, Target: r.TargetPublicKey, Status: polling.RequestStatus(r.Status), Reason: r.StatusReason,
			HasPassword: r.HasPassword, CreatedAt: r.CreatedAt.Time, UpdatedAt: r.UpdatedAt.Time,
			NodeName: r.NodeName, PollerName: r.PollerName, LatestAt: timePtr(r.LatestReadingAt),
		}
		if r.NodeID.Valid {
			id := uuid.UUID(r.NodeID.Bytes)
			row.NodeID = &id
		}
		if r.PollerID != uuid.Nil {
			id := r.PollerID
			row.PollerID = &id
		}
		out = append(out, row)
	}
	return out, nil
}

var _ polling.RequestStore = (*Store)(nil)

// removeUserRequests deletes every request of the user through the state rules, so a repeater another user also
// requested falls back to that user's option (and ver rises) instead of being left with no password in use.
func (s *Store) removeUserRequests(ctx context.Context, id uuid.UUID) error {
	targets, err := s.q.UserRequestTargets(ctx, id)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, t := range targets {
		err := s.MutateRequestTarget(ctx, t, id, func(st *polling.TargetState) error {
			for _, r := range append([]polling.Request(nil), st.Requests...) {
				if r.UserID == id {
					st.Remove(r.ID, now)
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, polling.ErrRequestNotFound) {
			return err
		}
	}
	return nil
}

// RequestedPasswords returns the sealed password in use for each of the targets that is requested. A target that is
// not requested is absent.
func (s *Store) RequestedPasswords(ctx context.Context, targets [][]byte) (map[string]polling.RequestedPassword, error) {
	rows, err := s.q.RequestedPasswords(ctx, targets)
	if err != nil {
		return nil, err
	}
	out := make(map[string]polling.RequestedPassword, len(rows))
	for _, r := range rows {
		out[string(r.TargetPublicKey)] = polling.RequestedPassword{RequestID: r.RequestID, Ver: r.Ver, CT: r.PasswordCt, Nonce: r.PasswordNonce}
	}
	return out, nil
}

// AddAdminStop records that the observer's device was given admin access by the target. It reports false when the
// observer was already recorded.
func (s *Store) AddAdminStop(ctx context.Context, target, observer []byte, at time.Time) (bool, error) {
	n, err := s.q.InsertPollingAdminStop(ctx, sqlc.InsertPollingAdminStopParams{
		TargetPublicKey: target, ObserverPublicKey: observer, ReportedAt: ts(at)})
	return n > 0, err
}

// TouchOtherState records that the observer's claim showed a state other than login_failed for the target.
func (s *Store) TouchOtherState(ctx context.Context, target, observer []byte, at time.Time) error {
	return s.q.TouchPollingOtherState(ctx, sqlc.TouchPollingOtherStateParams{
		TargetPublicKey: target, ObserverPublicKey: observer, SeenAt: ts(at)})
}

// OtherStateSeen is nil when no such state was ever recorded.
func (s *Store) OtherStateSeen(ctx context.Context, target, observer []byte) (*time.Time, error) {
	t, err := s.q.PollingOtherStateSeen(ctx, sqlc.PollingOtherStateSeenParams{TargetPublicKey: target, ObserverPublicKey: observer})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t.Time, nil
}

// DisableOfflineRequests disables the requests of every repeater no observer has heard since cutoff (and whose
// newest request was enabled before it), with the reason. It returns how many repeaters it changed.
func (s *Store) DisableOfflineRequests(ctx context.Context, cutoff, now time.Time, reason string) (int, error) {
	targets, err := s.q.OfflineRequestTargets(ctx, ts(cutoff))
	if err != nil {
		return 0, err
	}
	for _, t := range targets {
		err := s.MutateRequestTarget(ctx, t, uuid.Nil, func(st *polling.TargetState) error {
			st.Disable(reason, now)
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return len(targets), nil
}

// ListPollingAdminStops lists every recorded admin stop, newest first.
func (s *Store) ListPollingAdminStops(ctx context.Context) ([]api.PollingAdminStop, error) {
	rows, err := s.q.ListPollingAdminStops(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]api.PollingAdminStop, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.PollingAdminStop{TargetPublicKey: hex.EncodeToString(r.TargetPublicKey),
			ObserverPublicKey: hex.EncodeToString(r.ObserverPublicKey), ReportedAt: r.ReportedAt.Time.UnixMilli()})
	}
	return out, nil
}

// ListNoPasswordBlocked lists the repeaters that grant admin to a blank login.
func (s *Store) ListNoPasswordBlocked(ctx context.Context) ([]string, error) {
	keys, err := s.q.ListNoPasswordBlocked(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, hex.EncodeToString(k))
	}
	return out, nil
}

// DeletePollingAdminStop clears one stop; it reports false when there was none.
func (s *Store) DeletePollingAdminStop(ctx context.Context, target, observer []byte) (bool, error) {
	n, err := s.q.DeletePollingAdminStop(ctx, sqlc.DeletePollingAdminStopParams{TargetPublicKey: target, ObserverPublicKey: observer})
	return n > 0, err
}

// ClearNoPasswordBlocked lifts the blank-login block on a repeater; it reports false for an unknown repeater.
func (s *Store) ClearNoPasswordBlocked(ctx context.Context, target []byte) (bool, error) {
	found := false
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := q.LockPollingTarget(ctx, target); err != nil {
			return err
		}
		var err error
		if found, err = q.PollingTargetExists(ctx, target); err != nil || !found {
			return err
		}
		_, err = q.ClearNoPasswordBlocked(ctx, target)
		return err
	})
	return found, err
}

var _ polling.RequestedStore = (*Store)(nil)
