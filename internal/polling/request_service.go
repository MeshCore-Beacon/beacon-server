// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Node types a request may name.
const (
	nodeTypeRepeater   = 2
	nodeTypeRoomServer = 3
)

// RequestNode is the node a request names.
type RequestNode struct {
	PublicKey []byte
	NodeType  int16
	Name      string
}

// RequestRow is a request as the profile shows it. It carries no password.
type RequestRow struct {
	ID          uuid.UUID
	Target      []byte
	Status      RequestStatus
	Reason      string
	HasPassword bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
	NodeID      *uuid.UUID // the node with that key now; nil when unknown
	NodeName    string
	PollerID    *uuid.UUID // the observer polling it; nil when nobody is
	PollerName  string
	// PollerState is what the poller's live claim says about this repeater, or StateNotClaimed. Empty with no poller.
	PollerState string
	// PollerStateAt is when that claim was received; nil when the poller has no live claim.
	PollerStateAt *time.Time
	LatestAt      *time.Time // newest trusted reading
}

// StateNotClaimed is the poller state when its live claim does not list the repeater, or it has no live claim.
const StateNotClaimed = "not_claimed"

// ClaimState turns a state a claim carried into the one a request shows: lower case, at most 32 bytes.
func ClaimState(s string) string { return strings.ToLower(shortText(s)) }

// RequestStore is what the requests service needs from the database. MutateRequestTarget loads the target's state
// under its lock (and the user's, when userID is set, with UserRequests filled in), runs fn and stores the
// difference; an error from fn stores nothing.
type RequestStore interface {
	RequestNode(ctx context.Context, nodeID uuid.UUID) (RequestNode, error)
	MutateRequestTarget(ctx context.Context, target []byte, userID uuid.UUID, fn func(*TargetState) error) error
	// UserRequestTarget is ErrRequestNotFound unless the request is the user's.
	UserRequestTarget(ctx context.Context, userID, requestID uuid.UUID) ([]byte, error)
	// UserTelemetryRequests lists the user's requests. A claim received after liveAfter is a live claim.
	UserTelemetryRequests(ctx context.Context, userID uuid.UUID, liveAfter time.Time) ([]RequestRow, error)
}

// Elector re-assigns a requested repeater after its requests changed.
type Elector interface {
	ElectTarget(ctx context.Context, target []byte) error
}

// RequestService is the user-facing side of telemetry requests.
type RequestService struct {
	store   RequestStore
	sealer  *Sealer
	elector Elector // may be nil: the next election pass picks the change up
	limit   int
	now     func() time.Time
	log     *slog.Logger
}

func NewRequestService(store RequestStore, sealer *Sealer, elector Elector, limit int, now func() time.Time, log *slog.Logger) *RequestService {
	return &RequestService{store: store, sealer: sealer, elector: elector, limit: limit, now: now, log: log}
}

// Limit is how many requests one user may hold.
func (s *RequestService) Limit() int { return s.limit }

func (s *RequestService) List(ctx context.Context, userID uuid.UUID) ([]RequestRow, error) {
	return s.store.UserTelemetryRequests(ctx, userID, s.now().Add(-ClaimTTL))
}

func (s *RequestService) one(ctx context.Context, userID, id uuid.UUID) (RequestRow, error) {
	rows, err := s.store.UserTelemetryRequests(ctx, userID, s.now().Add(-ClaimTTL))
	if err != nil {
		return RequestRow{}, err
	}
	for _, r := range rows {
		if r.ID == id {
			return r, nil
		}
	}
	return RequestRow{}, ErrRequestNotFound
}

// elect assigns the target after a change. A failure is logged and left to the next election pass: the request is
// stored either way.
func (s *RequestService) elect(ctx context.Context, target []byte) {
	if s.elector == nil {
		return
	}
	if err := s.elector.ElectTarget(ctx, target); err != nil {
		s.log.Warn("telemetry request election", "target", short(target), "err", err)
	}
}

// Create adds the user's request for a repeater or room server. A nil password means the blank or hello login.
func (s *RequestService) Create(ctx context.Context, userID, nodeID uuid.UUID, password []byte) (RequestRow, error) {
	if password != nil && !ValidGuestPassword(password) {
		return RequestRow{}, ErrPasswordInvalid
	}
	node, err := s.store.RequestNode(ctx, nodeID)
	if err != nil {
		return RequestRow{}, err
	}
	if node.NodeType != nodeTypeRepeater && node.NodeType != nodeTypeRoomServer {
		return RequestRow{}, ErrNotARepeater
	}
	now := s.now()
	r := Request{ID: uuid.New(), UserID: userID, CreatedAt: now, EnabledAt: now}
	if password != nil {
		if r.PasswordCT, r.PasswordNonce, err = s.sealer.Seal(r.ID, node.PublicKey, password); err != nil {
			return RequestRow{}, err
		}
		r.HasPassword = true
	}
	err = s.store.MutateRequestTarget(ctx, node.PublicKey, userID, func(st *TargetState) error {
		// The same repeater twice is the clearer answer when the user is also at the limit.
		for _, x := range st.Requests {
			if x.UserID == userID {
				return ErrAlreadyRequested
			}
		}
		if st.UserRequests >= s.limit {
			return ErrRequestLimit
		}
		return st.Create(r, now)
	})
	if err != nil {
		return RequestRow{}, err
	}
	s.elect(ctx, node.PublicKey)
	return s.one(ctx, userID, r.ID)
}

// RequestUpdate is a change to one request. SetPassword with a nil Password removes the password.
type RequestUpdate struct {
	SetPassword bool
	Password    []byte
	Enable      bool
}

// Update changes the user's own request.
func (s *RequestService) Update(ctx context.Context, userID, id uuid.UUID, u RequestUpdate) (RequestRow, error) {
	if u.SetPassword && u.Password != nil && !ValidGuestPassword(u.Password) {
		return RequestRow{}, ErrPasswordInvalid
	}
	target, err := s.store.UserRequestTarget(ctx, userID, id)
	if err != nil {
		return RequestRow{}, err
	}
	now := s.now()
	err = s.store.MutateRequestTarget(ctx, target, userID, func(st *TargetState) error {
		r := st.find(id)
		if r == nil || r.UserID != userID {
			return ErrRequestNotFound
		}
		if u.SetPassword {
			var ct, nonce []byte
			if u.Password != nil {
				if ct, nonce, err = s.sealer.Seal(id, target, u.Password); err != nil {
					return err
				}
			}
			if err := st.SetPassword(id, ct, nonce, now); err != nil {
				return err
			}
		}
		if u.Enable {
			return st.Enable(id, now)
		}
		return nil
	})
	if err != nil {
		return RequestRow{}, err
	}
	s.elect(ctx, target)
	return s.one(ctx, userID, id)
}

// Delete removes the user's own request and its password.
func (s *RequestService) Delete(ctx context.Context, userID, id uuid.UUID) error {
	target, err := s.store.UserRequestTarget(ctx, userID, id)
	if err != nil {
		return err
	}
	err = s.store.MutateRequestTarget(ctx, target, userID, func(st *TargetState) error {
		if r := st.find(id); r == nil || r.UserID != userID || !st.Remove(id, s.now()) {
			return ErrRequestNotFound
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.elect(ctx, target)
	return nil
}
