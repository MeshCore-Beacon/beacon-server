// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// RequestedPassword is the sealed password in use for a requested target. CT is nil when the option in use is the
// blank or hello login.
type RequestedPassword struct {
	RequestID uuid.UUID
	Ver       int64
	CT, Nonce []byte
}

// RequestedStore is what requested targets need besides the election: the password in use, the claim-driven state
// changes and the poller lookup.
type RequestedStore interface {
	RequestedPasswords(ctx context.Context, targets [][]byte) (map[string]RequestedPassword, error)
	MutateRequestTarget(ctx context.Context, target []byte, userID uuid.UUID, fn func(*TargetState) error) error
	// AddAdminStop reports false when the observer was already recorded for the target.
	AddAdminStop(ctx context.Context, target, observer []byte, at time.Time) (bool, error)
	TouchOtherState(ctx context.Context, target, observer []byte, at time.Time) error
	OtherStateSeen(ctx context.Context, target, observer []byte) (*time.Time, error)
	CurrentPoller(ctx context.Context, target []byte) ([]byte, error)
}

// handleRequestedStates applies what a stored claim says about the requested targets it holds: the poller's
// login_failed, any observer's admin_stop, and any other state (which ends the grace after a password change).
// Nothing here can fail the claim; a failure is logged and the next claim repeats the report.
func (s *Service) handleRequestedStates(ctx context.Context, c Claim, now time.Time) {
	if s.reqs == nil {
		return
	}
	obs := short(c.Observer)
	for _, n := range c.Nodes {
		if !n.Requested {
			continue
		}
		var err error
		switch n.State {
		case "admin_stop":
			err = s.requestedAdminStop(ctx, c.Observer, n.Target, now)
		case "login_failed":
			err = s.requestedLoginFailed(ctx, c.Observer, n.Target, now)
		default:
			err = s.reqs.TouchOtherState(ctx, n.Target, c.Observer, now)
		}
		if err != nil {
			s.log.Warn("requested target state", "observer", obs, "target", short(n.Target), "state", n.State, "err", err)
		}
	}
}

func (s *Service) requestedAdminStop(ctx context.Context, observer, target []byte, now time.Time) error {
	added, err := s.reqs.AddAdminStop(ctx, target, observer, now)
	// A repeat report must not strike the next password in use, which this observer never tried.
	if err != nil || !added {
		return err
	}
	return s.reqs.MutateRequestTarget(ctx, target, uuid.Nil, func(st *TargetState) error {
		st.AdminStop(now)
		return nil
	})
}

func (s *Service) requestedLoginFailed(ctx context.Context, observer, target []byte, now time.Time) error {
	cur, err := s.reqs.CurrentPoller(ctx, target)
	if err != nil || string(cur) != string(observer) {
		return err // only the poller's report counts
	}
	seen, err := s.reqs.OtherStateSeen(ctx, target, observer)
	if err != nil {
		return err
	}
	return s.reqs.MutateRequestTarget(ctx, target, uuid.Nil, func(st *TargetState) error {
		if LoginFailedCounts(now, st.VerChangedAt, seen) {
			st.LoginFailed(now)
		}
		return nil
	})
}

// addEnvelopes seals the password in use to each observer that is assigned a requested target. A fresh envelope is
// built on every answer. An entry that cannot get its envelope is left out: an entry without pw tells the device to
// try a blank or hello login, which a repeater may answer with admin access, and a requested target missing from
// an answer is dropped by the device and offered again on the next one.
func (s *Service) addEnvelopes(ctx context.Context, observer []byte, rows []OwnerRow, nodes map[string]OwnerEntry) {
	var targets [][]byte
	for _, r := range rows {
		if r.Requested && r.PWOK && r.Rank >= 0 && !r.Excluded {
			targets = append(targets, r.Target)
		}
	}
	if len(targets) == 0 {
		return
	}
	drop := func(t []byte) { delete(nodes, hex.EncodeToString(t)) }
	if s.reqs == nil || s.sealer == nil {
		for _, t := range targets {
			drop(t)
		}
		return
	}
	pws, err := s.reqs.RequestedPasswords(ctx, targets)
	if err != nil {
		s.log.Warn("requested passwords", "observer", short(observer), "err", err)
		for _, t := range targets {
			drop(t)
		}
		return
	}
	for _, t := range targets {
		p, ok := pws[string(t)]
		if !ok {
			drop(t) // no longer requested
			continue
		}
		if p.CT == nil {
			continue // the blank or hello option: no pw
		}
		plain, err := s.sealer.Open(p.RequestID, t, p.CT, p.Nonce)
		if err != nil {
			s.log.Warn("requested password does not open", "target", short(t))
			drop(t)
			continue
		}
		env, err := SealEnvelope(s.rnd, observer, t, uint32(p.Ver), plain)
		clear(plain)
		if err != nil {
			s.log.Warn("requested envelope", "observer", short(observer), "target", short(t), "err", err)
			drop(t)
			continue
		}
		key := hex.EncodeToString(t)
		e := nodes[key]
		e.PW = &env
		nodes[key] = e
	}
}
