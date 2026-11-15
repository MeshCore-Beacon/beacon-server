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

// RequestedStore is what requested targets need besides the election: the password in use and the claim-driven
// state changes, which run through MutateRequestTarget so they are judged under the target's lock.
type RequestedStore interface {
	RequestedPasswords(ctx context.Context, targets [][]byte) (map[string]RequestedPassword, error)
	MutateRequestTarget(ctx context.Context, target []byte, userID uuid.UUID, fn func(*TargetState) error) error
	// RecordServedVersions stores the password version (keyed by target) the observer was just answered with.
	RecordServedVersions(ctx context.Context, observer []byte, vers map[string]int64, at time.Time) error
}

// handleRequestedStates applies what a stored claim says about the requested targets it holds: the poller's
// login_failed, an admin_stop from any observer handed the target, and any other state (which ends the login_failed
// grace). A report from an observer that was not handed the target is ignored and only counted in a debug line.
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
		accepted := false
		err := s.reqs.MutateRequestTarget(ctx, n.Target, uuid.Nil, func(st *TargetState) error {
			accepted = st.ApplyReport(c.Observer, n.State, now)
			return nil
		})
		switch {
		case err != nil:
			s.log.Warn("requested target state", "observer", obs, "target", short(n.Target), "err", err)
		case !accepted:
			s.log.Debug("requested target report ignored", "observer", obs, "target", short(n.Target))
		}
	}
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
	served := map[string]int64{}
	for _, t := range targets {
		p, ok := pws[string(t)]
		if !ok {
			drop(t) // no longer requested
			continue
		}
		served[string(t)] = p.Ver
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
	// A report from this observer is judged against what it was just given. Without the record the entries could
	// not be told apart later, so none of them is sent.
	if err := s.reqs.RecordServedVersions(ctx, observer, served, s.now()); err != nil {
		s.log.Warn("served versions", "observer", short(observer), "err", err)
		for t := range served {
			drop([]byte(t))
		}
	}
}
