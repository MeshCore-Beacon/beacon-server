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

// RequestedStore is what requested targets need besides the election: the claim-driven state changes, which are
// judged under the target's lock. The password in use arrives on the owner rows.
type RequestedStore interface {
	MutateRequestTarget(ctx context.Context, target []byte, userID uuid.UUID, fn func(*TargetState) error) error
	// ReplacePollingClaimReporting stores the claim and, in the same transaction, runs report on the state of each
	// target it names as requested. Nothing is stored when a report cannot be.
	ReplacePollingClaimReporting(ctx context.Context, c Claim, observerID uuid.UUID, receivedAt time.Time,
		report func(ClaimNode, *TargetState)) (bool, error)
	// RecordServedVersions stores the password version (keyed by target) the observer was just answered with.
	RecordServedVersions(ctx context.Context, observer []byte, vers map[string]int64, at time.Time) error
}

// claimReport applies what a claim says about a requested target it holds: the poller's login_failed, an admin_stop
// from any observer handed the target, and any other state (which ends the login_failed grace). A report from an
// observer that was not handed the target is ignored and added to ignored.
func claimReport(c Claim, now time.Time, fresh time.Duration, ignored *[][]byte) func(ClaimNode, *TargetState) {
	*ignored = nil
	return func(n ClaimNode, st *TargetState) {
		st.WorkingPollFor = fresh
		if !st.ApplyReport(c.Observer, n.State, now) {
			*ignored = append(*ignored, n.Target)
		}
	}
}

// addEnvelopes seals the password in use, read with the rows, to each observer that is assigned a requested target.
// A fresh envelope is built on every answer. An entry that cannot get its envelope is left out: an entry without pw tells the device to
// try a blank or hello login, which a repeater may answer with admin access, and a requested target missing from
// an answer is dropped by the device and offered again on the next one.
func (s *Service) addEnvelopes(ctx context.Context, observer []byte, rows []OwnerRow, nodes map[string]OwnerEntry) error {
	var given []OwnerRow
	for _, r := range rows {
		if r.Requested && r.PWOK && r.Rank >= 0 && !r.Excluded {
			given = append(given, r)
		}
	}
	if len(given) == 0 {
		return nil
	}
	drop := func(t []byte) { delete(nodes, hex.EncodeToString(t)) }
	if s.reqs == nil || s.sealer == nil {
		for _, r := range given {
			drop(r.Target)
		}
		return nil
	}
	served := map[string]int64{}
	for _, r := range given {
		t, p := r.Target, r.Password
		if p == nil {
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
	// not be told apart later; leaving them out would make the device drop them, so the answer fails (503) instead.
	if err := s.reqs.RecordServedVersions(ctx, observer, served, s.now()); err != nil {
		s.log.Warn("served versions", "observer", short(observer), "err", err)
		return err
	}
	return nil
}
