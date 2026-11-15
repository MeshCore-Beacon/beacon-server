// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Lock order, for every transaction that takes more than one of these locks:
//  1. observer seat locks (LockPollingSeats), in observer key order;
//  2. target locks (LockPollingTarget), in target key order;
//  3. the user's row lock (LockUserForRequests / LockUserCredentials).
//
// A path that finds it needs a lock it does not hold rolls back and starts again with the whole sorted set. Seat
// locks: requested elections (every accepting observer) and claims (their own). Target locks come before any write
// to the target's assignments or requests. Reading-insert locks (LockPollingObserver) are taken alone.
//
// After every committed transaction a target's ranks are 0..n-1, a waiting or active request is active exactly when
// its target has an assignment, and local assignments use no requested seat. Only the election that follows restores
// the capacity limit after a claim lowers it, and a usable rank 0 after its observer is blocked or unapproved.

// ObserverID returns the observer's row id, creating the row when it is new.
func (s *Store) ObserverID(ctx context.Context, pubkey []byte) (uuid.UUID, error) {
	id, _, err := s.UpsertObserver(ctx, pubkey, "")
	return id, err
}

// clipText cuts s to at most n bytes on a rune boundary and drops NULs, which text columns refuse.
func clipText(s string, n int) string {
	s = strings.ReplaceAll(s, "\x00", "")
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

func optTime(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return ts(*t)
}

func ptrConv[A, B any](p *A, f func(A) B) *B {
	if p == nil {
		return nil
	}
	v := f(*p)
	return &v
}

func (s *Store) pollTx(ctx context.Context, fn func(q *sqlc.Queries) error) error {
	if s.pool == nil {
		return errors.New("db: polling writes need the write pool")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(sqlc.New(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReplacePollingClaim stores the claim as the observer's whole candidate list. A claim that is not newer than the
// stored one, or arrives within polling.ClaimMinInterval of it, changes nothing and reports applied=false.
func (s *Store) ReplacePollingClaim(ctx context.Context, c polling.Claim, observerID uuid.UUID, receivedAt time.Time) (bool, error) {
	return s.ReplacePollingClaimReporting(ctx, c, observerID, receivedAt, nil)
}

// ReplacePollingClaimReporting is ReplacePollingClaim that also runs report, in the same transaction, on the state of
// each target the claim names as requested. If a report cannot be stored the claim is not stored either, so the
// device's next claim repeats it, and no election sees the new claim before its reports.
func (s *Store) ReplacePollingClaimReporting(ctx context.Context, c polling.Claim, observerID uuid.UUID, receivedAt time.Time,
	report func(polling.ClaimNode, *polling.TargetState)) (bool, error) {
	applied := false
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		applied = false
		// A claim can change the observer's capacity, so it waits for any election that decides on its seats.
		if err := q.LockPollingSeats(ctx, c.Observer); err != nil {
			return err
		}
		var reported []polling.ClaimNode
		if report != nil {
			for _, n := range c.Nodes {
				if n.Requested {
					reported = append(reported, n)
				}
			}
		}
		keys := make([][]byte, len(reported))
		for i, n := range reported {
			keys[i] = n.Target
		}
		if err := lockTargets(ctx, q, keys); err != nil {
			return err
		}
		n, err := q.UpsertPollingClaim(ctx, sqlc.UpsertPollingClaimParams{
			ObserverPublicKey: c.Observer, ObserverID: observerID,
			Origin: clipText(c.Origin, 128), Client: clipText(c.Client, 128), PolicyVersion: clipText(c.PolicyVersion, 32),
			Community: c.Community, AcceptsRequests: c.AcceptsRequests, RequestSlots: int16(c.RequestSlots),
			ClaimedAt: ts(c.Timestamp), ReceivedAt: ts(receivedAt),
			PreviousReceivedBy: ts(receivedAt.Add(-polling.ClaimMinInterval)),
		})
		if err != nil || n == 0 {
			return err
		}
		applied = true
		if err := q.DeletePollingCandidates(ctx, c.Observer); err != nil {
			return err
		}
		for _, n := range c.Nodes {
			if err := q.InsertPollingCandidate(ctx, sqlc.InsertPollingCandidateParams{
				ObserverPublicKey: c.Observer, TargetPublicKey: n.Target, KeyMode: n.KeyMode, ClaimedSnr: n.ClaimedSNR,
				Requested: n.Requested, State: n.State, Misses: int16(n.Misses),
				LastAttempt: optTime(n.LastAttempt), LastOk: optTime(n.LastOK),
			}); err != nil {
				return err
			}
		}
		for _, n := range reported {
			if err := mutateRequestTarget(ctx, q, n.Target, uuid.Nil, func(st *polling.TargetState) error {
				report(n, st)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return applied, err
}

// lockTargets takes the target locks in key order.
func lockTargets(ctx context.Context, q *sqlc.Queries, targets [][]byte) error {
	sorted := append([][]byte(nil), targets...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })
	for i, t := range sorted {
		if i > 0 && bytes.Equal(t, sorted[i-1]) {
			continue
		}
		if err := q.LockPollingTarget(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

// PollingClaimReceivedAt returns when Beacon received the observer's stored claim, nil when it has none.
func (s *Store) PollingClaimReceivedAt(ctx context.Context, observer []byte) (*time.Time, error) {
	t, err := s.q.PollingClaimReceivedAt(ctx, observer)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t.Time, nil
}

// InsertRepeaterReading stores one reading; inserted=false means it was already there, the observer stored one of
// the same target and method less than polling.ReadingMinInterval before receivedAt, or it is over its hourly budget.
func (s *Store) InsertRepeaterReading(ctx context.Context, r polling.Reading, observerID uuid.UUID, receivedAt time.Time) (bool, error) {
	inserted := false
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		var err error
		inserted, err = insertRepeaterReading(ctx, q, r, observerID, receivedAt)
		return err
	})
	return inserted, err
}

// insertRepeaterReading takes the observer's lock first, so concurrent inserts cannot both pass the checks.
func insertRepeaterReading(ctx context.Context, q *sqlc.Queries, r polling.Reading, observerID uuid.UUID, receivedAt time.Time) (bool, error) {
	if err := q.LockPollingObserver(ctx, r.Observer); err != nil {
		return false, err
	}
	tag := r.SampledAt.Unix()
	if r.Tag != nil {
		tag = int64(*r.Tag)
	}
	st := r.Stats
	i32 := func(v int) int32 { return int32(v) }
	f32 := func(v float64) float32 { return float32(v) }
	n, err := q.InsertRepeaterReading(ctx, sqlc.InsertRepeaterReadingParams{
		TargetPublicKey: r.Target, ObserverPublicKey: r.Observer, ObserverID: pgtype.UUID{Bytes: observerID, Valid: true},
		SampledAt: ts(r.SampledAt), ReceivedAt: ts(receivedAt),
		Method: r.Method, Tag: tag,
		BatteryMv: ptrConv(r.BatteryMV, i32), TemperatureC: ptrConv(r.TemperatureC, f32), PollSnr: ptrConv(r.PollSNR, f32),
		UptimeSecs: st.UptimeSecs, PacketsRecv: st.PacketsRecv, PacketsSent: st.PacketsSent,
		RecvFlood: st.RecvFlood, RecvDirect: st.RecvDirect, SentFlood: st.SentFlood, SentDirect: st.SentDirect,
		TxAirSecs: st.TxAirSecs, RxAirSecs: st.RxAirSecs, RecvErrors: st.RecvErrors,
		NoiseFloor: ptrConv(st.NoiseFloor, i32), LastRssi: ptrConv(st.LastRSSI, i32), TxQueue: ptrConv(st.TxQueue, i32),
		FloodDups: ptrConv(st.FloodDups, i32), DirectDups: ptrConv(st.DirectDups, i32), ErrEvents: ptrConv(st.ErrEvents, i32),
		LastSnr: ptrConv(st.LastSNR, f32),
		Lpp:     r.LPP, PacketHash: r.PacketHash, Client: clipText(r.Client, 128),
		RecentAfter: ts(receivedAt.Add(-polling.ReadingMinInterval)),
		BudgetAfter: ts(receivedAt.Add(-time.Hour)), Budget: polling.ReadingsPerObserverPerHour,
	})
	return n > 0, err
}

func (s *Store) HasLivePollingCandidate(ctx context.Context, observer, target []byte, receivedAfter time.Time) (bool, error) {
	return s.q.HasLivePollingCandidate(ctx, sqlc.HasLivePollingCandidateParams{
		ObserverPublicKey: observer, TargetPublicKey: target, ReceivedAt: ts(receivedAfter)})
}

// ListPollingTargets lists targets with a live candidate or any assignment row, so assignments of
// targets whose claims all expired are still reachable to be cleared.
func (s *Store) ListPollingTargets(ctx context.Context, receivedAfter time.Time) ([][]byte, error) {
	return s.q.ListPollingTargets(ctx, ts(receivedAfter))
}

func (s *Store) ListPollingCandidates(ctx context.Context, target []byte, receivedAfter, evidenceSince time.Time) ([]polling.Candidate, error) {
	return listPollingCandidates(ctx, s.q, target, receivedAfter, evidenceSince)
}

func listPollingCandidates(ctx context.Context, q sqlc.Querier, target []byte, receivedAfter, evidenceSince time.Time) ([]polling.Candidate, error) {
	rows, err := q.ListPollingCandidates(ctx, sqlc.ListPollingCandidatesParams{
		TargetPublicKey: target, ReceivedAt: ts(receivedAfter), FirstHeardAt: ts(evidenceSince)})
	if err != nil {
		return nil, err
	}
	out := make([]polling.Candidate, 0, len(rows))
	for _, r := range rows {
		c := polling.Candidate{Observer: r.ObserverPublicKey, EvidenceCount: int(r.EvidenceCount),
			ClaimedSNR: r.ClaimedSnr, State: r.State, Misses: int(r.Misses), Blocked: r.Blocked, Approved: r.Approved, Requested: r.Requested}
		if r.EvidenceCount > 0 {
			v := r.EvidenceSnr
			c.EvidenceSNR = &v
		}
		out = append(out, c)
	}
	return out, nil
}

// CurrentPoller returns the rank-0 observer key for the target, nil when there is none.
func (s *Store) CurrentPoller(ctx context.Context, target []byte) ([]byte, error) {
	return currentPoller(ctx, s.q, target)
}

func currentPoller(ctx context.Context, q sqlc.Querier, target []byte) ([]byte, error) {
	k, err := q.CurrentPollingPoller(ctx, target)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return k, err
}

// ReplacePollingAssignments swaps the target's whole order in one transaction under the target's election lock;
// nil or empty clears it.
func (s *Store) ReplacePollingAssignments(ctx context.Context, target []byte, a []polling.Assignment, at time.Time) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := q.LockPollingTarget(ctx, target); err != nil {
			return err
		}
		return replaceAssignments(ctx, q, target, a, at)
	})
}

func replaceAssignments(ctx context.Context, q *sqlc.Queries, target []byte, a []polling.Assignment, at time.Time) error {
	if err := q.DeletePollingAssignments(ctx, target); err != nil {
		return err
	}
	for _, x := range a {
		err := q.InsertPollingAssignment(ctx, sqlc.InsertPollingAssignmentParams{
			TargetPublicKey: target, ObserverPublicKey: x.Observer, Rank: int16(x.Rank),
			EvidenceSnr:   ptrConv(x.EvidenceSNR, func(v float64) float32 { return float32(v) }),
			EvidenceCount: int32(x.EvidenceCount), AssignedAt: ts(at), Requested: x.Requested,
		})
		if err != nil {
			return fmt.Errorf("assign %x: %w", x.Observer, err)
		}
	}
	return nil
}

// ElectPollingTarget locks the target, reads its candidates and current poller, and stores decide's order, all in
// one transaction, so two elections of one target cannot interleave. For a requested target it also reads the
// observers that could take the request, and moves the target's requests to active or waiting with the result.
func (s *Store) ElectPollingTarget(ctx context.Context, target []byte, receivedAfter, evidenceSince, at time.Time,
	decide func(in polling.ElectionInput) polling.Election) error {
	// A requested target shares observers' seats with other targets, so its election first takes every accepting
	// observer's seat lock. Whether it is requested is only known under the target lock, so a first pass that finds
	// it is, and holds no seat locks, is thrown away and run again with them. A candidate whose claim arrived after
	// the seat locks were listed is not locked either: that pass is thrown away too, a few times at most.
	seatLocks := false
	if tr, err := s.q.GetPollingTarget(ctx, target); err == nil && tr.ActiveRequestID.Valid {
		seatLocks = true
	}
	for attempt := 0; ; attempt++ {
		err := s.pollTx(ctx, func(q *sqlc.Queries) error {
			var locked map[string]bool
			if seatLocks {
				var err error
				if locked, err = lockSeatsAndTargets(ctx, q, target, receivedAfter); err != nil {
					return err
				}
			}
			return electTarget(ctx, q, target, locked, receivedAfter, evidenceSince, at, decide)
		})
		if errors.Is(err, errNeedSeatLocks) || errors.Is(err, errUnlockedObserver) {
			if attempt >= maxElectionRestarts {
				return err
			}
			seatLocks = true
			continue
		}
		return err
	}
}

// maxElectionRestarts bounds how often one election starts over because a claim arrived while it was locking.
const maxElectionRestarts = 5

var (
	errNeedSeatLocks    = errors.New("db: election needs the seat locks")
	errUnlockedObserver = errors.New("db: an observer arrived after the seat locks were taken")
)

// lockSeatsAndTargets takes the seat lock of every observer whose claim accepts requests, then the locks of the
// target and of every target holding a requested assignment of those observers, all in key order (see the lock
// order at the top of this file). It returns the observers it locked.
func lockSeatsAndTargets(ctx context.Context, q *sqlc.Queries, target []byte, receivedAfter time.Time) (map[string]bool, error) {
	keys, err := q.ListRequestObserverKeys(ctx, ts(receivedAfter))
	if err != nil {
		return nil, err
	}
	locked := make(map[string]bool, len(keys))
	for _, k := range keys {
		if err := q.LockPollingSeats(ctx, k); err != nil {
			return nil, err
		}
		locked[string(k)] = true
	}
	held, err := q.ListRequestedAssignedTargets(ctx, keys)
	if err != nil {
		return nil, err
	}
	return locked, lockTargets(ctx, q, append([][]byte{target}, held...))
}

func electTarget(ctx context.Context, q *sqlc.Queries, target []byte, locked map[string]bool, receivedAfter, evidenceSince, at time.Time,
	decide func(in polling.ElectionInput) polling.Election) error {
	if err := q.LockPollingTarget(ctx, target); err != nil {
		return err
	}
	in := polling.ElectionInput{}
	var err error
	if in.Cands, err = listPollingCandidates(ctx, q, target, receivedAfter, evidenceSince); err != nil {
		return err
	}
	if in.Current, err = currentPoller(ctx, q, target); err != nil {
		return err
	}
	tr, err := q.GetPollingTarget(ctx, target)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	in.Requested = err == nil && tr.ActiveRequestID.Valid
	if in.Requested && locked == nil {
		return errNeedSeatLocks
	}
	if in.Requested {
		if in.RequestCands, err = listRequestCandidates(ctx, q, target, receivedAfter, evidenceSince); err != nil {
			return err
		}
		for _, c := range in.RequestCands {
			if !locked[string(c.Observer)] {
				return errUnlockedObserver
			}
		}
	}
	el := decide(in)
	if err := replaceAssignments(ctx, q, target, el.Assignments, at); err != nil {
		return err
	}
	if !in.Requested {
		return nil
	}
	if err := mutateRequestTarget(ctx, q, target, uuid.Nil, func(st *polling.TargetState) error {
		st.SetAssigned(len(el.Assignments) > 0, el.WaitReason)
		return nil
	}); err != nil {
		return err
	}
	return preemptYoungerSeats(ctx, q, target, el.Assignments, in.RequestCands)
}

// preemptYoungerSeats takes back seats the new assignments need from younger requested targets of the same
// observers, so a seat that two elections both thought free ends up with the older request. A target left with no
// observer goes back to waiting. Runs under the seat locks, which order it against every other election.
func preemptYoungerSeats(ctx context.Context, q *sqlc.Queries, target []byte, chosen []polling.Assignment, cands []polling.Candidate) error {
	oldest, err := q.OldestTargetRequest(ctx, target)
	if err != nil || !oldest.Valid {
		return err
	}
	for _, a := range chosen {
		if !a.Requested {
			continue // a local poll takes no requested seat
		}
		var c *polling.Candidate
		for i := range cands {
			if bytes.Equal(cands[i].Observer, a.Observer) {
				c = &cands[i]
			}
		}
		if c == nil {
			continue
		}
		held, err := q.ListObserverRequestedSeats(ctx, sqlc.ListObserverRequestedSeatsParams{ObserverPublicKey: a.Observer, TargetPublicKey: target})
		if err != nil {
			return err
		}
		var older int
		var younger [][]byte
		for _, h := range held {
			if h.Oldest.Time.Before(oldest.Time) || (h.Oldest.Time.Equal(oldest.Time) && bytes.Compare(h.TargetPublicKey, target) < 0) {
				older++
			} else {
				younger = append(younger, h.TargetPublicKey)
			}
		}
		room := max(polling.RequestCapacity(*c)-1-older, 0)
		if len(younger) <= room {
			continue
		}
		for _, y := range younger[room:] {
			n, err := q.DeleteObserverTargetAssignment(ctx, sqlc.DeleteObserverTargetAssignmentParams{TargetPublicKey: y, ObserverPublicKey: a.Observer})
			if err != nil {
				return err
			}
			if n == 0 {
				continue
			}
			if err := q.CompactTargetAssignments(ctx, y); err != nil {
				return err
			}
			left, err := q.CountTargetAssignments(ctx, y)
			if err != nil {
				return err
			}
			if err := mutateRequestTarget(ctx, q, y, uuid.Nil, func(st *polling.TargetState) error {
				st.SetAssigned(left > 0, polling.ReasonNoSeat)
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func listRequestCandidates(ctx context.Context, q sqlc.Querier, target []byte, receivedAfter, evidenceSince time.Time) ([]polling.Candidate, error) {
	rows, err := q.ListRequestCandidates(ctx, sqlc.ListRequestCandidatesParams{
		TargetPublicKey: target, FirstHeardAt: ts(evidenceSince), ReceivedAfter: ts(receivedAfter)})
	if err != nil {
		return nil, err
	}
	out := make([]polling.Candidate, 0, len(rows))
	for _, r := range rows {
		ev := r.EvidenceSnr
		out = append(out, polling.Candidate{
			Observer: r.ObserverPublicKey, EvidenceSNR: &ev, EvidenceCount: int(r.EvidenceCount),
			ClaimedSNR: r.ClaimedSnr, State: r.State, Misses: int(r.Misses), Blocked: r.Blocked, Approved: r.Approved,
			Requested: r.Requested, RequestSlots: int(r.RequestSlots), Held: int(r.Held),
			AdminStopped: r.AdminStopped, SeatsElsewhere: int(r.SeatsElsewhere),
		})
	}
	return out, nil
}

func (s *Store) PollingOwnerRows(ctx context.Context, observer []byte, receivedAfter time.Time) ([]polling.OwnerRow, error) {
	rows, err := s.q.PollingOwnerRows(ctx, sqlc.PollingOwnerRowsParams{ObserverPublicKey: observer, ReceivedAt: ts(receivedAfter)})
	if err != nil {
		return nil, err
	}
	out := make([]polling.OwnerRow, 0, len(rows))
	for _, r := range rows {
		row := polling.OwnerRow{Target: r.TargetPublicKey, Rank: int(r.Rank), Assigned: int(r.Assigned),
			Requested: r.Requested, ClaimedRequested: r.ClaimedRequested,
			TargetRequested: r.TargetRequested, AcceptsRequests: r.AcceptsRequests, RequestSlots: int(r.RequestSlots),
			ObserverApproved: r.ObserverApproved, ObserverBlocked: r.ObserverBlocked}
		if r.RequestID.Valid && r.PasswordVer != nil {
			row.Password = &polling.RequestedPassword{RequestID: r.RequestID.Bytes, Ver: *r.PasswordVer, CT: r.PasswordCt, Nonce: r.PasswordNonce}
		}
		if r.LatestTs.Valid {
			t := r.LatestTs.Time
			row.LatestTS = &t
		}
		out = append(out, row)
	}
	return out, nil
}

// PollingObserverFlags reports false, false for an observer with no row.
func (s *Store) PollingObserverFlags(ctx context.Context, observer []byte) (blocked, approved bool, err error) {
	r, err := s.q.PollingObserverFlags(ctx, observer)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	return r.Blocked, r.Approved, err
}

func (s *Store) SetPollingObserver(ctx context.Context, observer []byte, blocked, approved bool, note string) error {
	return s.q.SetPollingObserver(ctx, sqlc.SetPollingObserverParams{ObserverPublicKey: observer, Blocked: blocked, Approved: approved, Note: note})
}

func (s *Store) PollingState(ctx context.Context) (paused bool, pausedUntil *time.Time, excluded [][]byte, err error) {
	r, err := s.q.GetPollingState(ctx)
	if err != nil {
		return false, nil, nil, err
	}
	if r.PausedUntil.Valid {
		t := r.PausedUntil.Time
		pausedUntil = &t
	}
	return r.Paused, pausedUntil, r.ExcludedNodes, nil
}

func (s *Store) SetPollingState(ctx context.Context, paused bool, pausedUntil *time.Time, excluded [][]byte) error {
	if excluded == nil {
		excluded = [][]byte{}
	}
	return s.q.SetPollingState(ctx, sqlc.SetPollingStateParams{Paused: paused, PausedUntil: optTime(pausedUntil), ExcludedNodes: excluded})
}

// CorroborateReadings recomputes, for readings received since receivedSince that carry a packet hash, how many
// other observers heard a matching reply packet near sampled_at. It returns how many readings it updated.
func (s *Store) CorroborateReadings(ctx context.Context, receivedSince time.Time, limit int) (int, error) {
	n, err := s.q.CorroborateReadings(ctx, sqlc.CorroborateReadingsParams{ReceivedAt: ts(receivedSince), Limit: int32(limit)})
	return int(n), err
}

// DeleteExpiredPolling drops expired claims (their candidates cascade), assignments whose candidate is gone,
// and readings received before readingsBefore. Expiry runs on receive time, not the observer's clock.
func (s *Store) DeleteExpiredPolling(ctx context.Context, claimsBefore, readingsBefore time.Time) error {
	if err := s.q.DeleteExpiredPollingClaims(ctx, ts(claimsBefore)); err != nil {
		return err
	}
	orphans, err := s.q.ListOrphanPollingTargets(ctx)
	if err != nil {
		return err
	}
	// One target at a time under its election lock, so cleanup and an election never delete in opposite orders. The
	// survivors are renumbered and the target's requests follow what is left, in the same transaction.
	for _, t := range orphans {
		if err := s.pollTx(ctx, func(q *sqlc.Queries) error {
			if err := q.LockPollingTarget(ctx, t); err != nil {
				return err
			}
			if err := q.DeleteOrphanPollingAssignments(ctx, t); err != nil {
				return err
			}
			if err := q.CompactTargetAssignments(ctx, t); err != nil {
				return err
			}
			requested, err := q.PollingTargetExists(ctx, t)
			if err != nil || !requested {
				return err
			}
			return mutateRequestTarget(ctx, q, t, uuid.Nil, func(*polling.TargetState) error { return nil })
		}); err != nil {
			return err
		}
	}
	if err := s.q.DeleteOldServedVersions(ctx, ts(claimsBefore)); err != nil {
		return err
	}
	return s.q.DeleteExpiredRepeaterReadings(ctx, ts(readingsBefore))
}

// ListPollingObservers returns every operator-set row, ordered by key.
func (s *Store) ListPollingObservers(ctx context.Context) ([]api.PollingObserver, error) {
	rows, err := s.q.ListPollingObservers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]api.PollingObserver, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.PollingObserver{PublicKey: hex.EncodeToString(r.ObserverPublicKey), Blocked: r.Blocked, Approved: r.Approved, Note: r.Note})
	}
	return out, nil
}

// PollingTargetsAdmin lists up to limit targets with a live candidate or an assignment, newest claim first,
// each with its candidates and assigned observers.
func (s *Store) PollingTargetsAdmin(ctx context.Context, receivedAfter time.Time, limit int) ([]api.PollingTarget, error) {
	ts0 := ts(receivedAfter)
	trows, err := s.q.PollingAdminTargets(ctx, sqlc.PollingAdminTargetsParams{ReceivedAfter: ts0, RowLimit: int32(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]api.PollingTarget, len(trows))
	keys := make([][]byte, len(trows))
	index := make(map[string]int, len(trows))
	for i, r := range trows {
		keys[i] = r.TargetPublicKey
		index[string(r.TargetPublicKey)] = i
		out[i] = api.PollingTarget{PublicKey: hex.EncodeToString(r.TargetPublicKey), Candidates: []api.PollingTargetCandidate{},
			LatestTrustedReading: epochMs(r.LatestTrusted), LatestReading: epochMs(r.LatestAny)}
	}
	if len(keys) == 0 {
		return out, nil
	}
	counts, err := s.q.PollingAdminRequestCounts(ctx, keys)
	if err != nil {
		return nil, err
	}
	for _, r := range counts {
		if i, ok := index[string(r.TargetPublicKey)]; ok {
			out[i].Requested, out[i].Requests = r.Requested, int(r.Requests)
		}
	}
	crows, err := s.q.PollingAdminTargetRows(ctx, sqlc.PollingAdminTargetRowsParams{Targets: keys, ReceivedAfter: ts0})
	if err != nil {
		return nil, err
	}
	for _, r := range crows {
		i, ok := index[string(r.TargetPublicKey)]
		if !ok {
			continue
		}
		c := api.PollingTargetCandidate{ObserverPublicKey: hex.EncodeToString(r.ObserverPublicKey),
			EvidenceSnr: ptrConv(r.EvidenceSnr, func(v float32) float64 { return float64(v) }), EvidenceCount: int(r.EvidenceCount),
			KeyMode: r.KeyMode, State: r.State, Misses: int(r.Misses), LastOk: epochMs(r.LastOk), Approved: r.Approved}
		if r.Rank >= 0 {
			rank := int(r.Rank)
			c.Rank = &rank
		}
		out[i].Candidates = append(out[i].Candidates, c)
	}
	return out, nil
}

func epochMs(t pgtype.Timestamptz) *int64 {
	if !t.Valid {
		return nil
	}
	ms := t.Time.UnixMilli()
	return &ms
}

// PollingElectionLock holds the election advisory lock on one dedicated connection.
type PollingElectionLock struct {
	conn *pgxpool.Conn
	q    *sqlc.Queries
}

// BeginPollingElection takes the election lock. ok is false when another process holds it.
func (s *Store) BeginPollingElection(ctx context.Context) (*PollingElectionLock, bool, error) {
	if s.pool == nil {
		return nil, false, errors.New("polling: store has no connection pool")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	q := sqlc.New(conn)
	ok, err := q.TryPollingElectionLock(ctx)
	if err != nil || !ok {
		conn.Release()
		return nil, false, err
	}
	return &PollingElectionLock{conn: conn, q: q}, true, nil
}

// Close releases the lock; if unlocking fails the connection is closed so the lock can't leak.
func (l *PollingElectionLock) Close(ctx context.Context) {
	if err := l.q.PollingElectionUnlock(ctx); err != nil {
		_ = l.conn.Hijack().Close(context.Background())
		return
	}
	l.conn.Release()
}

func nodeReading(r sqlc.ListNodeReadingsRow) api.NodeReading {
	return api.NodeReading{
		SampledAt: r.SampledAt.Time, Method: r.Method, Trusted: r.Trusted,
		BatteryMV: r.BatteryMv, TemperatureC: r.TemperatureC, NoiseFloorDB: r.NoiseFloor, LastRSSIDB: r.LastRssi,
		LastSNRDB: r.LastSnr, UptimeSeconds: r.UptimeSecs, QueueLength: r.TxQueue,
		AirtimeTxSecs: r.TxAirSecs, AirtimeRxSecs: r.RxAirSecs, PacketsReceived: r.PacketsRecv,
		PacketsSent: r.PacketsSent, ReceiveErrors: r.RecvErrors,
	}
}

// GetNodeReadings returns the node's readings in [since, until) ascending, preceded by the newest status reading
// before since as a counter baseline. until is open when zero. Untrusted readings are left out unless asked for and
// counted instead. An unknown node returns pgx.ErrNoRows.
func (s *Store) GetNodeReadings(ctx context.Context, nodeID uuid.UUID, since, until time.Time, includeUntrusted bool) ([]api.NodeReading, bool, int, error) {
	key, err := s.q.NodePublicKey(ctx, nodeID)
	if err != nil {
		return nil, false, 0, err
	}
	polled, err := s.q.NodeHasReadings(ctx, key)
	if err != nil || !polled {
		return []api.NodeReading{}, false, 0, err
	}
	from, to := ts(since), pgtype.Timestamptz{}
	if !until.IsZero() {
		to = ts(until)
	}
	base, err := s.q.LatestNodeStatusReadingBefore(ctx, sqlc.LatestNodeStatusReadingBeforeParams{
		TargetPublicKey: key, Since: from, IncludeUntrusted: includeUntrusted,
	})
	if err != nil {
		return nil, false, 0, err
	}
	rows, err := s.q.ListNodeReadings(ctx, sqlc.ListNodeReadingsParams{
		TargetPublicKey: key, Since: from, Until: to, IncludeUntrusted: includeUntrusted,
	})
	if err != nil {
		return nil, false, 0, err
	}
	untrusted := 0
	if !includeUntrusted {
		n, err := s.q.CountUntrustedNodeReadings(ctx, sqlc.CountUntrustedNodeReadingsParams{TargetPublicKey: key, Since: from, Until: to})
		if err != nil {
			return nil, false, 0, err
		}
		untrusted = int(n)
	}
	out := make([]api.NodeReading, 0, len(base)+len(rows))
	for _, r := range base {
		out = append(out, nodeReading(sqlc.ListNodeReadingsRow(r)))
	}
	for _, r := range rows {
		out = append(out, nodeReading(r))
	}
	return out, true, untrusted, nil
}

// GetNodePolling returns the node's assigned observers and its newest trusted reading time. An unknown node returns
// pgx.ErrNoRows.
func (s *Store) GetNodePolling(ctx context.Context, nodeID uuid.UUID) (*api.NodePolling, error) {
	key, err := s.q.NodePublicKey(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	out := &api.NodePolling{Standbys: []api.NodePollingObserver{}}
	if out.Polled, err = s.q.NodeHasReadings(ctx, key); err != nil {
		return nil, err
	}
	latest, err := s.q.NodeLatestTrustedReadingAt(ctx, key)
	if err != nil {
		return nil, err
	}
	if latest.Valid {
		ms := latest.Time.UnixMilli()
		out.LatestReadingAt = &ms
	}
	rows, err := s.q.NodePollingAssignments(ctx, key)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		o := api.NodePollingObserver{ObserverID: r.ObserverID, Name: r.Name, Rank: int(r.Rank)}
		if r.Rank == 0 {
			out.Poller = &o
		} else {
			out.Standbys = append(out.Standbys, o)
		}
	}
	return out, nil
}
