// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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
// stored one changes nothing and reports applied=false.
func (s *Store) ReplacePollingClaim(ctx context.Context, c polling.Claim, observerID uuid.UUID, receivedAt time.Time) (bool, error) {
	applied := false
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		n, err := q.UpsertPollingClaim(ctx, sqlc.UpsertPollingClaimParams{
			ObserverPublicKey: c.Observer, ObserverID: observerID,
			Origin: clipText(c.Origin, 128), Client: clipText(c.Client, 128), PolicyVersion: clipText(c.PolicyVersion, 32),
			Community: c.Community, AcceptsRequests: c.AcceptsRequests, RequestSlots: int16(c.RequestSlots),
			ClaimedAt: ts(c.Timestamp), ReceivedAt: ts(receivedAt),
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
		return nil
	})
	return applied, err
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

// InsertRepeaterReading stores one reading; inserted=false means it was already there, or the observer stored one
// of the same target and method less than polling.ReadingMinInterval before receivedAt.
func (s *Store) InsertRepeaterReading(ctx context.Context, r polling.Reading, observerID uuid.UUID, receivedAt time.Time) (bool, error) {
	tag := r.SampledAt.Unix()
	if r.Tag != nil {
		tag = int64(*r.Tag)
	}
	st := r.Stats
	i32 := func(v int) int32 { return int32(v) }
	f32 := func(v float64) float32 { return float32(v) }
	n, err := s.q.InsertRepeaterReading(ctx, sqlc.InsertRepeaterReadingParams{
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
			ClaimedSNR: r.ClaimedSnr, State: r.State, Misses: int(r.Misses), Blocked: r.Blocked, Approved: r.Approved}
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

// ReplacePollingAssignments swaps the target's whole order in one transaction; nil or empty clears it.
func (s *Store) ReplacePollingAssignments(ctx context.Context, target []byte, a []polling.Assignment, at time.Time) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error { return replaceAssignments(ctx, q, target, a, at) })
}

func replaceAssignments(ctx context.Context, q *sqlc.Queries, target []byte, a []polling.Assignment, at time.Time) error {
	if err := q.DeletePollingAssignments(ctx, target); err != nil {
		return err
	}
	for _, x := range a {
		err := q.InsertPollingAssignment(ctx, sqlc.InsertPollingAssignmentParams{
			TargetPublicKey: target, ObserverPublicKey: x.Observer, Rank: int16(x.Rank),
			EvidenceSnr:   ptrConv(x.EvidenceSNR, func(v float64) float32 { return float32(v) }),
			EvidenceCount: int32(x.EvidenceCount), AssignedAt: ts(at),
		})
		if err != nil {
			return fmt.Errorf("assign %x: %w", x.Observer, err)
		}
	}
	return nil
}

// ElectPollingTarget locks the target, reads its candidates and current poller, and stores decide's order, all in
// one transaction, so two elections of one target cannot interleave.
func (s *Store) ElectPollingTarget(ctx context.Context, target []byte, receivedAfter, evidenceSince, at time.Time,
	decide func(cands []polling.Candidate, current []byte) []polling.Assignment) error {
	return s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := q.LockPollingTarget(ctx, target); err != nil {
			return err
		}
		cands, err := listPollingCandidates(ctx, q, target, receivedAfter, evidenceSince)
		if err != nil {
			return err
		}
		cur, err := currentPoller(ctx, q, target)
		if err != nil {
			return err
		}
		return replaceAssignments(ctx, q, target, decide(cands, cur), at)
	})
}

func (s *Store) PollingOwnerRows(ctx context.Context, observer []byte, receivedAfter time.Time) ([]polling.OwnerRow, error) {
	rows, err := s.q.PollingOwnerRows(ctx, sqlc.PollingOwnerRowsParams{ObserverPublicKey: observer, ReceivedAt: ts(receivedAfter)})
	if err != nil {
		return nil, err
	}
	out := make([]polling.OwnerRow, 0, len(rows))
	for _, r := range rows {
		row := polling.OwnerRow{Target: r.TargetPublicKey, Rank: int(r.Rank), Assigned: int(r.Assigned)}
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
	if err := s.q.DeleteOrphanPollingAssignments(ctx); err != nil {
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
