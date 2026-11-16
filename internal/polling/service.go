// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/google/uuid"
)

const (
	ClaimTTL       = 3900 * time.Second
	ClaimFutureMax = 10 * time.Minute
	ReadingMaxAge  = 2 * time.Hour
	EvidenceWindow = 24 * time.Hour
	// Firmware claims at most once a minute and polls at most every 30 minutes.
	ClaimMinInterval   = 30 * time.Second
	ReadingMinInterval = 5 * time.Minute
	// 8 targets, two methods and 3 poll starts per 30-minute window stay under 100 an hour.
	ReadingsPerObserverPerHour = 120

	readingFutureMax = time.Minute
)

// Store is what the service needs from the database. The time parameters named
// receivedAfter are compared with when Beacon received the claim.
type Store interface {
	ObserverID(ctx context.Context, pubkey []byte) (uuid.UUID, error)
	ReplacePollingClaim(ctx context.Context, c Claim, observerID uuid.UUID, receivedAt time.Time) (bool, error)
	// PollingClaimReceivedAt is nil when the observer has no stored claim.
	PollingClaimReceivedAt(ctx context.Context, observer []byte) (*time.Time, error)
	// InsertRepeaterReading stores nothing when the same observer stored a reading of the target by the same
	// method within ReadingMinInterval of receivedAt.
	InsertRepeaterReading(ctx context.Context, r Reading, observerID uuid.UUID, receivedAt time.Time) (bool, error)
	HasLivePollingCandidate(ctx context.Context, observer, target []byte, receivedAfter time.Time) (bool, error)
	ListPollingTargets(ctx context.Context, receivedAfter time.Time) ([][]byte, error)
	// ElectPollingTarget runs decide on the target's live candidates and current poller and stores the result,
	// atomically per target. For a requested target it also loads the observers that could take the request and
	// moves its requests between waiting and active with the result.
	ElectPollingTarget(ctx context.Context, target []byte, receivedAfter, evidenceSince, at time.Time,
		decide func(in ElectionInput) Election) error
	PollingOwnerRows(ctx context.Context, observer []byte, receivedAfter time.Time) ([]OwnerRow, error)
	PollingObserverFlags(ctx context.Context, observer []byte) (blocked, approved bool, err error)
	PollingState(ctx context.Context) (paused bool, pausedUntil *time.Time, excluded [][]byte, err error)
}

// Service takes in claims and readings, runs the election and answers the firmware's questions.
type Service struct {
	store Store
	cfg   config.ResolvedPolling
	now   func() time.Time
	log   *slog.Logger
	// reqs and sealer are set when telemetry requests are on; without them no target is ever requested.
	reqs   RequestedStore
	sealer *Sealer
	rnd    io.Reader
}

func NewService(store Store, cfg config.ResolvedPolling, now func() time.Time, log *slog.Logger) *Service {
	return &Service{store: store, cfg: cfg, now: now, log: log}
}

// SetRequests turns on requested targets: claims that report them are acted on and owners answers carry their
// envelopes. Call it before the service takes traffic.
func (s *Service) SetRequests(st RequestedStore, sealer *Sealer) {
	s.reqs, s.sealer, s.rnd = st, sealer, rand.Reader
}

// short is the only form of an observer key that reaches the logs.
func short(key []byte) string {
	h := hex.EncodeToString(key)
	if len(h) > 8 {
		h = h[:8]
	}
	return h
}

func topicPrefix(topicHex string) string {
	if len(topicHex) > 8 {
		return topicHex[:8]
	}
	return topicHex
}

// blocked fails closed: a flags error counts as blocked, since this guards writes.
func (s *Service) blocked(ctx context.Context, observer []byte) bool {
	b, _, err := s.store.PollingObserverFlags(ctx, observer)
	if err != nil {
		s.log.Warn("polling observer flags", "observer", short(observer), "err", err)
		return true
	}
	if b {
		s.log.Debug("polling message from blocked observer", "observer", short(observer))
	}
	return b
}

func topicMatches(topicHex string, observer []byte) bool {
	return strings.EqualFold(topicHex, hex.EncodeToString(observer))
}

// HandleClaim stores a claim published under the observer's own topic and re-elects the targets it touches.
func (s *Service) HandleClaim(ctx context.Context, topicPubkeyHex string, raw []byte) {
	if !s.cfg.Enabled {
		return
	}
	c, err := ParseClaim(raw)
	if err != nil {
		// Parse errors can quote payload text, so only the topic prefix is logged.
		s.log.Debug("polling claim rejected", "observer", topicPrefix(topicPubkeyHex))
		return
	}
	if !topicMatches(topicPubkeyHex, c.Observer) {
		s.log.Debug("polling claim topic mismatch")
		return
	}
	now := s.now()
	if c.Timestamp.After(now.Add(ClaimFutureMax)) {
		s.log.Debug("polling claim too far ahead", "observer", short(c.Observer))
		return
	}
	obs := short(c.Observer)
	if s.blocked(ctx, c.Observer) {
		return
	}
	last, err := s.store.PollingClaimReceivedAt(ctx, c.Observer)
	if err != nil {
		s.log.Warn("polling last claim", "observer", obs, "err", err)
		return
	}
	if last != nil && now.Sub(*last) < ClaimMinInterval {
		s.log.Debug("polling claim too soon", "observer", obs)
		return
	}

	// Read the old targets first: they may need a new owner once the claim is replaced.
	targets := map[string][]byte{}
	prev, err := s.store.PollingOwnerRows(ctx, c.Observer, now.Add(-ClaimTTL))
	if err != nil {
		s.log.Warn("polling previous targets", "observer", obs, "err", err)
	}
	for _, r := range prev {
		targets[string(r.Target)] = r.Target
	}

	id, err := s.store.ObserverID(ctx, c.Observer)
	if err != nil {
		s.log.Warn("polling observer id", "observer", obs, "err", err)
		return
	}
	// With requests on, the claim's reports are stored with it, so a failed one leaves the claim out and the
	// device's next claim, which repeats it, is taken.
	var applied bool
	var ignored [][]byte
	if s.reqs != nil {
		applied, err = s.reqs.ReplacePollingClaimReporting(ctx, c, id, now, claimReport(c, now, WorkingPollFresh(s.cfg), &ignored))
	} else {
		applied, err = s.store.ReplacePollingClaim(ctx, c, id, now)
	}
	if err != nil {
		s.log.Warn("polling claim store", "observer", obs, "err", err)
		return
	}
	if !applied {
		return
	}
	for _, t := range ignored {
		s.log.Debug("requested target report ignored", "observer", obs, "target", short(t))
	}
	for _, n := range c.Nodes {
		targets[string(n.Target)] = n.Target
	}
	for _, t := range targets {
		if err := s.ElectTarget(ctx, t); err != nil {
			s.log.Warn("polling election", "observer", obs, "err", err)
		}
	}
}

// HandleReading stores each reading that is recent and covered by a live claim from the same observer.
func (s *Service) HandleReading(ctx context.Context, topicPubkeyHex string, raw []byte) {
	if !s.cfg.Enabled {
		return
	}
	rs, err := ParseReadings(raw)
	if err != nil {
		s.log.Debug("polling reading rejected", "observer", topicPrefix(topicPubkeyHex))
		return
	}
	if len(rs) == 0 || !topicMatches(topicPubkeyHex, rs[0].Observer) {
		s.log.Debug("polling reading topic mismatch")
		return
	}
	observer := rs[0].Observer
	obs := short(observer)
	if s.blocked(ctx, observer) {
		return
	}
	now := s.now()
	id, err := s.store.ObserverID(ctx, observer)
	if err != nil {
		s.log.Warn("polling observer id", "observer", obs, "err", err)
		return
	}
	for _, r := range rs {
		if r.SampledAt.After(now.Add(readingFutureMax)) || r.SampledAt.Before(now.Add(-ReadingMaxAge)) {
			continue
		}
		live, err := s.store.HasLivePollingCandidate(ctx, observer, r.Target, now.Add(-ClaimTTL))
		if err != nil {
			s.log.Warn("polling claim check", "observer", obs, "err", err)
			continue
		}
		if !live {
			continue
		}
		if _, err := s.store.InsertRepeaterReading(ctx, r, id, now); err != nil {
			s.log.Warn("polling reading store", "observer", obs, "err", err)
		}
	}
}

// Policy answers the firmware's policy question. It returns an error when the runtime state or the caller's flags
// cannot be read: the firmware keeps its saved policy on a non-200 answer, which is safer than a made-up one.
func (s *Service) Policy(ctx context.Context, c Caller) (Policy, error) {
	if !s.cfg.Enabled {
		return DisabledPolicy(), nil
	}
	paused, until, excluded, err := s.store.PollingState(ctx)
	if err != nil {
		s.log.Warn("polling state", "err", err)
		return Policy{}, err
	}
	if n := len(distinctExcluded(s.cfg.ExcludedNodes, excluded)); n > maxExcluded {
		s.log.Error("polling exclusions do not fit the policy; extra ones are not sent", "distinct", n, "max", maxExcluded)
	}
	rt := RuntimeState{Paused: paused, PausedUntil: until, Excluded: excluded}
	c.Blocked = false
	if c.Observer != nil {
		blocked, _, err := s.store.PollingObserverFlags(ctx, c.Observer)
		if err != nil {
			s.log.Warn("polling observer flags", "observer", short(c.Observer), "err", err)
			return Policy{}, err
		}
		c.Blocked = blocked
	}
	return BuildPolicy(s.cfg, rt, c, s.now()), nil
}

// Owners answers the firmware's owners question: the policy plus the caller's role for each claimed repeater. The
// answer reflects what is stored or is an error (503): a read that fails never leaves a requested entry out. The
// requested entries come from one statement, so they hold in a single snapshot.
func (s *Service) Owners(ctx context.Context, c Caller) (OwnersAnswer, error) {
	p, err := s.Policy(ctx, c)
	if err != nil {
		return OwnersAnswer{}, err
	}
	if !s.cfg.Enabled || c.Observer == nil || p.ObserverBlocked {
		return OwnersAnswer{Nodes: map[string]OwnerEntry{}, Policy: p}, nil
	}
	rows, err := s.store.PollingOwnerRows(ctx, c.Observer, s.now().Add(-ClaimTTL))
	if err != nil {
		s.log.Warn("polling owner rows", "observer", short(c.Observer), "err", err)
		return OwnersAnswer{}, err
	}
	excluded := make(map[string]bool, len(p.ExcludedNodes))
	for _, e := range p.ExcludedNodes {
		excluded[strings.ToLower(e)] = true
	}
	for i := range rows {
		rows[i].Excluded = excluded[hex.EncodeToString(rows[i].Target)]
		// No row of our own while the target has a poller: wait last in line. Only a target nobody holds fails open.
		if rows[i].Rank < 0 && rows[i].Assigned > 0 {
			rows[i].Rank = rows[i].Assigned
		}
	}
	OfferSeats(rows)
	// Approval can be withdrawn between elections. The flags come from the same read as the password, so a password
	// goes only to an observer that was allowed it when that password was in use.
	needApproval := s.requestsNeedApproval()
	for i := range rows {
		if rows[i].ObserverBlocked || (needApproval && !rows[i].ObserverApproved) {
			rows[i].PWOK = false
		}
	}
	ans := BuildOwners(rows, p)
	if err := s.addEnvelopes(ctx, c.Observer, rows, ans.Nodes); err != nil {
		return OwnersAnswer{}, err
	}
	return ans, nil
}

// requestsNeedApproval is true unless polling.request_observers resolved to all; an unresolved or unknown value needs approval.
func (s *Service) requestsNeedApproval() bool {
	return s.cfg.Authority == config.PollingAuthorityApproved || s.cfg.RequestObservers != config.RequestObserversAll
}

// ElectTarget recomputes the order of observers for one repeater. No candidates clears it.
func (s *Service) ElectTarget(ctx context.Context, target []byte) error {
	now := s.now()
	return s.store.ElectPollingTarget(ctx, target, now.Add(-ClaimTTL), now.Add(-EvidenceWindow), now, s.decide)
}

func (s *Service) decide(in ElectionInput) Election { return Decide(in, s.cfg) }

// ElectAll re-elects every known target. One failure does not stop the rest; the first error is returned.
func (s *Service) ElectAll(ctx context.Context) error {
	targets, err := s.store.ListPollingTargets(ctx, s.now().Add(-ClaimTTL))
	if err != nil {
		return err
	}
	var first error
	for _, t := range targets {
		if err := s.ElectTarget(ctx, t); err != nil {
			s.log.Warn("polling election", "target", short(t), "err", err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}
