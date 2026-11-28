// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import "github.com/MeshCore-Beacon/beacon-server/internal/config"

// MaxRequestedPollers is the poller and its standbys for a requested target.
const MaxRequestedPollers = 3

// MaxRequestSlots is the target limit of a device whose claim does not report max_slots.
const MaxRequestSlots = 8

// SlotCeiling is the most targets the server believes any claim's max_slots.
const SlotCeiling = 32

// SlotLimit is the target limit the server holds a claim to: its max_slots, or MaxRequestSlots when it has none.
func SlotLimit(maxSlots int) int {
	if maxSlots <= 0 {
		return MaxRequestSlots
	}
	return min(maxSlots, SlotCeiling)
}

// ElectionInput is what one target's election works from.
type ElectionInput struct {
	// Cands are the observers whose claim names the target.
	Cands []Candidate
	// Requested is true when a telemetry request is in use for the target; RequestCands are then the observers that
	// could take it.
	Requested    bool
	RequestCands []Candidate
	Current      []byte
}

// Election is the order to store, with the reason a requested target is waiting when nobody was chosen.
type Election struct {
	Assignments []Assignment
	WaitReason  string
}

// requestSeat reports whether the observer has a seat for the target: its claim's free seats plus those it already
// holds, less the older requested targets it is assigned.
// A claim is the observer's own word, so its numbers are held to its target limit.
func requestSeat(c Candidate) bool {
	return RequestCapacity(c)-c.SeatsElsewhere > 0
}

// RequestCapacity is how many requested targets the observer can hold: its free seats plus those it holds already.
func RequestCapacity(c Candidate) int {
	limit := SlotLimit(c.MaxSlots)
	capacity := min(max(c.RequestSlots, 0), limit) + min(max(c.Held, 0), limit)
	return min(capacity, limit)
}

// FreeRequestSeats is how many more requested targets an observer can be handed. open is false when its claim is not
// live or takes no requests: it then has none. Otherwise it is the claim's free seats, held to what its target
// limit leaves beside the targets it names, less the seats already handed to it that its claim does not name yet.
func FreeRequestSeats(open bool, requestSlots, maxSlots, named, pending int) int {
	if !open {
		return 0
	}
	free := min(max(requestSlots, 0), max(SlotLimit(maxSlots)-named, 0))
	return max(free-max(pending, 0), 0)
}

// ElectRequested ranks the observers for a requested target. Only an observer with a live claim that accepts
// requests, advert evidence for the target (RequestCands carry it), a free seat, no admin stop for the target, and,
// when approvedOnly, the operator's approval may take it. It never picks more than MaxRequestedPollers. wait says
// why nobody was chosen.
func ElectRequested(cands []Candidate, current []byte, approvedOnly bool) (out []Assignment, wait string) {
	var ok []Candidate
	seatShort := false
	for _, c := range cands {
		if !eligible(c) || !hasEvidence(c) || c.AdminStopped || (approvedOnly && !c.Approved) {
			continue
		}
		if !requestSeat(c) {
			seatShort = true
			continue
		}
		ok = append(ok, c)
	}
	out = Elect(ok, current)
	if len(out) > MaxRequestedPollers {
		out = out[:MaxRequestedPollers]
	}
	for i := range out {
		out[i].Requested = true
	}
	switch {
	case len(out) > 0:
		return out, ""
	case seatShort:
		return nil, ReasonNoSeat
	}
	return nil, ReasonNoObserver
}

// Decide is the election for one target. A target a local claimant holds keeps being polled as local, and the
// requests ride on its readings. Otherwise a requested target goes to observers that take requests. Observers that
// hold a target as requested but are not chosen are left out of the owners answer, which drops it on the device.
func Decide(in ElectionInput, cfg config.ResolvedPolling) Election {
	approved := cfg.Authority == config.PollingAuthorityApproved
	var local []Candidate
	for _, c := range in.Cands {
		if !c.Requested {
			local = append(local, c)
		}
	}
	if approved {
		local = approvedOnly(local)
	}
	if in.Requested {
		hasLocal := false
		for _, c := range local {
			if eligible(c) {
				hasLocal = true
				break
			}
		}
		if !hasLocal {
			out, wait := ElectRequested(in.RequestCands, in.Current, approved || cfg.RequestObservers != config.RequestObserversAll)
			return Election{Assignments: out, WaitReason: wait}
		}
	}
	return Election{Assignments: Elect(local, in.Current)}
}
