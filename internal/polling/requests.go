// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/google/uuid"
)

// RequestStatus is where a telemetry request stands, as the profile shows it.
type RequestStatus string

const (
	StatusWaiting      RequestStatus = "waiting"
	StatusActive       RequestStatus = "active"
	StatusLoginFailed  RequestStatus = "login_failed"
	StatusAdminStopped RequestStatus = "admin_stopped"
	StatusDisabled     RequestStatus = "disabled"
)

// Plain sentences shown to the user next to a status.
const (
	ReasonNoObserver   = "No observer can reach this repeater yet."
	ReasonNoSeat       = "Every observer that can reach this repeater is already polling its maximum."
	ReasonLoginFailed  = "The repeater did not accept this guest password. Enter a new one."
	ReasonBlankFailed  = "The repeater did not accept a blank or hello login. Enter a guest password."
	ReasonAdminStopped = "The repeater gave admin access to this password, so Beacon deleted it. Enter a guest password, never an admin one."
	ReasonBlankBlocked = "This repeater gives admin access to a blank login, so Beacon will not poll it without a guest password."
)

// ReasonOffline is the reason a request is disabled when no observer has heard its repeater for days days.
func ReasonOffline(days int) string {
	if days == 1 {
		return "Not heard by any observer for 1 day."
	}
	return fmt.Sprintf("Not heard by any observer for %d days.", days)
}

var offlineReason = regexp.MustCompile(`^Not heard by any observer for ([0-9]+) days?\.$`)

// Stable codes for the reasons above, for clients to translate.
const (
	CodeNoObserver   = "no_observer"
	CodeNoSeat       = "no_seat"
	CodeLoginFailed  = "login_failed"
	CodeBlankFailed  = "blank_login_failed"
	CodeAdminStopped = "admin_stopped"
	CodeBlankBlocked = "blank_blocked"
	CodeOffline      = "offline"
	CodeOther        = "other"
)

// ReasonCode maps a stored reason to its code and, for the offline reason, the number of days. An empty reason has
// no code.
func ReasonCode(reason string) (code string, days int) {
	switch reason {
	case "":
		return "", 0
	case ReasonNoObserver:
		return CodeNoObserver, 0
	case ReasonNoSeat:
		return CodeNoSeat, 0
	case ReasonLoginFailed:
		return CodeLoginFailed, 0
	case ReasonBlankFailed:
		return CodeBlankFailed, 0
	case ReasonAdminStopped:
		return CodeAdminStopped, 0
	case ReasonBlankBlocked:
		return CodeBlankBlocked, 0
	}
	if m := offlineReason.FindStringSubmatch(reason); m != nil {
		n, err := strconv.Atoi(m[1])
		if err == nil {
			return CodeOffline, n
		}
	}
	return CodeOther, 0
}

var (
	ErrRequestLimit      = errors.New("polling: request limit reached")
	ErrAlreadyRequested  = errors.New("polling: already requested")
	ErrNotARepeater      = errors.New("polling: not a repeater or room server")
	ErrPasswordInvalid   = errors.New("polling: password must be 1 to 15 bytes with no zero byte")
	ErrNoPasswordBlocked = errors.New("polling: this repeater grants admin to a blank login")
	ErrRequestNotFound   = errors.New("polling: request not found")
	ErrRequestsDisabled  = errors.New("polling: telemetry requests are off")
)

// LoginFailedGrace is how long after a password change a login_failed report is ignored, unless the observer has
// shown another state since.
const LoginFailedGrace = 30 * time.Minute

const blankKey = "blank"

// Request is one user's request for one repeater. The password is only ever held sealed.
type Request struct {
	ID             uuid.UUID
	UserID         uuid.UUID
	Status         RequestStatus
	Reason         string
	HasPassword    bool
	PasswordCT     []byte
	PasswordNonce  []byte
	PasswordFailed bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
	// EnabledAt is when the request was created or last re-enabled; the offline rule counts from here.
	EnabledAt time.Time
}

// TargetState is everything the state rules need about one repeater: all its requests and its shared counters. The
// methods are the rules; they change this value only, and the store persists the difference.
type TargetState struct {
	Target            []byte
	Requests          []Request
	ActiveID          uuid.UUID // the request whose password is in use; Nil when the target is not requested
	Ver               int64
	VerChangedAt      time.Time
	NoPasswordBlocked bool
	// UserRequests is how many requests the acting user holds across all repeaters, for the limit.
	UserRequests int
	// Deleted lists requests removed from Requests, for the store to delete.
	Deleted []uuid.UUID

	// What a claim's report is judged against, loaded under the target's lock. Holders maps each observer handed the
	// target as a requested one to its rank; AdminStops are the observers already recorded; OtherSeen is when each
	// last showed a state other than login_failed.
	Holders    map[string]int
	AdminStops map[string]bool
	// ServedVer is the password version each observer was last answered with; a report from it is about that one.
	ServedVer map[string]int64
	OtherSeen map[string]time.Time
	// LastReading is when each observer handed the target stored its newest trusted reading of it. WorkingPollFor
	// is how long such a reading shows a working login (see WorkingPoll); zero turns that check off.
	LastReading    map[string]time.Time
	WorkingPollFor time.Duration
	// NewAdminStops and TouchedOther are what ApplyReport added, for the store to write.
	NewAdminStops [][]byte
	TouchedOther  map[string]time.Time
}

func (t *TargetState) find(id uuid.UUID) *Request {
	for i := range t.Requests {
		if t.Requests[i].ID == id {
			return &t.Requests[i]
		}
	}
	return nil
}

func (t *TargetState) usable(r Request) bool {
	if r.Status == StatusAdminStopped || r.Status == StatusDisabled || r.PasswordFailed {
		return false
	}
	return r.HasPassword || !t.NoPasswordBlocked
}

// pick is the oldest request whose option is usable. Requests with no password are one shared option, so the
// oldest of them stands for it.
func (t *TargetState) pick() (uuid.UUID, bool) {
	var best *Request
	for i := range t.Requests {
		r := &t.Requests[i]
		if !t.usable(*r) {
			continue
		}
		if best == nil || r.CreatedAt.Before(best.CreatedAt) || (r.CreatedAt.Equal(best.CreatedAt) && r.ID.String() < best.ID.String()) {
			best = r
		}
	}
	if best == nil {
		return uuid.Nil, false
	}
	return best.ID, true
}

// activeKey names the password option in use: a request id for a password, "blank" for the shared blank or hello
// login, "" for none.
func (t *TargetState) activeKey() string {
	r := t.find(t.ActiveID)
	switch {
	case r == nil:
		return ""
	case !r.HasPassword:
		return blankKey
	}
	return r.ID.String()
}

// Settle chooses the request in use after a change and raises ver when the password in use is not the one named by
// old (taken from activeKey before the change). force raises it for an edit of the password already in use.
func (t *TargetState) Settle(now time.Time, old string, force bool) {
	id, ok := t.pick()
	if !ok {
		id = uuid.Nil
	}
	t.ActiveID = id
	key := t.activeKey()
	if key != old || (force && key != "") {
		t.Ver++
		t.VerChangedAt = now
	}
}

func blankOption(r Request) bool { return !r.HasPassword }

// Create adds a request. The caller has checked the user's limit.
func (t *TargetState) Create(r Request, now time.Time) error {
	for _, x := range t.Requests {
		if x.UserID == r.UserID {
			return ErrAlreadyRequested
		}
	}
	if !r.HasPassword && t.NoPasswordBlocked {
		return ErrNoPasswordBlocked
	}
	old := t.activeKey()
	r.Status, r.Reason, r.PasswordFailed, r.UpdatedAt = StatusWaiting, ReasonNoObserver, false, now
	t.Requests = append(t.Requests, r)
	t.Settle(now, old, false)
	return nil
}

// SetPassword replaces a request's sealed password; nil ct removes it (a blank login). A failed or admin-stopped
// request starts again. An admin-stopped one needs a password: a repeater that gave admin to one may do so for a
// blank login too.
func (t *TargetState) SetPassword(id uuid.UUID, ct, nonce []byte, now time.Time) error {
	r := t.find(id)
	if r == nil {
		return ErrRequestNotFound
	}
	if ct == nil && t.NoPasswordBlocked {
		return ErrNoPasswordBlocked
	}
	if ct == nil && r.Status == StatusAdminStopped {
		return ErrPasswordInvalid
	}
	old := t.activeKey()
	r.PasswordCT, r.PasswordNonce, r.HasPassword = ct, nonce, ct != nil
	r.PasswordFailed, r.UpdatedAt = false, now
	if r.Status == StatusLoginFailed || r.Status == StatusAdminStopped {
		r.Status, r.Reason = StatusWaiting, ReasonNoObserver
	}
	next, _ := t.pick()
	t.Settle(now, old, next == id)
	return nil
}

// Remove deletes a request. It reports false when there is none.
func (t *TargetState) Remove(id uuid.UUID, now time.Time) bool {
	for i, r := range t.Requests {
		if r.ID != id {
			continue
		}
		old := t.activeKey()
		t.Requests = append(t.Requests[:i:i], t.Requests[i+1:]...)
		t.Deleted = append(t.Deleted, id)
		t.Settle(now, old, false)
		return true
	}
	return false
}

// LoginFailed records that the poller was refused: the option in use failed, and the next one takes over. It
// reports false when nothing was in use.
func (t *TargetState) LoginFailed(now time.Time) bool {
	cur := t.find(t.ActiveID)
	if cur == nil {
		return false
	}
	old := t.activeKey()
	if cur.HasPassword {
		fail(cur, ReasonLoginFailed, now)
	} else {
		for i := range t.Requests {
			if blankOption(t.Requests[i]) && t.usable(t.Requests[i]) {
				fail(&t.Requests[i], ReasonBlankFailed, now)
			}
		}
	}
	t.Settle(now, old, false)
	return true
}

func fail(r *Request, reason string, now time.Time) {
	r.PasswordFailed, r.Status, r.Reason, r.UpdatedAt = true, StatusLoginFailed, reason, now
}

// AdminStop records that a repeater answered the login with admin access. A password in use is deleted and its
// request stopped; with none in use the repeater accepts a blank login as admin, so blank logins are blocked. It
// reports false when the target is not requested.
func (t *TargetState) AdminStop(now time.Time) bool {
	cur := t.find(t.ActiveID)
	if cur == nil {
		return false
	}
	old := t.activeKey()
	if cur.HasPassword {
		cur.PasswordCT, cur.PasswordNonce, cur.HasPassword = nil, nil, false
		cur.Status, cur.Reason, cur.PasswordFailed, cur.UpdatedAt = StatusAdminStopped, ReasonAdminStopped, false, now
	} else {
		t.NoPasswordBlocked = true
		for i := range t.Requests {
			r := &t.Requests[i]
			if blankOption(*r) && r.Status != StatusAdminStopped {
				r.Status, r.Reason, r.PasswordFailed, r.UpdatedAt = StatusAdminStopped, ReasonBlankBlocked, false, now
			}
		}
	}
	t.Settle(now, old, false)
	return true
}

// Disable stops the waiting and active requests of the target, for a repeater nobody hears any more. A failed or
// admin-stopped request keeps its state: only a new password starts it again.
func (t *TargetState) Disable(reason string, now time.Time) {
	old := t.activeKey()
	for i := range t.Requests {
		r := &t.Requests[i]
		if r.Status == StatusWaiting || r.Status == StatusActive {
			r.Status, r.Reason, r.UpdatedAt = StatusDisabled, reason, now
		}
	}
	t.Settle(now, old, false)
}

// Enable starts a disabled request again. Any other request is left as it is.
func (t *TargetState) Enable(id uuid.UUID, now time.Time) error {
	r := t.find(id)
	if r == nil {
		return ErrRequestNotFound
	}
	if r.Status != StatusDisabled {
		return nil
	}
	old := t.activeKey()
	r.Status, r.Reason, r.EnabledAt, r.UpdatedAt = StatusWaiting, ReasonNoObserver, now, now
	t.Settle(now, old, false)
	return nil
}

// SetAssigned follows the poll: the requests that are waiting or active become active when an observer polls the
// target and waiting, with the reason, when none does. It reports whether anything changed.
func (t *TargetState) SetAssigned(assigned bool, reason string) bool {
	changed := false
	for i := range t.Requests {
		r := &t.Requests[i]
		if r.Status != StatusWaiting && r.Status != StatusActive {
			continue
		}
		status, why := StatusActive, ""
		if !assigned {
			status, why = StatusWaiting, reason
		}
		if r.Status != status || r.Reason != why {
			r.Status, r.Reason = status, why
			changed = true
		}
	}
	return changed
}

// FollowAssignments keeps waiting and active true to the target's assignments whatever changed them: a waiting
// request becomes active while the target has one, and an active one with none left waits for an observer.
func (t *TargetState) FollowAssignments(assigned bool) {
	for i := range t.Requests {
		r := &t.Requests[i]
		switch {
		case assigned && r.Status == StatusWaiting:
			r.Status, r.Reason = StatusActive, ""
		case !assigned && r.Status == StatusActive:
			r.Status, r.Reason = StatusWaiting, ReasonNoObserver
		}
	}
}

// LoginFailedCounts says whether a login_failed report from the poller counts. After a ver change it is ignored
// until the observer has shown another state since, or the grace has passed.
func LoginFailedCounts(now, verChangedAt time.Time, otherStateSeenAt *time.Time) bool {
	if verChangedAt.IsZero() || now.Sub(verChangedAt) >= LoginFailedGrace {
		return true
	}
	return otherStateSeenAt != nil && otherStateSeenAt.After(verChangedAt)
}

// WorkingPoll says whether the observer's newest trusted reading shows the option in use working: stored after the
// option became current and recent enough that its polls have not stopped since. Some firmware keeps claiming
// login_failed after a refused password gives way to a blank option, even while blank polls succeed.
func WorkingPoll(now, verChangedAt time.Time, lastReading *time.Time, fresh time.Duration) bool {
	return fresh > 0 && !verChangedAt.IsZero() && lastReading != nil &&
		lastReading.After(verChangedAt) && now.Sub(*lastReading) <= fresh
}

// WorkingPollFresh is how long a reading counts as a working poll: two of the longest poll windows, so one late
// poll does not end it.
func WorkingPollFresh(cfg config.ResolvedPolling) time.Duration {
	directS := int(cfg.DirectWindow / time.Second)
	if directS <= 0 {
		directS = 3600
	}
	return 2 * time.Duration(oneHop(directS)) * time.Second
}

// ReadingKeep is how long cleanup keeps readings: the working-poll check needs the newest one for
// WorkingPollFresh, even when reading_retention is shorter.
func ReadingKeep(cfg config.ResolvedPolling) time.Duration {
	return max(cfg.ReadingRetention, WorkingPollFresh(cfg))
}

// Requested reports whether the target is currently requested: a request's option is in use.
func (t *TargetState) Requested() bool { return t.ActiveID != uuid.Nil }

// byAge sorts oldest first, ties by id, so every reader orders requests the same way.
func byAge(rs []Request) {
	sort.SliceStable(rs, func(i, j int) bool {
		if !rs[i].CreatedAt.Equal(rs[j].CreatedAt) {
			return rs[i].CreatedAt.Before(rs[j].CreatedAt)
		}
		return rs[i].ID.String() < rs[j].ID.String()
	})
}

// ApplyReport applies one claim's report of a requested target's state. Only an observer that was handed the target
// is believed, and only the poller on login_failed: anyone else's report is ignored (false). admin_stop from an
// observer already recorded changes nothing, so a repeat cannot strike the next password in use, which that
// observer never tried. Any other state ends the login_failed grace for that observer.
func (t *TargetState) ApplyReport(observer []byte, state string, now time.Time) bool {
	rank, held := t.Holders[string(observer)]
	if !held {
		return false
	}
	switch state {
	case "admin_stop":
		if t.AdminStops[string(observer)] {
			return true
		}
		if t.AdminStops == nil {
			t.AdminStops = map[string]bool{}
		}
		t.AdminStops[string(observer)] = true
		t.NewAdminStops = append(t.NewAdminStops, append([]byte(nil), observer...))
		// The report is about the password the observer was given. When another one is in use now, that one was
		// never tried by it and stays; the stop is still recorded against the observer.
		if v, ok := t.ServedVer[string(observer)]; ok && v != t.Ver {
			return true
		}
		t.AdminStop(now)
	case "login_failed":
		if rank != 0 {
			return false
		}
		var seen *time.Time
		if at, ok := t.OtherSeen[string(observer)]; ok {
			seen = &at
		}
		var read *time.Time
		if at, ok := t.LastReading[string(observer)]; ok {
			read = &at
		}
		// A working poll is better evidence than the claim's flag.
		if LoginFailedCounts(now, t.VerChangedAt, seen) && !WorkingPoll(now, t.VerChangedAt, read, t.WorkingPollFor) {
			t.LoginFailed(now)
		}
	default:
		if t.OtherSeen == nil {
			t.OtherSeen = map[string]time.Time{}
		}
		if t.TouchedOther == nil {
			t.TouchedOther = map[string]time.Time{}
		}
		t.OtherSeen[string(observer)], t.TouchedOther[string(observer)] = now, now
	}
	return true
}
