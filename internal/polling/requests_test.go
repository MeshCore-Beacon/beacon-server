// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/google/uuid"
)

var rt0 = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// req is a request created n minutes after rt0.
func req(n int, password bool) Request {
	return Request{ID: uuid.New(), UserID: uuid.New(), Status: StatusWaiting, HasPassword: password,
		PasswordCT: ifPw(password), PasswordNonce: ifPw(password), CreatedAt: rt0.Add(time.Duration(n) * time.Minute), EnabledAt: rt0}
}

func ifPw(has bool) []byte {
	if has {
		return []byte{1}
	}
	return nil
}

func newState(rs ...Request) *TargetState {
	t := &TargetState{Target: make([]byte, 32), Requests: rs}
	t.Settle(rt0, "", false)
	return t
}

func (t *TargetState) get(id uuid.UUID) Request {
	for _, r := range t.Requests {
		if r.ID == id {
			return r
		}
	}
	return Request{}
}

func TestPickActiveIsTheOldestUsableOption(t *testing.T) {
	a, b, c := req(0, true), req(1, true), req(2, false)
	cases := []struct {
		name string
		mod  func()
		want *Request
	}{
		{"oldest wins", func() {}, &a},
		{"failed one is skipped", func() { a.PasswordFailed = true }, &b},
		{"disabled one is skipped", func() { a.Status = StatusDisabled }, &b},
		{"admin stopped one is skipped", func() { a.Status = StatusAdminStopped }, &b},
		{"blank option stands when the passwords are gone", func() { a.PasswordFailed, b.PasswordFailed = true, true }, &c},
		{"none usable", func() { a.PasswordFailed, b.PasswordFailed, c.PasswordFailed = true, true, true }, nil},
	}
	for _, k := range cases {
		a, b, c = req(0, true), req(1, true), req(2, false)
		k.mod()
		tg := &TargetState{Requests: []Request{c, b, a}} // order of the slice does not matter
		id, ok := tg.pick()
		if (k.want == nil) == ok || (ok && id != k.want.ID) {
			t.Errorf("%s: got %v %v", k.name, id, ok)
		}
	}
	// A blocked target never offers the blank option.
	tg := &TargetState{Requests: []Request{req(0, false)}, NoPasswordBlocked: true}
	if _, ok := tg.pick(); ok {
		t.Error("blank option picked on a blocked target")
	}
}

func TestVerOnlyGoesUpWhenThePasswordInUseChanges(t *testing.T) {
	a, b := req(0, true), req(1, true)
	st := newState(a, b)
	if st.ActiveID != a.ID || st.Ver != 1 {
		t.Fatalf("first request: active=%v ver=%d", st.ActiveID, st.Ver)
	}
	// A second option behind the first changes nothing.
	c := req(2, false)
	if err := st.Create(c, rt0.Add(time.Hour)); err != nil || st.Ver != 1 {
		t.Fatalf("create behind: %v ver=%d", err, st.Ver)
	}
	// Removing a request that is not in use changes nothing.
	st.Remove(c.ID, rt0)
	if st.Ver != 1 {
		t.Fatalf("remove not in use: ver=%d", st.Ver)
	}
	// Removing the one in use falls back to the next and rises.
	st.Remove(a.ID, rt0.Add(2*time.Hour))
	if st.ActiveID != b.ID || st.Ver != 2 || !st.VerChangedAt.Equal(rt0.Add(2*time.Hour)) {
		t.Fatalf("fall back: active=%v ver=%d at=%v", st.ActiveID, st.Ver, st.VerChangedAt)
	}
	// Removing the last leaves nothing in use and still rises.
	st.Remove(b.ID, rt0)
	if st.ActiveID != uuid.Nil || st.Ver != 3 {
		t.Fatalf("last: active=%v ver=%d", st.ActiveID, st.Ver)
	}
	// Two blank requests are one option: the second going away is not a change.
	x, y := req(0, false), req(1, false)
	st = newState(x, y)
	v := st.Ver
	st.Remove(x.ID, rt0)
	if st.Ver != v || st.ActiveID != y.ID {
		t.Fatalf("blank to blank: ver %d -> %d", v, st.Ver)
	}
}

func TestLoginFailedMovesToTheNextOption(t *testing.T) {
	a, b := req(0, true), req(1, true)
	st := newState(a, b)
	if !st.LoginFailed(rt0.Add(time.Minute)) {
		t.Fatal("not applied")
	}
	if got := st.get(a.ID); got.Status != StatusLoginFailed || !got.PasswordFailed || got.Reason == "" || !got.UpdatedAt.Equal(rt0.Add(time.Minute)) {
		t.Fatalf("failed request: %+v", got)
	}
	if st.ActiveID != b.ID || st.Ver != 2 {
		t.Fatalf("next option: active=%v ver=%d", st.ActiveID, st.Ver)
	}
	// The one in use fails and nothing is left: not requested any more, the request keeps its reason.
	st.LoginFailed(rt0.Add(2 * time.Minute))
	if st.ActiveID != uuid.Nil || st.get(b.ID).Status != StatusLoginFailed || st.Ver != 3 {
		t.Fatalf("none left: active=%v %+v ver=%d", st.ActiveID, st.get(b.ID), st.Ver)
	}
	if st.LoginFailed(rt0) {
		t.Fatal("applied with nothing in use")
	}
}

func TestLoginFailedOnTheBlankOptionFailsEveryBlankRequest(t *testing.T) {
	a, b, c := req(0, false), req(1, false), req(2, true)
	st := newState(a, b, c)
	st.LoginFailed(rt0)
	for _, id := range []uuid.UUID{a.ID, b.ID} {
		if r := st.get(id); r.Status != StatusLoginFailed || !r.PasswordFailed {
			t.Errorf("blank request not failed: %+v", r)
		}
	}
	if r := st.get(c.ID); r.Status == StatusLoginFailed || st.ActiveID != c.ID {
		t.Errorf("password request disturbed: %+v active=%v", r, st.ActiveID)
	}
}

func TestNewPasswordRevivesAndRaisesVer(t *testing.T) {
	a, b := req(0, true), req(1, true)
	st := newState(a, b)
	st.LoginFailed(rt0)
	ver := st.Ver
	// The failed owner sets a new password: failure clears, it is the oldest usable again, so it is in use again.
	st.SetPassword(a.ID, []byte{9}, []byte{9}, rt0.Add(time.Minute))
	got := st.get(a.ID)
	if got.PasswordFailed || got.Status != StatusWaiting || st.ActiveID != a.ID || st.Ver != ver+1 {
		t.Fatalf("revived: %+v active=%v ver=%d want ver %d", got, st.ActiveID, st.Ver, ver+1)
	}
	// Changing the password of the request in use raises ver even though the same request stays in use.
	st.SetPassword(a.ID, []byte{8}, []byte{8}, rt0.Add(2*time.Minute))
	if st.Ver != ver+2 {
		t.Fatalf("edit in use: ver=%d", st.Ver)
	}
	// Changing one that is not in use does not.
	st.SetPassword(b.ID, []byte{7}, []byte{7}, rt0.Add(3*time.Minute))
	if st.Ver != ver+2 {
		t.Fatalf("edit not in use: ver=%d", st.Ver)
	}
	// When nothing was in use, the new password becomes the one in use.
	only := req(0, true)
	st = newState(only)
	st.LoginFailed(rt0)
	if st.ActiveID != uuid.Nil {
		t.Fatal("setup")
	}
	v := st.Ver
	st.SetPassword(only.ID, []byte{5}, []byte{5}, rt0)
	if st.ActiveID != only.ID || st.Ver != v+1 {
		t.Fatalf("nothing in use: active=%v ver=%d", st.ActiveID, st.Ver)
	}
	// Removing the password makes it a blank request.
	st.SetPassword(only.ID, nil, nil, rt0)
	if r := st.get(only.ID); r.HasPassword || r.PasswordCT != nil || st.ActiveID != only.ID {
		t.Fatalf("blank: %+v", r)
	}
}

func TestAdminStopWithAPasswordInUse(t *testing.T) {
	a, b := req(0, true), req(1, true)
	st := newState(a, b)
	ver := st.Ver
	if !st.AdminStop(rt0.Add(time.Minute)) {
		t.Fatal("not applied")
	}
	got := st.get(a.ID)
	if got.Status != StatusAdminStopped || got.HasPassword || got.PasswordCT != nil || got.PasswordNonce != nil || got.Reason == "" {
		t.Fatalf("stopped request keeps a password: %+v", got)
	}
	if st.ActiveID != b.ID || st.Ver != ver+1 || st.NoPasswordBlocked {
		t.Fatalf("other user's password: active=%v ver=%d blocked=%v", st.ActiveID, st.Ver, st.NoPasswordBlocked)
	}
	// It does not restart on its own, only with a new password.
	st.Settle(rt0, st.activeKey(), false)
	if st.get(a.ID).Status != StatusAdminStopped {
		t.Fatal("restarted")
	}
	st.SetPassword(a.ID, []byte{1}, []byte{1}, rt0)
	if st.get(a.ID).Status != StatusWaiting {
		t.Fatalf("new password did not restart it: %+v", st.get(a.ID))
	}
}

func TestAdminStopWithNoPasswordInUseBlocksBlankLogins(t *testing.T) {
	a, b, c := req(0, false), req(1, false), req(2, true)
	st := newState(a, b, c)
	if !st.AdminStop(rt0) {
		t.Fatal("not applied")
	}
	if !st.NoPasswordBlocked {
		t.Fatal("not blocked")
	}
	for _, id := range []uuid.UUID{a.ID, b.ID} {
		if r := st.get(id); r.Status != StatusAdminStopped {
			t.Errorf("blank request %v: %+v", id, r)
		}
	}
	if st.get(c.ID).Status == StatusAdminStopped || st.ActiveID != c.ID {
		t.Errorf("password request disturbed: %+v active=%v", st.get(c.ID), st.ActiveID)
	}
	// New blank requests are refused, a password one is fine.
	if err := st.Create(req(5, false), rt0); !errors.Is(err, ErrNoPasswordBlocked) {
		t.Errorf("blank create: %v", err)
	}
	if err := st.Create(req(5, true), rt0); err != nil {
		t.Errorf("password create: %v", err)
	}
	// Taking a password away from a request is a blank login too.
	if err := st.SetPassword(c.ID, nil, nil, rt0); !errors.Is(err, ErrNoPasswordBlocked) {
		t.Errorf("blank edit: %v", err)
	}
	// Nothing requested: an admin stop changes nothing.
	empty := newState()
	if empty.AdminStop(rt0) || empty.NoPasswordBlocked {
		t.Error("admin stop applied to a target nobody requested")
	}
}

func TestCreateRules(t *testing.T) {
	u := uuid.New()
	a := req(0, true)
	a.UserID = u
	st := newState(a)
	dup := req(1, true)
	dup.UserID = u
	if err := st.Create(dup, rt0); !errors.Is(err, ErrAlreadyRequested) {
		t.Errorf("duplicate: %v", err)
	}
	other := req(1, true)
	if err := st.Create(other, rt0); err != nil {
		t.Errorf("other user: %v", err)
	}
	if got := st.get(other.ID); got.Status != StatusWaiting || got.Reason != ReasonNoObserver {
		t.Errorf("new request is waiting with a reason: %+v", got)
	}
}

func TestAssignmentDrivesWaitingAndActive(t *testing.T) {
	a, b, gone := req(0, true), req(1, false), req(2, true)
	gone.Status = StatusAdminStopped
	st := newState(a, b, gone)
	st.SetAssigned(true, "")
	if st.get(a.ID).Status != StatusActive || st.get(b.ID).Status != StatusActive || st.get(gone.ID).Status != StatusAdminStopped {
		t.Fatalf("assigned: %+v", st.Requests)
	}
	if !st.SetAssigned(false, ReasonNoObserver) {
		t.Error("no change reported")
	}
	if r := st.get(a.ID); r.Status != StatusWaiting || r.Reason != ReasonNoObserver {
		t.Fatalf("no observer: %+v", r)
	}
	st.SetAssigned(false, ReasonNoSeat)
	if st.get(a.ID).Reason != ReasonNoSeat {
		t.Fatal("no seat reason")
	}
	if st.SetAssigned(false, ReasonNoSeat) {
		t.Error("reported a change when nothing changed")
	}
	// A failed request keeps its failure, the rest follow the poll.
	st.LoginFailed(rt0)
	st.SetAssigned(true, "")
	if st.get(a.ID).Status != StatusLoginFailed {
		t.Errorf("failed request overwritten: %+v", st.get(a.ID))
	}
}

func TestDisableAndEnable(t *testing.T) {
	a, b := req(0, true), req(1, true)
	st := newState(a, b)
	st.Disable("not heard for 7 days", rt0.Add(time.Hour))
	for _, r := range st.Requests {
		if r.Status != StatusDisabled || r.Reason != "not heard for 7 days" {
			t.Errorf("not disabled: %+v", r)
		}
	}
	if st.ActiveID != uuid.Nil {
		t.Error("a disabled request is still requested")
	}
	// Re-enabling is by hand, one request at a time, and starts it again.
	ver := st.Ver
	if err := st.Enable(a.ID, rt0.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := st.get(a.ID); got.Status != StatusWaiting || got.Reason != ReasonNoObserver || !got.EnabledAt.Equal(rt0.Add(2*time.Hour)) {
		t.Errorf("enabled: %+v", got)
	}
	if st.ActiveID != a.ID || st.Ver != ver+1 || st.get(b.ID).Status != StatusDisabled {
		t.Errorf("enable: active=%v ver=%d", st.ActiveID, st.Ver)
	}
	// Enabling a request that is not disabled changes nothing.
	before := st.Ver
	if err := st.Enable(a.ID, rt0); err != nil || st.Ver != before {
		t.Errorf("enable enabled: %v", err)
	}
	if err := st.Enable(uuid.New(), rt0); !errors.Is(err, ErrRequestNotFound) {
		t.Errorf("unknown: %v", err)
	}
}

func TestRemoveRecordsTheDeletion(t *testing.T) {
	a := req(0, true)
	st := newState(a)
	if !st.Remove(a.ID, rt0) || len(st.Requests) != 0 || len(st.Deleted) != 1 || st.Deleted[0] != a.ID {
		t.Fatalf("remove: %+v", st)
	}
	if st.Remove(a.ID, rt0) {
		t.Error("removed twice")
	}
}

func TestLoginFailedCounts(t *testing.T) {
	changed := rt0
	cases := []struct {
		name string
		now  time.Time
		seen *time.Time
		want bool
	}{
		{"long after the change", changed.Add(31 * time.Minute), nil, true},
		{"exactly thirty minutes", changed.Add(30 * time.Minute), nil, true},
		{"soon after the change", changed.Add(10 * time.Minute), nil, false},
		{"another state seen since the change", changed.Add(10 * time.Minute), ptr(changed.Add(time.Minute)), true},
		{"another state seen before the change", changed.Add(10 * time.Minute), ptr(changed.Add(-time.Minute)), false},
		{"never changed", rt0.Add(time.Minute), nil, true},
	}
	for _, c := range cases {
		vc := changed
		if c.name == "never changed" {
			vc = time.Time{}
		}
		if got := LoginFailedCounts(c.now, vc, c.seen); got != c.want {
			t.Errorf("%s: %v", c.name, got)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestWorkingPoll(t *testing.T) {
	changed, fresh := rt0, 4*time.Hour
	now := changed.Add(2 * time.Hour)
	cases := []struct {
		name    string
		changed time.Time
		read    *time.Time
		fresh   time.Duration
		want    bool
	}{
		{"reading since the change", changed, ptr(changed.Add(time.Hour)), fresh, true},
		{"reading before the change", changed, ptr(changed.Add(-time.Minute)), fresh, false},
		{"no reading", changed, nil, fresh, false},
		{"reading too old", changed.Add(-5 * time.Hour), ptr(now.Add(-fresh - time.Minute)), fresh, false},
		{"never changed", time.Time{}, ptr(now), fresh, false},
		{"check off", changed, ptr(changed.Add(time.Hour)), 0, false},
	}
	for _, c := range cases {
		if got := WorkingPoll(now, c.changed, c.read, c.fresh); got != c.want {
			t.Errorf("%s: %v", c.name, got)
		}
	}
	if got := WorkingPollFresh(config.ResolvedPolling{}); got != 4*time.Hour {
		t.Errorf("default freshness %v", got)
	}
}

// A login_failed claim from the poller does not fail an option its own recent reading shows working.
func TestApplyReportTrustsAWorkingPoll(t *testing.T) {
	poller := []byte{1}
	for _, read := range []bool{true, false} {
		st := &TargetState{}
		a := req(0, false)
		if err := st.Create(a, rt0); err != nil {
			t.Fatal(err)
		}
		st.Holders = map[string]int{string(poller): 0}
		st.WorkingPollFor = 4 * time.Hour
		if read {
			st.LastReading = map[string]time.Time{string(poller): rt0.Add(20 * time.Minute)}
		}
		st.ApplyReport(poller, "login_failed", rt0.Add(40*time.Minute))
		if failed := st.Requests[0].Status == StatusLoginFailed; failed == read {
			t.Errorf("reading=%v: status %s", read, st.Requests[0].Status)
		}
	}
}

func TestReasonCodes(t *testing.T) {
	for _, tc := range []struct {
		reason, code string
		days         int
	}{
		{"", "", 0},
		{ReasonNoObserver, "no_observer", 0},
		{ReasonNoSeat, "no_seat", 0},
		{ReasonLoginFailed, "login_failed", 0},
		{ReasonBlankFailed, "blank_login_failed", 0},
		{ReasonAdminStopped, "admin_stopped", 0},
		{ReasonBlankBlocked, "blank_blocked", 0},
		{ReasonOffline(7), "offline", 7},
		{ReasonOffline(1), "offline", 1},
		{"something else", "other", 0},
	} {
		if c, d := ReasonCode(tc.reason); c != tc.code || d != tc.days {
			t.Errorf("%q: %q %d", tc.reason, c, d)
		}
	}
	if ReasonOffline(1) != "Not heard by any observer for 1 day." || ReasonOffline(7) != "Not heard by any observer for 7 days." {
		t.Error("offline wording")
	}
}

func TestAdminStopAppliesToTheVersionServed(t *testing.T) {
	a, b := req(0, true), req(1, true)
	st := newState(a, b)
	o1, o2 := []byte{1}, []byte{2}
	st.Holders = map[string]int{string(o1): 0, string(o2): 1}
	st.ServedVer = map[string]int64{string(o1): st.Ver, string(o2): st.Ver}
	if !st.ApplyReport(o1, "admin_stop", rt0) || st.get(a.ID).HasPassword {
		t.Fatalf("the password served was kept: %+v", st.get(a.ID))
	}
	if st.ActiveID != b.ID {
		t.Fatal("the fallback is not in use")
	}
	// o2 was given the old version: its stop is recorded and the fallback stays.
	if !st.ApplyReport(o2, "admin_stop", rt0) {
		t.Fatal("report refused")
	}
	if r := st.get(b.ID); !r.HasPassword || r.Status == StatusAdminStopped {
		t.Fatalf("fallback destroyed: %+v", r)
	}
	if len(st.NewAdminStops) != 2 {
		t.Fatalf("%d stops recorded", len(st.NewAdminStops))
	}
	// Once it was served the new version, a stop does apply.
	o3 := []byte{3}
	st.Holders[string(o3)] = 2
	st.ServedVer[string(o3)] = st.Ver
	st.ApplyReport(o3, "admin_stop", rt0)
	if st.get(b.ID).HasPassword {
		t.Fatal("a stop on the current version did not delete it")
	}
}

func TestFollowAssignmentsKeepsStoppedRequests(t *testing.T) {
	st := &TargetState{Requests: []Request{
		{Status: StatusWaiting, Reason: ReasonNoSeat}, {Status: StatusActive},
		{Status: StatusLoginFailed, Reason: ReasonLoginFailed}, {Status: StatusAdminStopped, Reason: ReasonAdminStopped},
		{Status: StatusDisabled, Reason: ReasonOffline(7)},
	}}
	st.FollowAssignments(true)
	if r := st.Requests[0]; r.Status != StatusActive || r.Reason != "" {
		t.Fatalf("waiting with an assignment: %+v", r)
	}
	st.FollowAssignments(false)
	for i, want := range []RequestStatus{StatusWaiting, StatusWaiting, StatusLoginFailed, StatusAdminStopped, StatusDisabled} {
		if st.Requests[i].Status != want {
			t.Fatalf("request %d: %s, want %s", i, st.Requests[i].Status, want)
		}
	}
	if st.Requests[1].Reason != ReasonNoObserver || st.Requests[2].Reason != ReasonLoginFailed {
		t.Fatalf("reasons: %+v", st.Requests)
	}
}

func TestDisableKeepsCredentialStops(t *testing.T) {
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	st := &TargetState{Requests: []Request{
		{ID: uuid.New(), Status: StatusLoginFailed, Reason: ReasonLoginFailed, PasswordFailed: true, HasPassword: true},
		{ID: uuid.New(), Status: StatusAdminStopped, Reason: ReasonAdminStopped},
		{ID: uuid.New(), Status: StatusActive, HasPassword: true},
	}}
	st.Disable(ReasonOffline(7), now)
	for i, want := range []RequestStatus{StatusLoginFailed, StatusAdminStopped, StatusDisabled} {
		if st.Requests[i].Status != want {
			t.Fatalf("request %d: %s, want %s", i, st.Requests[i].Status, want)
		}
	}
	for _, r := range st.Requests[:2] {
		if err := st.Enable(r.ID, now); err != nil || st.find(r.ID).Status != r.Status {
			t.Fatalf("enable changed a stopped request: %v %s", err, st.find(r.ID).Status)
		}
	}
	if err := st.SetPassword(st.Requests[1].ID, nil, nil, now); !errors.Is(err, ErrPasswordInvalid) || st.Requests[1].Status != StatusAdminStopped {
		t.Fatalf("admin-stopped request became a blank login: %v %s", err, st.Requests[1].Status)
	}
	if err := st.SetPassword(st.Requests[1].ID, []byte("ct"), []byte("nonce"), now); err != nil || st.Requests[1].Status != StatusWaiting {
		t.Fatalf("a new password did not restart it: %v %s", err, st.Requests[1].Status)
	}
}
