// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	mailer "github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/google/uuid"
)

type fakeSend struct {
	locked    bool
	rows      []OutboxRow
	devices   map[uuid.UUID][]Device
	delivered []int64
	retry     []Retry
	settled   Settled
	emailSent []int64
	removed   []string
	reached   map[int64]map[uuid.UUID]bool
	marks     []DeviceMark
	expired   int
	expiredAt [2]time.Time
	notBefore time.Time
}

func (f *fakeSend) BeginSendLock(context.Context) (Unlocker, bool, error) {
	if f.locked {
		return nil, false, nil
	}
	closed := false
	return fakeLock{&closed}, true, nil
}

func (f *fakeSend) Expire(_ context.Context, a, b time.Time) (int64, error) {
	f.expired++
	f.expiredAt = [2]time.Time{a, b}
	return 0, nil
}

func (f *fakeSend) Pending(_ context.Context, nb, _ time.Time, _ int) ([]OutboxRow, error) {
	f.notBefore = nb
	return f.rows, nil
}

func (f *fakeSend) Devices(context.Context, []uuid.UUID) (map[uuid.UUID][]Device, error) {
	return f.devices, nil
}

func (f *fakeSend) DeliveredDevices(context.Context, []int64) (map[int64]map[uuid.UUID]bool, error) {
	return f.reached, nil
}

func (f *fakeSend) MarkDevicesDelivered(_ context.Context, m []DeviceMark, _ time.Time) error {
	f.marks = append(f.marks, m...)
	return nil
}

func (f *fakeSend) MarkEmailSent(_ context.Context, ids []int64, _ time.Time) error {
	f.emailSent = append(f.emailSent, ids...)
	return nil
}

// dev is a device whose id and payload key follow its handle.
func dev(platform, handle string) Device {
	key := make([]byte, PayloadKeySize)
	copy(key, handle)
	return Device{Session: uuid.NewSHA1(uuid.NameSpaceOID, []byte(handle)), Platform: platform, Token: handle, Key: key}
}

// Finish keeps what the pass decided, and reads it as the store does: delivered once both channels are done (or were
// not due), retried when a channel is, with the later hold.
func (f *fakeSend) Finish(_ context.Context, st Settled, _ time.Time, max int) error {
	if max != MaxAttempts {
		panic("attempts")
	}
	f.settled, f.delivered, f.retry = st, nil, nil
	in := func(ids []int64) map[int64]bool {
		m := map[int64]bool{}
		for _, id := range ids {
			m[id] = true
		}
		return m
	}
	emailDone, pushDone, sent := in(st.EmailDone), in(st.PushDone), in(f.emailSent)
	holds := map[int64]time.Time{}
	var order []int64
	for _, r := range append(append(append([]Retry{}, st.EmailRetry...), st.PushRetry...), st.PushHold...) {
		if _, ok := holds[r.ID]; !ok {
			order = append(order, r.ID)
		}
		if r.NotBefore.After(holds[r.ID]) {
			holds[r.ID] = r.NotBefore
		}
	}
	for _, id := range order {
		f.retry = append(f.retry, Retry{ID: id, NotBefore: holds[id]})
	}
	for _, r := range f.rows {
		if (!r.EmailDue || emailDone[r.ID] || sent[r.ID]) && (!r.PushDue || pushDone[r.ID]) {
			f.delivered = append(f.delivered, r.ID)
		}
	}
	return nil
}

func (f *fakeSend) RemoveDeviceToken(_ context.Context, tok string) error {
	f.removed = append(f.removed, tok)
	return nil
}

func (f *fakeSend) retried() []int64 {
	out := []int64{}
	for _, r := range f.retry {
		out = append(out, r.ID)
	}
	return out
}

type queued struct{ to, subject, text string }

type fakeMail struct {
	fail  error // every send fails with it
	calls int
	got   []queued
}

func (m *fakeMail) Send(_ context.Context, msg mailer.Message) error {
	m.calls++
	if m.fail != nil {
		return m.fail
	}
	m.got = append(m.got, queued{msg.To, msg.Subject, msg.Text})
	return nil
}

// fakePush answers each handle with its status in answer (delivered when absent), or fails every call with callErr.
type fakePush struct {
	calls   [][]PushMessage
	answer  map[string]PushResult
	callErr error
}

func (p *fakePush) Send(_ context.Context, msgs []PushMessage) ([]PushResult, error) {
	p.calls = append(p.calls, msgs)
	if p.callErr != nil {
		return nil, p.callErr
	}
	out := make([]PushResult, len(msgs))
	for i, m := range msgs {
		out[i] = p.answer[m.Handle]
	}
	return out, nil
}

func (p *fakePush) sent() []PushMessage {
	var out []PushMessage
	for _, c := range p.calls {
		out = append(out, c...)
	}
	return out
}

func emailSender(f SendStore, m mailer.Sender) *Sender {
	return NewSender(f, SenderConfig{Mail: m, PublicURL: publicURL})
}

func pushSender(f SendStore, p PushSender) *Sender {
	return NewSender(f, SenderConfig{Push: p, PushID: "inst_abc"})
}

var (
	userA, userB, userC = uuid.New(), uuid.New(), uuid.New()
	passOne             = uuid.New()
	publicURL, _        = url.Parse("https://beacon.example.org")
)

func row(id int64, user, pass uuid.UUID, kind, name string, value int64) OutboxRow {
	key := make([]byte, 32)
	key[0] = byte(id)
	return OutboxRow{ID: id, UserID: user, Pass: pass, TargetKind: "node", Key: key, Name: name, Kind: kind, Value: value, HasValue: true, Email: "a@example.org", EmailVerified: true,
		AlertEmail: true, AlertPush: true, EmailDue: true, PushDue: true}
}

func TestEmailGroupsOnePassIntoOneMessage(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{
		row(1, userA, passOne, KindGoneQuiet, "Hilltop", 30), row(2, userA, passOne, KindBatteryLow, "Hilltop", 3400),
		row(3, userA, uuid.New(), KindRebooted, "Hilltop", 5), row(4, userB, passOne, KindBackOnline, "Ridge", 0)}}
	f.rows[3].Email = "b@example.org"
	m := &fakeMail{}
	if err := emailSender(f, m).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 3 {
		t.Fatalf("%d mails", len(m.got))
	}
	first := m.got[0]
	if first.to != "a@example.org" || first.subject != "Beacon: 2 alerts" ||
		!strings.Contains(first.text, "Hilltop has not been heard for 30 hours.\n") || !strings.Contains(first.text, "Hilltop battery is low: 3.40 V.\n") ||
		!strings.HasSuffix(first.text, "https://beacon.example.org/?tab=Atlas\n") {
		t.Fatalf("%+v", first)
	}
	if m.got[1].subject != "Beacon: 1 alert" || m.got[2].to != "b@example.org" {
		t.Fatalf("%+v", m.got)
	}
	if len(f.delivered) != 4 || len(f.retry) != 0 {
		t.Fatalf("delivered %v retry %v", f.delivered, f.retry)
	}
}

func TestEmailNeverCarriesAnInjectedHeaderOrLine(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "x\r\nBcc: evil@example.org\r\nSubject: pwned", 5)}}
	m := &fakeMail{}
	if err := emailSender(f, m).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := m.got[0]
	if strings.ContainsAny(got.subject, "\r\n") || strings.Contains(got.text, "\r") {
		t.Fatalf("control characters: %q %q", got.subject, got.text)
	}
	for _, line := range strings.Split(got.text, "\n") {
		if strings.HasPrefix(line, "Bcc:") || strings.HasPrefix(line, "Subject:") {
			t.Fatalf("injected line %q", line)
		}
	}
	// The name is also capped.
	f.rows = []OutboxRow{row(1, userA, passOne, KindRebooted, strings.Repeat("n", 500), 5)}
	m.got = nil
	_ = emailSender(f, m).Run(context.Background())
	if strings.Count(m.got[0].text, "n") > 60 {
		t.Fatalf("name not capped: %q", m.got[0].text)
	}
}

func TestEmailWithoutAVerifiedAddressDropsAndAFailedSendRetries(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1)}}
	f.rows[0].EmailVerified = false
	f.rows[1].Email = ""
	m := &fakeMail{}
	_ = emailSender(f, m).Run(context.Background())
	if len(m.got) != 0 || len(f.delivered) != 2 {
		t.Fatalf("mails %d delivered %v", len(m.got), f.delivered)
	}
	f.rows = []OutboxRow{row(7, userA, passOne, KindRebooted, "A", 1)}
	m.fail = errors.New("smtp: 451 try later")
	_ = emailSender(f, m).Run(context.Background())
	// A mail the server refused is not delivered: it is retried, which counts an attempt and delays it.
	if len(f.retry) != 1 || f.retry[0].ID != 7 || len(f.delivered) != 0 {
		t.Fatalf("delivered %v retry %v", f.delivered, f.retry)
	}
}

// One refusal ends the run: the others would meet the same server, so they stay pending without an attempt.
func TestEmailFailureStopsThePassAndLeavesTheRestPending(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1)}}
	m := &fakeMail{fail: errors.New("smtp down")}
	_ = emailSender(f, m).Run(context.Background())
	if m.calls != 1 || len(f.retry) != 1 || f.retry[0].ID != 1 || len(f.delivered) != 0 {
		t.Fatalf("calls %d delivered %v retry %v", m.calls, f.delivered, f.retry)
	}
}

func TestEmailPassIsCapped(t *testing.T) {
	f := &fakeSend{}
	for i := 1; i <= MaxMailsPerPass+5; i++ {
		f.rows = append(f.rows, row(int64(i), uuid.New(), passOne, KindRebooted, "A", 1))
	}
	m := &fakeMail{}
	_ = emailSender(f, m).Run(context.Background())
	if len(m.got) != MaxMailsPerPass || len(f.delivered) != MaxMailsPerPass || len(f.retry) != 0 {
		t.Fatalf("mails %d delivered %d retry %d", len(m.got), len(f.delivered), len(f.retry))
	}
}

func TestSenderWindowsAndGuard(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	f := &fakeSend{}
	if err := NewSender(f, SenderConfig{Mail: &fakeMail{}, PublicURL: publicURL, Now: func() time.Time { return now }}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Older than two hours is dropped (not read, and deleted), delivered ones are kept a day.
	if !f.notBefore.Equal(now.Add(-2*time.Hour)) || !f.expiredAt[0].Equal(now.Add(-2*time.Hour)) || !f.expiredAt[1].Equal(now.Add(-24*time.Hour)) {
		t.Fatalf("%v %v", f.notBefore, f.expiredAt)
	}
	held := &fakeSend{locked: true, rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}}
	m := &fakeMail{}
	_ = emailSender(held, m).Run(context.Background())
	if held.expired != 0 || len(m.got) != 0 {
		t.Fatal("sent without the guard")
	}
}

func TestSentences(t *testing.T) {
	for _, tc := range []struct {
		kind, detail string
		value        int64
		want         string
	}{
		{KindGoneQuiet, "", 1, "N has not been heard for 1 hour."},
		{KindGoneQuiet, "", 30, "N has not been heard for 30 hours."},
		{KindBackOnline, "", 0, "N is back online."},
		{KindBatteryFalling, "", 250, "N battery is falling by about 0.25 V a day."},
		{KindTemperature, "", -25, "N temperature is -2.5 C, outside your limits."},
		{KindNoiseHigh, "", -80, "N noise floor is high: -80 dBm."},
		{KindClock, "", 7200, "N clock is off by 2 hours."},
		{KindClock, "", 900, "N clock is off by 15 minutes."},
		{KindClock, "", 172800, "N clock is off by 2 days."},
		{KindObserverOff, "", 90, "N has not reported for 90 minutes."},
		{KindRequestAttention, "login_failed", 0, "Telemetry request for N: the repeater did not accept the guest password."},
		{KindRequestAttention, "admin_stopped", 0, "Telemetry request for N: the password gave admin access and was deleted. Enter a guest password."},
		{KindRequestAttention, "disabled", 0, "Telemetry request for N was turned off: no observer has heard the repeater for days."},
	} {
		if got := Sentence(tc.kind, "N", tc.value, tc.detail, "c"); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.kind, got, tc.want)
		}
	}
}

func TestEmailSubjectUsesInstanceName(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "Hilltop", 5)}}
	m := &fakeMail{}
	s := emailSender(f, m)
	s.SetInstanceName("Beacon - Ottawa")
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 1 || m.got[0].subject != "Beacon - Ottawa: 1 alert" {
		t.Fatalf("%+v", m.got)
	}
	sub, _ := ComposeEmail("Réseau", []EmailNotice{{Kind: KindRebooted, Name: "A"}, {Kind: KindRebooted, Name: "B"}}, "u", "c")
	if sub != "Réseau: 2 alerts" {
		t.Fatal(sub)
	}
}

func TestPushSealsTheNoticeForEachDevice(t *testing.T) {
	id := uuid.New()
	r := row(41, userA, passOne, KindBatteryLow, "Hill\ntop", 3400)
	r.TargetID = id
	ios, android := dev("ios", "h_one"), dev("android", "h_two")
	f := &fakeSend{rows: []OutboxRow{r}, devices: map[uuid.UUID][]Device{userA: {ios, android}}}
	p := &fakePush{}
	if err := pushSender(f, p).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	sent := p.sent()
	if len(p.calls) != 1 || len(sent) != 2 || sent[0].Handle != "h_one" || sent[1].Handle != "h_two" || sent[0].CollapseKey != "41" ||
		sent[0].ID != "41:"+ios.Session.String() || sent[1].ID != "41:"+android.Session.String() {
		t.Fatalf("%+v", p.calls)
	}
	var got map[string]any
	if err := json.Unmarshal(open(t, ios.Key, "inst_abc", sent[0].Ciphertext), &got); err != nil {
		t.Fatal(err)
	}
	if got["kind"] != KindBatteryLow || got["name"] != "Hill top" || got["value"] != float64(3400) || got["id"] != id.String() || got["targetKind"] != "node" {
		t.Fatalf("%v", got)
	}
	// Each device's copy opens only with its own key.
	open(t, android.Key, "inst_abc", sent[1].Ciphertext)
	if len(f.delivered) != 1 || len(f.retry) != 0 || len(f.marks) != 2 {
		t.Fatalf("delivered %v retry %v marks %v", f.delivered, f.retry, f.marks)
	}
}

func TestPushBatchesAtMostMaxPushBatchPerCall(t *testing.T) {
	f := &fakeSend{devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a"), dev("android", "h_b")}}}
	for i := 1; i <= 120; i++ {
		f.rows = append(f.rows, row(int64(i), userA, passOne, KindRebooted, "A", 1))
	}
	p := &fakePush{}
	_ = pushSender(f, p).Run(context.Background())
	if len(p.calls) != 3 || len(p.calls[0]) != MaxPushBatch || len(p.calls[2]) != 40 || len(f.delivered) != 120 {
		t.Fatalf("calls %d delivered %d", len(p.calls), len(f.delivered))
	}
}

func TestPushResultsPerDevice(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1),
		row(3, uuid.New(), passOne, KindRebooted, "C", 1), row(4, userC, passOne, KindRebooted, "D", 1)},
		devices: map[uuid.UUID][]Device{
			userA: {dev("ios", "h_dead"), dev("android", "h_alive")},
			userB: {dev("ios", "h_busy")},
			userC: {dev("ios", "h_huge")},
		}}
	p := &fakePush{answer: map[string]PushResult{"h_dead": {Status: PushGone}, "h_busy": {Status: PushRetry, RetryAfter: 10 * time.Minute},
		"h_huge": {Status: PushDropped}}}
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	s := NewSender(f, SenderConfig{Push: p, PushID: "inst_abc", Now: func() time.Time { return now }})
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.removed) != 1 || f.removed[0] != "h_dead" {
		t.Fatalf("removed %v", f.removed)
	}
	// A reached its live device; B was rate limited (held back as asked); C has no device; D's message was refused, so
	// it is done for that device and never resent.
	if len(f.delivered) != 3 || f.delivered[0] != 1 || f.delivered[1] != 3 || f.delivered[2] != 4 {
		t.Fatalf("delivered %v", f.delivered)
	}
	if len(f.retry) != 1 || f.retry[0].ID != 2 || !f.retry[0].NotBefore.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("retry %+v", f.retry)
	}
	huge := dev("ios", "h_huge")
	found := false
	for _, m := range f.marks {
		found = found || (m.OutboxID == 4 && m.Session == huge.Session)
	}
	if !found {
		t.Fatalf("dropped device not marked: %+v", f.marks)
	}
}

// A refusal from the push server retries every notice and stops: the rest would meet the same answer.
func TestPushWholeCallRefusalRetriesEveryNoticeAndStops(t *testing.T) {
	f := &fakeSend{devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	for i := 1; i <= 150; i++ {
		f.rows = append(f.rows, row(int64(i), userA, passOne, KindRebooted, "A", 1))
	}
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, callErr := range []error{&PushError{Status: 403, Code: "suspended", msg: "push server answered 403 suspended"},
		&PushError{Status: 503, RetryAfter: time.Hour, msg: "push server answered 503"}} {
		p := &fakePush{callErr: callErr}
		s := NewSender(f, SenderConfig{Push: p, PushID: "inst_abc", Now: func() time.Time { return now }})
		if err := s.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(p.calls) != 1 || len(f.delivered) != 0 || len(f.retry) != 150 {
			t.Fatalf("%v: calls %d delivered %d retry %d", callErr, len(p.calls), len(f.delivered), len(f.retry))
		}
		var pe *PushError
		if errors.As(callErr, &pe) && pe.RetryAfter > 0 && !f.retry[149].NotBefore.Equal(now.Add(time.Hour)) {
			t.Fatalf("not held back: %+v", f.retry[149])
		}
	}
}

// A call with no answer (unreachable, or it timed out after sending part of the batch) retries only its batch; the
// rest is left untried, so the next pass reaches it while the batch waits out its delay.
func TestPushCallWithoutAnAnswerRetriesOnlyItsBatch(t *testing.T) {
	f := &fakeSend{devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	for i := 1; i <= 150; i++ {
		f.rows = append(f.rows, row(int64(i), userA, passOne, KindRebooted, "A", 1))
	}
	for _, callErr := range []error{&PushError{msg: "push server unreachable"}, errors.New("network")} {
		p := &fakePush{callErr: callErr}
		if err := pushSender(f, p).Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(p.calls) != 1 || len(f.retry) != MaxPushBatch || f.retry[0].ID != 1 || len(f.delivered) != 0 ||
			len(f.settled.PushDone) != 0 || len(f.settled.PushRetry) != MaxPushBatch {
			t.Fatalf("%v: calls %d retry %d settled %+v", callErr, len(p.calls), len(f.retry), f.settled)
		}
	}
}

// Each earlier push failure halves the batch a notice goes in, down to one.
func TestPushBatchShrinksAfterFailures(t *testing.T) {
	if batchLimit(0) != MaxPushBatch || batchLimit(1) != MaxPushBatch/2 || batchLimit(2) != MaxPushBatch/4 || batchLimit(10) != 1 {
		t.Fatal(batchLimit(0), batchLimit(1), batchLimit(2), batchLimit(10))
	}
	f := &fakeSend{devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	for i := 1; i <= 120; i++ {
		r := row(int64(i), userA, passOne, KindRebooted, "A", 1)
		if i <= 60 {
			r.PushAttempts = 1
		}
		f.rows = append(f.rows, r)
	}
	p := &fakePush{}
	_ = pushSender(f, p).Run(context.Background())
	var sizes []int
	for _, c := range p.calls {
		sizes = append(sizes, len(c))
	}
	// A batch takes the smallest limit of what it holds: 50, then 10 retried and 40 fresh, then the last 20 fresh.
	if len(sizes) != 3 || sizes[0] != MaxPushBatch/2 || sizes[1] != MaxPushBatch/2 || sizes[2] != 20 || len(f.delivered) != 120 {
		t.Fatalf("batches %v delivered %d", sizes, len(f.delivered))
	}
}

// Mail and push retry apart: a failing push server neither holds back the mail nor counts against it, and the capped
// mails stay untried.
func TestEmailCapAndPushFailureAreIndependent(t *testing.T) {
	f := &fakeSend{devices: map[uuid.UUID][]Device{}}
	for i := 1; i <= MaxMailsPerPass+4; i++ {
		u := uuid.New()
		f.rows = append(f.rows, row(int64(i), u, passOne, KindRebooted, "A", 1))
		f.devices[u] = []Device{dev("ios", "h_"+u.String())}
	}
	m, p := &fakeMail{}, &fakePush{callErr: &PushError{Status: 429, Code: "rate_limited", RetryAfter: 6 * time.Hour, msg: "429"}}
	_ = NewSender(f, SenderConfig{Mail: m, Push: p, PushID: "inst_abc", PublicURL: publicURL}).Run(context.Background())
	st := f.settled
	if len(m.got) != MaxMailsPerPass || len(f.emailSent) != MaxMailsPerPass || len(st.EmailRetry) != 0 || len(st.EmailDone) != 0 ||
		len(st.PushHold) != MaxMailsPerPass+4 || len(st.PushRetry) != 0 || len(st.PushDone) != 0 {
		t.Fatalf("mails %d sent %d settled %+v", len(m.got), len(f.emailSent), st)
	}
}

func TestPushRetryOnlyReachesTheDevicesStillMissingIt(t *testing.T) {
	ok, down := dev("ios", "h_ok"), dev("android", "h_down")
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}, devices: map[uuid.UUID][]Device{userA: {ok, down}}}
	p := &fakePush{answer: map[string]PushResult{"h_down": {Status: PushRetry}}}
	_ = pushSender(f, p).Run(context.Background())
	if len(f.marks) != 1 || f.marks[0].Session != ok.Session || len(f.retry) != 1 {
		t.Fatalf("marks %+v retry %v", f.marks, f.retry)
	}
	f.reached = map[int64]map[uuid.UUID]bool{1: {ok.Session: true}}
	f.marks, p.calls, p.answer = nil, nil, nil
	_ = pushSender(f, p).Run(context.Background())
	if sent := p.sent(); len(sent) != 1 || sent[0].Handle != "h_down" || len(f.delivered) != 1 || len(f.retry) != 0 {
		t.Fatalf("sent %+v delivered %v retry %v", sent, f.delivered, f.retry)
	}
}

// One pass sends both channels; a row is done once each channel the user has on is done.
func TestBothChannelsInOnePass(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	m, p := &fakeMail{}, &fakePush{}
	s := NewSender(f, SenderConfig{Mail: m, Push: p, PushID: "inst_abc", PublicURL: publicURL})
	_ = s.Run(context.Background())
	if len(m.got) != 1 || len(p.sent()) != 1 || len(f.emailSent) != 1 || len(f.delivered) != 1 {
		t.Fatalf("mails %d pushes %d emailSent %v delivered %v", len(m.got), len(p.sent()), f.emailSent, f.delivered)
	}
}

// A failed push retries the row, but the mail already went out: the retry sends only the push.
func TestRetryingOneChannelNeverRepeatsTheOther(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	m, p := &fakeMail{}, &fakePush{answer: map[string]PushResult{"h_a": {Status: PushRetry}}}
	s := NewSender(f, SenderConfig{Mail: m, Push: p, PushID: "inst_abc", PublicURL: publicURL})
	_ = s.Run(context.Background())
	if len(m.got) != 1 || len(f.emailSent) != 1 || len(f.retry) != 1 || len(f.delivered) != 0 {
		t.Fatalf("mails %d emailSent %v retry %v delivered %v", len(m.got), f.emailSent, f.retry, f.delivered)
	}
	f.rows[0].EmailDue = false
	p.answer = nil
	_ = s.Run(context.Background())
	if len(m.got) != 1 || len(p.sent()) != 2 || len(f.delivered) != 1 {
		t.Fatalf("mails %d pushes %d delivered %v", len(m.got), len(p.sent()), f.delivered)
	}
	// And the other way round: a refused mail retries the row, the push is not repeated.
	f2 := &fakeSend{rows: []OutboxRow{row(2, userA, passOne, KindRebooted, "A", 1)}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	m2, p2 := &fakeMail{fail: errors.New("smtp down")}, &fakePush{}
	s2 := NewSender(f2, SenderConfig{Mail: m2, Push: p2, PushID: "inst_abc", PublicURL: publicURL})
	_ = s2.Run(context.Background())
	if len(f2.retry) != 1 || len(f2.marks) != 1 {
		t.Fatalf("retry %v marks %v", f2.retry, f2.marks)
	}
}

// A channel the user turned off, or one with nowhere to go, counts as done.
func TestChannelsTheUserTurnedOff(t *testing.T) {
	noEmail := row(1, userA, passOne, KindRebooted, "A", 1)
	noEmail.AlertEmail = false
	noPush := row(2, userB, passOne, KindRebooted, "B", 1)
	noPush.AlertPush = false
	f := &fakeSend{rows: []OutboxRow{noEmail, noPush}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}, userB: {dev("ios", "h_b")}}}
	m, p := &fakeMail{}, &fakePush{}
	_ = NewSender(f, SenderConfig{Mail: m, Push: p, PushID: "inst_abc", PublicURL: publicURL}).Run(context.Background())
	if len(m.got) != 1 || m.got[0].text == "" || len(p.sent()) != 1 || p.sent()[0].Handle != "h_a" || len(f.delivered) != 2 {
		t.Fatalf("mails %+v pushes %+v delivered %v", m.got, p.sent(), f.delivered)
	}
	// A server that offers neither channel still finishes the rows.
	f2 := &fakeSend{rows: []OutboxRow{row(3, userA, passOne, KindRebooted, "A", 1)}}
	_ = NewSender(f2, SenderConfig{}).Run(context.Background())
	if len(f2.delivered) != 1 {
		t.Fatalf("delivered %v", f2.delivered)
	}
}

// A rate limit holds the push as long as asked and is not a failed attempt; a failure on another device of the same
// notice still is.
func TestPushRateLimitHoldsWithoutAnAttempt(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1)},
		devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_quota")}, userB: {dev("ios", "h_quota2"), dev("ios", "h_broken")}}}
	p := &fakePush{answer: map[string]PushResult{"h_quota": {Status: PushLimited, RetryAfter: 3 * time.Hour},
		"h_quota2": {Status: PushLimited, RetryAfter: time.Hour}, "h_broken": {Status: PushRetry}}}
	_ = NewSender(f, SenderConfig{Push: p, PushID: "inst_abc", Now: func() time.Time { return now }}).Run(context.Background())
	st := f.settled
	if len(st.PushHold) != 1 || st.PushHold[0].ID != 1 || !st.PushHold[0].NotBefore.Equal(now.Add(3*time.Hour)) ||
		len(st.PushRetry) != 1 || st.PushRetry[0].ID != 2 || len(st.PushDone) != 0 {
		t.Fatalf("%+v", st)
	}
	// A limit with no wait still holds, for a minute.
	f.rows, f.devices = f.rows[:1], map[uuid.UUID][]Device{userA: {dev("ios", "h_quota")}}
	p.answer = map[string]PushResult{"h_quota": {Status: PushLimited}}
	_ = NewSender(f, SenderConfig{Push: p, PushID: "inst_abc", Now: func() time.Time { return now }}).Run(context.Background())
	if st := f.settled; len(st.PushHold) != 1 || !st.PushHold[0].NotBefore.Equal(now.Add(time.Minute)) {
		t.Fatalf("%+v", st)
	}
}

func TestPushWaitStartsAtRelayReply(t *testing.T) {
	start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	reply := start.Add(90 * time.Second)
	for _, tc := range []struct {
		name string
		item PushResult
		err  error
		hold bool
		wait time.Duration
	}{
		{"item limited", PushResult{Status: PushLimited, RetryAfter: 30 * time.Second}, nil, true, 30 * time.Second},
		{"item fallback", PushResult{Status: PushLimited}, nil, true, defaultLimitWait},
		{"item failed", PushResult{Status: PushRetry, RetryAfter: time.Minute}, nil, false, time.Minute},
		{"batch limited", PushResult{}, &PushError{Status: 429, RetryAfter: time.Minute}, true, time.Minute},
		{"batch fallback", PushResult{}, &PushError{Status: 429}, true, defaultLimitWait},
		{"batch failed", PushResult{}, &PushError{Status: 503, RetryAfter: 20 * time.Second}, false, 20 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindGoneQuiet, "Hilltop", 30)},
				devices: map[uuid.UUID][]Device{userA: {dev("android", "h_fixture")}}}
			p := &fakePush{answer: map[string]PushResult{"h_fixture": tc.item}, callErr: tc.err}
			s := pushSender(f, p)
			s.now = func() time.Time {
				if len(p.calls) == 0 {
					return start
				}
				return reply
			}
			if err := s.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := f.settled.PushRetry
			if tc.hold {
				got = f.settled.PushHold
			}
			if len(got) != 1 || !got[0].NotBefore.Equal(reply.Add(tc.wait)) {
				t.Fatalf("settlement %v, want deadline %v", got, reply.Add(tc.wait))
			}
		})
	}
}

// A whole call refused with 429 holds every notice still to send, with no attempt counted, and stops.
func TestPushWholeCallRateLimitHoldsEverything(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	f := &fakeSend{devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	for i := 1; i <= 150; i++ {
		f.rows = append(f.rows, row(int64(i), userA, passOne, KindRebooted, "A", 1))
	}
	for _, tc := range []struct {
		after, want time.Duration
	}{{2 * time.Hour, 2 * time.Hour}, {0, time.Minute}} {
		p := &fakePush{callErr: &PushError{Status: 429, Code: "rate_limited", RetryAfter: tc.after, msg: "push server answered 429"}}
		_ = NewSender(f, SenderConfig{Push: p, PushID: "inst_abc", Now: func() time.Time { return now }}).Run(context.Background())
		st := f.settled
		if len(p.calls) != 1 || len(st.PushHold) != 150 || len(st.PushRetry) != 0 || !st.PushHold[149].NotBefore.Equal(now.Add(tc.want)) {
			t.Fatalf("after %v: calls %d hold %d retry %d", tc.after, len(p.calls), len(st.PushHold), len(st.PushRetry))
		}
	}
}
