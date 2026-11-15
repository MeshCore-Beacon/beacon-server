// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
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
	retry     []int64
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

// dev is a device whose id follows its token.
func dev(platform, token string) Device {
	return Device{Session: uuid.NewSHA1(uuid.NameSpaceOID, []byte(token)), Platform: platform, Token: token}
}

func (f *fakeSend) Finish(_ context.Context, d, r []int64, _ time.Time, max int) error {
	f.delivered, f.retry = d, r
	if max != MaxAttempts {
		panic("attempts")
	}
	return nil
}

func (f *fakeSend) RemoveDeviceToken(_ context.Context, tok string) error {
	f.removed = append(f.removed, tok)
	return nil
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

type fakePush struct {
	sent []PushMessage
	fail map[string]error
}

func (p *fakePush) Send(_ context.Context, m PushMessage) error {
	if err := p.fail[m.Token]; err != nil {
		return err
	}
	p.sent = append(p.sent, m)
	return nil
}

var (
	userA, userB = uuid.New(), uuid.New()
	passOne      = uuid.New()
	publicURL, _ = url.Parse("https://beacon.example.org")
)

func row(id int64, user, pass uuid.UUID, kind, name string, value int64) OutboxRow {
	key := make([]byte, 32)
	key[0] = byte(id)
	return OutboxRow{ID: id, UserID: user, Pass: pass, TargetKind: "node", Key: key, Name: name, Kind: kind, Value: value, Email: "a@example.org", EmailVerified: true}
}

func TestEmailGroupsOnePassIntoOneMessage(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{
		row(1, userA, passOne, KindGoneQuiet, "Hilltop", 30), row(2, userA, passOne, KindBatteryLow, "Hilltop", 3400),
		row(3, userA, uuid.New(), KindRebooted, "Hilltop", 5), row(4, userB, passOne, KindBackOnline, "Ridge", 0)}}
	f.rows[3].Email = "b@example.org"
	m := &fakeMail{}
	if err := NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background()); err != nil {
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
	if err := NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background()); err != nil {
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
	_ = NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background())
	if strings.Count(m.got[0].text, "n") > 60 {
		t.Fatalf("name not capped: %q", m.got[0].text)
	}
}

func TestEmailWithoutAVerifiedAddressDropsAndAFailedSendRetries(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1)}}
	f.rows[0].EmailVerified = false
	f.rows[1].Email = ""
	m := &fakeMail{}
	_ = NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background())
	if len(m.got) != 0 || len(f.delivered) != 2 {
		t.Fatalf("mails %d delivered %v", len(m.got), f.delivered)
	}
	f.rows = []OutboxRow{row(7, userA, passOne, KindRebooted, "A", 1)}
	m.fail = errors.New("smtp: 451 try later")
	_ = NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background())
	// A mail the server refused is not delivered: it is retried, which counts an attempt and delays it.
	if len(f.retry) != 1 || f.retry[0] != 7 || len(f.delivered) != 0 {
		t.Fatalf("delivered %v retry %v", f.delivered, f.retry)
	}
}

// One refusal ends the run: the others would meet the same server, so they stay pending without an attempt.
func TestEmailFailureStopsThePassAndLeavesTheRestPending(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1)}}
	m := &fakeMail{fail: errors.New("smtp down")}
	_ = NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background())
	if m.calls != 1 || len(f.retry) != 1 || f.retry[0] != 1 || len(f.delivered) != 0 {
		t.Fatalf("calls %d delivered %v retry %v", m.calls, f.delivered, f.retry)
	}
}

func TestEmailPassIsCapped(t *testing.T) {
	f := &fakeSend{}
	for i := 1; i <= MaxMailsPerPass+5; i++ {
		f.rows = append(f.rows, row(int64(i), uuid.New(), passOne, KindRebooted, "A", 1))
	}
	m := &fakeMail{}
	_ = NewSender(f, "email", m, nil, publicURL, nil).Run(context.Background())
	if len(m.got) != MaxMailsPerPass || len(f.delivered) != MaxMailsPerPass || len(f.retry) != 0 {
		t.Fatalf("mails %d delivered %d retry %d", len(m.got), len(f.delivered), len(f.retry))
	}
}

func TestPushMessagePerNoticePerDevice(t *testing.T) {
	r := row(1, userA, passOne, KindBatteryLow, "Hill\ntop", 3400)
	f := &fakeSend{rows: []OutboxRow{r}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "tok-1"), dev("android", "tok-2")}}}
	p := &fakePush{}
	if err := NewSender(f, "push", nil, p, nil, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 2 || p.sent[0].Token != "tok-1" || p.sent[1].Token != "tok-2" {
		t.Fatalf("%+v", p.sent)
	}
	m := p.sent[0]
	if m.Title != "Hill top" || m.Body != "Hill top battery is low: 3.40 V." || len(m.Data) != 2 || m.Data["targetKind"] != "node" ||
		m.Data["publicKey"] != "0100000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("%+v", m)
	}
	if len(f.delivered) != 1 || len(f.retry) != 0 {
		t.Fatalf("delivered %v retry %v", f.delivered, f.retry)
	}
}

func TestPushCarriesTargetIDWhenKnown(t *testing.T) {
	id := uuid.New()
	known, unknown := row(1, userA, passOne, KindBatteryLow, "Hilltop", 3400), row(2, userA, passOne, KindBatteryLow, "Gone", 3400)
	known.TargetID = id
	f := &fakeSend{rows: []OutboxRow{known, unknown}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "tok-1")}}}
	p := &fakePush{}
	_ = NewSender(f, "push", nil, p, nil, nil).Run(context.Background())
	if len(p.sent) != 2 || p.sent[0].Data["id"] != id.String() || len(p.sent[0].Data) != 3 {
		t.Fatalf("%+v", p.sent[0])
	}
	if _, ok := p.sent[1].Data["id"]; ok || len(p.sent[1].Data) != 2 {
		t.Fatalf("unknown radio carries an id: %+v", p.sent[1])
	}
}

func TestPushObserverTargetKind(t *testing.T) {
	r := row(1, userA, passOne, KindObserverOff, "North", 120)
	r.TargetKind = "observer"
	f := &fakeSend{rows: []OutboxRow{r}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "tok-1")}}}
	p := &fakePush{}
	_ = NewSender(f, "push", nil, p, nil, nil).Run(context.Background())
	if p.sent[0].Data["targetKind"] != "observer" {
		t.Fatalf("%+v", p.sent[0])
	}
}

func TestPushUnregisteredTokenIsDeletedAndFailuresRetry(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1), row(2, userB, passOne, KindRebooted, "B", 1), row(3, uuid.New(), passOne, KindRebooted, "C", 1)},
		devices: map[uuid.UUID][]Device{
			userA: {dev("ios", "dead"), dev("android", "alive")},
			userB: {dev("ios", "down")},
		}}
	p := &fakePush{fail: map[string]error{"dead": ErrUnregistered, "down": errors.New("push service answered 500")}}
	if err := NewSender(f, "push", nil, p, nil, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.removed) != 1 || f.removed[0] != "dead" {
		t.Fatalf("removed %v", f.removed)
	}
	// A reached one device; B's only device failed (retry); C has no device (nobody to tell: delivered).
	if len(f.delivered) != 2 || f.delivered[0] != 1 || f.delivered[1] != 3 || len(f.retry) != 1 || f.retry[0] != 2 {
		t.Fatalf("delivered %v retry %v", f.delivered, f.retry)
	}
	// A user whose only token is gone has nobody to tell either.
	f2 := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}, devices: map[uuid.UUID][]Device{userA: {dev("ios", "dead")}}}
	_ = NewSender(f2, "push", nil, &fakePush{fail: map[string]error{"dead": ErrUnregistered}}, nil, nil).Run(context.Background())
	if len(f2.delivered) != 1 || len(f2.retry) != 0 {
		t.Fatalf("delivered %v retry %v", f2.delivered, f2.retry)
	}
}

func TestSenderWindowsAndGuard(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	f := &fakeSend{}
	if err := NewSender(f, "email", &fakeMail{}, nil, publicURL, func() time.Time { return now }).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Older than two hours is dropped (not read, and deleted), delivered ones are kept a day.
	if !f.notBefore.Equal(now.Add(-2*time.Hour)) || !f.expiredAt[0].Equal(now.Add(-2*time.Hour)) || !f.expiredAt[1].Equal(now.Add(-24*time.Hour)) {
		t.Fatalf("%v %v", f.notBefore, f.expiredAt)
	}
	held := &fakeSend{locked: true, rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}}
	m := &fakeMail{}
	_ = NewSender(held, "email", m, nil, publicURL, nil).Run(context.Background())
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
		if got := Sentence(tc.kind, "N", tc.value, tc.detail); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.kind, got, tc.want)
		}
	}
}

// One device took the notice and the other answered 503: the notice is not delivered, it is retried.
func TestPushOneDeviceFailingKeepsTheNotice(t *testing.T) {
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)},
		devices: map[uuid.UUID][]Device{userA: {dev("ios", "ok-tok"), dev("android", "down-tok")}}}
	p := &fakePush{fail: map[string]error{"down-tok": errors.New("push service answered 503")}}
	if err := NewSender(f, "push", nil, p, nil, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.delivered) != 0 || len(f.retry) != 1 || f.retry[0] != 1 {
		t.Fatalf("delivered %v retry %v", f.delivered, f.retry)
	}
}

// The retry goes only to the device that did not get the notice.
func TestPushRetryOnlyReachesTheDevicesStillMissingIt(t *testing.T) {
	ok, down := dev("ios", "ok-tok"), dev("android", "down-tok")
	f := &fakeSend{rows: []OutboxRow{row(1, userA, passOne, KindRebooted, "A", 1)}, devices: map[uuid.UUID][]Device{userA: {ok, down}}}
	p := &fakePush{fail: map[string]error{"down-tok": errors.New("push service answered 503")}}
	if err := NewSender(f, "push", nil, p, nil, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.marks) != 1 || f.marks[0].Session != ok.Session || len(p.sent) != 1 {
		t.Fatalf("marks %+v sent %d", f.marks, len(p.sent))
	}
	// Next run: the store reports the first device as reached; only the second is tried, and it works now.
	f.reached = map[int64]map[uuid.UUID]bool{1: {ok.Session: true}}
	f.marks, p.sent, p.fail = nil, nil, nil
	if err := NewSender(f, "push", nil, p, nil, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].Token != "down-tok" || len(f.delivered) != 1 || len(f.retry) != 0 {
		t.Fatalf("sent %+v delivered %v retry %v", p.sent, f.delivered, f.retry)
	}
}
