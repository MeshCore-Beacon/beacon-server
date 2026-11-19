// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	mailer "github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/google/uuid"
)

const (
	// SendInterval is how often the outbox is drained.
	SendInterval = time.Minute
	// MaxAge: a notification not delivered within this long is dropped.
	MaxAge = 2 * time.Hour
	// PassTimeout: the notices of an evaluation pass that never finished (the process stopped) are sent after this.
	PassTimeout = 15 * time.Minute
	// MaxAttempts: a channel that failed this often for a notification gives up on it.
	MaxAttempts = 5
	sendBatch   = 300
	keepSent    = 24 * time.Hour
	// MailSendTimeout bounds one alert mail, so a slow SMTP server cannot hold the pass for long.
	MailSendTimeout = 30 * time.Second
	// pushPassBudget: no new push batch starts after this long into a pass; the rest waits for the next one.
	pushPassBudget = 2 * time.Minute
)

// OutboxRow is a queued notification with what delivery needs to know about its user.
type OutboxRow struct {
	ID            int64
	UserID, Pass  uuid.UUID
	TargetID      uuid.UUID // the radio's node or observer id, uuid.Nil when unknown
	TargetKind    string
	Key           []byte
	Name          string
	Kind, Detail  string
	Value         int64
	HasValue      bool // false when the notice has no figure
	Email         string
	EmailVerified bool
	// AlertEmail and AlertPush are the user's channel switches.
	AlertEmail, AlertPush bool
	// EmailDue and PushDue: the channel is not done and not held back, so this pass tries it. A channel that is
	// not due is left as it is.
	EmailDue, PushDue bool
	// PushAttempts is how often the push for this notice failed.
	PushAttempts int
}

// Device is a push handle of a live app session; Session identifies the device and Key is its payload key.
type Device struct {
	Session         uuid.UUID
	Platform, Token string
	Key             []byte
}

// DeviceMark records that a push notice is done for one device.
type DeviceMark struct {
	OutboxID int64
	Session  uuid.UUID
}

// Retry is a notice's channel to try again; NotBefore, when set, holds it back at least that long.
type Retry struct {
	ID        int64
	NotBefore time.Time
}

// Settled is what a pass decided for each channel. A notice in no list of a channel stays as it was there.
type Settled struct {
	IDs                   []int64 // every notice of the pass
	EmailDone, PushDone   []int64 // nothing (more) to send on the channel
	EmailRetry, PushRetry []Retry
	// PushHold: the push was rate limited, so it waits until NotBefore without counting an attempt.
	PushHold []Retry
}

// SendStore is what the sender needs from the database.
type SendStore interface {
	BeginSendLock(ctx context.Context) (Unlocker, bool, error)
	// Expire drops undelivered notifications created before undeliveredBefore and delivered ones from before
	// deliveredBefore.
	Expire(ctx context.Context, undeliveredBefore, deliveredBefore time.Time) (int64, error)
	// Pending returns undelivered notifications created at or after notBefore with a channel that is not done and
	// not held back for a retry after now, for live users, by user and pass.
	Pending(ctx context.Context, notBefore, now time.Time, limit int) ([]OutboxRow, error)
	Devices(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID][]Device, error)
	// DeliveredDevices returns, for each notice, the devices (sessions) it is already done for.
	DeliveredDevices(ctx context.Context, ids []int64) (map[int64]map[uuid.UUID]bool, error)
	MarkDevicesDelivered(ctx context.Context, marks []DeviceMark, now time.Time) error
	// MarkEmailSent records that the mail for these notices went out, which finishes their email channel.
	MarkEmailSent(ctx context.Context, ids []int64, now time.Time) error
	// Finish records the pass per channel: done channels are finished, a retried channel counts an attempt and is
	// held back with a growing delay (or until NotBefore when that is later) and gives up after maxAttempts, and a
	// held push waits until NotBefore with no attempt counted. A notice is delivered once both channels are done, and
	// dropped instead when one of them gave up.
	Finish(ctx context.Context, st Settled, now time.Time, maxAttempts int) error
	RemoveDeviceToken(ctx context.Context, token string) error
}

// MaxMailsPerPass caps the alert mails sent in one run.
const MaxMailsPerPass = 16

// SenderConfig picks the channels: email when Mail is set, push when Push is set.
type SenderConfig struct {
	Mail   mailer.Sender
	Push   PushSender
	PushID string // binds each sealed notice to this server
	// PublicURL is the web app's address, for the Atlas link in mail.
	PublicURL *url.URL
	Now       func() time.Time
}

// Sender drains the outbox by email and by push in one pass. Evaluation never waits on it.
type Sender struct {
	store    SendStore
	mail     mailer.Sender
	push     PushSender
	pushID   string
	atlasURL string
	name     string // the instance name for mail subjects
	tempUnit string // the instance temperature unit: "f" or "c"
	now      func() time.Time
}

func NewSender(store SendStore, cfg SenderConfig) *Sender {
	s := &Sender{store: store, mail: cfg.Mail, push: cfg.Push, pushID: cfg.PushID, name: "Beacon", now: cfg.Now}
	if s.now == nil {
		s.now = time.Now
	}
	if cfg.PublicURL != nil {
		u := *cfg.PublicURL
		u.Path, u.RawQuery = "/", "tab=Atlas"
		s.atlasURL = u.String()
	}
	return s
}

// outcome is where one channel stands for one notice after a pass. A notice absent from a channel's answer was
// not tried there (not due, capped, or the pass stopped early) and stays as it was, with no attempt counted.
type outcome struct {
	failed  bool      // try again later, counting an attempt; otherwise done
	limited bool      // rate limited: wait for hold, no attempt counted (failed wins when both)
	hold    time.Time // with failed or limited: not before this
}

// Run drains one batch. It does nothing when another process is sending.
func (s *Sender) Run(ctx context.Context) error {
	lock, ok, err := s.store.BeginSendLock(ctx)
	if err != nil || !ok {
		return err
	}
	defer lock.Close(ctx)
	now := s.now()
	if n, err := s.store.Expire(ctx, now.Add(-MaxAge), now.Add(-keepSent)); err != nil {
		return err
	} else if n > 0 {
		slog.Info("alert outbox cleaned", "component", "alerts", "dropped", n)
	}
	rows, err := s.store.Pending(ctx, now.Add(-MaxAge), now, sendBatch)
	if err != nil || len(rows) == 0 {
		return err
	}
	email, err := s.sendEmail(ctx, rows, now)
	if err != nil {
		return err
	}
	push, err := s.sendPush(ctx, rows, now)
	if err != nil {
		return err
	}
	var st Settled
	for _, r := range rows {
		st.IDs = append(st.IDs, r.ID)
		if o, ok := email[r.ID]; ok {
			if o.failed {
				st.EmailRetry = append(st.EmailRetry, Retry{ID: r.ID, NotBefore: o.hold})
			} else {
				st.EmailDone = append(st.EmailDone, r.ID)
			}
		}
		if o, ok := push[r.ID]; ok {
			switch {
			case o.failed:
				st.PushRetry = append(st.PushRetry, Retry{ID: r.ID, NotBefore: o.hold})
			case o.limited:
				st.PushHold = append(st.PushHold, Retry{ID: r.ID, NotBefore: o.hold})
			default:
				st.PushDone = append(st.PushDone, r.ID)
			}
		}
	}
	return s.store.Finish(ctx, st, now, MaxAttempts)
}

func (s *Sender) wantsEmail(r OutboxRow) bool {
	return s.mail != nil && r.AlertEmail && r.Email != "" && r.EmailVerified
}

// sendEmail sends one message for each user and pass whose mail is due, straight through the mailer, and records a
// notice's mail as sent only once the SMTP server accepted it. A failed send is retried later and ends the email part
// of the pass, since the rest would meet the same server; the capped rest waits for the next pass. Due rows that need
// no mail are done; sent rows are recorded here and left out of the answer.
func (s *Sender) sendEmail(ctx context.Context, rows []OutboxRow, now time.Time) (map[int64]outcome, error) {
	type group struct {
		first   OutboxRow
		ids     []int64
		notices []EmailNotice
	}
	var groups []*group
	byKey := map[[2]uuid.UUID]*group{}
	out := map[int64]outcome{}
	for _, r := range rows {
		if !r.EmailDue {
			continue
		}
		if !s.wantsEmail(r) {
			out[r.ID] = outcome{}
			continue
		}
		k := [2]uuid.UUID{r.UserID, r.Pass}
		g := byKey[k]
		if g == nil {
			g = &group{first: r}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.ids = append(g.ids, r.ID)
		g.notices = append(g.notices, EmailNotice{Kind: r.Kind, Name: r.Name, Detail: r.Detail, Value: r.Value})
	}
	var sentIDs []int64
	sent := 0
	for _, g := range groups {
		if sent >= MaxMailsPerPass || ctx.Err() != nil {
			break
		}
		subject, text := ComposeEmail(s.name, g.notices, s.atlasURL, s.tempUnit)
		sctx, cancel := context.WithTimeout(ctx, MailSendTimeout)
		err := s.mail.Send(sctx, mailer.Message{To: g.first.Email, Subject: subject, Text: text})
		cancel()
		if err != nil {
			slog.Warn("alert mail not sent", "component", "alerts", "user_id", g.first.UserID, "err", err)
			for _, id := range g.ids {
				out[id] = outcome{failed: true}
			}
			break
		}
		sent++
		sentIDs = append(sentIDs, g.ids...)
	}
	if len(sentIDs) > 0 {
		if err := s.store.MarkEmailSent(ctx, sentIDs, now); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// SetTemperatureUnit sets the unit alert text gives temperatures in: "f" or "c" (anything else reads as c).
func (s *Sender) SetTemperatureUnit(unit string) {
	s.tempUnit = unit
}

// SetInstanceName sets the name alert mail subjects carry; blank keeps "Beacon".
func (s *Sender) SetInstanceName(name string) {
	if name != "" {
		s.name = name
	}
}

type pushItem struct {
	row    int64
	user   uuid.UUID
	device Device
	msg    PushMessage
	limit  int // the largest batch this item may go in
}

// batchLimit halves the batch for each earlier push failure of the notice, so a batch that timed out goes again in
// smaller pieces instead of failing the same way. A rate limit is not a failure and does not shrink it.
func batchLimit(attempts int) int {
	if attempts >= 8 {
		return 1
	}
	return max(1, MaxPushBatch>>attempts)
}

// sendPush seals each due notice for each of the user's live devices that is not done with it yet and sends them in
// batches of at most MaxPushBatch. A notice is done once every device is (or the user has none); while any device must
// retry, so must the notice, and the retry goes only to the devices still missing it. A dead handle is deleted and
// its device no longer counts; a message the push server refused is done for that device. A rate-limited message
// waits as long as asked without counting an attempt, so a server over its quota keeps its notices until the quota
// resets or MaxAge passes. A call that fails as a whole retries its batch and ends the push part of the pass. When
// the push server answered, every notice still to send waits too (held, for a rate limit; retried otherwise); when it
// did not (unreachable, or too slow to answer), the rest is left for the next pass, so the notices of a slow batch
// cannot keep the others from ever being tried.
func (s *Sender) sendPush(ctx context.Context, rows []OutboxRow, now time.Time) (map[int64]outcome, error) {
	out := map[int64]outcome{}
	seen := map[uuid.UUID]bool{}
	var users []uuid.UUID
	var ids []int64
	for _, r := range rows {
		if !r.PushDue {
			continue
		}
		out[r.ID] = outcome{} // done unless a device below says otherwise
		if s.push == nil || !r.AlertPush {
			continue
		}
		ids = append(ids, r.ID)
		if !seen[r.UserID] {
			seen[r.UserID] = true
			users = append(users, r.UserID)
		}
	}
	if len(ids) == 0 {
		return out, nil
	}
	devices, err := s.store.Devices(ctx, users)
	if err != nil {
		return nil, err
	}
	reached, err := s.store.DeliveredDevices(ctx, ids)
	if err != nil {
		return nil, err
	}
	var marks []DeviceMark
	var items []pushItem
	for _, r := range rows {
		if !r.PushDue || !r.AlertPush {
			continue
		}
		plain := NoticePayload(r, s.tempUnit)
		for _, d := range devices[r.UserID] {
			if reached[r.ID][d.Session] {
				continue
			}
			ct, err := SealNotice(d.Key, s.pushID, plain)
			if err != nil {
				slog.Warn("push notice not sealed", "component", "alerts", "user_id", r.UserID, "err", err)
				marks = append(marks, DeviceMark{OutboxID: r.ID, Session: d.Session})
				continue
			}
			items = append(items, pushItem{row: r.ID, user: r.UserID, device: d, limit: batchLimit(r.PushAttempts),
				msg: PushMessage{Handle: d.Token, Ciphertext: ct, CollapseKey: strconv.FormatInt(r.ID, 10),
					ID: strconv.FormatInt(r.ID, 10) + ":" + d.Session.String()}})
		}
	}
	fail := func(id int64, hold time.Time) {
		o := out[id]
		o.failed = true
		if hold.After(o.hold) {
			o.hold = hold
		}
		out[id] = o
	}
	limit := func(id int64, hold time.Time) {
		o := out[id]
		o.limited = true
		if hold.After(o.hold) {
			o.hold = hold
		}
		out[id] = o
	}
	// wait leaves the notices of these items untried, unless they already failed or were held this pass.
	wait := func(rest []pushItem) {
		for _, it := range rest {
			if o := out[it.row]; !o.failed && !o.limited {
				delete(out, it.row)
			}
		}
	}
	gone := map[string]bool{}
	dropped := 0
	started := time.Now()
	for i := 0; i < len(items); {
		if ctx.Err() != nil || time.Since(started) > pushPassBudget {
			wait(items[i:])
			break
		}
		var batch []pushItem
		size := MaxPushBatch
		for ; i < len(items) && len(batch) < size; i++ {
			it := items[i]
			if gone[it.device.Token] {
				continue
			}
			if len(batch) >= it.limit {
				break
			}
			batch = append(batch, it)
			size = min(size, it.limit)
		}
		if len(batch) == 0 {
			continue
		}
		msgs := make([]PushMessage, len(batch))
		for j, it := range batch {
			msgs[j] = it.msg
		}
		res, err := s.push.Send(ctx, msgs)
		// Retry-After starts at the relay's reply, even when mail or earlier batches took time.
		receivedAt := s.now()
		if err != nil {
			var pe *PushError
			var hold time.Time
			answered := false
			if errors.As(err, &pe) {
				answered = pe.Status != 0
				if pe.RetryAfter > 0 {
					hold = receivedAt.Add(pe.RetryAfter)
				}
				if pe.Limited() {
					if hold.IsZero() {
						hold = receivedAt.Add(defaultLimitWait)
					}
					slog.Warn("push server rate limited this server: alert push notifications wait", "component", "alerts", "until", hold)
					for _, it := range batch {
						limit(it.row, hold)
					}
					for _, it := range items[i:] {
						if !gone[it.device.Token] {
							limit(it.row, hold)
						}
					}
					break
				}
				if pe.Suspended() {
					slog.Error("push server refused this server: alert push notifications will be retried", "component", "alerts", "err", err)
				} else {
					slog.Warn("alert push not sent", "component", "alerts", "err", err)
				}
			} else {
				slog.Warn("alert push not sent", "component", "alerts", "err", err)
			}
			for _, it := range batch {
				fail(it.row, hold)
			}
			if answered {
				for _, it := range items[i:] {
					if !gone[it.device.Token] {
						fail(it.row, hold)
					}
				}
			} else {
				wait(items[i:])
			}
			break
		}
		for j, it := range batch {
			switch res[j].Status {
			case PushDelivered:
				marks = append(marks, DeviceMark{OutboxID: it.row, Session: it.device.Session})
			case PushDropped:
				dropped++
				marks = append(marks, DeviceMark{OutboxID: it.row, Session: it.device.Session})
			case PushLimited:
				wait := res[j].RetryAfter
				if wait <= 0 {
					wait = defaultLimitWait
				}
				limit(it.row, receivedAt.Add(wait))
			case PushGone:
				if !gone[it.device.Token] {
					gone[it.device.Token] = true
					if err := s.store.RemoveDeviceToken(ctx, it.device.Token); err != nil {
						slog.Warn("push device not removed", "component", "alerts", "user_id", it.user, "err", err)
					}
				}
			default:
				var hold time.Time
				if res[j].RetryAfter > 0 {
					hold = receivedAt.Add(res[j].RetryAfter)
				}
				fail(it.row, hold)
			}
		}
	}
	if dropped > 0 {
		slog.Warn("push server refused alert notices as invalid or too large", "component", "alerts", "count", dropped)
	}
	if err := s.store.MarkDevicesDelivered(ctx, marks, now); err != nil {
		return nil, err
	}
	return out, nil
}
