// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/url"
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
	// MaxAttempts: a notification that failed this often is dropped.
	MaxAttempts = 5
	sendBatch   = 300
	keepSent    = 24 * time.Hour
	// MailSendTimeout bounds one alert mail, so a slow SMTP server cannot hold the pass for long.
	MailSendTimeout = 30 * time.Second
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
	Attempts      int
	Email         string
	EmailVerified bool
}

// Device is a registered push token of a live app session; Session identifies the device.
type Device struct {
	Session         uuid.UUID
	Platform, Token string
}

// DeviceMark records that a push notice reached one device.
type DeviceMark struct {
	OutboxID int64
	Session  uuid.UUID
}

// SendStore is what the sender needs from the database.
type SendStore interface {
	BeginSendLock(ctx context.Context) (Unlocker, bool, error)
	// Expire drops undelivered notifications created before undeliveredBefore and delivered ones from before
	// deliveredBefore.
	Expire(ctx context.Context, undeliveredBefore, deliveredBefore time.Time) (int64, error)
	// Pending returns undelivered notifications created at or after notBefore, and not held back for a retry
	// after now, for live users, by user and pass.
	Pending(ctx context.Context, notBefore, now time.Time, limit int) ([]OutboxRow, error)
	Devices(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID][]Device, error)
	// DeliveredDevices returns, for each notice, the devices (sessions) it already reached.
	DeliveredDevices(ctx context.Context, ids []int64) (map[int64]map[uuid.UUID]bool, error)
	MarkDevicesDelivered(ctx context.Context, marks []DeviceMark, now time.Time) error
	// Finish marks the delivered rows, counts an attempt for the retried ones (holding them back with a growing
	// delay) and drops the exhausted.
	Finish(ctx context.Context, delivered, retry []int64, now time.Time, maxAttempts int) error
	RemoveDeviceToken(ctx context.Context, token string) error
}

// MaxMailsPerPass caps the alert mails sent in one run.
const MaxMailsPerPass = 16

// Sender drains the outbox by email or by push. Evaluation never waits on it.
type Sender struct {
	store    SendStore
	delivery string
	mail     mailer.Sender
	push     PushSender
	atlasURL string
	now      func() time.Time
}

// NewSender builds the sender for one delivery mode. publicURL is the web app's address, for the Atlas link.
func NewSender(store SendStore, delivery string, mail mailer.Sender, push PushSender, publicURL *url.URL, now func() time.Time) *Sender {
	if now == nil {
		now = time.Now
	}
	s := &Sender{store: store, delivery: delivery, mail: mail, push: push, now: now}
	if publicURL != nil {
		u := *publicURL
		u.Path, u.RawQuery = "/", "tab=Atlas"
		s.atlasURL = u.String()
	}
	return s
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
	var delivered, retry []int64
	switch s.delivery {
	case "email":
		delivered, retry = s.sendEmail(ctx, rows)
	case "push":
		delivered, retry, err = s.sendPush(ctx, rows, now)
		if err != nil {
			return err
		}
	default:
		return nil
	}
	return s.store.Finish(ctx, delivered, retry, now, MaxAttempts)
}

// sendEmail sends one message for each user and pass, straight through the mailer, and reports a notice delivered
// only once the SMTP server accepted it. A user with no verified address has nothing to send to and the notices are
// dropped. A failed send is retried later (with a delay and an attempt counted) and ends the run, since the rest
// would meet the same server; the capped rest stays pending without an attempt.
func (s *Sender) sendEmail(ctx context.Context, rows []OutboxRow) (delivered, retry []int64) {
	type group struct {
		first   OutboxRow
		ids     []int64
		notices []EmailNotice
	}
	var groups []*group
	byKey := map[[2]uuid.UUID]*group{}
	for _, r := range rows {
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
	sent := 0
	for _, g := range groups {
		if g.first.Email == "" || !g.first.EmailVerified {
			delivered = append(delivered, g.ids...)
			continue
		}
		if sent >= MaxMailsPerPass || ctx.Err() != nil {
			continue
		}
		subject, text := ComposeEmail(g.notices, s.atlasURL)
		sctx, cancel := context.WithTimeout(ctx, MailSendTimeout)
		err := s.mail.Send(sctx, mailer.Message{To: g.first.Email, Subject: subject, Text: text})
		cancel()
		if err != nil {
			slog.Warn("alert mail not sent", "component", "alerts", "user_id", g.first.UserID, "err", err)
			retry = append(retry, g.ids...)
			break
		}
		sent++
		delivered = append(delivered, g.ids...)
	}
	return delivered, retry
}

// sendPush sends each notice to each of the user's live devices that has not got it yet, and records every device
// it reached. A notice is delivered once every device has it (or the user has none); while any device failed it is
// retried, and the retry goes only to the devices still missing it. An unregistered token is deleted and its device
// no longer counts.
func (s *Sender) sendPush(ctx context.Context, rows []OutboxRow, now time.Time) (delivered, retry []int64, err error) {
	seen := map[uuid.UUID]bool{}
	var users []uuid.UUID
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
		if !seen[r.UserID] {
			seen[r.UserID] = true
			users = append(users, r.UserID)
		}
	}
	devices, err := s.store.Devices(ctx, users)
	if err != nil {
		return nil, nil, err
	}
	reached, err := s.store.DeliveredDevices(ctx, ids)
	if err != nil {
		return nil, nil, err
	}
	var marks []DeviceMark
	gone := map[string]bool{}
	for _, r := range rows {
		if ctx.Err() != nil {
			break
		}
		devs := devices[r.UserID]
		if len(devs) == 0 {
			delivered = append(delivered, r.ID) // no app signed in: nobody to tell
			continue
		}
		msg := PushMessage{
			Title: SanitizeName(r.Name, r.Key),
			Body:  Sentence(r.Kind, SanitizeName(r.Name, r.Key), r.Value, r.Detail),
			Data:  map[string]string{"targetKind": r.TargetKind, "publicKey": hex.EncodeToString(r.Key)},
		}
		if r.TargetID != uuid.Nil {
			msg.Data["id"] = r.TargetID.String()
		}
		failed := false
		for _, d := range devs {
			if gone[d.Token] || reached[r.ID][d.Session] {
				continue
			}
			msg.Token = d.Token
			switch err := s.push.Send(ctx, msg); {
			case err == nil:
				marks = append(marks, DeviceMark{OutboxID: r.ID, Session: d.Session})
			case errors.Is(err, ErrUnregistered):
				gone[d.Token] = true
				if derr := s.store.RemoveDeviceToken(ctx, d.Token); derr != nil {
					slog.Warn("push token not removed", "component", "alerts", "user_id", r.UserID, "err", derr)
				}
			default:
				failed = true
				// err carries no token or credential, only a status.
				slog.Warn("push not sent", "component", "alerts", "user_id", r.UserID, "err", err)
			}
		}
		if failed {
			retry = append(retry, r.ID)
		} else {
			delivered = append(delivered, r.ID)
		}
	}
	if err := s.store.MarkDevicesDelivered(ctx, marks, now); err != nil {
		return nil, nil, err
	}
	return delivered, retry, nil
}
