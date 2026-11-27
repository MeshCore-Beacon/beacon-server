// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var (
	ErrNotOnAtlas  = errors.New("alerts: radio is not on the Atlas")
	ErrRuleMissing = errors.New("alerts: no such rule")
	ErrBadDevice   = errors.New("alerts: invalid device")
	ErrPushOff     = errors.New("alerts: push notifications are off on this server")
	ErrNoChannel   = errors.New("alerts: no channel given")
)

// StoredRule is a rule as the user sees it.
type StoredRule struct {
	ID             uuid.UUID
	Kind           string
	Threshold      *int
	High           *int
	Enabled        bool
	State          string
	StateChangedAt time.Time
	LastSentAt     *time.Time
}

// TargetInfo is one Atlas pin with what is known about the radio and its rules.
type TargetInfo struct {
	// ID is the node's or observer's id, uuid.Nil when the radio no longer exists.
	ID        uuid.UUID
	Kind      string // node or observer
	PublicKey string // lower-case hex
	Name      string
	Caps      Caps
	Rules     []StoredRule
}

// RuleInput is a rule to store.
type RuleInput struct {
	TargetKind string
	PublicKey  []byte
	Kind       string
	Threshold  *int
	High       *int
	Enabled    bool
}

// Store is what the service needs from the database; *db.Store satisfies it.
type Store interface {
	// Targets returns one entry per Atlas pin, in Atlas order.
	Targets(ctx context.Context, userID uuid.UUID, now time.Time) ([]TargetInfo, error)
	HasDevice(ctx context.Context, userID uuid.UUID) (bool, error)
	// PutRule stores the rule while holding the user's Atlas, so the radio cannot leave the Atlas meanwhile.
	// validate sees the radio's caps and whether it has a gone_quiet rule, and may refuse. ErrNotOnAtlas when the
	// radio is not pinned; users.ErrNotFound-like conditions come back as the store's own error.
	PutRule(ctx context.Context, userID uuid.UUID, in RuleInput, now time.Time,
		validate func(c Caps, hasGoneQuiet bool) (th, high *int, err error)) (StoredRule, error)
	DeleteRule(ctx context.Context, userID, id uuid.UUID) (bool, error)
	// RegisterDevice stores the handle and payload key of an app session and turns the user's push switch on.
	RegisterDevice(ctx context.Context, userID, sessionID uuid.UUID, platform, token string, key []byte) error
	RemoveDevice(ctx context.Context, userID, sessionID uuid.UUID) error
	UserChannels(ctx context.Context, userID uuid.UUID) (UserChannels, error)
	// SetUserChannels changes the switches given; nil leaves one as it is.
	SetUserChannels(ctx context.Context, userID uuid.UUID, email, push *bool) error
}

// UserChannels is a user's channel switches and what the email channel needs.
type UserChannels struct {
	Email, Push             bool
	HasEmail, EmailVerified bool
}

// ServerChannels is what this server offers. Portal is true when users sign in through a portal and have no address.
type ServerChannels struct {
	Email, Push, Portal bool
}

// EmailChannel is the email channel for one user. Reason says why it is not available: server_off, portal_account
// or unverified_email.
type EmailChannel struct {
	Available, Enabled bool
	Reason             string
}

// PushChannel is the push channel for one user.
type PushChannel struct {
	Available, Enabled, HasDevice bool
}

// Channels is how a user's alerts reach them.
type Channels struct {
	Email EmailChannel
	Push  PushChannel
}

// Service is the alert rules, channels and device registration behind /me/alerts and /me/devices.
type Service struct {
	store  Store
	server ServerChannels
	now    func() time.Time
}

func NewService(store Store, server ServerChannels, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, server: server, now: now}
}

// Channels reports the user's channels.
func (s *Service) Channels(ctx context.Context, userID uuid.UUID) (Channels, error) {
	uc, err := s.store.UserChannels(ctx, userID)
	if err != nil {
		return Channels{}, err
	}
	dev, err := s.store.HasDevice(ctx, userID)
	if err != nil {
		return Channels{}, err
	}
	c := Channels{Email: EmailChannel{Enabled: uc.Email}, Push: PushChannel{Available: s.server.Push, Enabled: uc.Push, HasDevice: dev}}
	switch {
	case s.server.Portal:
		c.Email.Reason = "portal_account"
	case !s.server.Email:
		c.Email.Reason = "server_off"
	case !uc.HasEmail || !uc.EmailVerified:
		c.Email.Reason = "unverified_email"
	default:
		c.Email.Available = true
	}
	return c, nil
}

// SetChannels changes the switches given and reports the channels. ErrNoChannel when neither is given.
func (s *Service) SetChannels(ctx context.Context, userID uuid.UUID, email, push *bool) (Channels, error) {
	if email == nil && push == nil {
		return Channels{}, ErrNoChannel
	}
	if err := s.store.SetUserChannels(ctx, userID, email, push); err != nil {
		return Channels{}, err
	}
	return s.Channels(ctx, userID)
}

// TargetView is a target with the kinds it can have.
type TargetView struct {
	TargetInfo
	Available        []string
	Unavailable      map[string]string
	UnavailableCodes map[string]string
}

// Overview is the answer to GET /me/alerts.
type Overview struct {
	Channels Channels
	Targets  []TargetView
}

func (s *Service) Overview(ctx context.Context, userID uuid.UUID) (Overview, error) {
	targets, err := s.store.Targets(ctx, userID, s.now())
	if err != nil {
		return Overview{}, err
	}
	ch, err := s.Channels(ctx, userID)
	if err != nil {
		return Overview{}, err
	}
	out := Overview{Channels: ch, Targets: make([]TargetView, 0, len(targets))}
	for _, t := range targets {
		av, un, codes := Availability(t.Caps)
		out.Targets = append(out.Targets, TargetView{TargetInfo: t, Available: av, Unavailable: un, UnavailableCodes: codes})
	}
	return out, nil
}

// PutRule creates or updates the rule for (radio, kind). Errors: ErrNotOnAtlas, *KindUnavailable, *InvalidThreshold.
func (s *Service) PutRule(ctx context.Context, userID uuid.UUID, in RuleInput) (StoredRule, error) {
	if in.TargetKind != TargetNode && in.TargetKind != TargetObserver {
		return StoredRule{}, &InvalidThreshold{Message: "Unknown target kind."}
	}
	return s.store.PutRule(ctx, userID, in, s.now(), func(c Caps, hasGQ bool) (*int, *int, error) {
		return ValidateRule(c, in.Kind, in.Threshold, in.High, in.Enabled, hasGQ)
	})
}

func (s *Service) DeleteRule(ctx context.Context, userID, id uuid.UUID) error {
	ok, err := s.store.DeleteRule(ctx, userID, id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRuleMissing
	}
	return nil
}

// maxHandle bounds a push handle; the push server's are far shorter.
const maxHandle = 256

// RegisterDevice stores the push handle (from the push server, starting h_) and the payload key (standard base64 of
// 32 bytes) of an app session; a handle another session held moves to this one. It turns the user's push switch on.
func (s *Service) RegisterDevice(ctx context.Context, userID, sessionID uuid.UUID, platform, handle, payloadKey string) error {
	if !s.server.Push {
		return ErrPushOff
	}
	handle = strings.TrimSpace(handle)
	if (platform != "ios" && platform != "android") || !strings.HasPrefix(handle, "h_") || len(handle) < 3 || len(handle) > maxHandle ||
		strings.IndexFunc(handle, func(r rune) bool { return r > unicode.MaxASCII || unicode.IsControl(r) || unicode.IsSpace(r) }) >= 0 {
		return ErrBadDevice
	}
	key, err := base64.StdEncoding.Strict().DecodeString(payloadKey)
	if err != nil || len(key) != PayloadKeySize {
		return ErrBadDevice
	}
	return s.store.RegisterDevice(ctx, userID, sessionID, platform, handle, key)
}

func (s *Service) RemoveDevice(ctx context.Context, userID, sessionID uuid.UUID) error {
	return s.store.RemoveDevice(ctx, userID, sessionID)
}
