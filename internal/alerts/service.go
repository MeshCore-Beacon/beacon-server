// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
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
	RegisterDevice(ctx context.Context, userID, sessionID uuid.UUID, platform, token string) error
	RemoveDevice(ctx context.Context, userID, sessionID uuid.UUID) error
}

// Service is the alert rules and device registration behind /me/alerts and /me/devices.
type Service struct {
	store    Store
	delivery string
	now      func() time.Time
}

// NewService builds the service; delivery is "push" or "email".
func NewService(store Store, delivery string, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, delivery: delivery, now: now}
}

// Delivery is "push" or "email".
func (s *Service) Delivery() string { return s.delivery }

// TargetView is a target with the kinds it can have.
type TargetView struct {
	TargetInfo
	Available        []string
	Unavailable      map[string]string
	UnavailableCodes map[string]string
}

// Overview is the answer to GET /me/alerts.
type Overview struct {
	Delivery  string
	HasDevice bool
	Targets   []TargetView
}

func (s *Service) Overview(ctx context.Context, userID uuid.UUID) (Overview, error) {
	targets, err := s.store.Targets(ctx, userID, s.now())
	if err != nil {
		return Overview{}, err
	}
	dev, err := s.store.HasDevice(ctx, userID)
	if err != nil {
		return Overview{}, err
	}
	out := Overview{Delivery: s.delivery, HasDevice: dev, Targets: make([]TargetView, 0, len(targets))}
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

const maxPushToken = 4096

// RegisterDevice stores the push token of an app session; a token another session held moves to this one.
func (s *Service) RegisterDevice(ctx context.Context, userID, sessionID uuid.UUID, platform, token string) error {
	token = strings.TrimSpace(token)
	if (platform != "ios" && platform != "android") || token == "" || len(token) > maxPushToken ||
		strings.IndexFunc(token, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) >= 0 {
		return ErrBadDevice
	}
	return s.store.RegisterDevice(ctx, userID, sessionID, platform, token)
}

func (s *Service) RemoveDevice(ctx context.Context, userID, sessionID uuid.UUID) error {
	return s.store.RemoveDevice(ctx, userID, sessionID)
}
