// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ alerts.Store = (*Store)(nil)

func i32p(v *int32) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}

func int4(v *int) *int32 {
	if v == nil {
		return nil
	}
	n := int32(*v)
	return &n
}

func storedRule(id uuid.UUID, kind string, th, high *int32, enabled bool, state string, changed, sent pgtype.Timestamptz) alerts.StoredRule {
	return alerts.StoredRule{ID: id, Kind: kind, Threshold: i32p(th), High: i32p(high), Enabled: enabled, State: state,
		StateChangedAt: changed.Time, LastSentAt: timePtr(sent)}
}

type radioCaps struct {
	node     map[string]sqlc.AlertNodeCapsRow
	observer map[string]sqlc.AlertObserverCapsRow
}

func loadRadioCaps(ctx context.Context, q sqlc.Querier, keys [][]byte, now time.Time) (radioCaps, error) {
	since := ts(now.Add(-alerts.DataWindow * time.Second))
	rc := radioCaps{node: map[string]sqlc.AlertNodeCapsRow{}, observer: map[string]sqlc.AlertObserverCapsRow{}}
	nodes, err := q.AlertNodeCaps(ctx, sqlc.AlertNodeCapsParams{Since: since, Keys: keys})
	if err != nil {
		return rc, err
	}
	for _, n := range nodes {
		rc.node[hex.EncodeToString(n.PublicKey)] = n
	}
	obs, err := q.AlertObserverCaps(ctx, sqlc.AlertObserverCapsParams{Since: since, Keys: keys})
	if err != nil {
		return rc, err
	}
	for _, o := range obs {
		rc.observer[hex.EncodeToString(o.PublicKey)] = o
	}
	return rc, nil
}

// pick returns the capabilities and display name for the radio, reading it as the requested kind when a row of that
// kind exists and otherwise as the other kind.
func (rc radioCaps) pick(key, want string) (kind string, c alerts.Caps, name string) {
	n, isNode := rc.node[key]
	o, isObs := rc.observer[key]
	asObserver := (want == alerts.TargetObserver && isObs) || (!isNode && isObs)
	switch {
	case asObserver:
		return alerts.TargetObserver, alerts.Caps{Observer: true, Polled: o.HasBattery || o.HasNoise || o.HasUptime,
			Battery: o.HasBattery, Noise: o.HasNoise, Uptime: o.HasUptime}, o.Name
	case isNode:
		return alerts.TargetNode, alerts.Caps{NodeType: n.NodeType, Polled: n.Readings > 0, Battery: n.HasBattery,
			Temperature: n.HasTemperature, Noise: n.HasNoise, Uptime: n.HasUptime}, n.Name
	}
	return want, alerts.Caps{Observer: want == alerts.TargetObserver}, ""
}

// Targets returns one entry per Atlas pin, in Atlas order. An observer pin whose key has a node row reads as a node,
// as the Atlas does, and a key pinned twice is listed once.
func (s *Store) Targets(ctx context.Context, userID uuid.UUID, now time.Time) ([]alerts.TargetInfo, error) {
	pins, err := s.q.ListUserAtlasPins(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(pins) == 0 {
		return []alerts.TargetInfo{}, nil
	}
	keys := make([][]byte, len(pins))
	for i, p := range pins {
		keys[i] = p.PublicKey
	}
	rc, err := loadRadioCaps(ctx, s.q, keys, now)
	if err != nil {
		return nil, err
	}
	rules, err := s.q.ListUserAlertRules(ctx, userID)
	if err != nil {
		return nil, err
	}
	byKey := map[string][]alerts.StoredRule{}
	for _, r := range rules {
		k := hex.EncodeToString(r.TargetPublicKey)
		byKey[k] = append(byKey[k], storedRule(r.ID, r.Kind, r.Threshold, r.ThresholdHigh, r.Enabled, r.State, r.StateChangedAt, r.LastSentAt))
	}
	seen := map[string]bool{}
	out := make([]alerts.TargetInfo, 0, len(pins))
	for _, p := range pins {
		k := hex.EncodeToString(p.PublicKey)
		if seen[k] {
			continue
		}
		seen[k] = true
		want := p.Kind
		if _, isNode := rc.node[k]; isNode && p.Kind == "observer" {
			want = alerts.TargetNode
		}
		kind, caps, name := rc.pick(k, want)
		if p.Name != "" {
			name = p.Name
		}
		r := byKey[k]
		if r == nil {
			r = []alerts.StoredRule{}
		}
		out = append(out, alerts.TargetInfo{Kind: kind, PublicKey: k, Name: name, Caps: caps, Rules: r})
	}
	return out, nil
}

func (s *Store) HasDevice(ctx context.Context, userID uuid.UUID) (bool, error) {
	return s.q.UserHasLiveDevice(ctx, userID)
}

func (s *Store) PutRule(ctx context.Context, userID uuid.UUID, in alerts.RuleInput, now time.Time,
	validate func(c alerts.Caps, hasGoneQuiet bool) (th, high *int, err error)) (alerts.StoredRule, error) {
	var out alerts.StoredRule
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		// The Atlas row is what an Atlas write locks, so the pin cannot go away under this transaction.
		if err := q.EnsureUserAtlas(ctx, userID); err != nil {
			return err
		}
		if _, err := q.LockUserAtlas(ctx, userID); err != nil {
			return err
		}
		pinned, err := q.UserHasAtlasPin(ctx, sqlc.UserHasAtlasPinParams{UserID: userID, PublicKey: in.PublicKey})
		if err != nil {
			return err
		}
		if !pinned {
			return alerts.ErrNotOnAtlas
		}
		rc, err := loadRadioCaps(ctx, q, [][]byte{in.PublicKey}, now)
		if err != nil {
			return err
		}
		kind, caps, _ := rc.pick(hex.EncodeToString(in.PublicKey), in.TargetKind)
		kinds, err := q.UserAlertRuleKinds(ctx, sqlc.UserAlertRuleKindsParams{UserID: userID, TargetPublicKey: in.PublicKey})
		if err != nil {
			return err
		}
		hasGQ := false
		for _, k := range kinds {
			hasGQ = hasGQ || k == alerts.KindGoneQuiet
		}
		th, high, err := validate(caps, hasGQ)
		if err != nil {
			return err
		}
		row, err := q.UpsertAlertRule(ctx, sqlc.UpsertAlertRuleParams{UserID: userID, TargetKind: kind, TargetPublicKey: in.PublicKey,
			Kind: in.Kind, Threshold: int4(th), ThresholdHigh: int4(high), Enabled: in.Enabled})
		if err != nil {
			return err
		}
		out = storedRule(row.ID, row.Kind, row.Threshold, row.ThresholdHigh, row.Enabled, row.State, row.StateChangedAt, row.LastSentAt)
		return nil
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == foreignKeyViolation {
		return alerts.StoredRule{}, users.ErrNotFound
	}
	return out, err
}

func (s *Store) DeleteRule(ctx context.Context, userID, id uuid.UUID) (bool, error) {
	n, err := s.q.DeleteAlertRule(ctx, sqlc.DeleteAlertRuleParams{ID: id, UserID: userID})
	return n > 0, err
}

func (s *Store) RegisterDevice(ctx context.Context, userID, sessionID uuid.UUID, platform, token string) error {
	err := s.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := q.UpsertUserDevice(ctx, sqlc.UpsertUserDeviceParams{SessionID: sessionID, UserID: userID, Platform: platform, PushToken: token}); err != nil {
			return err
		}
		// Crosses users on purpose: a phone belongs to one account, so the previous user's alerts never reach its next user.
		return q.DeleteDevicesWithToken(ctx, sqlc.DeleteDevicesWithTokenParams{PushToken: token, SessionID: sessionID})
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == foreignKeyViolation {
		return users.ErrNotFound
	}
	return err
}

func (s *Store) RemoveDevice(ctx context.Context, userID, sessionID uuid.UUID) error {
	err := s.q.DeleteUserDevice(ctx, sqlc.DeleteUserDeviceParams{SessionID: sessionID, UserID: userID})
	return err
}
