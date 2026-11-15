// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AlertEval adapts the store to alerts.EvalStore.
type AlertEval struct{ *Store }

var _ alerts.EvalStore = AlertEval{}

type alertsLock struct {
	conn   *pgxpool.Conn
	unlock func(ctx context.Context) error
}

func (l *alertsLock) Close(ctx context.Context) {
	if err := l.unlock(ctx); err != nil {
		_ = l.conn.Hijack().Close(context.Background()) // closing the connection drops the lock
		return
	}
	l.conn.Release()
}

// beginAlertsLock takes one of the alert advisory locks on a dedicated connection.
func (s *Store) beginAlertsLock(ctx context.Context, try func(*sqlc.Queries) (bool, error), unlock func(*sqlc.Queries, context.Context) error) (alerts.Unlocker, bool, error) {
	if s.pool == nil {
		return nil, false, errors.New("alerts: store has no connection pool")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	q := sqlc.New(conn)
	ok, err := try(q)
	if err != nil || !ok {
		conn.Release()
		return nil, false, err
	}
	return &alertsLock{conn: conn, unlock: func(c context.Context) error { return unlock(q, c) }}, true, nil
}

func (a AlertEval) BeginEvalLock(ctx context.Context) (alerts.Unlocker, bool, error) {
	return a.beginAlertsLock(ctx, func(q *sqlc.Queries) (bool, error) { return q.TryAlertsEvalLock(ctx) },
		func(q *sqlc.Queries, c context.Context) error { return q.AlertsEvalUnlock(c) })
}

func tsTime(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time
}

func optStamp(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return ts(t)
}

func (a AlertEval) RuleBatch(ctx context.Context, after uuid.UUID, limit int) ([]alerts.RuleRow, error) {
	rows, err := a.q.AlertRuleBatch(ctx, sqlc.AlertRuleBatchParams{AfterID: after, RowLimit: int32(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]alerts.RuleRow, len(rows))
	for i, r := range rows {
		out[i] = alerts.RuleRow{ID: r.ID, UserID: r.UserID, TargetKind: r.TargetKind, Key: r.TargetPublicKey, Kind: r.Kind,
			Threshold: i32p(r.Threshold), High: i32p(r.ThresholdHigh), State: alerts.State(r.State),
			LastSentAt: tsTime(r.LastSentAt), Cursor: tsTime(r.CursorAt), Streak: int(r.Streak), Version: r.Version,
			PinName: r.PinName, SiblingRecoveredAt: tsTime(r.SiblingRecoveredAt)}
	}
	return out, nil
}

func ip32(v *int32) *int { return i32p(v) }

func round(v float64) *int { n := int(math.Round(v)); return &n }

// LoadTargets reads every radio the rules name with a fixed number of queries, however many rules there are.
func (a AlertEval) LoadTargets(ctx context.Context, rules []alerts.RuleRow, now time.Time) (map[string]alerts.TargetData, error) {
	var nodeKeys, obsKeys, nodeFalling, obsFalling [][]byte
	seenN, seenO, fallN, fallO := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, r := range rules {
		k := string(r.Key)
		if r.TargetKind == alerts.TargetObserver {
			if !seenO[k] {
				seenO[k] = true
				obsKeys = append(obsKeys, r.Key)
			}
			if r.Kind == alerts.KindBatteryFalling && !fallO[k] {
				fallO[k] = true
				obsFalling = append(obsFalling, r.Key)
			}
			continue
		}
		if !seenN[k] {
			seenN[k] = true
			nodeKeys = append(nodeKeys, r.Key)
		}
		if r.Kind == alerts.KindBatteryFalling && !fallN[k] {
			fallN[k] = true
			nodeFalling = append(nodeFalling, r.Key)
		}
	}
	out := map[string]alerts.TargetData{}
	since := ts(now.Add(-alerts.DataWindow * time.Second))

	if len(nodeKeys) > 0 {
		nodes, err := a.q.AlertNodesData(ctx, nodeKeys)
		if err != nil {
			return nil, err
		}
		for _, n := range nodes {
			td := alerts.TargetData{Name: n.Name}
			td.Exists, td.LastHeard, td.AdvertAt = true, tsTime(n.LastSeen), tsTime(n.LastAdvertAt)
			td.DriftSec = n.DeviceClockDriftSeconds
			out[alerts.TargetID(alerts.TargetNode, n.PublicKey)] = td
		}
		reads, err := a.q.AlertNodeReadings(ctx, sqlc.AlertNodeReadingsParams{Keys: nodeKeys, Since: since})
		if err != nil {
			return nil, err
		}
		for _, r := range reads { // newest first per key, as the query returns them
			id := alerts.TargetID(alerts.TargetNode, r.TargetPublicKey)
			td, ok := out[id]
			if !ok {
				continue
			}
			rd := alerts.Reading{At: tsTime(r.SampledAt), BatteryMV: ip32(r.BatteryMv), Uptime: r.UptimeSecs}
			if r.TemperatureC != nil {
				rd.TempTenths = round(float64(*r.TemperatureC) * 10)
			}
			rd.NoiseDBm = ip32(r.NoiseFloor)
			td.Readings = append(td.Readings, rd)
			td.Polled = true
			if rd.At.After(td.LastHeard) {
				td.LastHeard = rd.At // a trusted reading counts as hearing the repeater
			}
			out[id] = td
		}
		if len(nodeFalling) > 0 {
			days, err := a.q.AlertNodeDayAgo(ctx, sqlc.AlertNodeDayAgoParams{Keys: nodeFalling, Since: since})
			if err != nil {
				return nil, err
			}
			for _, d := range days {
				id := alerts.TargetID(alerts.TargetNode, d.TargetPublicKey)
				if td, ok := out[id]; ok {
					td.DayAgo = &alerts.Reading{At: tsTime(d.SampledAt), BatteryMV: ip32(d.BatteryMv)}
					out[id] = td
				}
			}
		}
	}

	if len(obsKeys) > 0 {
		obs, err := a.q.AlertObserversData(ctx, obsKeys)
		if err != nil {
			return nil, err
		}
		byID := map[uuid.UUID]string{}
		ids := make([]uuid.UUID, 0, len(obs))
		for _, o := range obs {
			id := alerts.TargetID(alerts.TargetObserver, o.PublicKey)
			td := alerts.TargetData{Name: o.Name}
			td.Exists, td.LastHeard, td.StatusAt = true, tsTime(o.LastSeen), tsTime(o.LastStatusAt)
			out[id] = td
			byID[o.ID] = id
			ids = append(ids, o.ID)
		}
		if len(ids) > 0 {
			tel, err := a.q.AlertObserverTelemetry(ctx, sqlc.AlertObserverTelemetryParams{Ids: ids, Since: since})
			if err != nil {
				return nil, err
			}
			for _, r := range tel {
				id := byID[r.ObserverID]
				td := out[id]
				rd := alerts.Reading{At: tsTime(r.ReportedAt), BatteryMV: ip32(r.BatteryVoltageMv), Uptime: r.UptimeSeconds}
				if r.NoiseFloorDb != nil {
					rd.NoiseDBm = round(float64(*r.NoiseFloorDb))
				}
				td.Readings = append(td.Readings, rd)
				td.Polled = true
				out[id] = td
			}
			var fallIDs []uuid.UUID
			for _, k := range obsFalling {
				for _, o := range obs {
					if string(o.PublicKey) == string(k) {
						fallIDs = append(fallIDs, o.ID)
					}
				}
			}
			if len(fallIDs) > 0 {
				days, err := a.q.AlertObserverDayAgo(ctx, sqlc.AlertObserverDayAgoParams{Ids: fallIDs, Since: since})
				if err != nil {
					return nil, err
				}
				for _, d := range days {
					id := byID[d.ObserverID]
					td := out[id]
					td.DayAgo = &alerts.Reading{At: tsTime(d.ReportedAt), BatteryMV: ip32(d.BatteryVoltageMv)}
					out[id] = td
				}
			}
		}
	}
	for id, td := range out {
		sort.SliceStable(td.Readings, func(i, j int) bool { return td.Readings[i].At.After(td.Readings[j].At) })
		out[id] = td
	}
	return out, nil
}

func (a AlertEval) Apply(ctx context.Context, passID uuid.UUID, ups []alerts.Update, notes []alerts.Notice) error {
	if len(ups) == 0 {
		return nil
	}
	p := sqlc.ApplyAlertUpdatesParams{}
	for _, u := range ups {
		p.Ids = append(p.Ids, u.ID)
		p.Versions = append(p.Versions, u.Version)
		p.States = append(p.States, string(u.State))
		p.SentAts = append(p.SentAts, optStamp(u.SentAt))
		p.Cursors = append(p.Cursors, ts(u.Cursor))
		p.Streaks = append(p.Streaks, int32(u.Streak))
		p.RecoveredAts = append(p.RecoveredAts, optStamp(u.RecoveredAt))
	}
	return a.pollTx(ctx, func(q *sqlc.Queries) error {
		stored, err := q.ApplyAlertUpdates(ctx, p)
		if err != nil {
			return err
		}
		ok := make(map[uuid.UUID]bool, len(stored))
		for _, id := range stored {
			ok[id] = true
		}
		n := sqlc.InsertAlertNoticesParams{PassID: passID}
		for _, nt := range notes {
			if !ok[nt.RuleID] {
				continue // the rule was edited meanwhile; the next pass looks again
			}
			n.UserIds = append(n.UserIds, nt.UserID)
			n.TargetKinds = append(n.TargetKinds, nt.TargetKind)
			n.TargetKeys = append(n.TargetKeys, nt.Key)
			n.Names = append(n.Names, nt.Name)
			n.AlertKinds = append(n.AlertKinds, nt.Kind)
			n.Values = append(n.Values, nt.Value)
			n.Details = append(n.Details, "")
		}
		if len(n.UserIds) == 0 {
			return nil
		}
		return q.InsertAlertNotices(ctx, n)
	})
}

func (a AlertEval) AnnounceAttention(ctx context.Context, passID uuid.UUID) error {
	return a.pollTx(ctx, func(q *sqlc.Queries) error {
		if err := q.ResetRequestAttention(ctx); err != nil {
			return err
		}
		due, err := q.DueRequestAttention(ctx)
		if err != nil || len(due) == 0 {
			return err
		}
		n := sqlc.InsertAlertNoticesParams{PassID: passID}
		var ids []uuid.UUID
		var statuses []string
		for _, d := range due {
			ids, statuses = append(ids, d.ID), append(statuses, d.Status)
			n.UserIds = append(n.UserIds, d.UserID)
			n.TargetKinds = append(n.TargetKinds, alerts.TargetNode)
			n.TargetKeys = append(n.TargetKeys, d.TargetPublicKey)
			n.Names = append(n.Names, alerts.SanitizeName(d.NodeName, d.TargetPublicKey))
			n.AlertKinds = append(n.AlertKinds, alerts.KindRequestAttention)
			n.Values = append(n.Values, 0)
			n.Details = append(n.Details, d.Status)
		}
		if err := q.InsertAlertNotices(ctx, n); err != nil {
			return err
		}
		return q.MarkRequestAttention(ctx, sqlc.MarkRequestAttentionParams{Ids: ids, Statuses: statuses})
	})
}
