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

const alertsCursorKey = "alerts_eval_cursor"

func (a AlertEval) LoadCursor(ctx context.Context) (uuid.UUID, error) {
	var id uuid.UUID
	if ok, err := a.LoadState(ctx, alertsCursorKey, &id); err != nil || !ok {
		return uuid.Nil, err
	}
	return id, nil
}

func (a AlertEval) SaveCursor(ctx context.Context, after uuid.UUID) error {
	return a.SaveState(ctx, alertsCursorKey, after)
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
			PinName: r.PinName, SiblingRecoveredAt: tsTime(r.SiblingRecoveredAt), SiblingEnabled: r.SiblingEnabled}
	}
	return out, nil
}

func ip32(v *int32) *int { return i32p(v) }

func round(v float64) *int { n := int(math.Round(v)); return &n }

// LoadTargets reads every radio the rules name with a fixed number of queries, however many rules there are.
func (a AlertEval) LoadTargets(ctx context.Context, rules []alerts.RuleRow, now time.Time) (map[string]alerts.TargetData, error) {
	var nodeKeys, obsKeys, nodeFalling, obsFalling [][]byte
	nodeReboot, obsReboot, nodeClock := map[string]time.Time{}, map[string]time.Time{}, map[string]time.Time{} // the oldest cursor of a reboot rule
	seenN, seenO, fallN, fallO := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, r := range rules {
		k := string(r.Key)
		if r.Kind == alerts.KindRebooted {
			m := nodeReboot
			if r.TargetKind == alerts.TargetObserver {
				m = obsReboot
			}
			if c, ok := m[k]; !ok || r.Cursor.Before(c) {
				m[k] = r.Cursor
			}
		}
		if r.Kind == alerts.KindClock && r.TargetKind != alerts.TargetObserver {
			if c, ok := nodeClock[k]; !ok || r.Cursor.Before(c) {
				nodeClock[k] = r.Cursor
			}
		}
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
		nodes, err := a.q.AlertNodesData(ctx, sqlc.AlertNodesDataParams{Keys: nodeKeys, PollingEnabled: !a.pollingOff.Load()})
		if err != nil {
			return nil, err
		}
		for _, n := range nodes {
			td := alerts.TargetData{Name: n.Name}
			td.Exists, td.Polled, td.LastHeard = true, n.Polled, tsTime(n.LastSeen)
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
			if rd.At.After(td.LastHeard) {
				td.LastHeard = rd.At // a trusted reading counts as hearing the repeater
			}
			out[id] = td
		}
		if len(nodeReboot) > 0 {
			keys, cursors := rebootCursors(nodeReboot, now)
			ups, err := a.q.AlertNodeUptimes(ctx, sqlc.AlertNodeUptimesParams{Keys: keys, Cursors: cursors, PageSize: alerts.UptimePage})
			if err != nil {
				return nil, err
			}
			for _, r := range ups {
				id := alerts.TargetID(alerts.TargetNode, r.TargetPublicKey)
				if td, ok := out[id]; ok {
					td.Uptimes = append(td.Uptimes, alerts.Reading{At: tsTime(r.SampledAt), Uptime: r.UptimeSecs})
					out[id] = td
				}
			}
		}
		if len(nodeClock) > 0 {
			keys, cursors := rebootCursors(nodeClock, now)
			ds, err := a.q.AlertNodeDrifts(ctx, sqlc.AlertNodeDriftsParams{Keys: keys, Cursors: cursors})
			if err != nil {
				return nil, err
			}
			for _, r := range ds { // oldest first, as the query returns them
				id := alerts.TargetID(alerts.TargetNode, r.TargetPublicKey)
				if td, ok := out[id]; ok {
					td.Drifts = append(td.Drifts, alerts.Drift{At: tsTime(r.AdvertAt), Sec: r.DriftSeconds})
					out[id] = td
				}
			}
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
		obs, err := a.q.AlertObserversData(ctx, sqlc.AlertObserversDataParams{Keys: obsKeys, FreshSince: ts(now.Add(-alerts.ObserverFresh))})
		if err != nil {
			return nil, err
		}
		byID := map[uuid.UUID]string{}
		ids := make([]uuid.UUID, 0, len(obs))
		for _, o := range obs {
			id := alerts.TargetID(alerts.TargetObserver, o.PublicKey)
			td := alerts.TargetData{Name: o.Name}
			td.Exists, td.Polled, td.LastHeard, td.StatusAt = true, o.Polled, tsTime(o.LastSeen), tsTime(o.LastStatusAt)
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
				out[id] = td
			}
			if len(obsReboot) > 0 {
				var rids []uuid.UUID
				var rkeys [][]byte
				for _, o := range obs {
					if _, ok := obsReboot[string(o.PublicKey)]; ok {
						rids, rkeys = append(rids, o.ID), append(rkeys, o.PublicKey)
					}
				}
				cursors := make([]pgtype.Timestamptz, len(rkeys))
				for i, k := range rkeys {
					cursors[i] = ts(rebootFrom(obsReboot[string(k)], now))
				}
				if len(rids) > 0 {
					ups, err := a.q.AlertObserverUptimes(ctx, sqlc.AlertObserverUptimesParams{Ids: rids, Cursors: cursors, PageSize: alerts.UptimePage})
					if err != nil {
						return nil, err
					}
					for _, r := range ups {
						id := byID[r.ObserverID]
						td := out[id]
						td.Uptimes = append(td.Uptimes, alerts.Reading{At: tsTime(r.ReportedAt), Uptime: r.UptimeSeconds})
						out[id] = td
					}
				}
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
		sort.SliceStable(td.Uptimes, func(i, j int) bool { return td.Uptimes[i].At.After(td.Uptimes[j].At) })
		out[id] = td
	}
	return out, nil
}

// rebootFrom is where a reboot rule reads from: its cursor, but no further back than the data window.
func rebootFrom(cursor, now time.Time) time.Time {
	if floor := now.Add(-alerts.DataWindow * time.Second); cursor.Before(floor) {
		return floor
	}
	return cursor
}

func rebootCursors(m map[string]time.Time, now time.Time) ([][]byte, []pgtype.Timestamptz) {
	keys := make([][]byte, 0, len(m))
	cursors := make([]pgtype.Timestamptz, 0, len(m))
	for k, c := range m {
		keys = append(keys, []byte(k))
		cursors = append(cursors, ts(rebootFrom(c, now)))
	}
	return keys, cursors
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

// ---- delivery ----

var _ alerts.SendStore = AlertEval{}

func (a AlertEval) BeginSendLock(ctx context.Context) (alerts.Unlocker, bool, error) {
	return a.beginAlertsLock(ctx, func(q *sqlc.Queries) (bool, error) { return q.TryAlertsSendLock(ctx) },
		func(q *sqlc.Queries, c context.Context) error { return q.AlertsSendUnlock(c) })
}

func (a AlertEval) BeginPass(ctx context.Context, pass uuid.UUID) error {
	return a.q.BeginAlertPass(ctx, pass)
}

func (a AlertEval) FinishPass(ctx context.Context, pass uuid.UUID) error {
	return a.q.FinishAlertPass(ctx, pass)
}

func (a AlertEval) Expire(ctx context.Context, undeliveredBefore, deliveredBefore time.Time) (int64, error) {
	if err := a.q.DeleteOldAlertPasses(ctx, ts(undeliveredBefore)); err != nil {
		return 0, err
	}
	return a.q.ExpireAlertOutbox(ctx, sqlc.ExpireAlertOutboxParams{UndeliveredBefore: ts(undeliveredBefore), DeliveredBefore: ts(deliveredBefore)})
}

func (a AlertEval) Pending(ctx context.Context, notBefore, now time.Time, limit int) ([]alerts.OutboxRow, error) {
	rows, err := a.q.PendingAlerts(ctx, sqlc.PendingAlertsParams{NotBefore: ts(notBefore), Now: ts(now), PassTimeoutBefore: ts(now.Add(-alerts.PassTimeout)), RowLimit: int64(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]alerts.OutboxRow, len(rows))
	for i, r := range rows {
		out[i] = alerts.OutboxRow{ID: r.ID, TargetID: r.TargetID, UserID: r.UserID, Pass: r.PassID, TargetKind: r.TargetKind, Key: r.TargetPublicKey,
			Name: r.Name, Kind: r.AlertKind, Detail: r.Detail, Email: r.Email, EmailVerified: r.EmailVerified,
			AlertEmail: r.AlertEmail, AlertPush: r.AlertPush, EmailDue: r.EmailDue, PushDue: r.PushDue, PushAttempts: int(r.PushAttempts)}
		if r.Value != nil {
			out[i].Value, out[i].HasValue = *r.Value, true
		}
	}
	return out, nil
}

func (a AlertEval) Devices(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID][]alerts.Device, error) {
	rows, err := a.q.LiveUserDevices(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	out := map[uuid.UUID][]alerts.Device{}
	for _, r := range rows {
		out[r.UserID] = append(out[r.UserID], alerts.Device{Session: r.SessionID, Platform: r.Platform, Token: r.PushToken, Key: r.PayloadKey})
	}
	return out, nil
}

func (a AlertEval) DeliveredDevices(ctx context.Context, ids []int64) (map[int64]map[uuid.UUID]bool, error) {
	rows, err := a.q.DeliveredAlertDevices(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := map[int64]map[uuid.UUID]bool{}
	for _, r := range rows {
		if out[r.OutboxID] == nil {
			out[r.OutboxID] = map[uuid.UUID]bool{}
		}
		out[r.OutboxID][r.SessionID] = true
	}
	return out, nil
}

func (a AlertEval) MarkDevicesDelivered(ctx context.Context, marks []alerts.DeviceMark, now time.Time) error {
	if len(marks) == 0 {
		return nil
	}
	return a.pollTx(ctx, func(q *sqlc.Queries) error {
		for _, m := range marks {
			if err := q.MarkAlertDeviceDelivered(ctx, sqlc.MarkAlertDeviceDeliveredParams{OutboxID: m.OutboxID, SessionID: m.Session, DeliveredAt: ts(now)}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (a AlertEval) MarkEmailSent(ctx context.Context, ids []int64, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	return a.q.MarkAlertsEmailDone(ctx, sqlc.MarkAlertsEmailDoneParams{At: ts(now), Sent: true, Ids: ids})
}

func (a AlertEval) Finish(ctx context.Context, st alerts.Settled, now time.Time, maxAttempts int) error {
	if len(st.IDs) == 0 {
		return nil
	}
	return a.pollTx(ctx, func(q *sqlc.Queries) error {
		if len(st.EmailDone) > 0 {
			if err := q.MarkAlertsEmailDone(ctx, sqlc.MarkAlertsEmailDoneParams{At: ts(now), Ids: st.EmailDone}); err != nil {
				return err
			}
		}
		if len(st.PushDone) > 0 {
			if err := q.MarkAlertsPushDone(ctx, sqlc.MarkAlertsPushDoneParams{At: ts(now), Ids: st.PushDone}); err != nil {
				return err
			}
		}
		for h, ids := range byHold(st.EmailRetry, now) {
			if err := q.BumpAlertEmailAttempts(ctx, sqlc.BumpAlertEmailAttemptsParams{At: ts(now), HoldUntil: ts(h), MaxAttempts: int32(maxAttempts), Ids: ids}); err != nil {
				return err
			}
		}
		for h, ids := range byHold(st.PushRetry, now) {
			if err := q.BumpAlertPushAttempts(ctx, sqlc.BumpAlertPushAttemptsParams{At: ts(now), HoldUntil: ts(h), MaxAttempts: int32(maxAttempts), Ids: ids}); err != nil {
				return err
			}
		}
		for h, ids := range byHold(st.PushHold, now) {
			if err := q.HoldAlertPush(ctx, sqlc.HoldAlertPushParams{HoldUntil: ts(h), Ids: ids}); err != nil {
				return err
			}
		}
		if _, err := q.DeleteExhaustedAlerts(ctx, sqlc.DeleteExhaustedAlertsParams{Ids: st.IDs, MaxAttempts: int32(maxAttempts)}); err != nil {
			return err
		}
		return q.MarkAlertsDelivered(ctx, sqlc.MarkAlertsDeliveredParams{At: ts(now), Ids: st.IDs})
	})
}

// byHold groups retries by hold time, so each gets one update; a hold before now is no hold.
func byHold(retry []alerts.Retry, now time.Time) map[time.Time][]int64 {
	out := map[time.Time][]int64{}
	for _, r := range retry {
		h := r.NotBefore
		if h.Before(now) {
			h = now
		}
		out[h] = append(out[h], r.ID)
	}
	return out
}

func (a AlertEval) RemoveDeviceToken(ctx context.Context, token string) error {
	return a.q.DeleteDevicesByToken(ctx, token)
}
