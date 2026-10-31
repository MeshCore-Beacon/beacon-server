// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"sort"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) RecordScopeRepeats(ctx context.Context, ids []uuid.UUID, scope string, at time.Time, hashBytes uint8) error {
	if len(ids) == 0 {
		return nil
	}
	return s.q.RecordScopeRepeats(ctx, sqlc.RecordScopeRepeatsParams{NodeIds: ids, ScopeName: scope, HeardAt: pgtype.Timestamptz{Time: at, Valid: true}, HashBytes: int16(hashBytes)})
}

func (s *Store) DeleteOldScopeRepeats(ctx context.Context, cutoff time.Time) error {
	for _, work := range []struct {
		remove func(context.Context, pgtype.Timestamptz) (int64, error)
		before time.Time
	}{
		{s.q.DeleteOldScopeRepeats, cutoff}, {s.q.DeleteOldScopeReceivers, cutoff},
		{s.q.DeleteOldScopeSamples, time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)},
	} {
		for {
			n, err := work.remove(ctx, pgtype.Timestamptz{Time: work.before, Valid: true})
			if err != nil {
				return err
			}
			if n < 10000 {
				break
			}
			if err = ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// reportScopes parses the firmware's comma-separated list. Preserve case and
// wildcard membership; names without #/$ use the same derivation as scopestore.
func reportScopes(raw string) []string {
	if len(raw) > 4096 {
		return nil
	}
	set := map[string]bool{}
	for _, v := range strings.Split(raw, ",") {
		v = strings.TrimSpace(v)
		if v == "" || len(v) > 128 {
			continue
		}
		if v != "*" {
			v = scopestore.FromName(v).Name
		}
		if len(v) <= 128 {
			set[v] = true
		}
		if len(set) >= 64 {
			break
		}
	}
	result := make([]string, 0, len(set))
	for v := range set {
		result = append(result, v)
	}
	sort.Strings(result)
	return result
}

// enrichNodeScopes is one indexed, batched query for a map page (neighbors=true)
// or a repeater detail. It never runs once per node or scans raw packet history.
func (s *Store) enrichNodeScopes(ctx context.Context, nodes []api.NodeSummary, history ...bool) error {
	ids := make([]uuid.UUID, 0, len(nodes))
	byID := map[uuid.UUID]int{}
	for i, n := range nodes {
		if n.NodeType == 2 {
			ids = append(ids, n.ID)
			byID[n.ID] = i
		}
	}
	if len(ids) == 0 {
		return nil
	}
	until := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	since := until.Add(-24 * time.Hour)
	rows, err := s.q.GetNodeScopeEvidence(ctx, sqlc.GetNodeScopeEvidenceParams{NodeIds: ids, Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}})
	if err != nil {
		return err
	}
	values := map[uuid.UUID]map[string]*api.NodeScope{}
	reports := map[uuid.UUID]map[string]bool{}
	for _, id := range ids {
		values[id] = map[string]*api.NodeScope{}
		reports[id] = map[string]bool{}
		nodes[byID[id]].ScopeEvidence = &api.NodeScopeEvidence{Since: since.UnixMilli(), Until: until.UnixMilli(), Scopes: []api.NodeScope{}}
	}
	for _, r := range rows {
		idx, ok := byID[r.NodeID]
		if !ok {
			continue
		}
		e := nodes[idx].ScopeEvidence
		names := []string{r.Scope}
		if r.Source == "reported" || r.Source == "self" {
			names = reportScopes(r.Scope)
			if len(names) == 0 && strings.TrimSpace(r.Scope) != "" {
				continue
			}
			reports[r.NodeID][strings.Join(names, ",")] = true
			if len(names) == 0 {
				e.ReportedEmpty = true
			}
		}
		for _, name := range names {
			v := values[r.NodeID][name]
			if v == nil {
				v = &api.NodeScope{Name: name}
				values[r.NodeID][name] = v
			}
			var at *int64
			if r.CheckedAt.Valid {
				ms := r.CheckedAt.Time.UnixMilli()
				at = &ms
			}
			switch r.Source {
			case "repeat":
				v.RepeatObservations = r.Observations
				one, six := r.OneHour, r.SixHours
				v.RepeatObservations1h = &one
				v.RepeatObservations6h = &six
				if r.FirstSeen.Valid {
					ms := r.FirstSeen.Time.UnixMilli()
					v.FirstRepeatedAt = &ms
				}
				v.UniqueSamples = r.UniqueSamples
				if r.UniqueSince.Valid {
					ms := r.UniqueSince.Time.UnixMilli()
					v.UniqueSince = &ms
				}
				v.RepeatedAt = at
				v.MinHashBytes = r.HashBytes
			case "reported", "self":
				v.Reported = true
				if at != nil && (v.ReportedAt == nil || *at > *v.ReportedAt) {
					v.ReportedAt = at
				}
			case "heard":
				v.HeardAt = at
			}
		}
	}
	includeHistory := len(history) > 0 && history[0] && len(ids) == 1
	if includeHistory {
		includeHistory = false
		for _, scope := range values[ids[0]] {
			if scope.RepeatObservations > 0 {
				includeHistory = true
				break
			}
		}
	}
	if includeHistory {
		hours, err := s.q.GetNodeScopeHistory(ctx, sqlc.GetNodeScopeHistoryParams{NodeID: ids[0], Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}})
		if err != nil {
			return err
		}
		for _, h := range hours {
			nodes[byID[ids[0]]].ScopeEvidence.Hourly = append(nodes[byID[ids[0]]].ScopeEvidence.Hourly, api.NodeScopeHour{Name: h.ScopeName, Hour: h.Hour.Time.UnixMilli(), Reports: h.Observations, UniqueSamples: h.UniqueSamples, FirstSeen: h.FirstSeen.Time.UnixMilli(), LastSeen: h.LastSeen.Time.UnixMilli()})
		}
	}
	for id, scopes := range values {
		e := nodes[byID[id]].ScopeEvidence
		e.ConflictingReports = len(reports[id]) > 1
		for _, v := range scopes {
			e.Scopes = append(e.Scopes, *v)
		}
		sort.Slice(e.Scopes, func(i, j int) bool {
			a, b := e.Scopes[i], e.Scopes[j]
			if a.RepeatObservations != b.RepeatObservations {
				return a.RepeatObservations > b.RepeatObservations
			}
			return a.Name < b.Name
		})
	}
	return nil
}
