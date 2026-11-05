// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"sort"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) RecordScopeObservation(ctx context.Context, p ingest.ScopeObservation) error {
	return s.q.RecordScopeObservation(ctx, sqlc.RecordScopeObservationParams{HeardAt: pgtype.Timestamptz{Time: p.HeardAt, Valid: true}, Iata: p.IATA, ScopeName: p.Scope, ObserverID: p.ObserverID, UnresolvedHops: p.UnresolvedHops, AmbiguousHops: p.AmbiguousHops, PathKey: p.PathKey, HashBytes: int16(p.HashBytes), NodeIds: p.NodeIDs})
}

func (s *Store) GetScopeActivity(ctx context.Context, iatas []string, hours int) (*api.ScopeActivity, error) {
	if hours != 1 && hours != 6 {
		hours = 24
	}
	now := time.Now().UTC()
	until := now.Truncate(time.Hour).Add(time.Hour)
	since := until.Add(-time.Duration(hours) * time.Hour)
	out := &api.ScopeActivity{Since: since.UnixMilli(), Until: now.UnixMilli(), Scopes: []api.ScopeActivityRow{}, Observers: []api.ScopeObserverActivity{}, Countries: []api.ScopeCountryActivity{}}
	if iatas == nil {
		iatas = []string{}
	}
	rows, err := s.q.GetScopeReceiverActivity(ctx, sqlc.GetScopeReceiverActivityParams{Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}, Iatas: iatas})
	if err != nil {
		return nil, err
	}
	if len(rows) > 50000 {
		out.Truncated = true
		rows = rows[:50000]
	}
	scopes := map[string]*api.ScopeActivityRow{}
	hourRows := map[string]map[int64]*api.ScopeActivityHour{}
	observers := map[string]*api.ScopeObserverActivity{}
	countries := map[string]*api.ScopeCountryActivity{}
	scopeObservers := map[string]map[string]bool{}
	countryObservers := map[string]map[string]bool{}
	ensure := func(name string) *api.ScopeActivityRow {
		if scopes[name] == nil {
			scopes[name] = &api.ScopeActivityRow{Name: name, Hourly: []api.ScopeActivityHour{}}
			hourRows[name] = map[int64]*api.ScopeActivityHour{}
			scopeObservers[name] = map[string]bool{}
		}
		return scopes[name]
	}
	hourFor := func(name string, at int64) *api.ScopeActivityHour {
		ensure(name)
		if hourRows[name][at] == nil {
			hourRows[name][at] = &api.ScopeActivityHour{Hour: at}
		}
		return hourRows[name][at]
	}
	for _, r := range rows {
		v := ensure(r.ScopeName)
		v.ReceiverReports += r.Reports
		v.UniqueSamples += r.UniqueSamples
		v.UnresolvedHops += r.UnresolvedHops
		v.AmbiguousHops += r.AmbiguousHops
		h := hourFor(r.ScopeName, r.Hour.Time.UnixMilli())
		h.ReceiverReports += r.Reports
		h.UniqueSamples += r.UniqueSamples
		first, last := r.FirstSeen.Time.UnixMilli(), r.LastSeen.Time.UnixMilli()
		if out.FirstSeen == nil || first < *out.FirstSeen {
			out.FirstSeen = &first
		}
		if out.LastSeen == nil || last > *out.LastSeen {
			out.LastSeen = &last
		}
		observer := r.ObserverID.String()
		scopeObservers[r.ScopeName][observer] = true
		country := iatadb.CountryFor(r.Iata)
		key := observer + "/" + r.Iata + "/" + r.ScopeName
		if observers[key] == nil {
			observers[key] = &api.ScopeObserverActivity{ObserverID: r.ObserverID, Name: r.ObserverName, IATA: r.Iata, Country: country, Scope: r.ScopeName}
		}
		o := observers[key]
		o.Reports += r.Reports
		if last > o.LastSeen {
			o.LastSeen = last
		}
		ck := country + "/" + r.ScopeName
		if countries[ck] == nil {
			countries[ck] = &api.ScopeCountryActivity{Country: country, Scope: r.ScopeName}
			countryObservers[ck] = map[string]bool{}
		}
		c := countries[ck]
		c.Reports += r.Reports
		c.UniqueSamples += r.UniqueSamples
		countryObservers[ck][observer] = true
	}
	repeats, err := s.q.GetScopeRepeaterActivity(ctx, sqlc.GetScopeRepeaterActivityParams{Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}, Iatas: iatas})
	if err != nil {
		return nil, err
	}
	for _, r := range repeats {
		v := ensure(r.ScopeName)
		if r.Total == 1 {
			v.RepeatReports = r.Reports
			v.ActiveRepeaters = r.Nodes
		} else {
			hourFor(r.ScopeName, r.Hour.Time.UnixMilli()).RepeatReports = r.Reports
		}
	}
	for name, v := range scopes {
		v.ReceivingObservers = int64(len(scopeObservers[name]))
		for _, h := range hourRows[name] {
			v.Hourly = append(v.Hourly, *h)
		}
		sort.Slice(v.Hourly, func(i, j int) bool { return v.Hourly[i].Hour < v.Hourly[j].Hour })
		out.Scopes = append(out.Scopes, *v)
	}
	for _, v := range observers {
		out.Observers = append(out.Observers, *v)
	}
	for key, v := range countries {
		v.Observers = int64(len(countryObservers[key]))
		out.Countries = append(out.Countries, *v)
	}
	sort.Slice(out.Scopes, func(i, j int) bool {
		a, b := out.Scopes[i], out.Scopes[j]
		if a.ReceiverReports != b.ReceiverReports {
			return a.ReceiverReports > b.ReceiverReports
		}
		return a.Name < b.Name
	})
	sort.Slice(out.Observers, func(i, j int) bool {
		a, b := out.Observers[i], out.Observers[j]
		if a.Reports != b.Reports {
			return a.Reports > b.Reports
		}
		return a.ObserverID.String()+a.IATA+a.Scope < b.ObserverID.String()+b.IATA+b.Scope
	})
	sort.Slice(out.Countries, func(i, j int) bool {
		return out.Countries[i].Country+out.Countries[i].Scope < out.Countries[j].Country+out.Countries[j].Scope
	})
	return out, nil
}
