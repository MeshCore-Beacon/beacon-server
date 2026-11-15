// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

const (
	pollingElectionInterval    = time.Minute
	pollingCorroborateInterval = 5 * time.Minute
	pollingCleanupInterval     = time.Hour

	pollingCorroborateWindow = 30 * time.Minute
	pollingCorroborateBatch  = 500
	// Claims live 3900 s; a day on top keeps expired ones visible for debugging.
	pollingClaimKeep = 24 * time.Hour
)

var pollingNow = time.Now

// PollingLock is the held election lock (see db.PollingElectionLock).
type PollingLock interface{ Close(ctx context.Context) }

// PollingMaintStore is what the polling jobs need besides the service.
type PollingMaintStore interface {
	BeginPollingElection(ctx context.Context) (PollingLock, bool, error)
	CorroborateReadings(ctx context.Context, receivedSince time.Time, limit int) (int, error)
	DeleteExpiredPolling(ctx context.Context, claimsBefore, readingsBefore time.Time) error
}

type pollingMaint struct{ *db.Store }

// NewPollingMaint adapts the maintenance store.
func NewPollingMaint(s *db.Store) PollingMaintStore { return pollingMaint{s} }

func (m pollingMaint) BeginPollingElection(ctx context.Context) (PollingLock, bool, error) {
	l, ok, err := m.Store.BeginPollingElection(ctx)
	// Return a plain nil, not a nil *PollingElectionLock wrapped in a non-nil interface.
	if l == nil {
		return nil, ok, err
	}
	return l, ok, err
}

// PollingTasks re-elects pollers, recounts corroboration and expires old rows. The election runs
// on one process at a time; losing the lock is not an error.
func PollingTasks(svc *polling.Service, store PollingMaintStore, cfg config.ResolvedPolling) []Task {
	return []Task{
		{Name: "polling_election", Interval: pollingElectionInterval, Run: func(ctx context.Context) error {
			l, ok, err := store.BeginPollingElection(ctx)
			if err != nil || !ok {
				return err
			}
			defer l.Close(ctx)
			return svc.ElectAll(ctx)
		}},
		{Name: "polling_corroborate", Interval: pollingCorroborateInterval, Run: func(ctx context.Context) error {
			_, err := store.CorroborateReadings(ctx, pollingNow().Add(-pollingCorroborateWindow), pollingCorroborateBatch)
			return err
		}},
		{Name: "polling_cleanup", Interval: pollingCleanupInterval, Run: func(ctx context.Context) error {
			now := pollingNow()
			return store.DeleteExpiredPolling(ctx, now.Add(-pollingClaimKeep), now.Add(-polling.ReadingKeep(cfg)))
		}},
	}
}
