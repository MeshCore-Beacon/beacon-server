// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

type pollMaintStub struct {
	lockOK     bool
	locked     int
	unlocked   int
	corroSince time.Time
	corroLimit int
	claims     time.Time
	readings   time.Time
}

type stubLock struct{ s *pollMaintStub }

func (l stubLock) Close(context.Context) { l.s.unlocked++ }

func (s *pollMaintStub) BeginPollingElection(context.Context) (PollingLock, bool, error) {
	if !s.lockOK {
		return nil, false, nil
	}
	s.locked++
	return stubLock{s}, true, nil
}

func (s *pollMaintStub) CorroborateReadings(_ context.Context, since time.Time, limit int) (int, error) {
	s.corroSince, s.corroLimit = since, limit
	return 0, nil
}

func (s *pollMaintStub) DeleteExpiredPolling(_ context.Context, claims, readings time.Time) error {
	s.claims, s.readings = claims, readings
	return nil
}

// pollTargetsStore counts target listings; the other methods are never reached with no targets.
type pollTargetsStore struct {
	polling.Store
	listed int
}

func (s *pollTargetsStore) ListPollingTargets(context.Context, time.Time) ([][]byte, error) {
	s.listed++
	return nil, nil
}

func pollTask(t *testing.T, tasks []Task, name string) Task {
	t.Helper()
	for _, k := range tasks {
		if k.Name == name {
			return k
		}
	}
	t.Fatalf("no task %q", name)
	return Task{}
}

func newPollSvc(store polling.Store) *polling.Service {
	return polling.NewService(store, config.ResolvedPolling{Enabled: true}, time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestPollingTasksNamesAndIntervals(t *testing.T) {
	tasks := PollingTasks(newPollSvc(&pollTargetsStore{}), &pollMaintStub{lockOK: true}, config.ResolvedPolling{})
	want := map[string]time.Duration{
		"polling_election":    time.Minute,
		"polling_corroborate": 5 * time.Minute,
		"polling_cleanup":     time.Hour,
	}
	if len(tasks) != len(want) {
		t.Fatalf("got %d tasks", len(tasks))
	}
	for _, k := range tasks {
		if want[k.Name] != k.Interval {
			t.Errorf("%s interval %v", k.Name, k.Interval)
		}
	}
}

func TestPollingElectionTaskRunsElectAll(t *testing.T) {
	st, ms := &pollTargetsStore{}, &pollMaintStub{lockOK: true}
	task := pollTask(t, PollingTasks(newPollSvc(st), ms, config.ResolvedPolling{}), "polling_election")
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.listed != 1 || ms.locked != 1 || ms.unlocked != 1 {
		t.Fatal("listed/locked/unlocked", st.listed, ms.locked, ms.unlocked)
	}
	// Another process holds the lock: no work, no error.
	ms.lockOK = false
	if err := task.Run(context.Background()); err != nil || st.listed != 1 {
		t.Fatal("ran without the lock", st.listed, err)
	}
}

func TestPollingCorroborateAndCleanupArguments(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	old := pollingNow
	pollingNow = func() time.Time { return now }
	t.Cleanup(func() { pollingNow = old })
	ms := &pollMaintStub{lockOK: true}
	tasks := PollingTasks(newPollSvc(&pollTargetsStore{}), ms, config.ResolvedPolling{ReadingRetention: 744 * time.Hour})
	if err := pollTask(t, tasks, "polling_corroborate").Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ms.corroSince.Equal(now.Add(-30*time.Minute)) || ms.corroLimit != 500 {
		t.Fatal(ms.corroSince, ms.corroLimit)
	}
	if err := pollTask(t, tasks, "polling_cleanup").Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ms.claims.Equal(now.Add(-24 * time.Hour)) {
		t.Fatal("claims cutoff", ms.claims)
	}
}

func TestPollingCleanupUsesRetention(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	old := pollingNow
	pollingNow = func() time.Time { return now }
	t.Cleanup(func() { pollingNow = old })
	ms := &pollMaintStub{}
	tasks := PollingTasks(newPollSvc(&pollTargetsStore{}), ms, config.ResolvedPolling{ReadingRetention: 100 * time.Hour})
	if err := pollTask(t, tasks, "polling_cleanup").Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ms.readings.Equal(now.Add(-100 * time.Hour)) {
		t.Fatal("readings cutoff", ms.readings)
	}
}

func TestPollingCleanupKeepsReadingsForTheWorkingPollWindow(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	old := pollingNow
	pollingNow = func() time.Time { return now }
	t.Cleanup(func() { pollingNow = old })
	ms := &pollMaintStub{}
	cfg := config.ResolvedPolling{ReadingRetention: time.Hour, DirectWindow: 2 * time.Hour}
	if err := pollTask(t, PollingTasks(newPollSvc(&pollTargetsStore{}), ms, cfg), "polling_cleanup").Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := now.Add(-polling.WorkingPollFresh(cfg)); !ms.readings.Equal(want) {
		t.Fatal("readings cutoff", ms.readings, "want", want)
	}
}

type offlineStub struct {
	cutoff, now time.Time
	reason      string
}

func (s *offlineStub) DisableOfflineRequests(_ context.Context, cutoff, now time.Time, reason string) (int, error) {
	s.cutoff, s.now, s.reason = cutoff, now, reason
	return 2, nil
}

func TestRequestsOfflineTask(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	old := pollingNow
	pollingNow = func() time.Time { return now }
	t.Cleanup(func() { pollingNow = old })
	st := &offlineStub{}
	tasks := RequestTasks(st, 7)
	if len(tasks) != 1 || tasks[0].Name != "requests_offline" || tasks[0].Interval != time.Hour {
		t.Fatalf("%+v", tasks)
	}
	if err := tasks[0].Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !st.cutoff.Equal(now.AddDate(0, 0, -7)) || !st.now.Equal(now) || st.reason != "Not heard by any observer for 7 days." {
		t.Fatalf("%+v", st)
	}
}
