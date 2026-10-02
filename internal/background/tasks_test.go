// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
)

type refreshStub struct {
	calls []string
	errs  map[string]error
}

type observerCleanupStub struct {
	ids    []uuid.UUID
	err    error
	calls  int
	cutoff time.Time
}

func (s *observerCleanupStub) DeleteOldObservers(ctx context.Context, cutoff time.Time) ([]uuid.UUID, error) {
	s.calls++
	s.cutoff = cutoff
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.ids, s.err
}

func TestObserverCleanupTask(t *testing.T) {
	failure := errors.New("cleanup unavailable")
	for _, tc := range []struct {
		name      string
		retention time.Duration
		err       error
		cancelled bool
	}{
		{"disabled", 0, nil, false},
		{"negative disables", -time.Hour, nil, false},
		{"enabled", 24 * time.Hour, nil, false},
		{"failure", 24 * time.Hour, failure, false},
		{"cancelled", 24 * time.Hour, context.Canceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &observerCleanupStub{ids: []uuid.UUID{uuid.New(), uuid.New()}, err: tc.err}
			var invalidated []uuid.UUID
			task := ObserverCleanupTask(store, tc.retention, time.Minute, func(_ context.Context, id uuid.UUID) {
				invalidated = append(invalidated, id)
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			before := time.Now().Add(-tc.retention)
			err := task.Run(ctx)
			if !errors.Is(err, tc.err) || task.Name != "observer_cleanup" || task.Interval != time.Minute {
				t.Fatalf("unexpected task outcome: %v, %+v", err, task)
			}
			if tc.retention <= 0 {
				if store.calls != 0 || len(invalidated) != 0 {
					t.Fatal("disabled retention performed work")
				}
				return
			}
			if store.calls != 1 || store.cutoff.Before(before) || store.cutoff.After(time.Now().Add(-tc.retention)) {
				t.Fatal("incorrect cleanup cutoff or number of calls")
			}
			if tc.err != nil {
				if len(invalidated) != 0 {
					t.Fatal("failed cleanup invalidated cache entries")
				}
			} else if len(invalidated) != 2 || invalidated[0] != store.ids[0] || invalidated[1] != store.ids[1] {
				t.Fatal("deleted observers were not invalidated")
			}
		})
	}
}

func (s *refreshStub) refresh(ctx context.Context, name string) error {
	s.calls = append(s.calls, name)
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.errs[name]
}
func (s *refreshStub) RefreshRadioPresets(ctx context.Context) error {
	return s.refresh(ctx, "radio presets")
}

func TestViewRefreshTask(t *testing.T) {
	failure := errors.New("refresh failed")
	for _, tc := range []struct {
		name string
		errs map[string]error
	}{
		{"success", nil},
		{"failure", map[string]error{"radio presets": failure}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &refreshStub{errs: tc.errs}
			err := ViewRefreshTask(store, time.Minute).Run(context.Background())
			if len(store.calls) != 1 {
				t.Fatalf("refreshed %d views, want 1", len(store.calls))
			}
			if len(tc.errs) == 0 && err != nil {
				t.Fatal(err)
			}
			if len(tc.errs) > 0 && (!errors.Is(err, failure) || !strings.Contains(err.Error(), "radio presets")) {
				t.Errorf("err = %v", err)
			}
		})
	}
}

func TestViewRefreshTaskCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ViewRefreshTask(&refreshStub{}, time.Minute).Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
}

func TestSchedulerFailureIsNotComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var out bytes.Buffer
		previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
		defer func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags) }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		New([]Task{{Name: "fails", Interval: time.Millisecond, Run: func(context.Context) error { cancel(); return errors.New("refresh unavailable") }}}).Start(ctx)
		time.Sleep(2 * time.Millisecond) // advance the virtual clock to the first task tick
		synctest.Wait()
		failed := false
		for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n")) {
			var record map[string]any
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record["msg"] == "task complete" {
				t.Fatal("failed task reported complete")
			}
			if record["msg"] == "task failed" {
				failed = true
				if record["level"] != "ERROR" || record["error"] != "refresh unavailable" || record["task"] != "fails" {
					t.Fatalf("incorrect failure: %v", record)
				}
			}
		}
		if !failed {
			t.Fatal("scheduler did not report task failure")
		}
	})
}

type cleanupStub struct {
	calls   []string
	cutoffs map[string]time.Time
	oldest  time.Time // zero = no missing hour
}

func (s *cleanupStub) record(name string, cutoff time.Time) error {
	s.calls = append(s.calls, name)
	if s.cutoffs == nil {
		s.cutoffs = map[string]time.Time{}
	}
	s.cutoffs[name] = cutoff
	return nil
}
func (s *cleanupStub) DeleteOldTelemetry(_ context.Context, c time.Time) error {
	return s.record("telemetry", c)
}
func (s *cleanupStub) OldestMissingRollupHour(context.Context) (time.Time, bool, error) {
	s.calls = append(s.calls, "oldest")
	return s.oldest, !s.oldest.IsZero(), nil
}
func (s *cleanupStub) DeleteOldPackets(_ context.Context, c time.Time) error {
	return s.record("packets", c)
}
func (s *cleanupStub) DeleteOldChannelIATAs(_ context.Context, c time.Time) error {
	return s.record("channel iatas", c)
}
func (s *cleanupStub) DeleteOldTraceIATAs(_ context.Context, c time.Time) error {
	return s.record("trace iatas", c)
}
func (s *cleanupStub) DeleteOldTraceTags(_ context.Context, c time.Time) error {
	return s.record("trace tags", c)
}
func (s *cleanupStub) DeleteOldRollups(_ context.Context, c time.Time) error {
	return s.record("rollups", c)
}
func (s *cleanupStub) DeleteOldNodes(_ context.Context, c time.Time) error {
	return s.record("nodes", c)
}

func TestCleanupTask(t *testing.T) {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	cfg := CleanupConfig{TelemetryRetention: 31 * 24 * time.Hour, PacketRetention: 7 * 24 * time.Hour,
		RollupRetention: 90 * 24 * time.Hour, NodeDeleteAfter: 30 * 24 * time.Hour, Interval: time.Hour}
	retention := now.Add(-cfg.PacketRetention)
	for _, tc := range []struct {
		name   string
		oldest time.Time
		want   time.Time
	}{
		{"no unrolled hours", time.Time{}, retention},
		{"unrolled hour after the cutoff", retention.Add(time.Hour), retention},
		{"held behind an unrolled hour", retention.Add(-2 * time.Hour), retention.Add(-2*time.Hour - 35*time.Minute)},
		{"holdback capped", retention.Add(-72 * time.Hour), retention.Add(-24 * time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &cleanupStub{oldest: tc.oldest}
			if err := cleanupTask(s, cfg, func() time.Time { return now }).Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := []string{"telemetry", "oldest", "packets", "channel iatas", "trace iatas", "trace tags", "rollups", "nodes"}
			if !reflect.DeepEqual(s.calls, want) {
				t.Errorf("calls %v, want %v", s.calls, want)
			}
			for _, name := range []string{"packets", "channel iatas", "trace iatas"} {
				if !s.cutoffs[name].Equal(tc.want) {
					t.Errorf("%s cutoff %s, want %s", name, s.cutoffs[name], tc.want)
				}
			}
			if !s.cutoffs["trace tags"].Equal(tc.want.Add(-30 * time.Minute)) {
				t.Errorf("trace tags cutoff %s, want 30 min behind packets", s.cutoffs["trace tags"])
			}
			if !s.cutoffs["rollups"].Equal(now.Add(-cfg.RollupRetention)) || !s.cutoffs["telemetry"].Equal(now.Add(-cfg.TelemetryRetention)) {
				t.Errorf("rollup/telemetry cutoffs %v", s.cutoffs)
			}
		})
	}
}
