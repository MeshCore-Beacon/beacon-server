// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

type refreshStub struct {
	calls []string
	errs  map[string]error
}

func (s *refreshStub) refresh(ctx context.Context, name string) error {
	s.calls = append(s.calls, name)
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.errs[name]
}
func (s *refreshStub) RefreshHourlyStats(ctx context.Context) error {
	return s.refresh(ctx, "hourly stats")
}
func (s *refreshStub) RefreshTopNodes(ctx context.Context) error { return s.refresh(ctx, "top nodes") }
func (s *refreshStub) RefreshTopObservers(ctx context.Context) error {
	return s.refresh(ctx, "top observers")
}
func (s *refreshStub) RefreshPayloadBreakdown(ctx context.Context) error {
	return s.refresh(ctx, "payload breakdown")
}
func (s *refreshStub) RefreshTopTalkers(ctx context.Context) error {
	return s.refresh(ctx, "top talkers")
}
func (s *refreshStub) RefreshTopAdvertisers(ctx context.Context) error {
	return s.refresh(ctx, "top advertisers")
}
func (s *refreshStub) RefreshRadioPresets(ctx context.Context) error {
	return s.refresh(ctx, "radio presets")
}

func TestViewRefreshTask(t *testing.T) {
	first, second := errors.New("first failure"), errors.New("second failure")
	for _, tc := range []struct {
		name string
		errs map[string]error
	}{
		{"success", nil},
		{"partial failure", map[string]error{"hourly stats": first, "top talkers": second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &refreshStub{errs: tc.errs}
			task := ViewRefreshTask(store, time.Minute)
			err := task.Run(context.Background())
			if len(store.calls) != 7 {
				t.Fatalf("refreshed %d views, want 7", len(store.calls))
			}
			if len(tc.errs) == 0 && err != nil {
				t.Fatal(err)
			}
			for name, cause := range tc.errs {
				if !errors.Is(err, cause) {
					t.Errorf("missing cause %v in %v", cause, err)
				}
				if err == nil || !strings.Contains(err.Error(), name) {
					t.Errorf("missing view name %q in %v", name, err)
				}
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

type taskLog chan string

func (w taskLog) Write(p []byte) (int, error) {
	line := string(p)
	if strings.Contains(line, "complete") || strings.Contains(line, "failed:") {
		w <- line
	}
	return len(p), nil
}

func TestSchedulerFailureIsNotComplete(t *testing.T) {
	lines := make(taskLog, 1)
	previous := log.Writer()
	log.SetOutput(lines)
	defer log.SetOutput(previous)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	New([]Task{{Name: "fails", Interval: time.Millisecond, Run: func(context.Context) error {
		cancel()
		return errors.New("refresh unavailable")
	}}}).Start(ctx)
	select {
	case line := <-lines:
		if strings.Contains(line, "complete") || !strings.Contains(line, "refresh unavailable") {
			t.Fatalf("incorrect failure status: %s", line)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not report task outcome")
	}
}
