// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type idRange struct{ from, to int64 }

// fakeBackfiller records every call and holds the state in memory. Each batch reports rows rows.
// failCall makes the Nth backfill call (1-based) fail; saveErr fails every SaveState.
type fakeBackfiller struct {
	state    map[string]json.RawMessage
	minID    int64
	maxID    int64
	rows     int64
	ranges   int
	batches  []idRange
	failCall int
	saveErr  error
}

func newFakeBackfiller(minID, maxID int64) *fakeBackfiller {
	return &fakeBackfiller{state: map[string]json.RawMessage{}, minID: minID, maxID: maxID, rows: 7}
}

func (f *fakeBackfiller) ObservationIDRange(context.Context) (int64, int64, error) {
	f.ranges++
	return f.minID, f.maxID, nil
}

func (f *fakeBackfiller) BackfillObservationPayloadTypes(_ context.Context, from, to int64) (int64, error) {
	f.batches = append(f.batches, idRange{from, to})
	if f.failCall == len(f.batches) {
		return 0, errors.New("deadlock detected")
	}
	return f.rows, nil
}

func (f *fakeBackfiller) LoadState(_ context.Context, key string, v any) (bool, error) {
	raw, ok := f.state[key]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(raw, v)
}

func (f *fakeBackfiller) SaveState(_ context.Context, key string, v any) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f.state[key] = raw
	return nil
}

func (f *fakeBackfiller) saved(t *testing.T) db.ObservationPayloadTypeState {
	t.Helper()
	var st db.ObservationPayloadTypeState
	raw, ok := f.state[db.ObservationPayloadTypeKey]
	if !ok {
		t.Fatal("no state saved")
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func (f *fakeBackfiller) takeBatches() []idRange {
	b := f.batches
	f.batches = nil
	return b
}

func TestPayloadTypeBackfillTask(t *testing.T) {
	ctx := context.Background()
	const span = 1_000_000
	run := func(t *testing.T, task Task) {
		t.Helper()
		if err := task.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(t *testing.T, f *fakeBackfiller, batches []idRange, st db.ObservationPayloadTypeState) {
		t.Helper()
		if got := f.takeBatches(); !slices.Equal(got, batches) {
			t.Fatalf("batches %v, want %v", got, batches)
		}
		if got := f.saved(t); got != st {
			t.Fatalf("state %+v, want %+v", got, st)
		}
	}

	t.Run("walks to the max and stops", func(t *testing.T) {
		f := newFakeBackfiller(100, 2_350_000)
		task := payloadTypeBackfillTask(f, time.Minute, span, 2)
		if task.Name != "payload_type_backfill" || task.Interval != time.Minute {
			t.Fatalf("task %q every %v", task.Name, task.Interval)
		}
		before := testutil.ToFloat64(metrics.JobRows.WithLabelValues("payload_type_backfill"))
		run(t, task)
		expect(t, f, []idRange{{100, 1_000_100}, {1_000_100, 2_000_100}}, db.ObservationPayloadTypeState{NextID: 2_000_100})
		run(t, task)
		expect(t, f, []idRange{{2_000_100, 2_350_001}}, db.ObservationPayloadTypeState{NextID: 2_350_001, Done: true})
		if got := testutil.ToFloat64(metrics.JobRows.WithLabelValues("payload_type_backfill")) - before; got != 3*7 {
			t.Fatalf("job rows grew by %v, want 21", got)
		}
		ranges := f.ranges
		run(t, task)
		if f.ranges != ranges || len(f.batches) != 0 {
			t.Fatalf("a done job read the range (%d → %d) or ran batches %v", ranges, f.ranges, f.batches)
		}
	})

	t.Run("last batch of a run ends at the max", func(t *testing.T) {
		f := newFakeBackfiller(1, 1_500_000)
		task := payloadTypeBackfillTask(f, time.Minute, span, 2)
		run(t, task)
		expect(t, f, []idRange{{1, 1_000_001}, {1_000_001, 1_500_001}}, db.ObservationPayloadTypeState{NextID: 1_500_001})
		run(t, task)
		expect(t, f, nil, db.ObservationPayloadTypeState{NextID: 1_500_001, Done: true})
	})

	t.Run("saved position below the min starts at the min", func(t *testing.T) {
		f := newFakeBackfiller(5_000_000, 5_200_000)
		if err := f.SaveState(ctx, db.ObservationPayloadTypeKey, db.ObservationPayloadTypeState{NextID: 3_000_000}); err != nil {
			t.Fatal(err)
		}
		run(t, payloadTypeBackfillTask(f, time.Minute, span, 2))
		expect(t, f, []idRange{{5_000_000, 5_200_001}}, db.ObservationPayloadTypeState{NextID: 5_200_001, Done: true})
	})

	t.Run("the max grows between runs", func(t *testing.T) {
		f := newFakeBackfiller(1, 1_200_000)
		task := payloadTypeBackfillTask(f, time.Minute, span, 1)
		run(t, task)
		expect(t, f, []idRange{{1, 1_000_001}}, db.ObservationPayloadTypeState{NextID: 1_000_001})
		f.maxID = 2_600_000
		run(t, task)
		expect(t, f, []idRange{{1_000_001, 2_000_001}}, db.ObservationPayloadTypeState{NextID: 2_000_001})
		run(t, task)
		expect(t, f, []idRange{{2_000_001, 2_600_001}}, db.ObservationPayloadTypeState{NextID: 2_600_001})
		f.maxID = 2_700_000 // ingest above the captured max before the run that would finish
		run(t, task)
		expect(t, f, []idRange{{2_600_001, 2_700_001}}, db.ObservationPayloadTypeState{NextID: 2_700_001})
		run(t, task)
		expect(t, f, nil, db.ObservationPayloadTypeState{NextID: 2_700_001, Done: true})
	})

	t.Run("ids above 2^31", func(t *testing.T) {
		f := newFakeBackfiller(1<<31-10, 1<<31+10)
		run(t, payloadTypeBackfillTask(f, time.Minute, 8, 10))
		expect(t, f, []idRange{{1<<31 - 10, 1<<31 - 2}, {1<<31 - 2, 1<<31 + 6}, {1<<31 + 6, 1<<31 + 11}},
			db.ObservationPayloadTypeState{NextID: 1<<31 + 11, Done: true})
	})

	t.Run("empty table is done at once", func(t *testing.T) {
		f := newFakeBackfiller(0, 0)
		run(t, payloadTypeBackfillTask(f, time.Minute, span, 2))
		expect(t, f, nil, db.ObservationPayloadTypeState{NextID: 0, Done: true})
	})

	t.Run("a failed batch keeps the position", func(t *testing.T) {
		f := newFakeBackfiller(100, 2_350_000)
		f.failCall = 2
		task := payloadTypeBackfillTask(f, time.Minute, span, 3)
		if err := task.Run(ctx); err == nil {
			t.Fatal("want the batch error")
		}
		expect(t, f, []idRange{{100, 1_000_100}, {1_000_100, 2_000_100}}, db.ObservationPayloadTypeState{NextID: 1_000_100})
		f.failCall = 0
		run(t, task)
		expect(t, f, []idRange{{1_000_100, 2_000_100}, {2_000_100, 2_350_001}}, db.ObservationPayloadTypeState{NextID: 2_350_001, Done: true})
	})

	t.Run("a failed save fails the run", func(t *testing.T) {
		f := newFakeBackfiller(100, 2_350_000)
		f.saveErr = errors.New("save failed")
		if err := payloadTypeBackfillTask(f, time.Minute, span, 2).Run(ctx); !errors.Is(err, f.saveErr) {
			t.Fatalf("err %v, want the save error", err)
		}
		if len(f.batches) != 1 {
			t.Fatalf("batches %v after a failed save, want one", f.batches)
		}
	})

	t.Run("a failed save on completion fails the run", func(t *testing.T) {
		f := newFakeBackfiller(0, 0)
		f.saveErr = errors.New("save failed")
		if err := payloadTypeBackfillTask(f, time.Minute, span, 2).Run(ctx); !errors.Is(err, f.saveErr) {
			t.Fatalf("err %v, want the save error", err)
		}
	})

	t.Run("production values", func(t *testing.T) {
		f := newFakeBackfiller(1, 12_000_000)
		task := PayloadTypeBackfillTask(f, time.Minute)
		run(t, task)
		b := f.takeBatches()
		if len(b) != 10 || b[0] != (idRange{1, 1_000_001}) || b[9] != (idRange{9_000_001, 10_000_001}) {
			t.Fatalf("batches %v, want ten spans of 1,000,000 from 1", b)
		}
	})
}
