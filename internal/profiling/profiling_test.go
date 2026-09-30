// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package profiling

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"testing"
	"testing/synctest"
	"time"
)

func TestDisabledAndExpired(t *testing.T) {
	for _, tc := range []struct{ dir, until string }{
		{}, {filepath.Join(t.TempDir(), "unused"), time.Now().Add(-time.Hour).Format(time.RFC3339)},
	} {
		r, err := Start(context.Background(), tc.dir, tc.until, nil, nil)
		if err != nil || r != nil {
			t.Fatalf("got %v, %v", r, err)
		}
		if tc.dir != "" {
			if _, err := os.Stat(tc.dir); !os.IsNotExist(err) {
				t.Fatalf("expired capture created directory: %v", err)
			}
		}
	}
}

func TestRejectUnsafeConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, dir, until string }{
		{"missing deadline", t.TempDir(), ""},
		{"missing directory", "", time.Now().Add(time.Hour).Format(time.RFC3339)},
		{"invalid deadline", t.TempDir(), "tomorrow"},
		{"unbounded window", t.TempDir(), time.Now().Add(73 * time.Hour).Format(time.RFC3339)},
		{"relative directory", "profiles", time.Now().Add(time.Hour).Format(time.RFC3339)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := Start(context.Background(), tc.dir, tc.until, nil, nil)
			if r != nil {
				r.Stop()
			}
			if err == nil {
				t.Fatal("accepted unsafe configuration")
			}
		})
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	r, err := Start(context.Background(), dir, time.Now().Add(time.Hour).Format(time.RFC3339), nil, nil)
	if r != nil {
		r.Stop()
	}
	if err == nil {
		t.Fatal("accepted public directory")
	}
}

func TestDeadlineProducesPrivateProfileAndMetadata(t *testing.T) {
	requireSupportedPlatform(t)
	dir := filepath.Join(t.TempDir(), "profiles")
	until := time.Now().Add(500 * time.Millisecond)
	r, err := Start(context.Background(), dir, until.Format(time.RFC3339Nano), nil, func() any { return map[string]int{"acquired": 2} })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.done:
	case <-time.After(3 * time.Second):
		r.Stop()
		t.Fatal("did not stop at deadline")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if err != nil || len(files) != 1 {
		t.Fatalf("profiles %v, %v", files, err)
	}
	b, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(z)
	z.Close()
	if err != nil || len(data) == 0 {
		t.Fatalf("invalid profile: %v", err)
	}
	for _, name := range []string{files[0], files[0] + ".json"} {
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("permissions %v", info.Mode())
		}
	}
	var m struct {
		Reason                string
		StartedAt, FinishedAt time.Time
		Before, After         struct{ Pool map[string]int }
	}
	b, err = os.ReadFile(files[0] + ".json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Reason != "periodic" || !m.FinishedAt.After(m.StartedAt) || m.Before.Pool["acquired"] != 2 || m.After.Pool["acquired"] != 2 {
		t.Fatalf("bad metadata %+v", m)
	}
	r.request("reconfirm")
	r.Stop()
	files, _ = filepath.Glob(filepath.Join(dir, "*.pprof"))
	if len(files) != 1 {
		t.Fatal("capture after expiry")
	}
}

func TestStopReleasesProfiler(t *testing.T) {
	requireSupportedPlatform(t)
	active := make(chan struct{}, 2)
	old := slog.Default()
	slog.SetDefault(slog.New(captureObserver{Handler: slog.NewTextHandler(io.Discard, nil), active: active}))
	defer slog.SetDefault(old)
	dir := filepath.Join(t.TempDir(), "profiles")
	r, err := Start(context.Background(), dir, time.Now().Add(time.Hour).Format(time.RFC3339), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-active:
	case <-time.After(3 * time.Second):
		r.Stop()
		t.Fatal("capture did not start")
	}
	r.Stop()
	r.Stop()
	first, _ := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if len(first) != 1 {
		t.Fatal("active capture was not saved on shutdown")
	}
	b, err := os.ReadFile(first[0])
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, z); err != nil {
		t.Fatal(err)
	}
	z.Close()
	dir2 := filepath.Join(t.TempDir(), "profiles")
	r2, err := Start(context.Background(), dir2, time.Now().Add(400*time.Millisecond).Format(time.RFC3339Nano), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-r2.done
	files, _ := filepath.Glob(filepath.Join(dir2, "*.pprof"))
	if len(files) != 1 {
		t.Fatal("profiler was not released")
	}
}

func TestProfileWriteLimit(t *testing.T) {
	var b bytes.Buffer
	w := &limitedWriter{writer: &b, remaining: 4}
	if _, err := w.Write([]byte("12345")); err == nil {
		t.Fatal("accepted oversized profile")
	}
	if b.Len() > 4 {
		t.Fatal("exceeded file budget")
	}
	if w.err == nil {
		t.Fatal("write failure lost")
	}
}

func TestDirectoryBudgetSurvivesRestart(t *testing.T) {
	requireSupportedPlatform(t)
	dir := filepath.Join(t.TempDir(), "profiles")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "old.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(256 << 20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	r, err := Start(context.Background(), dir, time.Now().Add(time.Hour).Format(time.RFC3339), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		r.Stop()
		t.Fatal("budget did not stop recording")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatal("wrote past directory budget")
	}
}

func TestScheduleBoundsExtraCaptures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		requests := make(chan string, 1)
		type event struct {
			at     time.Duration
			reason string
		}
		var events []event
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			done <- schedule(ctx, requests, func(ctx context.Context, reason string) error {
				events = append(events, event{time.Since(start), reason})
				time.Sleep(30 * time.Second)
				return nil
			})
		}()
		synctest.Wait()
		time.Sleep(time.Minute)
		requests <- "reconfirm"
		synctest.Wait()
		if len(events) != 1 {
			t.Fatalf("ignored cooldown: %+v", events)
		}
		time.Sleep(4 * time.Minute)
		requests <- "reconfirm"
		synctest.Wait()
		time.Sleep(61 * time.Minute)
		cancel()
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		want := []event{{0, "periodic"}, {5 * time.Minute, "reconfirm"}, {35 * time.Minute, "periodic"}, {65 * time.Minute, "periodic"}}
		if !reflect.DeepEqual(events, want) {
			t.Fatalf("captures %+v, want %+v", events, want)
		}
	})
}

func TestScheduleDefersPeriodicCaptureInCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		requests := make(chan string, 1)
		var at []time.Duration
		var reasons []string
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			done <- schedule(ctx, requests, func(ctx context.Context, reason string) error {
				at = append(at, time.Since(start))
				reasons = append(reasons, reason)
				time.Sleep(30 * time.Second)
				return nil
			})
		}()
		synctest.Wait()
		time.Sleep(33 * time.Minute)
		requests <- "reconfirm"
		synctest.Wait()
		time.Sleep(33 * time.Minute)
		cancel()
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		wantAt := []time.Duration{0, 33 * time.Minute, 38 * time.Minute, 65 * time.Minute}
		wantReasons := []string{"periodic", "reconfirm", "periodic", "periodic"}
		if !reflect.DeepEqual(at, wantAt) || !reflect.DeepEqual(reasons, wantReasons) {
			t.Fatalf("captures %v %v, want %v %v", at, reasons, wantAt, wantReasons)
		}
	})
}

func TestScheduleOneCaptureCoversDeferredAndTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		requests := make(chan string, 1)
		var at []time.Duration
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			done <- schedule(ctx, requests, func(ctx context.Context, reason string) error {
				at = append(at, time.Since(start))
				time.Sleep(30 * time.Second)
				return nil
			})
		}()
		synctest.Wait()
		time.Sleep(33 * time.Minute)
		requests <- "reconfirm" // 33m: captured; the 35m tick is deferred to 38m
		synctest.Wait()
		time.Sleep(5 * time.Minute) // 38m: the deferred periodic and a fresh trigger are both due
		requests <- "reconfirm"
		synctest.Wait()
		time.Sleep(28 * time.Minute) // past the 65m tick so cancel can't race it
		cancel()
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		want := []time.Duration{0, 33 * time.Minute, 38 * time.Minute, 65 * time.Minute}
		if !reflect.DeepEqual(at, want) {
			t.Fatalf("captures %v, want %v", at, want)
		}
	})
}

func TestBudgetIgnoresNonRegularEntries(t *testing.T) {
	requireSupportedPlatform(t)
	dir := filepath.Join(t.TempDir(), "profiles")
	if err := os.MkdirAll(filepath.Join(dir, "lost+found"), 0700); err != nil {
		t.Fatal(err)
	}
	r, err := Start(context.Background(), dir, time.Now().Add(500*time.Millisecond).Format(time.RFC3339Nano), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.done:
	case <-time.After(3 * time.Second):
		r.Stop()
		t.Fatal("did not stop at deadline")
	}
	profiles, _ := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if len(profiles) != 1 {
		t.Fatalf("subdirectory stopped the recorder: %v", profiles)
	}
}

func TestTriggersAreConfigurable(t *testing.T) {
	r := &Recorder{triggers: map[string]bool{"custom": true}, requests: make(chan string, 1), done: make(chan struct{})}
	noop := func(context.Context) error { return nil }
	_ = r.WrapTask("reconfirm", noop)(context.Background())
	select {
	case got := <-r.requests:
		t.Fatalf("untriggered task requested %q", got)
	default:
	}
	_ = r.WrapTask("custom", noop)(context.Background())
	if got := <-r.requests; got != "custom" {
		t.Fatalf("got %q", got)
	}
}

func TestWrappedTaskPreservesContextAndError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	want := errors.New("task failed")
	r := &Recorder{triggers: map[string]bool{"reconfirm": true}, requests: make(chan string, 1), done: make(chan struct{})}
	run := r.WrapTask("reconfirm", func(got context.Context) error {
		if got.Err() != context.Canceled {
			t.Fatal("lost cancellation")
		}
		if label, ok := pprof.Label(got, "task"); !ok || label != "reconfirm" {
			t.Fatal("missing task label")
		}
		return want
	})
	for i := 0; i < 10; i++ {
		if err := run(ctx); err != want {
			t.Fatalf("lost task error: %v", err)
		}
	}
}

func requireSupportedPlatform(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("private profiling requires Unix permissions")
	}
}

type captureObserver struct {
	slog.Handler
	active chan<- struct{}
}

func (h captureObserver) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "CPU profiling capture started" {
		h.active <- struct{}{}
	}
	return h.Handler.Handle(ctx, record)
}
