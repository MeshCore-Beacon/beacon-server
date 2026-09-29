// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package profiling records bounded, opt-in CPU samples to private files.
package profiling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

const (
	captureDuration   = 30 * time.Second
	captureInterval   = 30 * time.Minute
	captureCooldown   = 5 * time.Minute
	maxProfileBytes   = 8 << 20
	maxMetadataBytes  = 16 << 10
	maxDirectoryBytes = 256 << 20
	maxDirectoryFiles = 512
)

type Recorder struct {
	root     *os.Root
	snapshot func() any
	requests chan string
	cancel   context.CancelFunc
	done     chan struct{}
}

type snapshot struct {
	ProcessCPU *processCPU `json:",omitempty"`
	Goroutines int
	Pool       any `json:",omitempty"`
}

type metadata struct {
	Reason     string
	StartedAt  time.Time
	FinishedAt time.Time
	GoVersion  string
	Before     snapshot
	After      snapshot
}

// Start leaves expired settings inactive so restarts cannot extend a capture window.
func Start(parent context.Context, dir, until string, poolSnapshot func() any) (*Recorder, error) {
	if dir == "" && until == "" {
		return nil, nil
	}
	if dir == "" || until == "" {
		return nil, errors.New("profiling requires both directory and deadline")
	}
	deadline, err := time.Parse(time.RFC3339Nano, until)
	if err != nil {
		return nil, fmt.Errorf("invalid profiling deadline: %w", err)
	}
	if !deadline.After(time.Now()) {
		return nil, nil
	}
	if time.Until(deadline) > 72*time.Hour {
		return nil, errors.New("profiling deadline must be within 72 hours")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil, errors.New("private CPU profiling is supported on Linux and macOS")
	}
	if !filepath.IsAbs(dir) {
		return nil, errors.New("profiling directory must be absolute")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("profiling directory must be private (0700) and not a symlink")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	r := &Recorder{root: root, snapshot: poolSnapshot, requests: make(chan string, 1), cancel: cancel, done: make(chan struct{})}
	slog.Info("CPU profiling enabled", "component", "profiling", "until", deadline.UTC())
	go r.run(ctx)
	return r, nil
}

func (r *Recorder) Stop() {
	if r == nil {
		return
	}
	r.cancel()
	<-r.done
}

// Request never holds up maintenance or queues a backlog of captures.
func (r *Recorder) Request(reason string) {
	if r == nil || reason != "reconfirm" {
		return
	}
	select {
	case <-r.done:
		return
	default:
	}
	select {
	case r.requests <- reason:
	default:
	}
}

func (r *Recorder) WrapTask(name string, run func(context.Context) error) func(context.Context) error {
	if r == nil {
		return run
	}
	return func(ctx context.Context) (err error) {
		select {
		case <-r.done:
			return run(ctx)
		default:
		}
		if name == "reconfirm" {
			r.Request(name)
		}
		pprof.Do(ctx, pprof.Labels("task", name), func(ctx context.Context) { err = run(ctx) })
		return err
	}
}

func (r *Recorder) run(ctx context.Context) {
	defer close(r.done)
	defer r.root.Close()
	defer r.cancel()
	defer slog.Info("CPU profiling stopped", "component", "profiling")
	if err := schedule(ctx, r.requests, r.capture); err != nil {
		slog.Warn("CPU profiling stopped after capture failure", "component", "profiling", "error", err)
	}
}

func schedule(ctx context.Context, requests <-chan string, capture func(context.Context, string) error) error {
	ticker := time.NewTicker(captureInterval)
	defer ticker.Stop()
	reason := "periodic"
	var last time.Time
	for {
		if ctx.Err() != nil {
			return nil
		}
		if last.IsZero() || time.Since(last) >= captureCooldown {
			last = time.Now()
			if err := capture(ctx, reason); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			reason = "periodic"
		case reason = <-requests:
		}
	}
}

func (r *Recorder) takeSnapshot() snapshot {
	s := snapshot{Goroutines: runtime.NumGoroutine()}
	s.ProcessCPU = cpuUsage()
	if r.snapshot != nil {
		s.Pool = r.snapshot()
	}
	return s
}

func (r *Recorder) checkBudget() error {
	d, err := r.root.Open(".")
	if err != nil {
		return err
	}
	entries, err := d.ReadDir(maxDirectoryFiles + 1)
	d.Close()
	if err != nil && err != io.EOF {
		return err
	}
	if len(entries) > maxDirectoryFiles-2 {
		return errors.New("profiling file budget exhausted")
	}
	var total int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("profiling directory contains a non-regular file")
		}
		total += info.Size()
	}
	if total > maxDirectoryBytes-maxProfileBytes-maxMetadataBytes {
		return errors.New("profiling storage budget exhausted")
	}
	return nil
}

func (r *Recorder) capture(ctx context.Context, reason string) error {
	if err := r.checkBudget(); err != nil {
		return err
	}
	started := time.Now().UTC()
	name := started.Format("20060102T150405.000000000Z") + ".pprof"
	partial := name + ".partial"
	f, err := r.root.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer r.root.Remove(partial)
	w := &limitedWriter{writer: f, remaining: maxProfileBytes}
	m := metadata{Reason: reason, StartedAt: started, GoVersion: runtime.Version(), Before: r.takeSnapshot()}
	if err := pprof.StartCPUProfile(w); err != nil {
		f.Close()
		return err
	}
	slog.Info("CPU profiling capture started", "component", "profiling", "file", name, "reason", reason)
	timer := time.NewTimer(captureDuration)
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	timer.Stop()
	pprof.StopCPUProfile()
	m.FinishedAt = time.Now().UTC()
	m.After = r.takeSnapshot()
	closeErr := f.Close()
	if w.err != nil {
		return w.err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := r.root.Rename(partial, name); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxMetadataBytes {
		return errors.New("profiling metadata budget exhausted")
	}
	meta, err := r.root.OpenFile(name+".json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = meta.Write(data)
	closeErr = meta.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	slog.Info("CPU profile saved", "component", "profiling", "file", name, "reason", reason, "duration", m.FinishedAt.Sub(started))
	return nil
}

type limitedWriter struct {
	writer    io.Writer
	remaining int
	err       error
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if len(p) > w.remaining {
		w.err = errors.New("CPU profile exceeded size limit")
		return 0, w.err
	}
	n, err := w.writer.Write(p)
	w.remaining -= n
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

type processCPU struct {
	UserSeconds   float64
	SystemSeconds float64
}
