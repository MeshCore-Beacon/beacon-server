// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
)

func TestBackupDownload(t *testing.T) {
	opts := backup.Options{ConnectionService: "synthetic-private-service", MaxBytes: 1024, Timeout: time.Second, ConfigPath: "saved.yaml"}
	var output string
	export := func(ctx context.Context, got backup.Options) error {
		if got.ConfigPath != opts.ConfigPath || got.ConnectionService != opts.ConnectionService || got.MaxBytes != opts.MaxBytes || got.Timeout != opts.Timeout {
			t.Fatal("startup options changed")
		}
		output = got.OutputPath
		return os.WriteFile(output, []byte("completed fixture"), 0600)
	}
	handler := backupDownload(opts, export)
	for _, query := range []string{"?database=other", "?path=/private", "?command=anything"} {
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest("GET", "/backup"+query, nil))
		if w.Code != 400 || output != "" {
			t.Fatal("caller-selected arguments accepted")
		}
	}
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/backup", strings.NewReader("body")))
	if w.Code != 400 || output != "" {
		t.Fatal("request body accepted")
	}
	w = httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/backup", nil))
	if w.Code != 200 || w.Body.String() != "completed fixture" || w.Header().Get("Content-Type") != "application/gzip" ||
		w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Length") != "17" || w.Header().Get("Content-Disposition") != `attachment; filename="beacon-backup.tar.gz"` {
		t.Fatalf("unexpected download: %d %v", w.Code, w.Header())
	}
	if _, err := os.Stat(filepath.Dir(output)); !os.IsNotExist(err) {
		t.Fatal("download staging retained")
	}
	for _, failure := range []error{errors.New("PRIVATE_CANARY"), context.DeadlineExceeded, backup.ErrTooLarge} {
		handler = backupDownload(opts, func(_ context.Context, got backup.Options) error {
			output = got.OutputPath
			return failure
		})
		w = httptest.NewRecorder()
		handler(w, httptest.NewRequest("GET", "/backup", nil))
		want := 500
		if errors.Is(failure, context.DeadlineExceeded) {
			want = 504
		}
		if errors.Is(failure, backup.ErrTooLarge) {
			want = 507
		}
		if w.Code != want || strings.Contains(w.Body.String(), "PRIVATE_CANARY") || w.Header().Get("Content-Disposition") != "" {
			t.Fatal("failure leaked diagnostics or published a download")
		}
		if _, err := os.Stat(filepath.Dir(output)); !os.IsNotExist(err) {
			t.Fatal("failed staging retained")
		}
	}
	w = httptest.NewRecorder()
	BackupRouter(backup.Options{}).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 503 {
		t.Fatal("unconfigured download enabled")
	}
}

func TestBackupDownloadConcurrencyAndCancellation(t *testing.T) {
	started, finished := make(chan struct{}), make(chan struct{})
	var output string
	handler := backupDownload(backup.Options{ConnectionService: "fixture"}, func(ctx context.Context, opts backup.Options) error {
		output = opts.OutputPath
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer close(finished)
		handler(httptest.NewRecorder(), httptest.NewRequest("GET", "/backup", nil).WithContext(ctx))
	}()
	<-started
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/backup", nil))
	if w.Code != http.StatusConflict {
		t.Fatal("concurrent export started")
	}
	cancel()
	<-finished
	if _, err := os.Stat(filepath.Dir(output)); !os.IsNotExist(err) {
		t.Fatal("cancelled staging retained")
	}
}
