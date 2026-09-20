// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
	"github.com/go-chi/chi/v5"
)

// BackupRouter returns an operator-only subrouter with one bounded export or
// transfer at a time. Its caller must apply the admin authentication middleware.
// The options are startup-owned; the request selects no targets or paths.
func BackupRouter(opts backup.Options) http.Handler {
	r := chi.NewRouter()
	r.Get("/", backupDownload(opts, backup.Export))
	return r
}

// backupDownload godoc
//
// @Summary Download a private database and saved-config backup
// @Description Opt-in operator export. Returns only a completed bundle; one export/transfer at a time. Contains secrets. Login sessions, external deployment files and import are outside this endpoint.
// @Tags Admin
// @Produce application/gzip
// @Security AdminKey
// @Success 200 {file} binary
// @Header 200 {string} Content-Disposition "attachment; filename=beacon-backup.tar.gz"
// @Failure 400 {object} map[string]APIError
// @Failure 401 {object} map[string]APIError
// @Failure 409 {object} map[string]APIError
// @Failure 500 {object} map[string]APIError
// @Failure 503 {object} map[string]APIError
// @Failure 504 {object} map[string]APIError
// @Failure 507 {object} map[string]APIError
// @Router /admin/backup [get]
func backupDownload(opts backup.Options, export func(context.Context, backup.Options) error) http.HandlerFunc {
	var busy atomic.Bool
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if opts.ConnectionService == "" {
			respondError(w, 503, "backup download is not configured")
			return
		}
		if r.URL.RawQuery != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			respondError(w, 400, "backup download accepts no query parameters or body")
			return
		}
		if !busy.CompareAndSwap(false, true) {
			respondError(w, 409, "a backup download is already in progress")
			return
		}
		defer busy.Store(false)
		dir, err := os.MkdirTemp("", "beacon-download-")
		if err != nil {
			respondError(w, 500, "cannot stage backup")
			return
		}
		defer os.RemoveAll(dir)
		requestOpts := opts
		requestOpts.OutputPath = filepath.Join(dir, "backup.tar.gz")
		if err := export(r.Context(), requestOpts); err != nil {
			if errors.Is(err, backup.ErrTooLarge) {
				respondError(w, http.StatusInsufficientStorage, "backup exceeds the configured export size limit")
			} else if errors.Is(err, context.DeadlineExceeded) {
				respondError(w, 504, "backup export timed out")
			} else if r.Context().Err() == nil {
				// Export discards pg_dump stderr and never returns connection settings.
				slog.Error("backup export failed", "component", "backup", "error", err)
				respondError(w, 500, "backup export failed; check private client configuration and capacity")
			}
			return
		}
		file, err := os.Open(requestOpts.OutputPath)
		if err != nil {
			respondError(w, 500, "cannot read completed backup")
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			respondError(w, 500, "cannot inspect completed backup")
			return
		}
		// Keep slow downloads from retaining the only slot and private files
		// indefinitely. The native server supports ResponseController deadlines.
		controller := http.NewResponseController(w)
		_ = controller.SetWriteDeadline(time.Now().Add(backup.DefaultTimeout))
		defer controller.SetWriteDeadline(time.Time{})
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", `attachment; filename="beacon-backup.tar.gz"`)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		_, _ = io.Copy(w, file)
	}
}
