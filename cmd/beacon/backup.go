// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A failed optional export prerequisite must not stop ingest or the public API.
// Empty options leave the authenticated backup endpoint unavailable (503).
func configureBackup(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, dsn, configPath string) backup.Options {
	if !cfg.Backup.Enabled {
		return backup.Options{}
	}
	if cfg.Auth.APIKey == "" {
		slog.Error("backup unavailable: configure an admin bearer key", "component", "backup")
		return backup.Options{}
	}
	service, err := backup.ConnectionService(dsn)
	if err != nil {
		slog.Error("backup unavailable: POSTGRES_DSN must be a supported single-host PostgreSQL URL; keyword DSNs remain valid for Beacon", "component", "backup")
		return backup.Options{}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var serverVersion int
	if err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&serverVersion); err != nil {
		slog.Error("backup unavailable: could not determine PostgreSQL server version", "component", "backup")
		return backup.Options{}
	}
	if err := backup.CheckClient(ctx, serverVersion); err != nil {
		// CheckClient never returns process output or connection settings.
		slog.Error("backup unavailable", "component", "backup", "error", err)
		return backup.Options{}
	}
	return backup.Options{ConfigPath: configPath, MaxBytes: backup.DefaultMaxBytes,
		Timeout: backup.DefaultTimeout, Version: version, ConnectionService: service}
}
