// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestBackupPreconditionsDegradeWithoutDatabaseOrExit(t *testing.T) {
	cfg := &config.Config{}
	if got := configureBackup(context.Background(), cfg, nil, "", "saved.yaml"); got.ConnectionService != "" {
		t.Fatal("disabled backup configured")
	}
	cfg.Backup.Enabled = true
	if got := configureBackup(context.Background(), cfg, nil, "", "saved.yaml"); got.ConnectionService != "" {
		t.Fatal("missing admin key configured")
	}
	cfg.Auth.APIKey = "test-only-key"
	if got := configureBackup(context.Background(), cfg, nil, "host=localhost user=test dbname=test", "saved.yaml"); got.ConnectionService != "" {
		t.Fatal("keyword connection configured for backup")
	}
}
