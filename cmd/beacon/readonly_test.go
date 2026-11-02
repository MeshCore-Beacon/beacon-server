// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBootModeReadOnly(t *testing.T) {
	if got, want := bootModeFor(false), (bootMode{true, true, true, true, true, true, true}); got != want {
		t.Errorf("bootModeFor(false) = %+v, want %+v", got, want)
	}
	if got := bootModeFor(true); got != (bootMode{}) {
		t.Errorf("bootModeFor(true) = %+v, want every step off", got)
	}

	cfg, err := pgxpool.ParseConfig("postgres://beacon@127.0.0.1:5432/beacon?application_name=beacon-maint")
	if err != nil {
		t.Fatal(err)
	}
	readOnlyTune("maint", cfg)
	if got := cfg.ConnConfig.RuntimeParams["default_transaction_read_only"]; got != "on" {
		t.Errorf("default_transaction_read_only = %q, want on", got)
	}
	if got := cfg.ConnConfig.RuntimeParams["application_name"]; got != "beacon-maint" {
		t.Errorf("application_name = %q, want it kept", got)
	}
}
