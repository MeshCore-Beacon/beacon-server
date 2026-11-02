// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import "github.com/jackc/pgx/v5/pgxpool"

// readOnlyWarning is logged once at boot when server.read_only is set.
const readOnlyWarning = "server.read_only: no migrations, ingest, background jobs or MeshMapper imports; every database session is read-only"

// bootMode says which boot steps that write may run. Each field gates one group of call sites in main.
type bootMode struct {
	// Migrate runs the schema migrations.
	Migrate bool
	// Seed writes the configured IATAs, regions and scopes.
	Seed bool
	// Backfill enables the resumable channel history job after startup and key changes.
	Backfill bool
	// Ingest starts the MQTT ingest workers.
	Ingest bool
	// Coalescer runs and flushes the presence write coalescer.
	Coalescer bool
	// Jobs runs the rollup catch-up and the background scheduler.
	Jobs bool
	// MeshMapper runs the MeshMapper importers and the restores that can prune or clear rows.
	MeshMapper bool
}

// bootModeFor returns every step for a normal server and none for a read-only one.
func bootModeFor(readOnly bool) bootMode {
	on := !readOnly
	return bootMode{Migrate: on, Seed: on, Backfill: on, Ingest: on, Coalescer: on, Jobs: on, MeshMapper: on}
}

// readOnlyTune makes every session of a pool read-only, so a write fails with SQLSTATE 25006
// instead of changing the database. It is passed to db.OpenPools when server.read_only is set.
func readOnlyTune(_ string, cfg *pgxpool.Config) {
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
}
