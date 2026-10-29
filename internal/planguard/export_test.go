// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

// Test-only access for the external test package: no production caller needs these, so they stay unexported.
var (
	LoadDiffSpec = loadDiffSpec // reads a diff case file from an fs.FS (the CLI parses its own bytes with ParseDiffSpec)
	FixtureHash  = fixtureHash  // the hash a finished fixture marker must hold for these generator inputs
)
