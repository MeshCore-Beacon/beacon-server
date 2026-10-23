// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package planguard holds the planguard corpus and its frozen statement texts, so a binary can check plans
// without a checkout. Import it as pgdata; the engine is internal/planguard.
package planguard

import "embed"

// Files holds corpus.yaml, the sql/ texts it names, the fixture generator (fixture/: its SQL steps and the
// aggregate CSVs they read) and the committed plan signatures (baseline/<tier>/).
//
//go:embed corpus.yaml sql fixture baseline
var Files embed.FS
