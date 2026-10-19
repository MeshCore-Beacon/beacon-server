// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package planguard holds the planguard corpus and its frozen statement texts, so a binary can check plans
// without a checkout. Import it as pgdata; the engine is internal/planguard.
package planguard

import "embed"

// Files holds corpus.yaml and the sql/ texts it names.
//
//go:embed corpus.yaml sql
var Files embed.FS
