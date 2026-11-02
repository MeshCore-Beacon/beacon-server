// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "embed"

// SQLCSources holds the generated sqlc files, so a binary can read every statement's text without a checkout.
//
//go:embed sqlc/*.sql.go
var SQLCSources embed.FS
