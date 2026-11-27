// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "github.com/MeshCore-Beacon/beacon-server/internal/polling"

var _ polling.Store = (*Store)(nil)
