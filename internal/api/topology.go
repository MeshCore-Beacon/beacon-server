// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package api

import "github.com/google/uuid"

// TopologyLinks is a bounded set of undirected, adjacent known-route segments.
// It does not imply radio reachability or join across missing path entries.
type TopologyLinks struct {
	Links  [][2]uuid.UUID `json:"links"`
	Capped bool           `json:"capped"`
	Since  int64          `json:"since"`
	Until  int64          `json:"until"`
}
