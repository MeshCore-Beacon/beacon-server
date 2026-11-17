// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import "fmt"

const (
	defaultAtlasLimit = 20
	maxAtlasLimit     = 100
)

// AtlasConfig sets how many cards one Atlas may hold.
type AtlasConfig struct {
	Limit *int `yaml:"limit"` // default 20
}

// ResolvedAtlas is AtlasConfig with its default applied.
type ResolvedAtlas struct {
	Limit int
}

// Resolve applies the default and rejects a limit outside 1 to 100.
func (a AtlasConfig) Resolve() (ResolvedAtlas, error) {
	r := ResolvedAtlas{Limit: defaultAtlasLimit}
	if a.Limit != nil {
		r.Limit = *a.Limit
	}
	if r.Limit < 1 || r.Limit > maxAtlasLimit {
		return ResolvedAtlas{}, fmt.Errorf("atlas.limit must be between 1 and %d", maxAtlasLimit)
	}
	return r, nil
}
