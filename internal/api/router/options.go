// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

// Options names the startup settings used by the router. Adding an optional
// route does not change New's signature or require unrelated callers to change.
type Options struct {
	MaxConnsPerIP        int
	MaxConnectsPerMinute int
	CORS                 config.CORSConfig
	Server               config.ServerConfig
	Auth                 config.AuthConfig
	RateLimit            config.ResolvedRateLimitConfig
	// AdminRoutes mounts operator-only subrouters at literal paths such as
	// "/accounts". Each feature owns its dependencies, methods and handlers.
	// The router applies authentication to every path and method in this group.
	AdminRoutes map[string]http.Handler
}
