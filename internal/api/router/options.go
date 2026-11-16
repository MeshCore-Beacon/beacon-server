// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
)

// Options names the startup settings used by the router. Adding an optional
// route does not change New's signature or require unrelated callers to change.
type Options struct {
	MaxConnsPerIP        int
	MaxConnectsPerMinute int
	WSSendBytesPerSecond int
	WSSendBurstBytes     int
	WSAllowedOrigins     []string
	WSRequireOrigin      bool
	CORS                 config.CORSConfig
	Server               config.ServerConfig
	Auth                 config.AuthConfig
	RateLimit            config.ResolvedRateLimitConfig
	Scopes               handlers.ScopeMembership // region-filtered scope lists; nil lists none
	RollupRetention      time.Duration            // longest historical stats window
	MinAppVersion        string                   // empty means no mobile app requirement
	MinWebVersion        string                   // empty means no Beacon Web requirement
	ServerVersion        string
	BuildVersion         string
	BuildRevision        string
	// AdminRoutes mounts operator-only subrouters at literal paths such as
	// "/accounts". Each feature owns its dependencies, methods and handlers.
	// The router applies authentication to every path and method in this group.
	AdminRoutes map[string]http.Handler
	// Guards guards the public API group; nil runs it unguarded and reports no guards.
	Guards *guards.Guard
	// GuestPolling answers the firmware policy and owners routes. Leave it an untyped nil when
	// polling is not wired: a nil *polling.Service stored here is not == nil and would be called.
	GuestPolling handlers.GuestPolling
	// PollingEnabled is reported as features.polling in /info.
	PollingEnabled bool
	// TemperatureUnit (c or f) and DistanceUnit (metric or imperial) are reported in /info display; empty reads as c and metric.
	TemperatureUnit, DistanceUnit string
	// AccountsMode is reported as features.accounts: "local" or anything else for off.
	AccountsMode string
	// RegistrationOpen is reported as features.registration while accounts are local.
	RegistrationOpen bool
	// PortalURL is reported as features.portalUrl while accounts are in portal mode.
	PortalURL string
	// AlertDelivery is "push" or "email" when alerts are on, else empty; it is reported as features.alerts and
	// features.alertDelivery.
	AlertDelivery string
	// Accounts serves the sign-in and profile routes. Leave it an untyped nil when accounts are off: a nil
	// *users.Service stored here is not == nil and would be called.
	Accounts handlers.AccountsService
	// AuthOptions configures those routes; with Enabled false they answer 404 accounts_disabled.
	AuthOptions handlers.AuthOptions
	// TelemetryRequests is reported as features.telemetryRequests in /info.
	TelemetryRequests bool
	// MeMounts adds routes under /me (Atlas, telemetry requests); they need accounts on.
	MeMounts []handlers.MeMount
	// APIPlanLayout is reported in the admin config's guards view.
	APIPlanLayout string
}
