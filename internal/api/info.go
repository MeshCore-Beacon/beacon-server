// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"strconv"
	"strings"
)

// Client floors this server code needs. Bump the matching one when a release breaks older
// clients; config can raise them, never lower them. Empty means no requirement.
const (
	MinAppVersion = "2.3.0" // BEACON Mobile: alert push through the push server
	MinWebVersion = "2.3.0" // Beacon Web: alert channel switches
	// BEACON Mobile: the first release that sends a /ws ticket, required while websocket.require_origin is on
	TicketAppVersion = "2.1.1"
)

// AppFloor is the oldest BEACON Mobile release this server accepts, given its config.
func AppFloor(configured string, requireOrigin bool) string {
	floor := HigherVersion(MinAppVersion, configured)
	if requireOrigin {
		floor = HigherVersion(floor, TicketAppVersion)
	}
	return floor
}

// Info is the public server description clients check before using a server.
type Info struct {
	// MinAppVersion is the oldest allowed app version (X.Y.Z); null means no requirement.
	MinAppVersion *string `json:"minAppVersion" example:"0.1.1"`
	// MinWebVersion is the oldest allowed Beacon Web version (X.Y.Z); null means no requirement.
	MinWebVersion *string `json:"minWebVersion" example:"2.0.3"`
	ServerVersion string  `json:"serverVersion" example:"2.0.2"`
	// Features tells clients which optional parts of the API this server offers.
	Features InfoFeatures `json:"features"`
	// Display holds the instance-wide units clients show.
	Display InfoDisplay `json:"display"`
	// Limits are the instance caps clients enforce before asking.
	Limits InfoLimits `json:"limits"`
}

// InfoDisplay is the unit choice for this instance. Wire values stay Celsius and metres.
type InfoDisplay struct {
	TemperatureUnit string `json:"temperatureUnit" enums:"c,f" example:"c"`
	DistanceUnit    string `json:"distanceUnit" enums:"metric,imperial" example:"metric"`
}

// InfoLimits holds the instance caps.
type InfoLimits struct {
	// Atlas is the most cards one Atlas holds, in the browser or synced to an account.
	Atlas int `json:"atlas" example:"20"`
}

// InfoFeatures lists the optional features a server offers. Clients hide what is off.
type InfoFeatures struct {
	Polling  bool   `json:"polling" example:"true"`
	Accounts string `json:"accounts" example:"off"`
	// Registration is true when new sign-ups are open; only meaningful when accounts is "local".
	Registration      bool `json:"registration" example:"false"`
	TelemetryRequests bool `json:"telemetryRequests" example:"false"`
	// Alerts is true when the server sends alerts by at least one channel.
	Alerts bool `json:"alerts" example:"false"`
	// AlertChannels says which channels the server sends alerts by.
	AlertChannels InfoAlertChannels `json:"alertChannels"`
	// Push names the push server the app gets a device handle from, and this server's Push ID there; set only while
	// alertChannels.push is true.
	Push *InfoPush `json:"push,omitempty"`
	// PortalURL is the sign-in portal's base URL; set only when accounts is "portal".
	PortalURL string `json:"portalUrl,omitempty" example:"https://portal.example.org"`
	// AtlasTelemetry is true when the server accepts the telemetry view and telemetryTiles on Atlas pins and the 72h
	// node telemetry range. Always true on this version.
	AtlasTelemetry bool `json:"atlasTelemetry" example:"true"`
	// Atlas7d is true when the server accepts the 7d Atlas range. Always true on this version.
	Atlas7d bool `json:"atlas7d" example:"true"`
	// MapTelemetry is true when the server returns recent telemetry readings for map labels. Always true on this version.
	MapTelemetry bool `json:"mapTelemetry" example:"true"`
	// AppDeviceLabel is true when POST /auth/app/token takes a deviceLabel. Always true on this version.
	AppDeviceLabel bool `json:"appDeviceLabel" example:"true"`
}

// InfoAlertChannels is the alert channels a server offers.
type InfoAlertChannels struct {
	Email bool `json:"email" example:"false"`
	Push  bool `json:"push" example:"false"`
}

// InfoPush is where the app registers for this server's push notifications.
type InfoPush struct {
	Server string `json:"server" example:"https://push.meshcorebeacon.ca"`
	PushID string `json:"pushId" example:"inst_7f3k2m9qxa"`
}

// PushDomainCheck answers the push server's GET /.well-known/beacon-push.
type PushDomainCheck struct {
	PushID string `json:"pushId"`
	MAC    string `json:"mac"`
}

// WSTicket lets a client that sends no Origin open /ws, in the X-Beacon-Ticket header.
type WSTicket struct {
	Ticket string `json:"ticket" example:"AAABmZ1s4CgLBvU1n1yN3v8hQm9YtVq2"`
	// ExpiresAt is when the ticket stops opening /ws, in epoch ms.
	ExpiresAt int64 `json:"expiresAt" example:"1793470167000"`
}

// HigherVersion returns the newer of two X.Y.Z versions; empty sorts oldest.
func HigherVersion(a, b string) string {
	if versionLess(a, b) {
		return b
	}
	return a
}

func versionLess(a, b string) bool {
	if a == "" || b == "" {
		return a == "" && b != ""
	}
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(pa) && i < len(pb); i++ {
		x, _ := strconv.Atoi(pa[i])
		y, _ := strconv.Atoi(pb[i])
		if x != y {
			return x < y
		}
	}
	return len(pa) < len(pb)
}
