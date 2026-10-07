// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// Info is the public server description the mobile app checks before using a server.
type Info struct {
	// MinAppVersion is the oldest allowed app version (X.Y.Z); null means no requirement.
	MinAppVersion *string `json:"minAppVersion" example:"0.1.1"`
	ServerVersion string  `json:"serverVersion" example:"2.0.2"`
}
