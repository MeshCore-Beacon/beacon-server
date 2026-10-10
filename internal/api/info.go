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
	MinAppVersion = "2.0.0" // BEACON Mobile: top advertisers sort
	MinWebVersion = "2.0.4" // Beacon Web: top advertisers sort
)

// Info is the public server description clients check before using a server.
type Info struct {
	// MinAppVersion is the oldest allowed app version (X.Y.Z); null means no requirement.
	MinAppVersion *string `json:"minAppVersion" example:"0.1.1"`
	// MinWebVersion is the oldest allowed Beacon Web version (X.Y.Z); null means no requirement.
	MinWebVersion *string `json:"minWebVersion" example:"2.0.3"`
	ServerVersion string  `json:"serverVersion" example:"2.0.2"`
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
