// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"fmt"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

// telemetryRequestsSealer decides whether telemetry requests run. It returns the sealer when they do; otherwise it
// returns the one-line reason they are off ("" when the operator never asked for them, which needs no log line).
// An unusable key is an error: it is a mistake to fix, not a reason to quietly run without requests.
func telemetryRequestsSealer(p config.ResolvedPolling, accountsOn bool, secretKey string) (*polling.Sealer, string, error) {
	switch {
	case !p.Enabled:
		return nil, "", nil
	case p.RequestsPerUser == 0:
		return nil, "polling.requests_per_user is 0", nil
	case !accountsOn:
		return nil, "accounts are off", nil
	case strings.TrimSpace(secretKey) == "":
		return nil, "BEACON_SECRET_KEY is not set", nil
	}
	key, err := polling.ParseSecretKey(secretKey)
	if err != nil {
		return nil, "", fmt.Errorf("BEACON_SECRET_KEY: %w", err)
	}
	s, err := polling.NewSealer(key)
	if err != nil {
		return nil, "", fmt.Errorf("BEACON_SECRET_KEY: %w", err)
	}
	return s, "", nil
}
