// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestTelemetryRequestsSealer(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	on := config.ResolvedPolling{Enabled: true, RequestsPerUser: 3}
	cases := []struct {
		name     string
		p        config.ResolvedPolling
		accounts bool
		key      string
		sealer   bool
		reason   string
		err      bool
	}{
		{"everything on", on, true, key, true, "", false},
		{"polling off says nothing", config.ResolvedPolling{RequestsPerUser: 3}, true, key, false, "", false},
		{"limit zero", config.ResolvedPolling{Enabled: true}, true, key, false, "requests_per_user", false},
		{"accounts off", on, false, key, false, "accounts are off", false},
		{"key unset", on, true, "  ", false, "BEACON_SECRET_KEY is not set", false},
		{"key not base64", on, true, "not base64!", false, "", true},
		{"key too short", on, true, base64.StdEncoding.EncodeToString([]byte("short")), false, "", true},
	}
	for _, c := range cases {
		s, reason, err := telemetryRequestsSealer(c.p, c.accounts, c.key)
		if (s != nil) != c.sealer || !strings.Contains(reason, c.reason) || (reason == "") != (c.reason == "") || (err != nil) != c.err {
			t.Errorf("%s: sealer=%v reason=%q err=%v", c.name, s != nil, reason, err)
		}
		if err != nil && strings.Contains(err.Error(), c.key) && c.key != "" {
			t.Errorf("%s: error repeats the key: %v", c.name, err)
		}
	}
}
