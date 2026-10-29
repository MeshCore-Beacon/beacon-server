// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"net/url"
	"strings"
)

// NodeTelemetryConfig connects to a separately operated, validating intake.
// It does not enable RF collection or authorize individual participants.
type NodeTelemetryConfig struct {
	GatewayURL string `yaml:"gateway_url" json:"-"`
}

func ParseNodeTelemetryURL(value string) (*url.URL, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawPath != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || strings.TrimSpace(value) != value {
		return nil, fmt.Errorf("node_telemetry.gateway_url must be an HTTP(S) service URL without credentials, queries or encoded paths")
	}
	return parsed, nil
}
