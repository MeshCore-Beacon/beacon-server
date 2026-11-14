// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// PollingConfig controls repeater polling. Bounds match the firmware clamps,
// so a configured value is never silently changed on the device.
type PollingConfig struct {
	Enabled          bool     `yaml:"enabled"`
	MQTTHosts        []string `yaml:"mqtt_hosts"`
	DirectWindow     duration `yaml:"direct_window"`     // default 1h
	Grace            duration `yaml:"grace"`             // default 30m
	MaxRequests      int      `yaml:"max_requests"`      // default 2
	ReadingRetention duration `yaml:"reading_retention"` // default 744h
	MinClientVersion string   `yaml:"min_client_version"`
	ExcludedNodes    []string `yaml:"excluded_nodes"` // 64 hex each
	Paused           bool     `yaml:"paused"`
	Authority        string   `yaml:"authority"` // evidence (default) or approved
}

// ResolvedPolling is PollingConfig with defaults applied and values parsed.
type ResolvedPolling struct {
	Enabled          bool
	MQTTHosts        []string // lowercased
	DirectWindow     time.Duration
	Grace            time.Duration
	MaxRequests      int
	ReadingRetention time.Duration
	MinClientVersion string
	ExcludedNodes    [][]byte
	Paused           bool
	Authority        string
}

// Who may be elected poller: anyone with a live claim, ranked on advert evidence, or only operator-approved
// observers. Evidence is reported by the observer itself, so approval is the setting that keeps strangers out.
const (
	PollingAuthorityEvidence = "evidence"
	PollingAuthorityApproved = "approved"
)

const (
	maxPollingHosts         = 4
	maxPollingExcludedNodes = 16
)

var pollingVersionRe = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

// Resolve applies defaults (zero means unset), validates bounds and parses values.
func (p PollingConfig) Resolve() (ResolvedPolling, error) {
	r := ResolvedPolling{
		Enabled:          p.Enabled,
		DirectWindow:     p.DirectWindow.Duration,
		Grace:            p.Grace.Duration,
		MaxRequests:      p.MaxRequests,
		ReadingRetention: p.ReadingRetention.Duration,
		MinClientVersion: p.MinClientVersion,
		Paused:           p.Paused,
		Authority:        p.Authority,
	}
	if r.Authority == "" {
		r.Authority = PollingAuthorityEvidence
	}
	if r.DirectWindow == 0 {
		r.DirectWindow = time.Hour
	}
	if r.Grace == 0 {
		r.Grace = 30 * time.Minute
	}
	if r.MaxRequests == 0 {
		r.MaxRequests = 2
	}
	if r.ReadingRetention == 0 {
		r.ReadingRetention = 744 * time.Hour
	}

	if len(p.MQTTHosts) > maxPollingHosts {
		return ResolvedPolling{}, fmt.Errorf("polling.mqtt_hosts: at most %d hosts", maxPollingHosts)
	}
	r.MQTTHosts = make([]string, 0, len(p.MQTTHosts))
	for _, h := range p.MQTTHosts {
		if !validPollingHost(h) {
			return ResolvedPolling{}, fmt.Errorf("polling.mqtt_hosts: %q must be 1 to 63 characters of letters, digits, dot or hyphen", h)
		}
		r.MQTTHosts = append(r.MQTTHosts, strings.ToLower(h))
	}
	if r.Enabled && len(r.MQTTHosts) == 0 {
		return ResolvedPolling{}, fmt.Errorf("polling.mqtt_hosts: required when polling.enabled is true")
	}
	if r.DirectWindow < 30*time.Minute || r.DirectWindow > 24*time.Hour {
		return ResolvedPolling{}, fmt.Errorf("polling.direct_window must be between 30m and 24h")
	}
	if r.Grace < 10*time.Minute || r.Grace > 2*time.Hour {
		return ResolvedPolling{}, fmt.Errorf("polling.grace must be between 10m and 2h")
	}
	if r.MaxRequests < 1 || r.MaxRequests > 3 {
		return ResolvedPolling{}, fmt.Errorf("polling.max_requests must be between 1 and 3")
	}
	if r.ReadingRetention < 0 {
		return ResolvedPolling{}, fmt.Errorf("polling.reading_retention must not be negative")
	}
	if r.MinClientVersion != "" && !pollingVersionRe.MatchString(r.MinClientVersion) {
		return ResolvedPolling{}, fmt.Errorf("polling.min_client_version must be X.Y.Z or empty")
	}
	if r.Authority != PollingAuthorityEvidence && r.Authority != PollingAuthorityApproved {
		return ResolvedPolling{}, fmt.Errorf("polling.authority must be evidence or approved")
	}
	if len(p.ExcludedNodes) > maxPollingExcludedNodes {
		return ResolvedPolling{}, fmt.Errorf("polling.excluded_nodes: at most %d entries", maxPollingExcludedNodes)
	}
	for _, s := range p.ExcludedNodes {
		b, err := hex.DecodeString(s)
		if err != nil || len(b) != 32 {
			return ResolvedPolling{}, fmt.Errorf("polling.excluded_nodes: entries must be 64 hex characters")
		}
		r.ExcludedNodes = append(r.ExcludedNodes, b)
	}
	return r, nil
}

func validPollingHost(h string) bool {
	if len(h) < 1 || len(h) > 63 {
		return false
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-':
		default:
			return false
		}
	}
	return true
}
