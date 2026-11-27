// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func make17ValidKeys() []string {
	keys := make([]string, 17)
	for i := range keys {
		keys[i] = fmt.Sprintf("%064x", i+1)
	}
	return keys
}

func TestPollingDefaults(t *testing.T) {
	r, err := PollingConfig{}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if r.DirectWindow != time.Hour || r.Grace != 30*time.Minute || r.MaxRequests != 2 || r.ReadingRetention != 744*time.Hour {
		t.Fatalf("bad defaults: %+v", r)
	}
	if r.Enabled {
		t.Fatal("polling must default to disabled")
	}
	if r.Authority != PollingAuthorityEvidence {
		t.Fatalf("authority = %q", r.Authority)
	}
}

func TestPollingRequestSettings(t *testing.T) {
	r, err := PollingConfig{}.Resolve()
	if err != nil || r.RequestsPerUser != 6 || r.OfflineDisableDays != 7 || r.RequestObservers != RequestObserversAll {
		t.Fatalf("defaults: %+v %v", r, err)
	}
	if r, err = (PollingConfig{RequestObservers: "all"}).Resolve(); err != nil || r.RequestObservers != RequestObserversAll {
		t.Errorf("all stays available: %+v %v", r, err)
	}
	if r, err = (PollingConfig{RequestObservers: "approved"}).Resolve(); err != nil || r.RequestObservers != RequestObserversApproved {
		t.Errorf("approved stays available: %+v %v", r, err)
	}
	// An explicit 0 turns requests off; it is not the default.
	zero, five := 0, 5
	if r, err = (PollingConfig{RequestsPerUser: &zero}).Resolve(); err != nil || r.RequestsPerUser != 0 {
		t.Errorf("zero: %+v %v", r, err)
	}
	r, err = PollingConfig{RequestsPerUser: &five, OfflineDisableDays: 30, RequestObservers: "approved"}.Resolve()
	if err != nil || r.RequestsPerUser != 5 || r.OfflineDisableDays != 30 || r.RequestObservers != RequestObserversApproved {
		t.Errorf("set: %+v %v", r, err)
	}
	neg, big := -1, 21
	for name, c := range map[string]PollingConfig{
		"negative limit": {RequestsPerUser: &neg}, "huge limit": {RequestsPerUser: &big},
		"negative days": {OfflineDisableDays: -1}, "huge days": {OfflineDisableDays: 366},
		"observers unknown": {RequestObservers: "some"}, "observers case": {RequestObservers: "All"},
	} {
		if _, err := c.Resolve(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestPollingAcceptsAuthority(t *testing.T) {
	for _, a := range []string{PollingAuthorityEvidence, PollingAuthorityApproved} {
		r, err := PollingConfig{Authority: a}.Resolve()
		if err != nil || r.Authority != a {
			t.Errorf("%q: %q %v", a, r.Authority, err)
		}
	}
}

func TestPollingRejects(t *testing.T) {
	for name, c := range map[string]PollingConfig{
		"five hosts":        {MQTTHosts: []string{"a.b", "c.d", "e.f", "g.h", "i.j"}},
		"host with port":    {MQTTHosts: []string{"mqtt.example.net:443"}},
		"host too long":     {MQTTHosts: []string{strings.Repeat("a", 64)}},
		"empty host":        {MQTTHosts: []string{""}},
		"window under 30m":  {DirectWindow: duration{29 * time.Minute}},
		"window over 24h":   {DirectWindow: duration{25 * time.Hour}},
		"grace under 10m":   {Grace: duration{9 * time.Minute}},
		"grace over 2h":     {Grace: duration{121 * time.Minute}},
		"max requests 4":    {MaxRequests: 4},
		"max requests neg":  {MaxRequests: -1},
		"bad excluded node": {ExcludedNodes: []string{"abc"}},
		"17 excluded":       {ExcludedNodes: make17ValidKeys()},
		"enabled, no hosts": {Enabled: true},
		"version two parts": {MinClientVersion: "1.18"},
		"version with v":    {MinClientVersion: "v1.18.0"},
		"version suffix":    {MinClientVersion: "1.18.0-dev"},
		"version junk":      {MinClientVersion: "junk"},
		"version spaced":    {MinClientVersion: " 1.18.0"},
		"authority unknown": {Authority: "open"},
		"authority case":    {Authority: "Approved"},
	} {
		if _, err := c.Resolve(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestPollingAcceptsMinClientVersion(t *testing.T) {
	for _, v := range []string{"", "1.18.0", "10.0.12"} {
		if _, err := (PollingConfig{MinClientVersion: v}).Resolve(); err != nil {
			t.Errorf("%q: %v", v, err)
		}
	}
}

func TestPollingLowercasesHosts(t *testing.T) {
	r, err := PollingConfig{MQTTHosts: []string{"MQTT1.Meshcore.CA"}}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.MQTTHosts) != 1 || r.MQTTHosts[0] != "mqtt1.meshcore.ca" {
		t.Fatalf("got %v", r.MQTTHosts)
	}
}

func TestPollingExcludedNodesDecoded(t *testing.T) {
	r, err := PollingConfig{ExcludedNodes: make17ValidKeys()[:2]}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.ExcludedNodes) != 2 || len(r.ExcludedNodes[0]) != 32 || r.ExcludedNodes[0][31] != 1 {
		t.Fatalf("got %v", r.ExcludedNodes)
	}
}

func TestPollingReadingRetentionMinimum(t *testing.T) {
	_, err := PollingConfig{ReadingRetention: duration{59 * time.Minute}}.Resolve()
	if err == nil || !strings.Contains(err.Error(), "polling.reading_retention") || !strings.Contains(err.Error(), "1h") {
		t.Fatalf("59m must be rejected naming key and minimum, got %v", err)
	}
	r, err := PollingConfig{ReadingRetention: duration{time.Hour}}.Resolve()
	if err != nil || r.ReadingRetention != time.Hour {
		t.Fatalf("1h must be accepted: %+v %v", r, err)
	}
	r, err = PollingConfig{}.Resolve()
	if err != nil || r.ReadingRetention != 744*time.Hour {
		t.Fatalf("unset must resolve to 744h: %+v %v", r, err)
	}
}
