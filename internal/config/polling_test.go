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
