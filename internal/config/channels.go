// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"gopkg.in/yaml.v3"
)

// HashtagConfig is a hashtag channel; a plain string in YAML is a Beacon-wide one.
type HashtagConfig struct {
	Name   string `yaml:"name"`
	Region string `yaml:"region"` // optional region slug
}

func (h *HashtagConfig) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&h.Name)
	}
	type plain HashtagConfig
	return node.Decode((*plain)(h))
}

// ChannelScope places a configured channel under a region, or Beacon-wide when Region is empty.
type ChannelScope struct {
	Fingerprint []byte
	Region      string
}

func (c *Config) validateChannelKeys() error {
	for i, h := range c.ChannelKeys.Hashtags {
		if strings.TrimSpace(h.Name) == "" {
			return fmt.Errorf("channel_keys.hashtags[%d] needs a name", i)
		}
		if h.Region != "" && c.region(h.Region) == nil {
			return fmt.Errorf("channel #%s region %q is not a configured region", h.Name, h.Region)
		}
	}
	for hash, k := range c.ChannelKeys.Keys {
		if k.Region != "" && c.region(k.Region) == nil {
			return fmt.Errorf("channel key %s region %q is not a configured region", hash, k.Region)
		}
	}
	return nil
}

// ChannelScopes lists every configured channel's region placement. Keys that
// aren't valid hex are skipped; startup already warns about them.
func (c *Config) ChannelScopes() []ChannelScope {
	var scopes []ChannelScope
	for _, h := range c.ChannelKeys.Hashtags {
		_, _, fingerprint := keystore.DeriveHashtagKey(h.Name)
		scopes = append(scopes, ChannelScope{Fingerprint: fingerprint, Region: h.Region})
	}
	for _, k := range c.ChannelKeys.Keys {
		if key, err := hex.DecodeString(k.Key); err == nil {
			scopes = append(scopes, ChannelScope{Fingerprint: keystore.Fingerprint(key), Region: k.Region})
		}
	}
	return scopes
}
