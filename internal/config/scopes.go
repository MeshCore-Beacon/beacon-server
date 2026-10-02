// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

func (c *Config) validateScopes() error {
	for i, s := range c.Scopes {
		if strings.TrimSpace(s.Name) == "" {
			return fmt.Errorf("scopes[%d].name is required", i)
		}
		if s.Region == "" {
			return fmt.Errorf("scope %q needs a region", s.Name)
		}
		if c.region(s.Region) == nil {
			return fmt.Errorf("scope %q region %q is not a configured region", s.Name, s.Region)
		}
	}
	return nil
}

func (c *Config) region(slug string) *RegionConfig {
	for i := range c.Regions {
		if c.Regions[i].Slug == slug {
			return &c.Regions[i]
		}
	}
	return nil
}

// ManualScopeMembers maps each region IATA to the normalized names of its configured scopes.
func (c *Config) ManualScopeMembers() map[string][]string {
	members := map[string][]string{}
	for _, s := range c.Scopes {
		region := c.region(s.Region)
		if region == nil {
			continue
		}
		name := scopestore.FromName(s.Name).Name
		for _, iata := range region.IATAs {
			iata = strings.ToUpper(iata)
			members[iata] = append(members[iata], name)
		}
	}
	return members
}
