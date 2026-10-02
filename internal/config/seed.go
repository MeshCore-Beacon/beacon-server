// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

// Seeder is the database interface required to seed config data on startup.
type Seeder interface {
	UpsertIATA(ctx context.Context, iata string) error
	UpsertIATADetails(ctx context.Context, iata string, name string, lat, lng *float64) error
	UpsertIATABorder(ctx context.Context, iata string, border json.RawMessage) error
	UpsertRegion(ctx context.Context, slug, name, description string, displayOrder int, centerLat, centerLng *float64, zoomLevel *int) (int32, error)
	SetRegionIATAs(ctx context.Context, regionID int32, iatas []string) error
	UpsertTransportScope(ctx context.Context, name, displayName string, transportKey, keyFingerprint []byte) error
	SetChannelConfigScopes(ctx context.Context, fingerprints [][]byte, regions []string) error
}

// Seed applies config-defined regions, IATA overrides to the database.
// It is safe to call on every startup — all operations are upserts.
func Seed(ctx context.Context, cfg *Config, db Seeder) error {
	slog.Info(fmt.Sprintf("config: seeding %d IATAs, %d regions, %d scopes", len(cfg.IATAs), len(cfg.Regions), len(cfg.Scopes)), "component", "config")
	// IATA overrides
	for iata, details := range cfg.IATAs {
		if err := db.UpsertIATADetails(ctx, iata, details.Name, details.Lat, details.Lng); err != nil {
			return err
		}
		if details.BorderFile != "" {
			raw, err := os.ReadFile(details.BorderFile)
			if err != nil {
				return fmt.Errorf("iata %s: reading border file %s: %w", iata, details.BorderFile, err)
			}
			border, err := ValidateBorder(raw)
			if err != nil {
				return fmt.Errorf("iata %s: invalid border in %s: %w", iata, details.BorderFile, err)
			}
			if err := db.UpsertIATABorder(ctx, iata, border); err != nil {
				return err
			}
		}
	}
	// Regions
	for _, r := range cfg.Regions {
		id, err := db.UpsertRegion(ctx, r.Slug, r.Name, r.Description, r.DisplayOrder, r.CenterLat, r.CenterLng, r.ZoomLevel)
		if err != nil {
			return err
		}
		for _, iata := range r.IATAs {
			if err := db.UpsertIATA(ctx, iata); err != nil {
				return err
			}
		}
		// Config owns the member list, including when it takes over an imported slug.
		if err := db.SetRegionIATAs(ctx, id, r.IATAs); err != nil {
			return err
		}
	}
	// Channel region placement
	var fingerprints [][]byte
	var regions []string
	for _, scope := range cfg.ChannelScopes() {
		fingerprints, regions = append(fingerprints, scope.Fingerprint), append(regions, scope.Region)
	}
	if err := db.SetChannelConfigScopes(ctx, fingerprints, regions); err != nil {
		return err
	}
	// Transport Codes
	for _, s := range cfg.Scopes {
		entry := scopestore.FromName(s.Name)
		if err := db.UpsertTransportScope(ctx, entry.Name, "", entry.TransportKey, entry.KeyFingerprint); err != nil {
			return err
		}
	}
	return nil
}
