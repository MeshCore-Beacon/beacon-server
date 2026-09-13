// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Seeder is the database interface required to seed config data on startup.
type Seeder interface {
	UpsertIATA(ctx context.Context, iata string) error
	UpsertIATADetails(ctx context.Context, iata string, name string, lat, lng *float64) error
	UpsertIATABorder(ctx context.Context, iata string, border json.RawMessage) error
	UpsertRegion(ctx context.Context, slug, name, description string, displayOrder int, centerLat, centerLng *float64, zoomLevel *int) (int32, error)
	UpsertRegionIATA(ctx context.Context, regionID int32, iata string) error
	UpsertTransportScope(ctx context.Context, name, displayName string, transportKey, keyFingerprint []byte) error
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
			if err := db.UpsertRegionIATA(ctx, id, iata); err != nil {
				return err
			}
		}
	}
	// Transport Codes
	for _, s := range cfg.Scopes {
		name := normalizeScopeName(s.Name)
		key := deriveScopeKey(name)
		h := sha256.Sum256(key)
		fingerprint := h[:8]
		if err := db.UpsertTransportScope(ctx, name, "", key, fingerprint); err != nil {
			return err
		}
	}
	return nil
}

// normalizeScopeName ensures the scope name has a # or $ prefix.
// Plain names get # prepended: "bc" → "#bc".
func normalizeScopeName(name string) string {
	if strings.HasPrefix(name, "#") || strings.HasPrefix(name, "$") {
		return name
	}
	return "#" + name
}

// deriveScopeKey derives the 16-byte transport key from a normalized scope name.
// key = SHA256(name)[:16]
func deriveScopeKey(name string) []byte {
	h := sha256.Sum256([]byte(name))
	return h[:16]
}
