package config

import (
	"context"
	"log"
)

// Seeder is the database interface required to seed config data on startup.
type Seeder interface {
	UpsertIATA(ctx context.Context, iata string) error
	UpsertIATADetails(ctx context.Context, iata string, name string, lat, lng *float64) error
	UpsertRegion(ctx context.Context, slug, name, description string, displayOrder int, centerLat, centerLng *float64, zoomLevel *int) (int32, error)
	UpsertRegionIATA(ctx context.Context, regionID int32, iata string) error
}

// Seed applies config-defined regions, IATA overrides to the database.
// It is safe to call on every startup — all operations are upserts.
func Seed(ctx context.Context, cfg *Config, db Seeder) error {
	log.Printf("config: seeding %d IATAs, %d regions", len(cfg.IATAs), len(cfg.Regions))
	// IATA overrides
	for iata, details := range cfg.IATAs {
		if err := db.UpsertIATADetails(ctx, iata, details.Name, details.Lat, details.Lng); err != nil {
			return err
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
	return nil
}
