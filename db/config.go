package db

import (
	"context"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func (s *Store) UpsertIATADetails(ctx context.Context, iata string, name string, lat, lng *float64) error {
	return s.q.UpsertIATADetails(ctx, sqlc.UpsertIATADetailsParams{
		Iata:        iata,
		DisplayName: &name,
		ApproxLat:   lat,
		ApproxLng:   lng,
	})
}

func (s *Store) ListIATAs(ctx context.Context) ([]api.IATA, error) {
	rows, err := s.q.ListIATAs(ctx)
	if err != nil {
		return nil, err
	}
	iatas := make([]api.IATA, 0, len(rows))
	for _, v := range rows {
		iatas = append(iatas, api.IATA{
			IATA:        v.Iata,
			DisplayName: v.DisplayName,
			Lat:         v.ApproxLat,
			Lng:         v.ApproxLng,
		})
	}
	return iatas, nil
}

func (s *Store) GetIATA(ctx context.Context, iata string) (*api.IATA, error) {
	i, err := s.q.GetIATA(ctx, iata)
	if err != nil {
		return nil, err
	}
	return &api.IATA{
		IATA:        i.Iata,
		DisplayName: i.DisplayName,
		Lat:         i.ApproxLat,
		Lng:         i.ApproxLng,
	}, nil
}

func (s *Store) UpsertRegion(ctx context.Context, slug, name, description string, displayOrder int, centerLat, centerLng *float64, zoomLevel *int) (int32, error) {
	var zl *int32
	if zoomLevel != nil {
		z := int32(*zoomLevel)
		zl = &z
	}
	do := int32(displayOrder)
	return s.q.UpsertRegion(ctx, sqlc.UpsertRegionParams{
		Slug:         slug,
		Name:         name,
		Description:  &description,
		DisplayOrder: &do,
		CenterLat:    centerLat,
		CenterLng:    centerLng,
		ZoomLevel:    zl,
	})
}

func (s *Store) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	rows, err := s.q.ListRegions(ctx)
	if err != nil {
		return nil, err
	}
	regions := make([]api.RegionSummary, 0, len(rows))
	for _, v := range rows {
		regions = append(regions, api.RegionSummary{
			ID:   int(v.ID),
			Slug: v.Slug,
			Name: v.Name,
		})
	}
	return regions, nil
}

func (s *Store) GetRegion(ctx context.Context, regionID int32) (*api.Region, error) {
	region, err := s.q.GetRegion(ctx, regionID)
	if err != nil {
		return nil, err
	}
	result := api.Region{
		RegionSummary: api.RegionSummary{
			ID:   int(region.ID),
			Slug: region.Slug,
			Name: region.Name,
		},
		Description: region.Description,
		CenterLat:   region.CenterLat,
		CenterLng:   region.CenterLng,
	}
	var zoomLevel *int
	if region.ZoomLevel != nil {
		z := int(*region.ZoomLevel)
		zoomLevel = &z
	}
	result.ZoomLevel = zoomLevel
	iatas, err := s.q.GetRegionIATAs(ctx, regionID)
	if err != nil {
		return nil, err
	}
	result.IATAs = iatas
	return &result, nil
}

func (s *Store) GetRegionBySlug(ctx context.Context, slug string) (*api.Region, error) {
	region, err := s.q.GetRegionBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	result := api.Region{
		RegionSummary: api.RegionSummary{
			ID:   int(region.ID),
			Slug: region.Slug,
			Name: region.Name,
		},
		Description: region.Description,
		CenterLat:   region.CenterLat,
		CenterLng:   region.CenterLng,
	}
	var zoomLevel *int
	if region.ZoomLevel != nil {
		z := int(*region.ZoomLevel)
		zoomLevel = &z
	}
	result.ZoomLevel = zoomLevel
	iatas, err := s.q.GetRegionIATAs(ctx, region.ID)
	if err != nil {
		return nil, err
	}
	result.IATAs = iatas
	return &result, nil
}

func (s *Store) UpsertRegionIATA(ctx context.Context, regionID int32, iata string) error {
	return s.q.UpsertRegionIATA(ctx, sqlc.UpsertRegionIATAParams{
		RegionID: regionID,
		Iata:     iata,
	})
}
