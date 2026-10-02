// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"encoding/json"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

// CatalogueScope is regional source metadata, not per-node forwarding evidence.
type CatalogueScope struct {
	Name       string `json:"name"`
	Repeaters  int    `json:"repeaters"`
	Default    int    `json:"default"`
	Monitored  bool   `json:"monitored"`
	Wardriving bool   `json:"wardriving"`
}

type PublicScopeCatalogue struct {
	IATA        string           `json:"iata"`
	URL         string           `json:"url"`
	GeneratedAt int64            `json:"generatedAt"`
	CheckedAt   int64            `json:"checkedAt"`
	FreshUntil  int64            `json:"freshUntil"`
	LastError   string           `json:"lastError,omitempty"`
	Repeaters   int              `json:"repeaters"`
	Scoped      int              `json:"scoped"`
	Scopes      []CatalogueScope `json:"scopes"`
}

// Catalogues returns an immutable snapshot of discovered sources. No request
// triggers an upstream fetch or reads ingestion tables. Callers must not mutate it.
func (i *Importer) Catalogues() []PublicScopeCatalogue {
	if i != nil {
		if snapshot := i.catalogues.Load(); snapshot != nil {
			return snapshot.([]PublicScopeCatalogue)
		}
	}
	return []PublicScopeCatalogue{}
}

func (i *Importer) publishCatalogues() {
	result := make([]PublicScopeCatalogue, 0, len(i.sources))
	for _, s := range i.sources {
		c := PublicScopeCatalogue{IATA: s.iata, URL: s.url, LastError: s.cache.LastError, Scopes: []CatalogueScope{}}
		if !s.cache.CheckedAt.IsZero() {
			c.CheckedAt = s.cache.CheckedAt.UnixMilli()
			c.FreshUntil = s.cache.CheckedAt.Add(i.interval + 5*time.Minute).UnixMilli()
		}
		if len(s.cache.Payload) > 0 {
			// Reuse the importer validation before exposing saved metadata. Invalid
			// saved catalogues are omitted just as they are from the scope matcher.
			if _, generated, err := decode(s.cache.Payload, s.iata); err == nil {
				var payload struct {
					Repeaters int
					Scoped    int
					Scopes    []CatalogueScope
				}
				if json.Unmarshal(s.cache.Payload, &payload) == nil {
					c.GeneratedAt = generated.UnixMilli()
					c.Repeaters, c.Scoped = payload.Repeaters, payload.Scoped
					for _, scope := range payload.Scopes {
						scope.Name = scopestore.FromName(scope.Name).Name
						c.Scopes = append(c.Scopes, scope)
					}
				}
			}
		}
		result = append(result, c)
	}
	i.catalogues.Store(result)
}
