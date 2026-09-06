-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Preserve the endpoint resolution already computed for the live observation.
-- SQL NULL means no snapshot was captured (legacy rows); {} records no endpoints.
-- No default/backfill: current node metadata cannot reconstruct past names.
ALTER TABLE packet_observations
  ADD COLUMN IF NOT EXISTS resolved_endpoints JSONB;
