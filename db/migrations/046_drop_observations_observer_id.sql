-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Advert pages read idx_observations_adverts; lookups by observer use idx_observations_observer.
DROP INDEX CONCURRENTLY IF EXISTS idx_observations_observer_id;
