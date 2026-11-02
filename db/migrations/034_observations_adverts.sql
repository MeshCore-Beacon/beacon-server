-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Observer advert pages read only advert observations, in id order.
CREATE INDEX CONCURRENTLY idx_observations_adverts ON packet_observations (observer_id, id) WHERE payload_type = 4;
