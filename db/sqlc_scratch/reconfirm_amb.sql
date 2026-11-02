-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- sqlc type-check stub only; db/route_maintenance.go creates this as a TEMP table per reconfirm sweep.
CREATE TABLE reconfirm_amb (iata text NOT NULL, len integer NOT NULL, prefix bytea);
