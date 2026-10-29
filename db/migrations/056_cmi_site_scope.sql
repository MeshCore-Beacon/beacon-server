-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Message pages across every channel filtered by sites and a scope walk one range per (site, scope).
CREATE INDEX CONCURRENTLY idx_cmi_site_scope ON channel_message_iatas USING btree (iata, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
