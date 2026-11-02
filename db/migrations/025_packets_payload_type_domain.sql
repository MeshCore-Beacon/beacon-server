-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Decoded headers carry 4 payload-type bits; an absent type filter is passed as the full domain.
ALTER TABLE packets ADD CONSTRAINT packets_payload_type_domain CHECK (payload_type BETWEEN 0 AND 15) NOT VALID;
