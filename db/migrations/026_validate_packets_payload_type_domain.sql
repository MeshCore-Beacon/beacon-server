-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Scans packets under SHARE UPDATE EXCLUSIVE; writes continue.
ALTER TABLE packets VALIDATE CONSTRAINT packets_payload_type_domain;
