-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Names sorted the way the web's tables sort them (Intl.Collator with numeric on): ICU root order
-- with digit runs compared as numbers, so "Node 9" comes before "Node 10".
CREATE COLLATION IF NOT EXISTS natural_text (provider = icu, locale = 'und-u-kn');
