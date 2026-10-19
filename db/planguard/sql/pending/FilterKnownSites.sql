-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: FilterKnownSites :one
-- Keeps only the requested IATAs that exist in packet_sites itself (never a static list). Lossless: an IATA
-- with no packet_sites row contributes no row to any packet_sites statement. Go runs it before the typed,
-- scoped and since statements and returns an empty page when nothing survives.
SELECT COALESCE(array_agg(u ORDER BY u), '{}')::bpchar[] AS iatas
FROM unnest($1::bpchar[]) u
WHERE EXISTS (SELECT 1 FROM packet_sites ps WHERE ps.iata = u)
