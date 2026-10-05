-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: LockObserverDirectoryCreation :one
SELECT pg_try_advisory_xact_lock(7261930284521)::boolean;

-- name: PruneObserverDirectorySnapshots :exec
DELETE FROM observer_directory_snapshots WHERE expires_at <= now();

-- name: ObserverDirectoryHasCapacity :one
SELECT count(*) < 1024 AS available FROM observer_directory_snapshots;

-- name: ObserverDirectoryWithinBudget :one
SELECT coalesce(sum(pg_column_size(items)::bigint + pg_column_size(metadata)), 0) <= 134217728 AS available
FROM observer_directory_snapshots;

-- name: FindObserverDirectorySnapshot :one
SELECT id FROM observer_directory_snapshots WHERE query_key = $1 AND expires_at > now();

-- name: CreateObserverDirectorySnapshot :one
WITH hours AS MATERIALIZED (
 SELECT g.h, coalesce(r.status, 'missing') AS status
 FROM generate_series($1::timestamptz, $2::timestamptz - interval '1 hour', interval '1 hour') g(h)
 LEFT JOIN analytics_rollup_hours r ON r.hour = g.h
), coverage AS (
 SELECT count(*)::integer AS expected,
        count(*) FILTER (WHERE status = 'complete')::integer AS complete,
        count(*) FILTER (WHERE status = 'partial')::integer AS partial,
        count(*) FILTER (WHERE status = 'missing')::integer AS missing
 FROM hours
), candidates AS MATERIALIZED (
 SELECT o.id, o.display_name, o.observer_type,
        coalesce(o.last_iata, '')::text AS iata,
        CASE WHEN greatest(coalesce(o.last_status_at, o.last_seen), o.last_seen) > now() - interval '5 minutes'
             THEN 'online' ELSE 'offline' END AS status,
        CASE WHEN o.radio_freq_mhz IS NOT NULL AND o.radio_bw_khz IS NOT NULL AND o.radio_sf IS NOT NULL
             THEN concat(o.radio_freq_mhz, ',', o.radio_bw_khz, ',', o.radio_sf) END AS radio,
        coalesce((SELECT jsonb_agg(ts.name ORDER BY ts.name) FROM observer_scopes os JOIN transport_scopes ts ON ts.id = os.scope_id WHERE os.observer_id = o.id), '[]'::jsonb) AS scopes
 FROM observers o
 WHERE NOT $12::boolean AND (coalesce(cardinality($3::bpchar[]), 0) = 0 OR o.last_iata = ANY($3::bpchar[]))
   AND ($4::text = '' OR EXISTS (SELECT 1 FROM observer_brokers ob WHERE ob.observer_id = o.id AND ob.broker_name = $4))
   AND ($5::text = '' OR o.display_name ILIKE '%' || $5 || '%')
   AND ($6::text = '' OR CASE WHEN greatest(coalesce(o.last_status_at, o.last_seen), o.last_seen) > now() - interval '5 minutes' THEN 'online' ELSE 'offline' END = $6)
   AND ($7::text = '' OR EXISTS (SELECT 1 FROM observer_scopes os JOIN transport_scopes ts ON ts.id = os.scope_id WHERE os.observer_id = o.id AND ts.name = $7))
), traffic AS (
 SELECT a.observer_id, sum(a.observation_count)::bigint AS observations
 FROM analytics_hourly_observer_identity a
 JOIN hours h ON h.h = a.hour AND h.status = 'complete'
 JOIN candidates c ON c.id = a.observer_id
 WHERE (coalesce(cardinality($3::bpchar[]), 0) = 0 OR a.iata = ANY($3::bpchar[]))
 GROUP BY a.observer_id
), listed AS (
 SELECT c.*, CASE WHEN v.complete = v.expected THEN coalesce(t.observations, 0) END AS observations
 FROM candidates c CROSS JOIN coverage v LEFT JOIN traffic t ON t.observer_id = c.id
 WHERE $8::text = '' OR c.observer_type = $8
), packed AS (
 SELECT coalesce(jsonb_agg(
   jsonb_strip_nulls(jsonb_build_object('id', id, 'displayName', display_name, 'observerType', observer_type, 'iata', iata, 'status', status, 'radio', radio, 'scopes', scopes))
   || jsonb_build_object('observationCount', observations)
   ORDER BY CASE WHEN $9::text = 'traffic' THEN observations END DESC NULLS LAST,
            lower(coalesce(display_name, '')) COLLATE "C", id
 ), '[]'::jsonb) AS items, max(observations) AS maximum
 FROM listed
)
INSERT INTO observer_directory_snapshots (id, query_key, metadata, items)
SELECT $10, $11,
 jsonb_build_object(
   'windowStart', (extract(epoch FROM $1::timestamptz)*1000)::bigint,
   'windowEnd', (extract(epoch FROM $2::timestamptz)*1000)::bigint,
   'sort', $9::text, 'effectiveSort', CASE WHEN v.complete = v.expected THEN $9::text ELSE 'name' END,
   'maxObservationCount', CASE WHEN v.complete = v.expected THEN coalesce(p.maximum, 0) END,
   'observerTypes', (SELECT coalesce(jsonb_agg(t.observer_type ORDER BY t.observer_type), '[]'::jsonb) FROM (SELECT DISTINCT observer_type FROM candidates WHERE observer_type IS NOT NULL AND observer_type <> '') t),
   'coverage', jsonb_build_object('status', CASE WHEN v.complete = v.expected THEN 'complete' WHEN v.complete > 0 OR v.partial > 0 THEN 'partial' ELSE 'unavailable' END,
      'expectedHours', v.expected, 'completeHours', v.complete, 'partialHours', v.partial, 'missingHours', v.missing)
 ), p.items
FROM packed p CROSS JOIN coverage v
RETURNING id;

-- name: GetObserverDirectoryPage :one
SELECT (metadata || jsonb_build_object(
 'snapshot', id,
 'generatedAt', (extract(epoch FROM created_at)*1000)::bigint,
 'expiresAt', (extract(epoch FROM expires_at)*1000)::bigint,
 'hasMore', jsonb_array_length(items)::bigint > $1::bigint + $2::integer,
 'nextCursor', CASE WHEN jsonb_array_length(items)::bigint > $1::bigint + $2::integer THEN $1::bigint + $2::integer END,
 'items', (SELECT coalesce(jsonb_agg(e.item ORDER BY e.ordinality), '[]'::jsonb)
           FROM jsonb_array_elements(s.items) WITH ORDINALITY e(item, ordinality)
           WHERE e.ordinality > $1::bigint AND e.ordinality <= $1::bigint + $2::integer)
))::text AS page
FROM observer_directory_snapshots s WHERE id = $3 AND expires_at > now();
