-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListObserverDirectory :one
WITH hours AS MATERIALIZED (
 SELECT g.h, coalesce(r.status, 'missing') AS status,
        coalesce(r.status = 'complete', false) AND g.h >= $1::timestamptz
          AND g.h + interval '1 hour' <= $2::timestamptz AS use_rollup
 FROM generate_series(date_trunc('hour', $1::timestamptz, 'UTC'), $2::timestamptz, interval '1 hour') g(h)
 LEFT JOIN analytics_rollup_hours r ON r.hour = g.h
 WHERE g.h < $2::timestamptz
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
), raw_traffic AS MATERIALIZED (
 -- Disjoint hour slices keep raw scans bounded and never recount a completed whole hour.
 SELECT h.h, r.observer_id, r.observations
 FROM hours h
 CROSS JOIN LATERAL (
   SELECT o.observer_id, count(*)::bigint AS observations
   FROM packet_observations o
   WHERE o.heard_at >= greatest(h.h, $1::timestamptz)
     AND o.heard_at < least(h.h + interval '1 hour', $2::timestamptz)
     AND (coalesce(cardinality($3::bpchar[]), 0) = 0 OR o.iata = ANY($3::bpchar[]))
   GROUP BY o.observer_id
 ) r
 JOIN candidates c ON c.id = r.observer_id
 WHERE NOT h.use_rollup
), coverage AS (
 SELECT count(*)::integer AS expected,
        count(*) FILTER (WHERE h.use_rollup)::integer AS complete,
        count(*) FILTER (WHERE NOT h.use_rollup AND (h.status IN ('complete', 'partial') OR r.h IS NOT NULL))::integer AS partial,
        count(*) FILTER (WHERE NOT h.use_rollup AND h.status = 'missing' AND r.h IS NULL)::integer AS missing
 FROM hours h LEFT JOIN (SELECT DISTINCT h FROM raw_traffic) r ON r.h = h.h
), traffic AS (
 SELECT observer_id, sum(observations)::bigint AS observations
 FROM (
   SELECT a.observer_id, a.observation_count AS observations
   FROM analytics_hourly_observer_identity a
   JOIN hours h ON h.h = a.hour AND h.use_rollup
   JOIN candidates c ON c.id = a.observer_id
   WHERE (coalesce(cardinality($3::bpchar[]), 0) = 0 OR a.iata = ANY($3::bpchar[]))
   UNION ALL
   SELECT observer_id, observations FROM raw_traffic
 ) counts
 GROUP BY observer_id
), listed AS (
 SELECT c.*, coalesce(t.observations, 0)::bigint AS observations
 FROM candidates c LEFT JOIN traffic t ON t.observer_id = c.id
 WHERE $8::text = '' OR c.observer_type = $8
), paged AS (
 SELECT * FROM listed
 ORDER BY CASE WHEN $9::text = 'traffic' THEN observations END DESC NULLS LAST,
          lower(coalesce(display_name, '')) COLLATE "C", id
 LIMIT $11::integer OFFSET $10::bigint
), packed AS (
 SELECT coalesce(jsonb_agg(
   jsonb_strip_nulls(jsonb_build_object('id', id, 'displayName', display_name, 'observerType', observer_type, 'iata', iata, 'status', status, 'radio', radio, 'scopes', scopes))
   || jsonb_build_object('observationCount', observations)
   ORDER BY CASE WHEN $9::text = 'traffic' THEN observations END DESC NULLS LAST,
            lower(coalesce(display_name, '')) COLLATE "C", id
 ), '[]'::jsonb) AS items
 FROM paged
), totals AS (
 SELECT count(*) AS n, max(observations) AS maximum FROM listed
)
SELECT jsonb_build_object(
   'items', p.items,
   'generatedAt', (extract(epoch FROM now())*1000)::bigint,
   'hasMore', t.n > $10::bigint + $11::integer,
   'nextCursor', CASE WHEN t.n > $10::bigint + $11::integer THEN $10::bigint + $11::integer END,
   'windowStart', (extract(epoch FROM $1::timestamptz)*1000)::bigint,
   'windowEnd', (extract(epoch FROM $2::timestamptz)*1000)::bigint,
   'sort', $9::text, 'effectiveSort', $9::text,
   'maxObservationCount', coalesce(t.maximum, 0),
   'observerTypes', (SELECT coalesce(jsonb_agg(t.observer_type ORDER BY t.observer_type), '[]'::jsonb) FROM (SELECT DISTINCT observer_type FROM candidates WHERE observer_type IS NOT NULL AND observer_type <> '') t),
   'coverage', jsonb_build_object('status', CASE WHEN v.complete = v.expected THEN 'complete' WHEN v.complete > 0 OR v.partial > 0 THEN 'partial' ELSE 'unavailable' END,
      'expectedHours', v.expected, 'completeHours', v.complete, 'partialHours', v.partial, 'missingHours', v.missing)
)::text AS page
FROM packed p CROSS JOIN coverage v CROSS JOIN totals t;
