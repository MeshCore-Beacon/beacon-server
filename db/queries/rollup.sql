-- ============================================================
-- HOURLY ANALYTICS ROLLUPS
-- An hour [H, H+1h) is eligible once now() >= H + 95 min (ingest clamps heard_at to ±30 min).
-- ============================================================

-- name: TryRollupLock :one
SELECT pg_try_advisory_lock(25189, 1);

-- name: RollupUnlock :exec
SELECT pg_advisory_unlock(25189, 1);

-- name: RegisterRollupHours :exec
-- Adds a 'missing' row for every eligible hour since the earliest retained observation
-- (and not before @since), zero-observation hours included.
INSERT INTO analytics_rollup_hours (hour, status)
SELECT h, 'missing'
-- No observations yet means nothing to register (GREATEST would ignore the NULL MIN).
FROM (SELECT CASE WHEN MIN(heard_at) IS NOT NULL
                   THEN date_trunc('hour', GREATEST(MIN(heard_at), @since::timestamptz), 'UTC') END AS first_hour
      FROM packet_observations) f,
     generate_series(f.first_hour, date_trunc('hour', now() - INTERVAL '95 minutes', 'UTC'), INTERVAL '1 hour') h
WHERE f.first_hour IS NOT NULL
ON CONFLICT (hour) DO NOTHING;

-- name: MarkPartialRollupHours :exec
-- Hours whose raw rows cleanup already started deleting can never be rolled completely.
-- Readers report the new status, so the revision moves too.
WITH marked AS (
  UPDATE analytics_rollup_hours h SET status = 'partial'
  FROM analytics_raw_state r
  WHERE h.status = 'missing'
    AND r.raw_deleted_before IS NOT NULL
    AND h.hour <= r.raw_deleted_before + INTERVAL '30 minutes'
  RETURNING 1
)
UPDATE analytics_state SET revision = revision + 1 WHERE EXISTS (SELECT 1 FROM marked);

-- name: ListMissingRollupHours :many
SELECT hour FROM analytics_rollup_hours
WHERE status = 'missing'
ORDER BY hour
LIMIT $1;

-- name: ListDirtyRollupHours :many
-- Only complete hours whose raw rows are still intact can be re-rolled. Queued missing hours
-- wait: their first roll consumes the entry only if the entry predates its snapshot.
SELECT d.hour FROM analytics_dirty_hours d
JOIN analytics_rollup_hours h ON h.hour = d.hour AND h.status = 'complete'
CROSS JOIN analytics_raw_state r
WHERE r.raw_deleted_before IS NULL OR d.hour > r.raw_deleted_before + INTERVAL '30 minutes'
ORDER BY d.hour
LIMIT $1;

-- name: DeleteStaleDirtyHours :exec
-- Dirty hours that can no longer be re-rolled.
DELETE FROM analytics_dirty_hours d
WHERE EXISTS (SELECT 1 FROM analytics_rollup_hours h WHERE h.hour = d.hour AND h.status = 'partial')
   OR d.hour <= (SELECT raw_deleted_before + INTERVAL '30 minutes' FROM analytics_raw_state);

-- name: LockRollupHour :one
-- Returns the hour's status, or 'partial' if raw deletion has reached it since registration.
SELECT CASE
         WHEN r.raw_deleted_before IS NOT NULL AND h.hour <= r.raw_deleted_before + INTERVAL '30 minutes'
           THEN 'partial'
         ELSE h.status
       END::text AS status
FROM analytics_rollup_hours h CROSS JOIN analytics_raw_state r
WHERE h.hour = $1
FOR UPDATE OF h;

-- name: SetRollupHourPartial :exec
WITH marked AS (
  UPDATE analytics_rollup_hours SET status = 'partial' WHERE hour = $1 AND status = 'missing' RETURNING 1
)
UPDATE analytics_state SET revision = revision + 1 WHERE EXISTS (SELECT 1 FROM marked);

-- name: FillRollupObs :exec
INSERT INTO rollup_obs (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count,
                        path_len, rssi, snr, airtime_ms, payload_type, route_type, origin_pubkey, scope_id)
SELECT po.packet_hash, po.observer_id, po.iata, po.heard_at, po.path_length_byte, po.hash_size, po.hop_count,
       COALESCE(octet_length(po.path_bytes), 0), po.rssi, po.snr, po.airtime_ms, po.payload_type,
       p.route_type, p.origin_pubkey, p.scope_id
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at >= $1::timestamptz AND po.heard_at < $1::timestamptz + INTERVAL '1 hour';

-- name: DeleteRollupHour :exec
WITH a AS (DELETE FROM analytics_hourly_iata_observations ta WHERE ta.hour = @hour::timestamptz),
     b AS (DELETE FROM analytics_hourly_payload_breakdown tb WHERE tb.hour = @hour::timestamptz),
     c AS (DELETE FROM analytics_hourly_signal tc WHERE tc.hour = @hour::timestamptz),
     d AS (DELETE FROM analytics_hourly_paths td WHERE td.hour = @hour::timestamptz),
     e AS (DELETE FROM analytics_hourly_observer_activity te WHERE te.hour = @hour::timestamptz),
     f AS (DELETE FROM analytics_hourly_observer_identity tf WHERE tf.hour = @hour::timestamptz),
     g AS (DELETE FROM analytics_hourly_advert_hearings tg WHERE tg.hour = @hour::timestamptz),
     h AS (DELETE FROM analytics_hourly_advert_sets th WHERE th.hour = @hour::timestamptz),
     i AS (DELETE FROM analytics_hourly_talker_sets ti WHERE ti.hour = @hour::timestamptz),
     j AS (DELETE FROM analytics_hourly_packet_sets tj WHERE tj.hour = @hour::timestamptz),
     k AS (DELETE FROM analytics_hourly_scope_sets tk WHERE tk.hour = @hour::timestamptz),
     l AS (DELETE FROM analytics_hourly_scope_observers tl WHERE tl.hour = @hour::timestamptz)
DELETE FROM analytics_hourly_scope_nodes tm WHERE tm.hour = @hour::timestamptz;

-- name: RollIATAObservations :exec
INSERT INTO analytics_hourly_iata_observations (hour, iata, observation_count)
SELECT @hour::timestamptz, iata, count(*) FROM rollup_obs GROUP BY iata;

-- name: RollPayloadBreakdown :exec
INSERT INTO analytics_hourly_payload_breakdown (hour, iata, payload_type, count)
SELECT @hour::timestamptz, iata, payload_type, count(*)
FROM rollup_obs WHERE payload_type IS NOT NULL
GROUP BY iata, payload_type;

-- name: RollSignal :exec
-- kind = grouping(snr_bin, rssi_bin): 3 = per IATA totals, 1 = per SNR bin, 2 = per RSSI bin.
-- rssi=0 AND snr=0 means "not reported".
INSERT INTO analytics_hourly_signal (hour, iata, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum)
SELECT @hour::timestamptz, b.iata, grouping(b.snr_bin, b.rssi_bin)::integer,
       COALESCE(b.snr_bin, -1)::integer, COALESCE(b.rssi_bin, -1)::integer,
       count(*), count(b.snr), COALESCE(sum(b.snr), 0)::double precision,
       count(b.rssi), COALESCE(sum(b.rssi), 0)::double precision
FROM (
    SELECT s.iata, s.snr, s.rssi,
           width_bucket(s.snr, -30, 30, 12) AS snr_bin,
           width_bucket(s.rssi, -140, 0, 14) AS rssi_bin
    FROM (
        SELECT ro.iata,
               CASE WHEN NOT (COALESCE(ro.rssi, 0) = 0 AND COALESCE(ro.snr, 0) = 0)
                         AND ro.snr > '-Infinity'::real AND ro.snr < 'Infinity'::real
                    THEN ro.snr::double precision END AS snr,
               CASE WHEN NOT (COALESCE(ro.rssi, 0) = 0 AND COALESCE(ro.snr, 0) = 0)
                    THEN ro.rssi::double precision END AS rssi
        FROM rollup_obs ro
    ) s
) b
GROUP BY GROUPING SETS ((b.iata), (b.iata, b.snr_bin), (b.iata, b.rssi_bin));

-- name: RollPaths :exec
-- category: 0 = routed, 1 = zero-hop, 2 = TRACE, 3 = invalid (meshcore-go IsValidPathLen).
INSERT INTO analytics_hourly_paths (hour, iata, category, hash_bytes, entries, receptions)
SELECT @hour::timestamptz, c.iata, c.category, c.hash_bytes, c.entries, count(*)
FROM (
  SELECT k.iata, k.category,
         CASE WHEN k.category = 0 THEN k.hash_size ELSE 0 END::integer AS hash_bytes,
         CASE WHEN k.category = 0 THEN k.hop_count ELSE 0 END::integer AS entries
  FROM (
    SELECT iata, hash_size, hop_count,
           CASE WHEN payload_type = 9 THEN 2
                WHEN payload_type IS NULL OR payload_type NOT BETWEEN 0 AND 15
                  OR NOT (path_length_byte BETWEEN 0 AND 191
                    AND hash_size BETWEEN 1 AND 3 AND hop_count BETWEEN 0 AND 63
                    AND hash_size = (path_length_byte >> 6) + 1
                    AND hop_count = (path_length_byte & 63)
                    AND hash_size::integer * hop_count::integer <= 64
                    AND path_len = hash_size::integer * hop_count::integer)
                THEN 3
                WHEN hop_count = 0 THEN 1
                ELSE 0 END::integer AS category
    FROM rollup_obs
  ) k
) c
GROUP BY c.iata, c.category, c.hash_bytes, c.entries;

-- name: RollObserverActivity :exec
INSERT INTO analytics_hourly_observer_activity (hour, observer_id, payload_type, observations, airtime_ms, airtime_n,
                                                snr_sum, snr_n, snr_min, rssi_sum, rssi_n)
SELECT @hour::timestamptz, observer_id, COALESCE(payload_type, -1)::smallint,
       count(*), sum(airtime_ms)::real, count(airtime_ms),
       sum(snr)    FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real,
       count(snr)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)),
       min(snr)    FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real,
       sum(rssi)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint,
       count(rssi) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))
FROM rollup_obs
GROUP BY observer_id, COALESCE(payload_type, -1);

-- name: RollObserverIdentity :exec
INSERT INTO analytics_hourly_observer_identity (hour, iata, observer_id, observation_count, display_name, observer_type)
SELECT @hour::timestamptz, r.iata, r.observer_id, r.n, o.display_name, o.observer_type
FROM (SELECT iata, observer_id, count(*) AS n FROM rollup_obs GROUP BY iata, observer_id) r
LEFT JOIN observers o ON o.id = r.observer_id;

-- name: RollAdvertHearings :exec
INSERT INTO analytics_hourly_advert_hearings (hour, iata, origin_pubkey, observations, last_heard, name, node_type)
SELECT @hour::timestamptz, r.iata, r.origin_pubkey, r.n, r.last_heard, n.name, n.node_type
FROM (SELECT iata, origin_pubkey, count(*) AS n, max(heard_at) AS last_heard
      FROM rollup_obs
      WHERE payload_type = 4 AND origin_pubkey IS NOT NULL
      GROUP BY iata, origin_pubkey) r
LEFT JOIN nodes n ON n.public_key = r.origin_pubkey;

-- name: RollAdvertSets :exec
-- Each distinct ADVERT packet counts once, under the exact set of IATAs that heard it this hour.
INSERT INTO analytics_hourly_advert_sets (hour, origin_pubkey, iatas, advert_packets, flood_packets, direct_packets)
SELECT @hour::timestamptz, origin_pubkey, iatas, count(*),
       count(*) FILTER (WHERE route_type IN (0, 1)),
       count(*) FILTER (WHERE route_type IN (2, 3))
FROM (SELECT origin_pubkey, route_type, array_agg(DISTINCT iata ORDER BY iata) AS iatas
      FROM rollup_obs
      WHERE payload_type = 4 AND origin_pubkey IS NOT NULL
      GROUP BY packet_hash, origin_pubkey, route_type) p
GROUP BY origin_pubkey, iatas;

-- name: RollTalkerSets :exec
INSERT INTO analytics_hourly_talker_sets (hour, sender_name, iatas, messages, last_sent)
SELECT @hour::timestamptz, sender_name, iatas, count(*), max(sent_at)
FROM (SELECT cm.sender_name, max(cm.sent_at) AS sent_at, array_agg(DISTINCT ro.iata ORDER BY ro.iata) AS iatas
      FROM rollup_obs ro
      JOIN channel_messages cm ON cm.packet_hash = ro.packet_hash
      WHERE cm.sender_name IS NOT NULL
      GROUP BY cm.id, cm.sender_name) m
GROUP BY sender_name, iatas;

-- name: RollPacketSets :exec
INSERT INTO analytics_hourly_packet_sets (hour, iatas, packets)
SELECT @hour::timestamptz, iatas, count(*)
FROM (SELECT array_agg(DISTINCT iata ORDER BY iata) AS iatas FROM rollup_obs GROUP BY packet_hash) p
GROUP BY iatas;

-- name: RollScopeSets :exec
INSERT INTO analytics_hourly_scope_sets (hour, iatas, scope_id, packets)
SELECT @hour::timestamptz, iatas, scope_id, count(*)
FROM (SELECT scope_id, array_agg(DISTINCT iata ORDER BY iata) AS iatas
      FROM rollup_obs WHERE scope_id IS NOT NULL
      GROUP BY packet_hash, scope_id) p
GROUP BY iatas, scope_id;

-- name: RollScopeObservers :exec
INSERT INTO analytics_hourly_scope_observers (hour, iata, scope_id, observer_id)
SELECT DISTINCT @hour::timestamptz, iata, scope_id, observer_id
FROM rollup_obs WHERE scope_id IS NOT NULL;

-- name: RollScopeNodes :exec
INSERT INTO analytics_hourly_scope_nodes (hour, iata, scope_id, origin_pubkey)
SELECT DISTINCT @hour::timestamptz, iata, scope_id, origin_pubkey
FROM rollup_obs WHERE payload_type = 4 AND origin_pubkey IS NOT NULL AND scope_id IS NOT NULL;

-- name: RollupContentHash :one
-- Order-independent fingerprint of every family row for the hour.
SELECT md5(concat_ws('|',
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_iata_observations t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_payload_breakdown t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_signal t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_paths t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_observer_activity t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_observer_identity t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_advert_hearings t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_advert_sets t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_talker_sets t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_packet_sets t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_scope_sets t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_scope_observers t WHERE t.hour = @hour::timestamptz),
  (SELECT string_agg(t::text, ',' ORDER BY t::text) FROM analytics_hourly_scope_nodes t WHERE t.hour = @hour::timestamptz)
))::text AS content_hash;

-- name: FinishRollupHour :one
-- Marks the hour complete and bumps the revision only when its content changed.
WITH prev AS (
  SELECT ph.content_hash FROM analytics_rollup_hours ph WHERE ph.hour = @hour::timestamptz
), upd AS (
  UPDATE analytics_rollup_hours uh
  SET status = 'complete', rolled_at = now(), content_hash = @content_hash::text
  WHERE uh.hour = @hour::timestamptz
), undirty AS (
  DELETE FROM analytics_dirty_hours dh WHERE dh.hour = @hour::timestamptz
), rev AS (
  UPDATE analytics_state SET revision = revision + 1
  WHERE (SELECT content_hash FROM prev) IS DISTINCT FROM @content_hash::text
  RETURNING revision
)
SELECT EXISTS (SELECT 1 FROM rev)::boolean AS changed;

-- name: GetAnalyticsRevision :one
-- Both counters only grow, so their sum changes whenever rolled content or coverage does.
SELECT (s.revision + r.coverage)::bigint FROM analytics_state s CROSS JOIN analytics_raw_state r;

-- name: OldestMissingRollupHour :one
SELECT min(hour)::timestamptz FROM analytics_rollup_hours WHERE status = 'missing';

-- name: DeleteOldRollups :exec
-- Bumps coverage when hours go, so cached responses that included them are not reused.
WITH a AS (DELETE FROM analytics_hourly_iata_observations ta WHERE ta.hour < @cutoff::timestamptz),
     b AS (DELETE FROM analytics_hourly_payload_breakdown tb WHERE tb.hour < @cutoff::timestamptz),
     c AS (DELETE FROM analytics_hourly_signal tc WHERE tc.hour < @cutoff::timestamptz),
     d AS (DELETE FROM analytics_hourly_paths td WHERE td.hour < @cutoff::timestamptz),
     e AS (DELETE FROM analytics_hourly_observer_activity te WHERE te.hour < @cutoff::timestamptz),
     f AS (DELETE FROM analytics_hourly_observer_identity tf WHERE tf.hour < @cutoff::timestamptz),
     g AS (DELETE FROM analytics_hourly_advert_hearings tg WHERE tg.hour < @cutoff::timestamptz),
     h AS (DELETE FROM analytics_hourly_advert_sets th WHERE th.hour < @cutoff::timestamptz),
     i AS (DELETE FROM analytics_hourly_talker_sets ti WHERE ti.hour < @cutoff::timestamptz),
     j AS (DELETE FROM analytics_hourly_packet_sets tj WHERE tj.hour < @cutoff::timestamptz),
     k AS (DELETE FROM analytics_hourly_scope_sets tk WHERE tk.hour < @cutoff::timestamptz),
     l AS (DELETE FROM analytics_dirty_hours tl WHERE tl.hour < @cutoff::timestamptz),
     n AS (DELETE FROM analytics_hourly_scope_observers tn WHERE tn.hour < @cutoff::timestamptz),
     o AS (DELETE FROM analytics_hourly_scope_nodes tob WHERE tob.hour < @cutoff::timestamptz),
     m AS (DELETE FROM analytics_rollup_hours tm WHERE tm.hour < @cutoff::timestamptz RETURNING 1)
UPDATE analytics_raw_state SET coverage = coverage + 1 WHERE EXISTS (SELECT 1 FROM m);
