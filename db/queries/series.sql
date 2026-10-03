-- name: GetStatsSeries :many
-- One row per hour in [since, until) plus a summary row (hour NULL) over its complete hours.
-- Counts sum; observers, IATAs and scopes are distinct across the window; packets count
-- once per hour heard. Each branch pair keeps "all IATAs" and "these IATAs" plans separate.
WITH hours AS (
    SELECT g.h::timestamptz AS hour, COALESCE(r.status, 'missing')::text AS status
    FROM generate_series(@since::timestamptz, @until::timestamptz, INTERVAL '1 hour') g(h)
    LEFT JOIN analytics_rollup_hours r ON r.hour = g.h
    WHERE g.h < @until::timestamptz
), complete AS (
    SELECT hour FROM hours WHERE status = 'complete'
), obs AS (
    SELECT o.hour, o.iata, o.observation_count FROM analytics_hourly_iata_observations o JOIN complete c ON c.hour = o.hour
    WHERE COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT o.hour, o.iata, o.observation_count FROM analytics_hourly_iata_observations o JOIN complete c ON c.hour = o.hour
    WHERE cardinality(@iatas::bpchar[]) > 0 AND o.iata = ANY(@iatas::bpchar[])
), obs_agg AS (
    SELECT hour, sum(observation_count)::bigint AS observations, count(DISTINCT iata)::bigint AS active_iatas
    FROM obs GROUP BY GROUPING SETS ((), (hour))
), pkt AS (
    SELECT s.hour, s.packets FROM analytics_hourly_packet_sets s JOIN complete c ON c.hour = s.hour
    WHERE COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT s.hour, s.packets FROM analytics_hourly_packet_sets s JOIN complete c ON c.hour = s.hour
    WHERE cardinality(@iatas::bpchar[]) > 0 AND s.iatas && @iatas::bpchar[]
), pkt_agg AS (
    SELECT hour, sum(packets)::bigint AS unique_packets FROM pkt GROUP BY GROUPING SETS ((), (hour))
), obsr AS (
    SELECT i.hour, i.observer_id FROM analytics_hourly_observer_identity i JOIN complete c ON c.hour = i.hour
    WHERE COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT i.hour, i.observer_id FROM analytics_hourly_observer_identity i JOIN complete c ON c.hour = i.hour
    WHERE cardinality(@iatas::bpchar[]) > 0 AND i.iata = ANY(@iatas::bpchar[])
), obsr_agg AS (
    SELECT hour, count(DISTINCT observer_id)::bigint AS active_observers FROM obsr GROUP BY GROUPING SETS ((), (hour))
), scp AS (
    SELECT s.hour, s.scope_id, s.packets FROM analytics_hourly_scope_sets s JOIN complete c ON c.hour = s.hour
    WHERE COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT s.hour, s.scope_id, s.packets FROM analytics_hourly_scope_sets s JOIN complete c ON c.hour = s.hour
    WHERE cardinality(@iatas::bpchar[]) > 0 AND s.iatas && @iatas::bpchar[]
), scp_agg AS (
    SELECT hour, sum(packets)::bigint AS scoped_packets, count(DISTINCT scope_id)::bigint AS active_scopes
    FROM scp GROUP BY GROUPING SETS ((), (hour))
), pth AS (
    SELECT p.hour, p.entries FROM analytics_hourly_paths p JOIN complete c ON c.hour = p.hour
    WHERE p.category = 0 AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT p.hour, p.entries FROM analytics_hourly_paths p JOIN complete c ON c.hour = p.hour
    WHERE p.category = 0 AND cardinality(@iatas::bpchar[]) > 0 AND p.iata = ANY(@iatas::bpchar[])
), pth_agg AS (
    SELECT hour, max(entries)::integer AS max_path_entries FROM pth GROUP BY GROUPING SETS ((), (hour))
), sig AS (
    SELECT s.hour, s.snr_sum, s.snr_samples, s.rssi_sum, s.rssi_samples FROM analytics_hourly_signal s JOIN complete c ON c.hour = s.hour
    WHERE s.kind = 3 AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT s.hour, s.snr_sum, s.snr_samples, s.rssi_sum, s.rssi_samples FROM analytics_hourly_signal s JOIN complete c ON c.hour = s.hour
    WHERE s.kind = 3 AND cardinality(@iatas::bpchar[]) > 0 AND s.iata = ANY(@iatas::bpchar[])
), sig_agg AS (
    SELECT hour, sum(snr_sum)::double precision AS snr_sum, sum(snr_samples)::bigint AS snr_samples,
           sum(rssi_sum)::double precision AS rssi_sum, sum(rssi_samples)::bigint AS rssi_samples
    FROM sig GROUP BY GROUPING SETS ((), (hour))
), keys AS (
    SELECT hour, status FROM hours
    UNION ALL
    SELECT NULL::timestamptz, 'summary'::text
)
SELECT k.hour, k.status,
       COALESCE(o.observations, 0)::bigint AS observations,
       COALESCE(p.unique_packets, 0)::bigint AS unique_packets,
       COALESCE(ob.active_observers, 0)::bigint AS active_observers,
       COALESCE(o.active_iatas, 0)::bigint AS active_iatas,
       COALESCE(s.scoped_packets, 0)::bigint AS scoped_packets,
       COALESCE(s.active_scopes, 0)::bigint AS active_scopes,
       COALESCE(pa.max_path_entries, 0)::integer AS max_path_entries,
       COALESCE(sg.snr_sum, 0)::double precision AS snr_sum,
       COALESCE(sg.snr_samples, 0)::bigint AS snr_samples,
       COALESCE(sg.rssi_sum, 0)::double precision AS rssi_sum,
       COALESCE(sg.rssi_samples, 0)::bigint AS rssi_samples
FROM keys k
LEFT JOIN obs_agg o ON o.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN pkt_agg p ON p.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN obsr_agg ob ON ob.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN scp_agg s ON s.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN pth_agg pa ON pa.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN sig_agg sg ON sg.hour IS NOT DISTINCT FROM k.hour
ORDER BY k.hour NULLS LAST;

-- name: GetEarliestCompleteRollupHour :one
SELECT min(hour)::timestamptz FROM analytics_rollup_hours WHERE status = 'complete';

-- name: GetLatestCompleteRollupHour :one
SELECT max(hour)::timestamptz FROM analytics_rollup_hours WHERE status = 'complete';
