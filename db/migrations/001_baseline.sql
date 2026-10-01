-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- 2.0.0 schema baseline: pg_dump of a fresh database migrated through the 1.x
-- chain (001_initial_schema..045). Add new numbered migrations after this file.

CREATE FUNCTION archive_delete_packets(cutoff timestamp with time zone, batch_size integer) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE hashes bytea[]; deleted bigint;
BEGIN
    -- Serialize cleanup runners before overlapping aggregate upserts.
    PERFORM pg_advisory_xact_lock(hashtext('beacon.archive_delete_packets'));
    IF batch_size < 1 THEN RAISE EXCEPTION 'batch_size must be positive'; END IF;
    SELECT array_agg(p.packet_hash) INTO hashes FROM (
        SELECT ep.packet_hash FROM packets ep
        WHERE ep.last_heard_at < cutoff
        ORDER BY ep.last_heard_at, ep.packet_hash
        LIMIT batch_size FOR UPDATE OF ep SKIP LOCKED
    ) p;

    WITH expired_observations AS MATERIALIZED (
        SELECT po.* FROM packet_observations po WHERE po.packet_hash = ANY(hashes)
    ),
archived_hourly_iata_stats AS (
    INSERT INTO analytics_hourly_iata_stats (iata, hour, observation_count, unique_packets)
    SELECT iata, hour, observation_count, unique_packets FROM (
SELECT
  iata,
  date_trunc('hour', heard_at, 'UTC')::timestamptz AS hour,
  COUNT(*) AS observation_count,
  COUNT(DISTINCT packet_hash) AS unique_packets
FROM expired_observations
WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
GROUP BY iata, date_trunc('hour', heard_at, 'UTC')
    ) batch
    ON CONFLICT (iata,hour) DO UPDATE SET
        observation_count = analytics_hourly_iata_stats.observation_count + EXCLUDED.observation_count,
        unique_packets = analytics_hourly_iata_stats.unique_packets + EXCLUDED.unique_packets
),
archived_payload_breakdown_by_iata AS (
    INSERT INTO analytics_payload_breakdown_by_iata (iata, payload_type, bucket, count)
    SELECT iata, payload_type, bucket, count FROM (
SELECT
  iata,
  payload_type,
  date_trunc('hour', heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(*) AS count
FROM expired_observations
WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
  AND payload_type IS NOT NULL
GROUP BY iata, payload_type, date_trunc('hour', heard_at, 'UTC')
    ) batch
    ON CONFLICT (iata,payload_type,bucket) DO UPDATE SET
        count = analytics_payload_breakdown_by_iata.count + EXCLUDED.count
),
archived_top_observers_by_iata AS (
    INSERT INTO analytics_top_observers_by_iata (iata, observer_id, bucket, observation_count, display_name, observer_type)
    SELECT iata, observer_id, bucket, observation_count, display_name, observer_type FROM (
SELECT
  po.iata,
  po.observer_id,
  o.display_name,
  o.observer_type,
  date_trunc('hour', po.heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(*) AS observation_count
FROM expired_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
GROUP BY po.iata, po.observer_id, o.display_name, o.observer_type, date_trunc('hour', po.heard_at, 'UTC')
    ) batch
    ON CONFLICT (iata,observer_id,bucket) DO UPDATE SET
        observation_count = analytics_top_observers_by_iata.observation_count + EXCLUDED.observation_count,
        display_name = EXCLUDED.display_name,
        observer_type = EXCLUDED.observer_type
),
archived_top_talkers_by_iata AS (
    INSERT INTO analytics_top_talkers_by_iata (iata, sender_name, bucket, message_count, last_sent)
    SELECT iata, sender_name, bucket, message_count, last_sent FROM (
SELECT
  po.iata,
  cm.sender_name,
  date_trunc('hour', cm.sent_at, 'UTC')::timestamptz AS bucket,
  COUNT(DISTINCT cm.id) AS message_count,
  MAX(cm.sent_at) AS last_sent
FROM channel_messages cm
JOIN expired_observations po ON po.packet_hash = cm.packet_hash
WHERE cm.sender_name IS NOT NULL
  AND cm.sent_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
GROUP BY po.iata, cm.sender_name, date_trunc('hour', cm.sent_at, 'UTC')
    ) batch
    ON CONFLICT (iata,sender_name,bucket) DO UPDATE SET
        message_count = analytics_top_talkers_by_iata.message_count + EXCLUDED.message_count,
        last_sent = GREATEST(analytics_top_talkers_by_iata.last_sent, EXCLUDED.last_sent)
),
archived_top_advertisers_by_iata AS (
    INSERT INTO analytics_top_advertisers_by_iata (iata, node_id, bucket, advert_count, flood_advert_count, direct_advert_count, last_heard, name, node_type)
    SELECT iata, node_id, bucket, advert_count, flood_advert_count, direct_advert_count, last_heard, name, node_type FROM (
SELECT
  po.iata,
  n.id AS node_id,
  n.name,
  n.node_type,
  date_trunc('hour', po.heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(DISTINCT p.packet_hash) AS advert_count,
  COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.route_type IN (0, 1)) AS flood_advert_count,
  COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.route_type IN (2, 3)) AS direct_advert_count,
  MAX(po.heard_at) AS last_heard
FROM packets p
JOIN expired_observations po ON po.packet_hash = p.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE p.payload_type = 4 -- ADVERT
  AND po.heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
GROUP BY po.iata, n.id, n.name, n.node_type, date_trunc('hour', po.heard_at, 'UTC')
    ) batch
    ON CONFLICT (iata,node_id,bucket) DO UPDATE SET
        advert_count = analytics_top_advertisers_by_iata.advert_count + EXCLUDED.advert_count,
        flood_advert_count = analytics_top_advertisers_by_iata.flood_advert_count + EXCLUDED.flood_advert_count,
        direct_advert_count = analytics_top_advertisers_by_iata.direct_advert_count + EXCLUDED.direct_advert_count,
        last_heard = GREATEST(analytics_top_advertisers_by_iata.last_heard, EXCLUDED.last_heard),
        name = EXCLUDED.name,
        node_type = EXCLUDED.node_type
),
archived_observer_activity_hourly AS (
    INSERT INTO analytics_observer_activity_hourly (observer_id, payload_type, bucket, observations, airtime_ms, airtime_n, snr_sum, snr_n, snr_min, rssi_sum, rssi_n)
    SELECT observer_id, payload_type, bucket, observations, airtime_ms, airtime_n, snr_sum, snr_n, snr_min, rssi_sum, rssi_n FROM (
SELECT
  observer_id,
  COALESCE(payload_type, -1)::smallint AS payload_type,
  date_trunc('hour', heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(*)::bigint AS observations,
  SUM(airtime_ms)::real AS airtime_ms,
  COUNT(airtime_ms)::bigint AS airtime_n,
  SUM(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real   AS snr_sum,
  COUNT(snr) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS snr_n,
  MIN(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real   AS snr_min,
  SUM(rssi)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_sum,
  COUNT(rssi) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_n
FROM expired_observations
WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
GROUP BY observer_id, COALESCE(payload_type, -1), date_trunc('hour', heard_at, 'UTC')
    ) batch
    ON CONFLICT (observer_id,payload_type,bucket) DO UPDATE SET
        observations = analytics_observer_activity_hourly.observations + EXCLUDED.observations,
        airtime_ms = CASE WHEN analytics_observer_activity_hourly.airtime_ms IS NULL AND EXCLUDED.airtime_ms IS NULL THEN NULL ELSE COALESCE(analytics_observer_activity_hourly.airtime_ms, 0) + COALESCE(EXCLUDED.airtime_ms, 0) END,
        airtime_n = analytics_observer_activity_hourly.airtime_n + EXCLUDED.airtime_n,
        snr_sum = CASE WHEN analytics_observer_activity_hourly.snr_sum IS NULL AND EXCLUDED.snr_sum IS NULL THEN NULL ELSE COALESCE(analytics_observer_activity_hourly.snr_sum, 0) + COALESCE(EXCLUDED.snr_sum, 0) END,
        snr_n = analytics_observer_activity_hourly.snr_n + EXCLUDED.snr_n,
        snr_min = LEAST(analytics_observer_activity_hourly.snr_min, EXCLUDED.snr_min),
        rssi_sum = CASE WHEN analytics_observer_activity_hourly.rssi_sum IS NULL AND EXCLUDED.rssi_sum IS NULL THEN NULL ELSE COALESCE(analytics_observer_activity_hourly.rssi_sum, 0) + COALESCE(EXCLUDED.rssi_sum, 0) END,
        rssi_n = analytics_observer_activity_hourly.rssi_n + EXCLUDED.rssi_n
),
archived_signal_stats_hourly AS (
    INSERT INTO analytics_signal_stats_hourly (iata, hour, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum)
    SELECT iata, hour, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum FROM (
WITH samples AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                     AND snr > '-Infinity'::real AND snr < 'Infinity'::real
                THEN snr::double precision END AS snr,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                THEN rssi::double precision END AS rssi
    FROM expired_observations
    WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '720 hours'

), binned AS (
    SELECT *, width_bucket(snr, -30, 30, 12) AS snr_bin,
              width_bucket(rssi, -140, 0, 14) AS rssi_bin
    FROM samples
)
SELECT iata, hour, grouping(snr_bin, rssi_bin)::integer AS kind,
       COALESCE(snr_bin, -1)::integer AS snr_bin,
       COALESCE(rssi_bin, -1)::integer AS rssi_bin,
       count(*)::bigint AS receptions,
       count(snr)::bigint AS snr_samples,
       COALESCE(sum(snr), 0)::double precision AS snr_sum,
       count(rssi)::bigint AS rssi_samples,
       COALESCE(sum(rssi), 0)::double precision AS rssi_sum
FROM binned
GROUP BY GROUPING SETS ((iata, hour), (iata, hour, snr_bin), (iata, hour, rssi_bin))
    ) batch
    ON CONFLICT (iata,hour,kind,snr_bin,rssi_bin) DO UPDATE SET
        receptions = analytics_signal_stats_hourly.receptions + EXCLUDED.receptions,
        snr_samples = analytics_signal_stats_hourly.snr_samples + EXCLUDED.snr_samples,
        snr_sum = analytics_signal_stats_hourly.snr_sum + EXCLUDED.snr_sum,
        rssi_samples = analytics_signal_stats_hourly.rssi_samples + EXCLUDED.rssi_samples,
        rssi_sum = analytics_signal_stats_hourly.rssi_sum + EXCLUDED.rssi_sum
),
archived_path_stats_hourly AS (
    INSERT INTO analytics_path_stats_hourly (iata, hour, category, hash_bytes, entries, receptions)
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM (
WITH classified AS (
    SELECT iata, heard_at, hash_size, hop_count,
           CASE WHEN payload_type = 9 THEN 2
                -- Match meshcore-go IsValidPathLen (1/2/3-byte hashes, max 64 path bytes).
                WHEN payload_type IS NULL OR payload_type NOT BETWEEN 0 AND 15
                  OR NOT (path_length_byte BETWEEN 0 AND 191
                    AND hash_size BETWEEN 1 AND 3 AND hop_count BETWEEN 0 AND 63
                    AND hash_size = (path_length_byte >> 6) + 1
                    AND hop_count = (path_length_byte & 63)
                    AND hash_size::integer * hop_count::integer <= 64
                    AND COALESCE(octet_length(path_bytes), 0) = hash_size::integer * hop_count::integer)
                THEN 3
                WHEN hop_count = 0 THEN 1
                ELSE 0 END::integer AS category
    FROM expired_observations
    WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '720 hours'

), buckets AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour, category,
           CASE WHEN category = 0 THEN hash_size ELSE 0 END::integer AS hash_bytes,
           CASE WHEN category = 0 THEN hop_count ELSE 0 END::integer AS entries
    FROM classified
)
SELECT iata, hour, category, hash_bytes, entries, count(*)::bigint AS receptions
FROM buckets GROUP BY iata, hour, category, hash_bytes, entries
    ) batch
    ON CONFLICT (iata,hour,category,hash_bytes,entries) DO UPDATE SET
        receptions = analytics_path_stats_hourly.receptions + EXCLUDED.receptions
)
    DELETE FROM packets WHERE packet_hash = ANY(hashes);
    GET DIAGNOSTICS deleted = ROW_COUNT;

    -- Once per completed cleanup, including when there were no raw packets to delete.
    IF deleted < batch_size THEN
        DELETE FROM analytics_hourly_iata_stats WHERE hour < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_payload_breakdown_by_iata WHERE bucket < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_top_observers_by_iata WHERE bucket < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_top_talkers_by_iata WHERE bucket < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_top_advertisers_by_iata WHERE bucket < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_observer_activity_hourly WHERE bucket < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_signal_stats_hourly WHERE hour < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
        DELETE FROM analytics_path_stats_hourly WHERE hour < date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days';
    END IF;
    RETURN deleted;
END $$;

CREATE TABLE accounts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    deactivated_at timestamp with time zone,
    CONSTRAINT accounts_name_check CHECK ((((char_length(name) >= 1) AND (char_length(name) <= 128)) AND (name = btrim(name))))
);

CREATE TABLE analytics_hourly_iata_stats (
    iata character(3) NOT NULL,
    hour timestamp with time zone NOT NULL,
    observation_count bigint DEFAULT 0 NOT NULL,
    unique_packets bigint DEFAULT 0 NOT NULL
);

CREATE TABLE packet_observations (
    id bigint NOT NULL,
    packet_hash bytea NOT NULL,
    observer_id uuid NOT NULL,
    iata character(3) NOT NULL,
    heard_at timestamp with time zone NOT NULL,
    path_length_byte smallint NOT NULL,
    hash_size smallint NOT NULL,
    hop_count smallint NOT NULL,
    path_bytes bytea,
    rssi smallint,
    snr real,
    propagation_time_ms integer,
    radio_freq_mhz real,
    spread_factor smallint,
    bandwidth_khz real,
    coding_rate smallint,
    source_broker text,
    payload_type smallint,
    airtime_ms real
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_vacuum_cost_limit='1000');

CREATE VIEW analytics_live_hourly_iata_stats AS
SELECT
  iata,
  date_trunc('hour', heard_at, 'UTC')::timestamptz AS hour,
  COUNT(*) AS observation_count,
  COUNT(DISTINCT packet_hash) AS unique_packets
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '30 days'
GROUP BY iata, date_trunc('hour', heard_at, 'UTC');

CREATE VIEW analytics_live_observer_activity_hourly AS
SELECT
  observer_id,
  COALESCE(payload_type, -1)::smallint AS payload_type,
  date_trunc('hour', heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(*)::bigint AS observations,
  SUM(airtime_ms)::real AS airtime_ms,
  COUNT(airtime_ms)::bigint AS airtime_n,
  SUM(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real   AS snr_sum,
  COUNT(snr) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS snr_n,
  MIN(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real   AS snr_min,
  SUM(rssi)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_sum,
  COUNT(rssi) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_n
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '30 days'
GROUP BY observer_id, COALESCE(payload_type, -1), date_trunc('hour', heard_at, 'UTC');

CREATE VIEW analytics_live_path_stats_hourly AS
WITH classified AS (
    SELECT iata, heard_at, hash_size, hop_count,
           CASE WHEN payload_type = 9 THEN 2
                -- Match meshcore-go IsValidPathLen (1/2/3-byte hashes, max 64 path bytes).
                WHEN payload_type IS NULL OR payload_type NOT BETWEEN 0 AND 15
                  OR NOT (path_length_byte BETWEEN 0 AND 191
                    AND hash_size BETWEEN 1 AND 3 AND hop_count BETWEEN 0 AND 63
                    AND hash_size = (path_length_byte >> 6) + 1
                    AND hop_count = (path_length_byte & 63)
                    AND hash_size::integer * hop_count::integer <= 64
                    AND COALESCE(octet_length(path_bytes), 0) = hash_size::integer * hop_count::integer)
                THEN 3
                WHEN hop_count = 0 THEN 1
                ELSE 0 END::integer AS category
    FROM packet_observations
    WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '720 hours'
      AND heard_at < date_trunc('hour', NOW(), 'UTC')
), buckets AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour, category,
           CASE WHEN category = 0 THEN hash_size ELSE 0 END::integer AS hash_bytes,
           CASE WHEN category = 0 THEN hop_count ELSE 0 END::integer AS entries
    FROM classified
)
SELECT iata, hour, category, hash_bytes, entries, count(*)::bigint AS receptions
FROM buckets GROUP BY iata, hour, category, hash_bytes, entries;

CREATE VIEW analytics_live_payload_breakdown_by_iata AS
SELECT
  iata,
  payload_type,
  date_trunc('hour', heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(*) AS count
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '30 days'
  AND payload_type IS NOT NULL
GROUP BY iata, payload_type, date_trunc('hour', heard_at, 'UTC');

CREATE VIEW analytics_live_signal_stats_hourly AS
WITH samples AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                     AND snr > '-Infinity'::real AND snr < 'Infinity'::real
                THEN snr::double precision END AS snr,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                THEN rssi::double precision END AS rssi
    FROM packet_observations
    WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '720 hours'
      AND heard_at < date_trunc('hour', NOW(), 'UTC')
), binned AS (
    SELECT *, width_bucket(snr, -30, 30, 12) AS snr_bin,
              width_bucket(rssi, -140, 0, 14) AS rssi_bin
    FROM samples
)
SELECT iata, hour, grouping(snr_bin, rssi_bin)::integer AS kind,
       COALESCE(snr_bin, -1)::integer AS snr_bin,
       COALESCE(rssi_bin, -1)::integer AS rssi_bin,
       count(*)::bigint AS receptions,
       count(snr)::bigint AS snr_samples,
       COALESCE(sum(snr), 0)::double precision AS snr_sum,
       count(rssi)::bigint AS rssi_samples,
       COALESCE(sum(rssi), 0)::double precision AS rssi_sum
FROM binned
GROUP BY GROUPING SETS ((iata, hour), (iata, hour, snr_bin), (iata, hour, rssi_bin));

CREATE TABLE nodes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    public_key bytea NOT NULL,
    node_type smallint NOT NULL,
    name text,
    latitude double precision,
    longitude double precision,
    location_source text,
    last_advert_at timestamp with time zone,
    supports_multibyte_paths boolean DEFAULT false NOT NULL,
    supports_multibyte_traces boolean DEFAULT false NOT NULL,
    default_scope_id integer,
    min_firmware_version text GENERATED ALWAYS AS (
CASE
    WHEN supports_multibyte_paths THEN '1.14.0+'::text
    WHEN supports_multibyte_traces THEN '1.11.0+'::text
    ELSE NULL::text
END) STORED,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    radio_freq_mhz real,
    radio_sf smallint,
    radio_bw_khz real,
    metadata jsonb,
    device_clock_drift_seconds integer
)
WITH (autovacuum_vacuum_scale_factor='0.05');

CREATE TABLE packets (
    packet_hash bytea NOT NULL,
    payload_type smallint NOT NULL,
    payload_version smallint NOT NULL,
    route_type smallint NOT NULL,
    transport_codes_present boolean DEFAULT false,
    region_code integer,
    sub_region_code integer,
    scope_id integer,
    origin_pubkey bytea,
    raw_payload bytea NOT NULL,
    raw_header bytea NOT NULL,
    parsed_payload jsonb,
    decrypted boolean DEFAULT false,
    channel_hash bytea,
    trace_tag bytea,
    first_heard_at timestamp with time zone NOT NULL,
    last_heard_at timestamp with time zone NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_vacuum_cost_limit='1000');

CREATE VIEW analytics_live_top_advertisers_by_iata AS
SELECT
  po.iata,
  n.id AS node_id,
  n.name,
  n.node_type,
  date_trunc('hour', po.heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(DISTINCT p.packet_hash) AS advert_count,
  COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.route_type IN (0, 1)) AS flood_advert_count,
  COUNT(DISTINCT p.packet_hash) FILTER (WHERE p.route_type IN (2, 3)) AS direct_advert_count,
  MAX(po.heard_at) AS last_heard
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE p.payload_type = 4 -- ADVERT
  AND po.heard_at > NOW() - INTERVAL '30 days'
GROUP BY po.iata, n.id, n.name, n.node_type, date_trunc('hour', po.heard_at, 'UTC');

CREATE TABLE observers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    public_key bytea NOT NULL,
    display_name text,
    observer_type text,
    software_version text,
    hardware_model text,
    firmware_version text,
    firmware_build text,
    radio_freq_mhz real,
    radio_sf smallint,
    radio_bw_khz real,
    radio_cr smallint,
    battery_level real,
    uptime_seconds bigint,
    status_metadata jsonb,
    last_status_at timestamp with time zone,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    observation_count bigint DEFAULT 0,
    metadata jsonb,
    region_scope text
);

CREATE VIEW analytics_live_top_observers_by_iata AS
SELECT
  po.iata,
  po.observer_id,
  o.display_name,
  o.observer_type,
  date_trunc('hour', po.heard_at, 'UTC')::timestamptz AS bucket,
  COUNT(*) AS observation_count
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at > NOW() - INTERVAL '30 days'
GROUP BY po.iata, po.observer_id, o.display_name, o.observer_type, date_trunc('hour', po.heard_at, 'UTC');

CREATE TABLE channel_messages (
    id bigint NOT NULL,
    channel_id integer NOT NULL,
    packet_hash bytea NOT NULL,
    sender_name text,
    sender_pubkey bytea,
    content text,
    sent_at timestamp with time zone NOT NULL
);

CREATE VIEW analytics_live_top_talkers_by_iata AS
SELECT
  po.iata,
  cm.sender_name,
  date_trunc('hour', cm.sent_at, 'UTC')::timestamptz AS bucket,
  COUNT(DISTINCT cm.id) AS message_count,
  MAX(cm.sent_at) AS last_sent
FROM channel_messages cm
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
WHERE cm.sender_name IS NOT NULL
  AND cm.sent_at > NOW() - INTERVAL '30 days'
GROUP BY po.iata, cm.sender_name, date_trunc('hour', cm.sent_at, 'UTC');

CREATE TABLE analytics_observer_activity_hourly (
    observer_id uuid NOT NULL,
    payload_type smallint NOT NULL,
    bucket timestamp with time zone NOT NULL,
    observations bigint DEFAULT 0 NOT NULL,
    airtime_ms real,
    airtime_n bigint DEFAULT 0 NOT NULL,
    snr_sum real,
    snr_n bigint DEFAULT 0 NOT NULL,
    snr_min real,
    rssi_sum bigint,
    rssi_n bigint DEFAULT 0 NOT NULL
);

CREATE TABLE analytics_path_stats_hourly (
    iata character(3) NOT NULL,
    hour timestamp with time zone NOT NULL,
    category integer NOT NULL,
    hash_bytes integer NOT NULL,
    entries integer NOT NULL,
    receptions bigint DEFAULT 0 NOT NULL
);

CREATE TABLE analytics_payload_breakdown_by_iata (
    iata character(3) NOT NULL,
    payload_type smallint NOT NULL,
    bucket timestamp with time zone NOT NULL,
    count bigint DEFAULT 0 NOT NULL
);

CREATE TABLE analytics_signal_stats_hourly (
    iata character(3) NOT NULL,
    hour timestamp with time zone NOT NULL,
    kind integer NOT NULL,
    snr_bin integer NOT NULL,
    rssi_bin integer NOT NULL,
    receptions bigint DEFAULT 0 NOT NULL,
    snr_samples bigint DEFAULT 0 NOT NULL,
    snr_sum double precision,
    rssi_samples bigint DEFAULT 0 NOT NULL,
    rssi_sum double precision
);

CREATE TABLE analytics_top_advertisers_by_iata (
    iata character(3) NOT NULL,
    node_id uuid NOT NULL,
    bucket timestamp with time zone NOT NULL,
    advert_count bigint DEFAULT 0 NOT NULL,
    flood_advert_count bigint DEFAULT 0 NOT NULL,
    direct_advert_count bigint DEFAULT 0 NOT NULL,
    last_heard timestamp with time zone,
    name text,
    node_type smallint
);

CREATE TABLE analytics_top_observers_by_iata (
    iata character(3) NOT NULL,
    observer_id uuid NOT NULL,
    bucket timestamp with time zone NOT NULL,
    observation_count bigint DEFAULT 0 NOT NULL,
    display_name text,
    observer_type text
);

CREATE TABLE analytics_top_talkers_by_iata (
    iata character(3) NOT NULL,
    sender_name text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    message_count bigint DEFAULT 0 NOT NULL,
    last_sent timestamp with time zone
);

CREATE TABLE channel_iatas (
    channel_hash bytea NOT NULL,
    iata character(3) NOT NULL,
    last_heard timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE channel_keys (
    channel_id integer NOT NULL,
    key_bytes bytea NOT NULL,
    key_fingerprint bytea NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    added_by text
);

CREATE SEQUENCE channel_messages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE channel_messages_id_seq OWNED BY channel_messages.id;

CREATE TABLE channels (
    id integer NOT NULL,
    channel_hash bytea NOT NULL,
    key_fingerprint bytea,
    name text,
    hashtag text,
    is_hashtag boolean DEFAULT false,
    is_public boolean DEFAULT false,
    key_known boolean DEFAULT false,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    message_count bigint DEFAULT 0
);

CREATE SEQUENCE channels_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE channels_id_seq OWNED BY channels.id;

CREATE TABLE iata_codes (
    iata character(3) NOT NULL,
    display_name text,
    approx_lat double precision,
    approx_lng double precision,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    border jsonb
);

CREATE TABLE known_routes (
    id bigint NOT NULL,
    path_key bytea NOT NULL,
    node_ids uuid[] NOT NULL,
    hash_prefix bytea[] NOT NULL,
    iata character(3) NOT NULL,
    hop_count integer NOT NULL,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    observation_count bigint DEFAULT 1 NOT NULL,
    last_reconfirmed_at timestamp with time zone DEFAULT now() NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.02', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_cost_limit='1000');

ALTER TABLE known_routes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME known_routes_new_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE meshmapper_scope_catalogues (
    iata text NOT NULL,
    url text NOT NULL,
    payload jsonb,
    etag text,
    checked_at timestamp with time zone,
    attempted_at timestamp with time zone NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    last_error text DEFAULT ''::text NOT NULL
);

CREATE TABLE meshmapper_zone_boundaries (
    iata text NOT NULL,
    url text NOT NULL,
    feature jsonb,
    etag text,
    checked_at timestamp with time zone,
    attempted_at timestamp with time zone NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    last_error text DEFAULT ''::text NOT NULL
);

CREATE MATERIALIZED VIEW mv_hourly_iata_stats AS
WITH combined AS (
    SELECT iata, hour, observation_count, unique_packets FROM analytics_live_hourly_iata_stats
    UNION ALL
    SELECT iata, hour, observation_count, unique_packets FROM analytics_hourly_iata_stats
    WHERE hour >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
), observers_by_hour AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour, observer_id
    FROM packet_observations WHERE heard_at > NOW() - INTERVAL '30 days'
    UNION
    SELECT iata, bucket AS hour, observer_id FROM analytics_top_observers_by_iata
    WHERE bucket >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
), observer_counts AS (SELECT iata, hour, COUNT(*) AS n FROM observers_by_hour GROUP BY iata,hour)
SELECT combined.iata, combined.hour, SUM(observation_count)::bigint AS observation_count, SUM(unique_packets)::bigint AS unique_packets, MAX(o.n)::bigint AS active_observers
FROM combined JOIN observer_counts o USING (iata,hour) GROUP BY combined.iata,combined.hour;

CREATE MATERIALIZED VIEW mv_observer_activity_hourly AS
WITH combined AS (
    SELECT observer_id, payload_type, bucket, observations, airtime_ms, airtime_n, snr_sum, snr_n, snr_min, rssi_sum, rssi_n FROM analytics_live_observer_activity_hourly
    UNION ALL
    SELECT observer_id, payload_type, bucket, observations, airtime_ms, airtime_n, snr_sum, snr_n, snr_min, rssi_sum, rssi_n FROM analytics_observer_activity_hourly
    WHERE bucket >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
)
SELECT observer_id, payload_type, bucket, SUM(observations)::bigint AS observations, SUM(airtime_ms)::real AS airtime_ms, SUM(airtime_n)::bigint AS airtime_n, SUM(snr_sum)::real AS snr_sum, SUM(snr_n)::bigint AS snr_n, MIN(snr_min)::real AS snr_min, SUM(rssi_sum)::bigint AS rssi_sum, SUM(rssi_n)::bigint AS rssi_n
FROM combined GROUP BY observer_id,payload_type,bucket;

CREATE MATERIALIZED VIEW mv_path_stats_hourly AS
WITH combined AS (
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM analytics_live_path_stats_hourly
    UNION ALL
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM analytics_path_stats_hourly
    WHERE hour >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
      AND hour < date_trunc('hour', NOW(), 'UTC')
)
SELECT iata, hour, category, hash_bytes, entries, SUM(receptions)::bigint AS receptions
FROM combined GROUP BY iata,hour,category,hash_bytes,entries;

CREATE MATERIALIZED VIEW mv_payload_breakdown_by_iata AS
WITH combined AS (
    SELECT iata, payload_type, bucket, count FROM analytics_live_payload_breakdown_by_iata
    UNION ALL
    SELECT iata, payload_type, bucket, count FROM analytics_payload_breakdown_by_iata
    WHERE bucket >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
)
SELECT iata, payload_type, bucket, SUM(count)::bigint AS count
FROM combined GROUP BY iata,payload_type,bucket;

CREATE TABLE node_iatas (
    node_id uuid NOT NULL,
    iata character(3) NOT NULL,
    first_heard timestamp with time zone DEFAULT now() NOT NULL,
    last_heard timestamp with time zone DEFAULT now() NOT NULL,
    observation_count bigint DEFAULT 0
)
WITH (autovacuum_vacuum_scale_factor='0.05');

CREATE MATERIALIZED VIEW mv_radio_presets AS
SELECT
    concat(o.radio_freq_mhz, ',', o.radio_bw_khz, ',', o.radio_sf)::text AS preset,
    (SELECT po.iata FROM packet_observations po WHERE po.observer_id = o.id ORDER BY po.heard_at DESC LIMIT 1) AS iata,
    'observer' AS source_type,
    COUNT(*) AS count
FROM observers o
WHERE o.radio_freq_mhz IS NOT NULL
    AND o.radio_sf IS NOT NULL
    AND o.radio_bw_khz IS NOT NULL
GROUP BY concat(o.radio_freq_mhz, ',', o.radio_bw_khz, ',', o.radio_sf)::text,
         (SELECT po.iata FROM packet_observations po WHERE po.observer_id = o.id ORDER BY po.heard_at DESC LIMIT 1)
HAVING (SELECT po.iata FROM packet_observations po WHERE po.observer_id = o.id ORDER BY po.heard_at DESC LIMIT 1) IS NOT NULL

UNION ALL

SELECT
    concat(n.radio_freq_mhz, ',', n.radio_bw_khz, ',', n.radio_sf) AS preset,
    ni.iata,
    'node' AS source_type,
    COUNT(*) AS count
FROM nodes n
JOIN node_iatas ni ON ni.node_id = n.id
WHERE n.radio_freq_mhz IS NOT NULL
    AND n.radio_sf IS NOT NULL
    AND n.radio_bw_khz IS NOT NULL
GROUP BY concat(n.radio_freq_mhz, ',', n.radio_bw_khz, ',', n.radio_sf), ni.iata;

CREATE MATERIALIZED VIEW mv_signal_stats_hourly AS
WITH combined AS (
    SELECT iata, hour, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum FROM analytics_live_signal_stats_hourly
    UNION ALL
    SELECT iata, hour, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum FROM analytics_signal_stats_hourly
    WHERE hour >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
      AND hour < date_trunc('hour', NOW(), 'UTC')
)
SELECT iata, hour, kind, snr_bin, rssi_bin, SUM(receptions)::bigint AS receptions, SUM(snr_samples)::bigint AS snr_samples, SUM(snr_sum)::double precision AS snr_sum, SUM(rssi_samples)::bigint AS rssi_samples, SUM(rssi_sum)::double precision AS rssi_sum
FROM combined GROUP BY iata,hour,kind,snr_bin,rssi_bin;

CREATE MATERIALIZED VIEW mv_top_advertisers_by_iata AS
WITH combined AS (
    SELECT iata, node_id, bucket, advert_count, flood_advert_count, direct_advert_count, last_heard, name, node_type FROM analytics_live_top_advertisers_by_iata
    UNION ALL
    SELECT iata, node_id, bucket, advert_count, flood_advert_count, direct_advert_count, last_heard, name, node_type FROM analytics_top_advertisers_by_iata
    WHERE bucket >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
), totals AS (SELECT iata, node_id, bucket, SUM(advert_count)::bigint AS advert_count, SUM(flood_advert_count)::bigint AS flood_advert_count, SUM(direct_advert_count)::bigint AS direct_advert_count, MAX(last_heard) AS last_heard, MAX(name) AS name, MAX(node_type) AS node_type
FROM combined GROUP BY iata,node_id,bucket)
SELECT t.iata, t.node_id, COALESCE(n.name,t.name) AS name, COALESCE(n.node_type,t.node_type) AS node_type,
       t.bucket, t.advert_count, t.flood_advert_count, t.direct_advert_count, t.last_heard
FROM totals t LEFT JOIN nodes n ON n.id=t.node_id;

CREATE MATERIALIZED VIEW mv_top_nodes_by_iata AS
SELECT
  ni.iata,
  ni.node_id,
  n.name,
  n.node_type,
  ni.observation_count,
  ni.last_heard
FROM node_iatas ni
JOIN nodes n ON n.id = ni.node_id
WHERE ni.last_heard > NOW() - INTERVAL '7 days';

CREATE MATERIALIZED VIEW mv_top_observers_by_iata AS
WITH combined AS (
    SELECT iata, observer_id, bucket, observation_count, display_name, observer_type FROM analytics_live_top_observers_by_iata
    UNION ALL
    SELECT iata, observer_id, bucket, observation_count, display_name, observer_type FROM analytics_top_observers_by_iata
    WHERE bucket >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
), totals AS (SELECT iata, observer_id, bucket, SUM(observation_count)::bigint AS observation_count, MAX(display_name) AS display_name, MAX(observer_type) AS observer_type
FROM combined GROUP BY iata,observer_id,bucket)
SELECT t.iata, t.observer_id, COALESCE(o.display_name,t.display_name) AS display_name,
       COALESCE(o.observer_type,t.observer_type) AS observer_type, t.bucket, t.observation_count
FROM totals t LEFT JOIN observers o ON o.id=t.observer_id;

CREATE MATERIALIZED VIEW mv_top_talkers_by_iata AS
WITH combined AS (
    SELECT iata, sender_name, bucket, message_count, last_sent FROM analytics_live_top_talkers_by_iata
    UNION ALL
    SELECT iata, sender_name, bucket, message_count, last_sent FROM analytics_top_talkers_by_iata
    WHERE bucket >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
)
SELECT iata, sender_name, bucket, SUM(message_count)::bigint AS message_count, MAX(last_sent) AS last_sent
FROM combined GROUP BY iata,sender_name,bucket;

CREATE TABLE node_neighbors (
    node_id uuid NOT NULL,
    neighbor_id uuid NOT NULL,
    iata character(3) NOT NULL,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    observation_count bigint DEFAULT 1 NOT NULL,
    snr real,
    region_scope text
);

CREATE TABLE node_short_ids (
    node_id uuid NOT NULL,
    iata character(3) NOT NULL,
    prefix_4 bytea NOT NULL,
    prefix_1 bytea GENERATED ALWAYS AS (SUBSTRING(prefix_4 FROM 1 FOR 1)) STORED,
    prefix_2 bytea GENERATED ALWAYS AS (SUBSTRING(prefix_4 FROM 1 FOR 2)) STORED,
    prefix_3 bytea GENERATED ALWAYS AS (SUBSTRING(prefix_4 FROM 1 FOR 3)) STORED
)
WITH (autovacuum_vacuum_scale_factor='0.05');

CREATE TABLE observer_brokers (
    observer_id uuid NOT NULL,
    broker_name text NOT NULL,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_packet_at timestamp with time zone,
    auth_ok boolean DEFAULT true
);

CREATE TABLE observer_locations (
    observer_id uuid NOT NULL,
    iata character(3),
    latitude double precision,
    longitude double precision,
    reported_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE observer_owners (
    observer_id uuid NOT NULL,
    owner_node_id uuid,
    owner_pubkey bytea,
    contact_name text,
    contact_email text,
    notes text,
    source text,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE observer_scopes (
    observer_id uuid NOT NULL,
    scope_id integer NOT NULL,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE observer_telemetry (
    id bigint NOT NULL,
    observer_id uuid NOT NULL,
    reported_at timestamp with time zone NOT NULL,
    battery_voltage_mv integer,
    airtime_tx_secs real,
    airtime_rx_secs real,
    noise_floor_db real,
    uptime_seconds bigint,
    queue_length integer,
    debug_flags integer,
    receive_errors integer
);

CREATE SEQUENCE observer_telemetry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE observer_telemetry_id_seq OWNED BY observer_telemetry.id;

CREATE SEQUENCE packet_observations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE packet_observations_id_seq OWNED BY packet_observations.id;

CREATE TABLE region_iatas (
    region_id integer NOT NULL,
    iata character(3) NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE regions (
    id integer NOT NULL,
    slug text NOT NULL,
    name text NOT NULL,
    description text,
    display_order integer DEFAULT 0,
    center_lat double precision,
    center_lng double precision,
    zoom_level integer DEFAULT 8,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE SEQUENCE regions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE regions_id_seq OWNED BY regions.id;

CREATE TABLE trace_iatas (
    trace_tag bytea NOT NULL,
    iata character(3) NOT NULL,
    last_heard timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE transport_scopes (
    id integer NOT NULL,
    name text NOT NULL,
    display_name text,
    transport_key bytea NOT NULL,
    key_fingerprint bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    imported_only boolean DEFAULT false NOT NULL
);

CREATE SEQUENCE transport_scopes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE transport_scopes_id_seq OWNED BY transport_scopes.id;

ALTER TABLE ONLY channel_messages ALTER COLUMN id SET DEFAULT nextval('channel_messages_id_seq'::regclass);

ALTER TABLE ONLY channels ALTER COLUMN id SET DEFAULT nextval('channels_id_seq'::regclass);

ALTER TABLE ONLY observer_telemetry ALTER COLUMN id SET DEFAULT nextval('observer_telemetry_id_seq'::regclass);

ALTER TABLE ONLY packet_observations ALTER COLUMN id SET DEFAULT nextval('packet_observations_id_seq'::regclass);

ALTER TABLE ONLY regions ALTER COLUMN id SET DEFAULT nextval('regions_id_seq'::regclass);

ALTER TABLE ONLY transport_scopes ALTER COLUMN id SET DEFAULT nextval('transport_scopes_id_seq'::regclass);

ALTER TABLE ONLY accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY analytics_hourly_iata_stats
    ADD CONSTRAINT analytics_hourly_iata_stats_pkey PRIMARY KEY (iata, hour);

ALTER TABLE ONLY analytics_observer_activity_hourly
    ADD CONSTRAINT analytics_observer_activity_hourly_pkey PRIMARY KEY (observer_id, payload_type, bucket);

ALTER TABLE ONLY analytics_path_stats_hourly
    ADD CONSTRAINT analytics_path_stats_hourly_pkey PRIMARY KEY (iata, hour, category, hash_bytes, entries);

ALTER TABLE ONLY analytics_payload_breakdown_by_iata
    ADD CONSTRAINT analytics_payload_breakdown_by_iata_pkey PRIMARY KEY (iata, payload_type, bucket);

ALTER TABLE ONLY analytics_signal_stats_hourly
    ADD CONSTRAINT analytics_signal_stats_hourly_pkey PRIMARY KEY (iata, hour, kind, snr_bin, rssi_bin);

ALTER TABLE ONLY analytics_top_advertisers_by_iata
    ADD CONSTRAINT analytics_top_advertisers_by_iata_pkey PRIMARY KEY (iata, node_id, bucket);

ALTER TABLE ONLY analytics_top_observers_by_iata
    ADD CONSTRAINT analytics_top_observers_by_iata_pkey PRIMARY KEY (iata, observer_id, bucket);

ALTER TABLE ONLY analytics_top_talkers_by_iata
    ADD CONSTRAINT analytics_top_talkers_by_iata_pkey PRIMARY KEY (iata, sender_name, bucket);

ALTER TABLE ONLY channel_iatas
    ADD CONSTRAINT channel_iatas_pkey PRIMARY KEY (channel_hash, iata);

ALTER TABLE ONLY channel_keys
    ADD CONSTRAINT channel_keys_pkey PRIMARY KEY (channel_id);

ALTER TABLE ONLY channel_messages
    ADD CONSTRAINT channel_messages_packet_hash_key UNIQUE (packet_hash);

ALTER TABLE ONLY channel_messages
    ADD CONSTRAINT channel_messages_pkey PRIMARY KEY (id);

ALTER TABLE ONLY channels
    ADD CONSTRAINT channels_channel_hash_key_fingerprint_key UNIQUE (channel_hash, key_fingerprint);

ALTER TABLE ONLY channels
    ADD CONSTRAINT channels_hashtag_key UNIQUE (hashtag);

ALTER TABLE ONLY channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (id);

ALTER TABLE ONLY iata_codes
    ADD CONSTRAINT iata_codes_pkey PRIMARY KEY (iata);

ALTER TABLE ONLY known_routes
    ADD CONSTRAINT known_routes_pkey PRIMARY KEY (iata, path_key);

ALTER TABLE ONLY meshmapper_scope_catalogues
    ADD CONSTRAINT meshmapper_scope_catalogues_pkey PRIMARY KEY (iata, url);

ALTER TABLE ONLY meshmapper_zone_boundaries
    ADD CONSTRAINT meshmapper_zone_boundaries_pkey PRIMARY KEY (iata);

ALTER TABLE ONLY node_iatas
    ADD CONSTRAINT node_iatas_pkey PRIMARY KEY (node_id, iata);

ALTER TABLE ONLY node_neighbors
    ADD CONSTRAINT node_neighbors_pkey PRIMARY KEY (node_id, neighbor_id, iata);

ALTER TABLE ONLY node_short_ids
    ADD CONSTRAINT node_short_ids_pkey PRIMARY KEY (node_id, iata);

ALTER TABLE ONLY nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY nodes
    ADD CONSTRAINT nodes_public_key_key UNIQUE (public_key);

ALTER TABLE ONLY observer_brokers
    ADD CONSTRAINT observer_brokers_pkey PRIMARY KEY (observer_id, broker_name);

ALTER TABLE ONLY observer_locations
    ADD CONSTRAINT observer_locations_pkey PRIMARY KEY (observer_id, reported_at);

ALTER TABLE ONLY observer_owners
    ADD CONSTRAINT observer_owners_pkey PRIMARY KEY (observer_id);

ALTER TABLE ONLY observer_scopes
    ADD CONSTRAINT observer_scopes_pkey PRIMARY KEY (observer_id, scope_id);

ALTER TABLE ONLY observer_telemetry
    ADD CONSTRAINT observer_telemetry_observer_id_reported_at_key UNIQUE (observer_id, reported_at);

ALTER TABLE ONLY observer_telemetry
    ADD CONSTRAINT observer_telemetry_pkey PRIMARY KEY (id);

ALTER TABLE ONLY observers
    ADD CONSTRAINT observers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY observers
    ADD CONSTRAINT observers_public_key_key UNIQUE (public_key);

ALTER TABLE ONLY packet_observations
    ADD CONSTRAINT packet_observations_packet_hash_observer_id_key UNIQUE (packet_hash, observer_id);

ALTER TABLE ONLY packet_observations
    ADD CONSTRAINT packet_observations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY packets
    ADD CONSTRAINT packets_pkey PRIMARY KEY (packet_hash);

ALTER TABLE ONLY region_iatas
    ADD CONSTRAINT region_iatas_pkey PRIMARY KEY (region_id, iata);

ALTER TABLE ONLY regions
    ADD CONSTRAINT regions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY regions
    ADD CONSTRAINT regions_slug_key UNIQUE (slug);

ALTER TABLE ONLY trace_iatas
    ADD CONSTRAINT trace_iatas_pkey PRIMARY KEY (trace_tag, iata);

ALTER TABLE ONLY transport_scopes
    ADD CONSTRAINT transport_scopes_name_key UNIQUE (name);

ALTER TABLE ONLY transport_scopes
    ADD CONSTRAINT transport_scopes_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX accounts_name_uidx ON accounts USING btree (name) WHERE (deactivated_at IS NULL);

CREATE INDEX idx_analytics_hourly_iata_stats_time ON analytics_hourly_iata_stats USING btree (hour);

CREATE UNIQUE INDEX idx_analytics_hourly_iata_stats_view ON mv_hourly_iata_stats USING btree (iata, hour);

CREATE INDEX idx_analytics_observer_activity_hourly_time ON analytics_observer_activity_hourly USING btree (bucket);

CREATE UNIQUE INDEX idx_analytics_observer_activity_hourly_view ON mv_observer_activity_hourly USING btree (observer_id, payload_type, bucket);

CREATE INDEX idx_analytics_path_stats_hourly_time ON analytics_path_stats_hourly USING btree (hour);

CREATE UNIQUE INDEX idx_analytics_path_stats_hourly_view ON mv_path_stats_hourly USING btree (iata, hour, category, hash_bytes, entries);

CREATE INDEX idx_analytics_payload_breakdown_by_iata_time ON analytics_payload_breakdown_by_iata USING btree (bucket);

CREATE UNIQUE INDEX idx_analytics_payload_breakdown_by_iata_view ON mv_payload_breakdown_by_iata USING btree (iata, payload_type, bucket);

CREATE INDEX idx_analytics_signal_stats_hourly_time ON analytics_signal_stats_hourly USING btree (hour);

CREATE UNIQUE INDEX idx_analytics_signal_stats_hourly_view ON mv_signal_stats_hourly USING btree (iata, hour, kind, snr_bin, rssi_bin);

CREATE INDEX idx_analytics_top_advertisers_by_iata_time ON analytics_top_advertisers_by_iata USING btree (bucket);

CREATE UNIQUE INDEX idx_analytics_top_advertisers_by_iata_view ON mv_top_advertisers_by_iata USING btree (iata, node_id, bucket);

CREATE INDEX idx_analytics_top_observers_by_iata_time ON analytics_top_observers_by_iata USING btree (bucket);

CREATE UNIQUE INDEX idx_analytics_top_observers_by_iata_view ON mv_top_observers_by_iata USING btree (iata, observer_id, bucket);

CREATE INDEX idx_analytics_top_talkers_by_iata_time ON analytics_top_talkers_by_iata USING btree (bucket);

CREATE UNIQUE INDEX idx_analytics_top_talkers_by_iata_view ON mv_top_talkers_by_iata USING btree (iata, sender_name, bucket);

CREATE INDEX idx_channel_iatas_iata ON channel_iatas USING btree (iata);

CREATE INDEX idx_channel_messages_channel ON channel_messages USING btree (channel_id, sent_at DESC);

CREATE INDEX idx_channel_messages_sent_brin ON channel_messages USING brin (sent_at);

CREATE INDEX idx_channels_hash ON channels USING btree (channel_hash);

CREATE UNIQUE INDEX idx_channels_hash_no_key ON channels USING btree (channel_hash) WHERE (key_fingerprint IS NULL);

CREATE INDEX idx_channels_hashtag ON channels USING btree (hashtag) WHERE (hashtag IS NOT NULL);

CREATE INDEX idx_channels_last_seen_id ON channels USING btree (last_seen DESC, id DESC);

CREATE INDEX idx_known_routes_hop_count ON known_routes USING btree (iata, hop_count);

CREATE INDEX idx_known_routes_iata_last_seen ON known_routes USING btree (iata, last_seen DESC);

CREATE INDEX idx_known_routes_last_seen ON known_routes USING btree (last_seen DESC);

CREATE INDEX idx_known_routes_reconfirm ON known_routes USING btree (last_reconfirmed_at);

CREATE INDEX idx_mv_path_stats_hourly_hour ON mv_path_stats_hourly USING btree (hour);

CREATE UNIQUE INDEX idx_mv_radio_presets ON mv_radio_presets USING btree (preset, iata, source_type);

CREATE INDEX idx_mv_signal_stats_hourly_hour ON mv_signal_stats_hourly USING btree (hour);

CREATE UNIQUE INDEX idx_mv_top_nodes ON mv_top_nodes_by_iata USING btree (iata, node_id);

CREATE INDEX idx_node_iatas_iata ON node_iatas USING btree (iata, last_heard DESC);

CREATE INDEX idx_node_neighbors_neighbor ON node_neighbors USING btree (neighbor_id, iata);

CREATE INDEX idx_node_neighbors_node ON node_neighbors USING btree (node_id, iata);

CREATE INDEX idx_nodes_location ON nodes USING btree (latitude, longitude) WHERE ((latitude IS NOT NULL) AND (longitude IS NOT NULL));

CREATE INDEX idx_nodes_min_firmware ON nodes USING btree (min_firmware_version) WHERE (min_firmware_version IS NOT NULL);

CREATE INDEX idx_nodes_multibyte_paths ON nodes USING btree (supports_multibyte_paths) WHERE supports_multibyte_paths;

CREATE INDEX idx_nodes_multibyte_traces ON nodes USING btree (supports_multibyte_traces) WHERE supports_multibyte_traces;

CREATE INDEX idx_nodes_pubkey ON nodes USING btree (public_key);

CREATE INDEX idx_nodes_type_last_seen ON nodes USING btree (node_type, last_seen DESC);

CREATE INDEX idx_observations_heard_brin ON packet_observations USING brin (heard_at);

CREATE INDEX idx_observations_iata_heard ON packet_observations USING btree (iata, heard_at DESC);

CREATE INDEX idx_observations_observer ON packet_observations USING btree (observer_id, heard_at DESC);

CREATE INDEX idx_observations_packet ON packet_observations USING btree (packet_hash);

CREATE INDEX idx_observations_route_evidence ON packet_observations USING btree (iata, hash_size, decode(md5(path_bytes), 'hex'::text), heard_at DESC, id DESC) WHERE ((path_bytes IS NOT NULL) AND (payload_type IS NOT NULL) AND (payload_type <> 9) AND (hop_count >= 2));

CREATE INDEX idx_observer_locations_recent ON observer_locations USING btree (observer_id, reported_at DESC);

CREATE INDEX idx_observer_owners_node ON observer_owners USING btree (owner_node_id) WHERE (owner_node_id IS NOT NULL);

CREATE INDEX idx_observers_last_seen ON observers USING btree (last_seen DESC);

CREATE INDEX idx_observers_pubkey ON observers USING btree (public_key);

CREATE INDEX idx_observers_type ON observers USING btree (observer_type) WHERE (observer_type IS NOT NULL);

CREATE INDEX idx_packets_channel ON packets USING btree (channel_hash, first_heard_at DESC) WHERE (channel_hash IS NOT NULL);

CREATE INDEX idx_packets_first_heard_brin ON packets USING brin (first_heard_at);

CREATE INDEX idx_packets_last_heard ON packets USING btree (last_heard_at DESC);

CREATE INDEX idx_packets_origin ON packets USING btree (origin_pubkey, first_heard_at DESC) WHERE (origin_pubkey IS NOT NULL);

CREATE INDEX idx_packets_payload_type ON packets USING btree (payload_type, first_heard_at DESC);

CREATE INDEX idx_packets_route_type ON packets USING btree (route_type, first_heard_at DESC);

CREATE INDEX idx_packets_scope ON packets USING btree (scope_id) WHERE (scope_id IS NOT NULL);

CREATE INDEX idx_packets_trace_tag ON packets USING btree (trace_tag) WHERE (trace_tag IS NOT NULL);

CREATE INDEX idx_region_iatas_iata ON region_iatas USING btree (iata);

CREATE INDEX idx_short_ids_p1 ON node_short_ids USING btree (iata, prefix_1);

CREATE INDEX idx_short_ids_p2 ON node_short_ids USING btree (iata, prefix_2);

CREATE INDEX idx_short_ids_p3 ON node_short_ids USING btree (iata, prefix_3);

CREATE INDEX idx_short_ids_p4 ON node_short_ids USING btree (iata, prefix_4);

CREATE INDEX idx_telemetry_observer_recent ON observer_telemetry USING btree (observer_id, reported_at DESC);

CREATE INDEX idx_telemetry_reported_brin ON observer_telemetry USING brin (reported_at);

CREATE INDEX idx_trace_iatas_iata ON trace_iatas USING btree (iata);

CREATE INDEX idx_transport_scopes_fingerprint ON transport_scopes USING btree (key_fingerprint);

ALTER TABLE ONLY channel_iatas
    ADD CONSTRAINT channel_iatas_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;

ALTER TABLE ONLY channel_keys
    ADD CONSTRAINT channel_keys_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES channels(id) ON DELETE CASCADE;

ALTER TABLE ONLY channel_messages
    ADD CONSTRAINT channel_messages_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES channels(id);

ALTER TABLE ONLY channel_messages
    ADD CONSTRAINT channel_messages_packet_hash_fkey FOREIGN KEY (packet_hash) REFERENCES packets(packet_hash) ON DELETE CASCADE;

ALTER TABLE ONLY known_routes
    ADD CONSTRAINT known_routes_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;

ALTER TABLE ONLY node_iatas
    ADD CONSTRAINT node_iatas_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;

ALTER TABLE ONLY node_iatas
    ADD CONSTRAINT node_iatas_node_id_fkey FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY node_neighbors
    ADD CONSTRAINT node_neighbors_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;

ALTER TABLE ONLY node_neighbors
    ADD CONSTRAINT node_neighbors_neighbor_id_fkey FOREIGN KEY (neighbor_id) REFERENCES nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY node_neighbors
    ADD CONSTRAINT node_neighbors_node_id_fkey FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY node_short_ids
    ADD CONSTRAINT node_short_ids_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;

ALTER TABLE ONLY node_short_ids
    ADD CONSTRAINT node_short_ids_node_id_fkey FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY nodes
    ADD CONSTRAINT nodes_default_scope_id_fkey FOREIGN KEY (default_scope_id) REFERENCES transport_scopes(id);

ALTER TABLE ONLY observer_brokers
    ADD CONSTRAINT observer_brokers_observer_id_fkey FOREIGN KEY (observer_id) REFERENCES observers(id) ON DELETE CASCADE;

ALTER TABLE ONLY observer_locations
    ADD CONSTRAINT observer_locations_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata);

ALTER TABLE ONLY observer_locations
    ADD CONSTRAINT observer_locations_observer_id_fkey FOREIGN KEY (observer_id) REFERENCES observers(id) ON DELETE CASCADE;

ALTER TABLE ONLY observer_owners
    ADD CONSTRAINT observer_owners_observer_id_fkey FOREIGN KEY (observer_id) REFERENCES observers(id) ON DELETE CASCADE;

ALTER TABLE ONLY observer_owners
    ADD CONSTRAINT observer_owners_owner_node_id_fkey FOREIGN KEY (owner_node_id) REFERENCES nodes(id);

ALTER TABLE ONLY observer_scopes
    ADD CONSTRAINT observer_scopes_observer_id_fkey FOREIGN KEY (observer_id) REFERENCES observers(id) ON DELETE CASCADE;

ALTER TABLE ONLY observer_scopes
    ADD CONSTRAINT observer_scopes_scope_id_fkey FOREIGN KEY (scope_id) REFERENCES transport_scopes(id) ON DELETE CASCADE;

ALTER TABLE ONLY observer_telemetry
    ADD CONSTRAINT observer_telemetry_observer_id_fkey FOREIGN KEY (observer_id) REFERENCES observers(id) ON DELETE CASCADE;

ALTER TABLE ONLY packet_observations
    ADD CONSTRAINT packet_observations_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata);

ALTER TABLE ONLY packet_observations
    ADD CONSTRAINT packet_observations_observer_id_fkey FOREIGN KEY (observer_id) REFERENCES observers(id);

ALTER TABLE ONLY packet_observations
    ADD CONSTRAINT packet_observations_packet_hash_fkey FOREIGN KEY (packet_hash) REFERENCES packets(packet_hash) ON DELETE CASCADE;

ALTER TABLE ONLY packets
    ADD CONSTRAINT packets_scope_id_fkey FOREIGN KEY (scope_id) REFERENCES transport_scopes(id);

ALTER TABLE ONLY region_iatas
    ADD CONSTRAINT region_iatas_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;

ALTER TABLE ONLY region_iatas
    ADD CONSTRAINT region_iatas_region_id_fkey FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE;

ALTER TABLE ONLY trace_iatas
    ADD CONSTRAINT trace_iatas_iata_fkey FOREIGN KEY (iata) REFERENCES iata_codes(iata) ON DELETE CASCADE;


--

--
