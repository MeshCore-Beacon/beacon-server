-- ============================================================
-- Tower schema migration
-- ============================================================

-- ============================================================
-- IATA CODES
-- ============================================================

CREATE TABLE iata_codes (
  iata          CHAR(3) PRIMARY KEY,
  display_name  TEXT,
  approx_lat    DOUBLE PRECISION,
  approx_lng    DOUBLE PRECISION,
  added_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- REGIONS
-- ============================================================

CREATE TABLE regions (
  id            SERIAL PRIMARY KEY,
  slug          TEXT UNIQUE NOT NULL,
  name          TEXT NOT NULL,
  description   TEXT,
  display_order INT DEFAULT 0,
  center_lat    DOUBLE PRECISION,
  center_lng    DOUBLE PRECISION,
  zoom_level    INT DEFAULT 8,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE region_iatas (
  region_id  INT NOT NULL REFERENCES regions(id) ON DELETE CASCADE,
  iata       CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  added_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (region_id, iata)
);

CREATE INDEX idx_region_iatas_iata ON region_iatas(iata);

-- ============================================================
-- NODES (must come before observers due to observer_owners FK)
-- ============================================================

CREATE TABLE nodes (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  public_key      BYTEA UNIQUE NOT NULL,
  node_type       SMALLINT NOT NULL,
  name            TEXT,
  latitude        DOUBLE PRECISION,
  longitude       DOUBLE PRECISION,
  location_source TEXT,
  last_advert_at  TIMESTAMPTZ,
  supports_multibyte_paths  BOOLEAN NOT NULL DEFAULT FALSE,
  supports_multibyte_traces BOOLEAN NOT NULL DEFAULT FALSE,
  min_firmware_version TEXT GENERATED ALWAYS AS (
    CASE
      WHEN supports_multibyte_paths  THEN '1.14.0+'
      WHEN supports_multibyte_traces THEN '1.11.0+'
      ELSE NULL
    END
  ) STORED,
  first_seen      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  radio_freq_mhz  REAL,
  radio_sf        SMALLINT,
  radio_bw_khz    REAL,
  metadata        JSONB
);

CREATE INDEX idx_nodes_type_last_seen ON nodes(node_type, last_seen DESC);
CREATE INDEX idx_nodes_location ON nodes(latitude, longitude)
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
CREATE INDEX idx_nodes_pubkey ON nodes(public_key);
CREATE INDEX idx_nodes_multibyte_paths  ON nodes(supports_multibyte_paths)  WHERE supports_multibyte_paths;
CREATE INDEX idx_nodes_multibyte_traces ON nodes(supports_multibyte_traces) WHERE supports_multibyte_traces;
CREATE INDEX idx_nodes_min_firmware ON nodes(min_firmware_version) WHERE min_firmware_version IS NOT NULL;

-- ============================================================
-- OBSERVERS
-- ============================================================

CREATE TABLE observers (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  public_key        BYTEA UNIQUE NOT NULL,
  display_name      TEXT,
  observer_type     TEXT,
  software_version  TEXT,
  hardware_model    TEXT,
  firmware_version  TEXT,
  firmware_build    TEXT,
  radio_freq_mhz    REAL,
  radio_sf          SMALLINT,
  radio_bw_khz      REAL,
  radio_cr          SMALLINT,
  battery_level     REAL,
  uptime_seconds    BIGINT,
  status_metadata   JSONB,
  last_status_at    TIMESTAMPTZ,
  first_seen        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  observation_count BIGINT DEFAULT 0,
  metadata          JSONB
);

CREATE INDEX idx_observers_last_seen ON observers(last_seen DESC);
CREATE INDEX idx_observers_pubkey ON observers(public_key);
CREATE INDEX idx_observers_type ON observers(observer_type) WHERE observer_type IS NOT NULL;

CREATE TABLE observer_brokers (
  observer_id     UUID NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
  broker_name     TEXT NOT NULL,
  first_seen      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_packet_at  TIMESTAMPTZ,
  auth_ok         BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (observer_id, broker_name)
);

CREATE TABLE observer_locations (
  observer_id   UUID NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
  iata          CHAR(3) REFERENCES iata_codes(iata),
  latitude      DOUBLE PRECISION,
  longitude     DOUBLE PRECISION,
  reported_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (observer_id, reported_at)
);

CREATE INDEX idx_observer_locations_recent ON observer_locations(observer_id, reported_at DESC);

CREATE TABLE observer_telemetry (
  id                  BIGSERIAL PRIMARY KEY,
  observer_id         UUID NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
  reported_at         TIMESTAMPTZ NOT NULL,
  battery_voltage_mv  INT,
  airtime_tx_pct      REAL,
  airtime_rx_pct      REAL,
  noise_floor_db      REAL,
  uptime_seconds      BIGINT,
  queue_length        INT,
  debug_flags         INT,
  receive_errors      INT,
  UNIQUE (observer_id, reported_at)
);

CREATE INDEX idx_telemetry_reported_brin ON observer_telemetry USING BRIN (reported_at);
CREATE INDEX idx_telemetry_observer_recent ON observer_telemetry(observer_id, reported_at DESC);

CREATE TABLE observer_owners (
  observer_id   UUID PRIMARY KEY REFERENCES observers(id) ON DELETE CASCADE,
  owner_node_id UUID REFERENCES nodes(id),
  owner_pubkey  BYTEA,
  contact_name  TEXT,
  contact_email TEXT,
  notes         TEXT,
  source        TEXT,
  added_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_observer_owners_node ON observer_owners(owner_node_id) WHERE owner_node_id IS NOT NULL;

-- ============================================================
-- NODE IATAS AND SHORT IDS
-- ============================================================

CREATE TABLE node_iatas (
  node_id           UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  iata              CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  first_heard       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_heard        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  observation_count BIGINT DEFAULT 0,
  PRIMARY KEY (node_id, iata)
);

CREATE INDEX idx_node_iatas_iata ON node_iatas(iata, last_heard DESC);

CREATE TABLE node_short_ids (
  node_id   UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  iata      CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  prefix_4  BYTEA NOT NULL,
  prefix_1  BYTEA GENERATED ALWAYS AS (substring(prefix_4 from 1 for 1)) STORED,
  prefix_2  BYTEA GENERATED ALWAYS AS (substring(prefix_4 from 1 for 2)) STORED,
  prefix_3  BYTEA GENERATED ALWAYS AS (substring(prefix_4 from 1 for 3)) STORED,
  PRIMARY KEY (node_id, iata)
);

CREATE INDEX idx_short_ids_p1 ON node_short_ids(iata, prefix_1);
CREATE INDEX idx_short_ids_p2 ON node_short_ids(iata, prefix_2);
CREATE INDEX idx_short_ids_p3 ON node_short_ids(iata, prefix_3);
CREATE INDEX idx_short_ids_p4 ON node_short_ids(iata, prefix_4);

-- ============================================================
-- PACKETS
-- ============================================================

CREATE TABLE packets (
  packet_hash             BYTEA PRIMARY KEY,
  payload_type            SMALLINT NOT NULL,
  payload_version         SMALLINT NOT NULL,
  route_type              SMALLINT NOT NULL,
  transport_codes_present BOOLEAN DEFAULT FALSE,
  region_code             INT,
  sub_region_code         INT,
  origin_pubkey           BYTEA,
  raw_payload             BYTEA NOT NULL,
  raw_header              BYTEA NOT NULL,
  parsed_payload          JSONB,
  decrypted               BOOLEAN DEFAULT FALSE,
  channel_hash            BYTEA,
  first_heard_at          TIMESTAMPTZ NOT NULL,
  last_heard_at           TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_packets_first_heard_brin ON packets USING BRIN (first_heard_at);
CREATE INDEX idx_packets_payload_type ON packets(payload_type, first_heard_at DESC);
CREATE INDEX idx_packets_route_type ON packets(route_type, first_heard_at DESC);
CREATE INDEX idx_packets_origin ON packets(origin_pubkey, first_heard_at DESC)
  WHERE origin_pubkey IS NOT NULL;
CREATE INDEX idx_packets_channel ON packets(channel_hash, first_heard_at DESC)
  WHERE channel_hash IS NOT NULL;

-- ============================================================
-- PACKET OBSERVATIONS
-- ============================================================

CREATE TABLE packet_observations (
  id                  BIGSERIAL PRIMARY KEY,
  packet_hash         BYTEA NOT NULL REFERENCES packets(packet_hash) ON DELETE CASCADE,
  observer_id         UUID NOT NULL REFERENCES observers(id),
  iata                CHAR(3) NOT NULL REFERENCES iata_codes(iata),
  heard_at            TIMESTAMPTZ NOT NULL,
  path_length_byte    SMALLINT NOT NULL,
  hash_size           SMALLINT NOT NULL,
  hop_count           SMALLINT NOT NULL,
  path_bytes          BYTEA,
  rssi                SMALLINT,
  snr                 REAL,
  propagation_time_ms INT,
  radio_freq_mhz      REAL,
  spread_factor       SMALLINT,
  bandwidth_khz       REAL,
  coding_rate         SMALLINT,
  source_broker       TEXT,
  UNIQUE (packet_hash, observer_id, heard_at)
);

CREATE INDEX idx_observations_heard_brin ON packet_observations USING BRIN (heard_at);
CREATE INDEX idx_observations_iata_heard ON packet_observations(iata, heard_at DESC);
CREATE INDEX idx_observations_observer ON packet_observations(observer_id, heard_at DESC);
CREATE INDEX idx_observations_packet ON packet_observations(packet_hash);

-- ============================================================
-- CHANNELS AND CHAT MESSAGES
-- ============================================================

CREATE TABLE channels (
  id               SERIAL PRIMARY KEY,
  channel_hash     BYTEA NOT NULL,
  -- key_fingerprint is the first 8 bytes of SHA256(key_bytes).
  -- Combined with channel_hash it uniquely identifies a channel,
  -- since a 1-byte hash has high collision probability.
  -- NULL when the key is unknown (hash-only record).
  key_fingerprint  BYTEA,
  name             TEXT,
  hashtag          TEXT UNIQUE,         -- e.g. "meshcore"; only set for hashtag-derived channels
  is_hashtag       BOOLEAN DEFAULT FALSE,
  is_public        BOOLEAN DEFAULT FALSE,
  key_known        BOOLEAN DEFAULT FALSE,
  first_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  message_count    BIGINT DEFAULT 0,
  -- Unique on (hash, fingerprint): allows multiple channels with the same
  -- hash as long as they have different keys. NULL fingerprint is allowed
  -- once per hash (the unknown-key record).
  UNIQUE (channel_hash, key_fingerprint)
);

CREATE UNIQUE INDEX idx_channels_hash_no_key 
ON channels (channel_hash) 
WHERE key_fingerprint IS NULL;

CREATE INDEX idx_channels_last_seen ON channels(last_seen DESC);
CREATE INDEX idx_channels_hash ON channels(channel_hash);
CREATE INDEX idx_channels_hashtag ON channels(hashtag) WHERE hashtag IS NOT NULL;

CREATE TABLE channel_keys (
  channel_id      INT PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
  key_bytes       BYTEA NOT NULL,
  key_fingerprint BYTEA NOT NULL, -- first 8 bytes of SHA256(key_bytes)
  added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  added_by        TEXT
);

CREATE TABLE channel_messages (
  id            BIGSERIAL PRIMARY KEY,
  channel_id    INT NOT NULL REFERENCES channels(id),
  packet_hash   BYTEA NOT NULL REFERENCES packets(packet_hash),
  sender_name   TEXT,
  sender_pubkey BYTEA,
  content       TEXT,
  sent_at       TIMESTAMPTZ NOT NULL,
  UNIQUE (packet_hash)
);

CREATE INDEX idx_channel_messages_channel ON channel_messages(channel_id, sent_at DESC);
CREATE INDEX idx_channel_messages_sent_brin ON channel_messages USING BRIN (sent_at);

-- ============================================================
-- MATERIALIZED VIEWS
-- ============================================================

CREATE MATERIALIZED VIEW mv_hourly_iata_stats AS
SELECT
  iata,
  date_trunc('hour', heard_at)::timestamptz AS hour,
  COUNT(*) AS observation_count,
  COUNT(DISTINCT packet_hash) AS unique_packets,
  COUNT(DISTINCT observer_id) AS active_observers
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '7 days'
GROUP BY iata, date_trunc('hour', heard_at);

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

CREATE UNIQUE INDEX idx_mv_top_nodes
  ON mv_top_nodes_by_iata(iata, node_id);

CREATE UNIQUE INDEX idx_mv_hourly_iata_stats
  ON mv_hourly_iata_stats(iata, hour);

CREATE MATERIALIZED VIEW mv_radio_presets AS
SELECT
    concat(o.radio_freq_mhz, ',', o.radio_bw_khz, ',', o.radio_sf) AS preset,
    (SELECT po.iata FROM packet_observations po WHERE po.observer_id = o.id ORDER BY po.heard_at DESC LIMIT 1) AS iata,
    'observer' AS source_type,
    COUNT(*) AS count
FROM observers o
WHERE o.radio_freq_mhz IS NOT NULL
    AND o.radio_sf IS NOT NULL
    AND o.radio_bw_khz IS NOT NULL
GROUP BY concat(o.radio_freq_mhz, ',', o.radio_bw_khz, ',', o.radio_sf),
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

CREATE UNIQUE INDEX idx_mv_radio_presets
    ON mv_radio_presets(preset, iata, source_type);
