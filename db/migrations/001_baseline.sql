-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- 2.0.0 schema baseline: pg_dump of a fresh database migrated through the 1.x
-- chain (001_initial_schema..045). Add new numbered migrations after this file.

CREATE TABLE accounts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    deactivated_at timestamp with time zone,
    CONSTRAINT accounts_name_check CHECK ((((char_length(name) >= 1) AND (char_length(name) <= 128)) AND (name = btrim(name))))
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
    last_heard_at timestamp with time zone NOT NULL,
    observation_count bigint DEFAULT 0 NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_vacuum_cost_limit='1000');

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
    region_scope text,
    last_iata character(3),
    last_iata_at timestamp with time zone
);

CREATE TABLE channel_messages (
    id bigint NOT NULL,
    channel_id integer NOT NULL,
    packet_hash bytea NOT NULL,
    sender_name text,
    sender_pubkey bytea,
    content text,
    sent_at timestamp with time zone NOT NULL
);

CREATE TABLE channel_config_scopes (
    key_fingerprint bytea NOT NULL,
    region_slug text
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
    message_count bigint DEFAULT 0 NOT NULL
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

CREATE TABLE meshmapper_channel_catalogues (
    iata text NOT NULL,
    url text NOT NULL,
    payload jsonb,
    etag text,
    checked_at timestamp with time zone,
    attempted_at timestamp with time zone NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    last_error text DEFAULT ''::text NOT NULL
);

CREATE TABLE meshmapper_channel_members (
    iata text NOT NULL,
    key_fingerprint bytea NOT NULL
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

CREATE TABLE meshmapper_zone_lists (
    country text NOT NULL,
    payload jsonb,
    etag text,
    fetched_at timestamp with time zone,
    attempted_at timestamp with time zone NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    last_error text DEFAULT ''::text NOT NULL
);

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
    o.last_iata AS iata,
    'observer'::text AS source_type,
    COUNT(*) AS count
FROM observers o
WHERE o.radio_freq_mhz IS NOT NULL
    AND o.radio_sf IS NOT NULL
    AND o.radio_bw_khz IS NOT NULL
    AND o.last_iata IS NOT NULL
GROUP BY 1, 2

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
GROUP BY 1, 2;

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
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    imported boolean DEFAULT false NOT NULL
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

CREATE TABLE trace_tags (
    trace_tag bytea PRIMARY KEY,
    first_heard_at timestamp with time zone NOT NULL,
    last_heard_at timestamp with time zone NOT NULL,
    packet_count bigint DEFAULT 0 NOT NULL,
    trace_type text,
    scope_id integer,
    best_payload jsonb,
    best_path_len integer DEFAULT -1 NOT NULL,
    heard boolean DEFAULT false NOT NULL -- false: times are the packet's provisional ones until a hearing is recorded
);

-- Canonical IATA set: sorted, unique, non-null, non-empty.
CREATE FUNCTION iata_set_is_canonical(s bpchar[]) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT cardinality(s) > 0
       AND array_position(s, NULL) IS NULL
       AND s = (SELECT array_agg(x ORDER BY x) FROM (SELECT DISTINCT unnest(s) AS x) d)
$$;

-- Hourly rollups. hour = UTC date_trunc('hour', heard_at). No FKs: history outlives deletions.
CREATE TABLE analytics_hourly_iata_observations (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    observation_count bigint NOT NULL,
    PRIMARY KEY (hour, iata)
);

CREATE TABLE analytics_hourly_payload_breakdown (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    payload_type smallint NOT NULL,
    count bigint NOT NULL,
    PRIMARY KEY (hour, iata, payload_type)
);

CREATE TABLE analytics_hourly_signal (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    kind integer NOT NULL,
    snr_bin integer NOT NULL,
    rssi_bin integer NOT NULL,
    receptions bigint NOT NULL,
    snr_samples bigint NOT NULL,
    snr_sum double precision NOT NULL,
    rssi_samples bigint NOT NULL,
    rssi_sum double precision NOT NULL,
    PRIMARY KEY (hour, iata, kind, snr_bin, rssi_bin)
);

CREATE TABLE analytics_hourly_paths (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    category integer NOT NULL,
    hash_bytes integer NOT NULL,
    entries integer NOT NULL,
    receptions bigint NOT NULL,
    PRIMARY KEY (hour, iata, category, hash_bytes, entries)
);

CREATE TABLE analytics_hourly_observer_activity (
    hour timestamp with time zone NOT NULL,
    observer_id uuid NOT NULL,
    payload_type smallint NOT NULL,
    observations bigint NOT NULL,
    airtime_ms real,
    airtime_n bigint NOT NULL,
    snr_sum real,
    snr_n bigint NOT NULL,
    snr_min real,
    rssi_sum bigint,
    rssi_n bigint NOT NULL,
    PRIMARY KEY (hour, observer_id, payload_type)
);

CREATE TABLE analytics_hourly_observer_identity (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    observer_id uuid NOT NULL,
    observation_count bigint NOT NULL,
    display_name text,
    observer_type text,
    PRIMARY KEY (hour, iata, observer_id)
);

-- ADVERT hearings per origin per IATA: top nodes (observations) and advertiser last_heard.
CREATE TABLE analytics_hourly_advert_hearings (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    origin_pubkey bytea NOT NULL,
    observations bigint NOT NULL,
    last_heard timestamp with time zone NOT NULL,
    name text,
    node_type smallint,
    PRIMARY KEY (hour, iata, origin_pubkey)
);

-- Distinct ADVERT packets per origin, grouped by the set of IATAs that heard each packet that hour.
CREATE TABLE analytics_hourly_advert_sets (
    hour timestamp with time zone NOT NULL,
    origin_pubkey bytea NOT NULL,
    iatas character(3)[] NOT NULL CHECK (iata_set_is_canonical(iatas)),
    advert_packets bigint NOT NULL,
    flood_packets bigint NOT NULL,
    direct_packets bigint NOT NULL,
    PRIMARY KEY (hour, origin_pubkey, iatas)
);

-- Distinct channel messages per sender, grouped by the set of IATAs that heard each message that hour.
CREATE TABLE analytics_hourly_talker_sets (
    hour timestamp with time zone NOT NULL,
    sender_name text NOT NULL,
    iatas character(3)[] NOT NULL CHECK (iata_set_is_canonical(iatas)),
    messages bigint NOT NULL,
    last_sent timestamp with time zone NOT NULL,
    PRIMARY KEY (hour, sender_name, iatas)
);

CREATE TABLE analytics_hourly_packet_sets (
    hour timestamp with time zone NOT NULL,
    iatas character(3)[] NOT NULL CHECK (iata_set_is_canonical(iatas)),
    packets bigint NOT NULL,
    PRIMARY KEY (hour, iatas)
);

CREATE TABLE analytics_hourly_scope_sets (
    hour timestamp with time zone NOT NULL,
    iatas character(3)[] NOT NULL CHECK (iata_set_is_canonical(iatas)),
    scope_id integer NOT NULL,
    packets bigint NOT NULL,
    PRIMARY KEY (hour, iatas, scope_id)
);

-- Observers that heard a scoped packet, per IATA; distinct counts span any IATA filter.
CREATE TABLE analytics_hourly_scope_observers (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    scope_id integer NOT NULL,
    observer_id uuid NOT NULL,
    PRIMARY KEY (hour, scope_id, iata, observer_id)
);

-- Nodes whose ADVERTs were heard in a scope, per IATA.
CREATE TABLE analytics_hourly_scope_nodes (
    hour timestamp with time zone NOT NULL,
    iata character(3) NOT NULL,
    scope_id integer NOT NULL,
    origin_pubkey bytea NOT NULL,
    PRIMARY KEY (hour, scope_id, iata, origin_pubkey)
);

CREATE TABLE analytics_rollup_hours (
    hour timestamp with time zone PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('complete', 'partial', 'missing')),
    rolled_at timestamp with time zone,
    content_hash text
);

-- Singletons: the rollup writes revision, cleanup writes raw_deleted_before.
CREATE TABLE analytics_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL DEFAULT 0
);

INSERT INTO analytics_state DEFAULT VALUES;

-- coverage counts retention deletions of rollup hours; cache keys combine it with revision.
CREATE TABLE analytics_raw_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    raw_deleted_before timestamp with time zone,
    coverage bigint NOT NULL DEFAULT 0
);

INSERT INTO analytics_raw_state DEFAULT VALUES;

CREATE TABLE analytics_dirty_hours (
    hour timestamp with time zone PRIMARY KEY,
    enqueued_at timestamp with time zone NOT NULL DEFAULT now()
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

ALTER TABLE ONLY meshmapper_channel_catalogues
    ADD CONSTRAINT meshmapper_channel_catalogues_pkey PRIMARY KEY (iata, url);

ALTER TABLE ONLY meshmapper_channel_members
    ADD CONSTRAINT meshmapper_channel_members_pkey PRIMARY KEY (iata, key_fingerprint);

ALTER TABLE ONLY meshmapper_scope_catalogues
    ADD CONSTRAINT meshmapper_scope_catalogues_pkey PRIMARY KEY (iata, url);

ALTER TABLE ONLY meshmapper_zone_boundaries
    ADD CONSTRAINT meshmapper_zone_boundaries_pkey PRIMARY KEY (iata);

ALTER TABLE ONLY meshmapper_zone_lists
    ADD CONSTRAINT meshmapper_zone_lists_pkey PRIMARY KEY (country);

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

CREATE INDEX idx_channel_iatas_iata ON channel_iatas USING btree (iata);

CREATE INDEX idx_channel_messages_channel ON channel_messages USING btree (channel_id, sent_at DESC);

CREATE INDEX idx_channel_messages_sent_brin ON channel_messages USING brin (sent_at);

CREATE INDEX idx_channel_config_scopes_fingerprint ON channel_config_scopes USING btree (key_fingerprint);

CREATE INDEX idx_meshmapper_channel_members_fingerprint ON meshmapper_channel_members USING btree (key_fingerprint);

CREATE INDEX idx_channels_hash ON channels USING btree (channel_hash);

CREATE UNIQUE INDEX idx_channels_hash_no_key ON channels USING btree (channel_hash) WHERE (key_fingerprint IS NULL);

CREATE INDEX idx_channels_hashtag ON channels USING btree (hashtag) WHERE (hashtag IS NOT NULL);

CREATE INDEX idx_channels_last_seen_id ON channels USING btree (last_seen DESC, id DESC);

CREATE INDEX idx_known_routes_hop_count ON known_routes USING btree (iata, hop_count);

CREATE INDEX idx_known_routes_iata_last_seen ON known_routes USING btree (iata, last_seen DESC);

CREATE INDEX idx_known_routes_iata_keyset ON known_routes USING btree (iata, date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC);

CREATE INDEX idx_known_routes_keyset ON known_routes USING btree (date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC);

CREATE INDEX idx_known_routes_last_seen ON known_routes USING btree (last_seen DESC);

CREATE INDEX idx_known_routes_reconfirm ON known_routes USING btree (last_reconfirmed_at);

CREATE UNIQUE INDEX idx_mv_radio_presets ON mv_radio_presets USING btree (preset, iata, source_type);

CREATE INDEX idx_node_iatas_iata ON node_iatas USING btree (iata, last_heard DESC);

CREATE INDEX idx_node_neighbors_neighbor ON node_neighbors USING btree (neighbor_id, iata);

CREATE INDEX idx_node_neighbors_node ON node_neighbors USING btree (node_id, iata);

CREATE INDEX idx_nodes_location ON nodes USING btree (latitude, longitude) WHERE ((latitude IS NOT NULL) AND (longitude IS NOT NULL));

CREATE INDEX idx_nodes_min_firmware ON nodes USING btree (min_firmware_version) WHERE (min_firmware_version IS NOT NULL);

CREATE INDEX idx_nodes_multibyte_paths ON nodes USING btree (supports_multibyte_paths) WHERE supports_multibyte_paths;

CREATE INDEX idx_nodes_multibyte_traces ON nodes USING btree (supports_multibyte_traces) WHERE supports_multibyte_traces;

CREATE INDEX idx_nodes_pubkey ON nodes USING btree (public_key);

CREATE INDEX idx_nodes_type_last_seen ON nodes USING btree (node_type, last_seen DESC);

CREATE INDEX idx_observations_heard ON packet_observations USING btree (heard_at);

CREATE INDEX idx_observations_iata_heard ON packet_observations USING btree (iata, heard_at DESC);

CREATE INDEX idx_observations_observer ON packet_observations USING btree (observer_id, heard_at DESC);

CREATE INDEX idx_observations_observer_id ON packet_observations USING btree (observer_id, id);

CREATE INDEX idx_observations_packet ON packet_observations USING btree (packet_hash);

CREATE INDEX idx_observations_route_evidence ON packet_observations USING btree (iata, hash_size, decode(md5(path_bytes), 'hex'::text), heard_at DESC, id DESC) WHERE ((path_bytes IS NOT NULL) AND (payload_type IS NOT NULL) AND (payload_type <> 9) AND (hop_count >= 2));

CREATE INDEX idx_observer_locations_recent ON observer_locations USING btree (observer_id, reported_at DESC);

CREATE INDEX idx_observer_owners_node ON observer_owners USING btree (owner_node_id) WHERE (owner_node_id IS NOT NULL);

CREATE INDEX idx_observers_last_iata ON observers USING btree (last_iata) WHERE (last_iata IS NOT NULL);

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

CREATE INDEX idx_trace_tags_last_heard ON trace_tags USING btree (last_heard_at DESC);

CREATE INDEX idx_trace_tags_keyset ON trace_tags USING btree (date_trunc('milliseconds', last_heard_at, 'UTC') DESC, trace_tag DESC);

CREATE INDEX idx_trace_tags_first_heard ON trace_tags USING btree (first_heard_at);

CREATE INDEX idx_analytics_observer_activity_observer ON analytics_hourly_observer_activity USING btree (observer_id, hour);

CREATE INDEX idx_analytics_packet_sets_iatas ON analytics_hourly_packet_sets USING gin (iatas);

CREATE INDEX idx_analytics_scope_sets_iatas ON analytics_hourly_scope_sets USING gin (iatas);

CREATE INDEX idx_analytics_scope_sets_scope ON analytics_hourly_scope_sets USING btree (scope_id, hour);

CREATE INDEX idx_analytics_advert_sets_iatas ON analytics_hourly_advert_sets USING gin (iatas);

CREATE INDEX idx_analytics_talker_sets_iatas ON analytics_hourly_talker_sets USING gin (iatas);

CREATE INDEX idx_analytics_rollup_hours_missing ON analytics_rollup_hours USING btree (hour) WHERE (status = 'missing'::text);

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
