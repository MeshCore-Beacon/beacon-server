-- Scope reports remain separate from deduplicated packet/path/receiving-area/hour samples.
ALTER TABLE node_scope_hourly ADD COLUMN unique_samples bigint NOT NULL DEFAULT 0 CHECK (unique_samples >= 0);
ALTER TABLE node_scope_hourly ADD COLUMN unique_first_seen timestamptz;

-- Only the current and previous hour are needed to deduplicate live ingestion.
-- Aggregate counters survive this bounded set's expiry; no raw payload is stored.
CREATE TABLE scope_seen_paths (
    hour timestamptz NOT NULL,
    iata text NOT NULL CHECK (length(iata) = 3),
    scope_name text NOT NULL CHECK (length(scope_name) BETWEEN 1 AND 128),
    path_key bytea NOT NULL CHECK (octet_length(path_key) = 32),
    PRIMARY KEY (hour, iata, scope_name, path_key)
);
CREATE TABLE scope_receiver_hourly (
    hour timestamptz NOT NULL,
    iata text NOT NULL CHECK (length(iata) = 3),
    scope_name text NOT NULL CHECK (length(scope_name) BETWEEN 1 AND 128),
    observer_id uuid NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
    reports bigint NOT NULL CHECK (reports > 0),
    unique_samples bigint NOT NULL CHECK (unique_samples >= 0),
    unresolved_hops bigint NOT NULL CHECK (unresolved_hops >= 0),
    ambiguous_hops bigint NOT NULL CHECK (ambiguous_hops >= 0),
    first_seen timestamptz NOT NULL,
    last_seen timestamptz NOT NULL,
    PRIMARY KEY (hour, iata, scope_name, observer_id)
);
