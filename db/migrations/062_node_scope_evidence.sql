-- MQTT scope lists and observed repeat activity are distinct from advert defaults.
-- Existing reports have no reliable scope timestamp: leave their new date NULL.
ALTER TABLE node_neighbors ADD COLUMN region_scope_checked_at timestamptz;
ALTER TABLE observers ADD COLUMN region_scope_checked_at timestamptz;

CREATE TABLE node_scope_hourly (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    scope_name text NOT NULL CHECK (length(scope_name) BETWEEN 1 AND 128),
    hour timestamptz NOT NULL,
    observations bigint NOT NULL CHECK (observations > 0),
    first_seen timestamptz NOT NULL,
    last_seen timestamptz NOT NULL,
    min_hash_bytes smallint NOT NULL CHECK (min_hash_bytes BETWEEN 1 AND 3),
    PRIMARY KEY (node_id, scope_name, hour)
);
CREATE INDEX node_scope_hourly_retention ON node_scope_hourly(hour);
