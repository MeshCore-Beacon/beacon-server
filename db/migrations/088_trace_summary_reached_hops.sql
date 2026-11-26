-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The trailer lists the requested route. Each consumed hop contributes one SNR byte.
CREATE FUNCTION trace_reached_hops(payload jsonb) RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN jsonb_typeof(payload) IS DISTINCT FROM 'object' THEN -1 ELSE LEAST(
        CASE WHEN jsonb_typeof(payload->'pathHashes') = 'array'
             THEN jsonb_array_length(payload->'pathHashes') ELSE 0 END,
        CASE WHEN jsonb_typeof(payload->'snrValues') = 'array'
             THEN jsonb_array_length(payload->'snrValues') ELSE 0 END
    ) END
$$;

-- Older ingest binaries supply the requested length explicitly. Normalize it
-- before ON CONFLICT compares the incoming score with the saved observation.
CREATE FUNCTION set_trace_reached_hops() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.best_path_len := trace_reached_hops(NEW.best_payload);
    RETURN NEW;
END;
$$;

CREATE TRIGGER score_trace_observation BEFORE INSERT ON trace_tags
FOR EACH ROW WHEN (NEW.best_payload IS NOT NULL) EXECUTE FUNCTION set_trace_reached_hops();

-- The summary may outlive its packets. Keep its saved observation on an equal score,
-- or replace it with a retained packet that reached more hops.
WITH candidates AS (
    SELECT trace_tag, best_payload AS payload, trace_reached_hops(best_payload) AS reached,
           true AS saved, NULL::bytea AS packet_hash
    FROM trace_tags
    UNION ALL
    SELECT trace_tag, parsed_payload, trace_reached_hops(parsed_payload), false, packet_hash
    FROM packets WHERE trace_tag IS NOT NULL
), best AS (
    SELECT DISTINCT ON (trace_tag) trace_tag, payload, reached FROM candidates
    ORDER BY trace_tag, reached DESC, saved DESC, packet_hash
)
UPDATE trace_tags t SET best_payload = best.payload, best_path_len = best.reached
FROM best WHERE t.trace_tag = best.trace_tag
  AND (t.best_path_len <> best.reached OR t.best_payload IS DISTINCT FROM best.payload);
