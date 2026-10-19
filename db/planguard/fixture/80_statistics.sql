-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 80: ANALYZE samples 300 x the largest statistics target of a table's columns, at random, so on a
-- table above 60,000 rows two builds would get different statistics and could plan differently. One column per
-- such table that no statement filters, joins or sorts on gets a target large enough to cover the table, so
-- ANALYZE reads every row; every other column keeps default_statistics_target = 200, and its statistics come out
-- the same on every build.
ALTER TABLE known_routes ALTER COLUMN path_key SET STATISTICS 10000;
-- The reference copy's known_routes.iata statistics list every region it holds as a most common value, so a
-- region they miss (a new one, or one loaded after ANALYZE) is estimated at one row. 200 entries cannot hold the
-- fixture's 400-odd regions; 1000 can, which keeps that behaviour.
ALTER TABLE known_routes ALTER COLUMN iata SET STATISTICS 1000;
ALTER TABLE packets ALTER COLUMN raw_payload SET STATISTICS 10000;
ALTER TABLE packet_observations ALTER COLUMN airtime_ms SET STATISTICS 10000;
ALTER TABLE analytics_hourly_iata_observations ALTER COLUMN observation_count SET STATISTICS 10000;
ALTER TABLE analytics_hourly_packet_sets ALTER COLUMN packets SET STATISTICS 10000;
ALTER TABLE analytics_hourly_observer_identity ALTER COLUMN display_name SET STATISTICS 10000;
ALTER TABLE analytics_hourly_paths ALTER COLUMN receptions SET STATISTICS 10000;
ALTER TABLE analytics_hourly_signal ALTER COLUMN rssi_sum SET STATISTICS 10000;
ALTER TABLE analytics_hourly_advert_hearings ALTER COLUMN name SET STATISTICS 10000;
ALTER TABLE analytics_hourly_advert_sets ALTER COLUMN direct_packets SET STATISTICS 10000;
ALTER TABLE analytics_hourly_scope_sets ALTER COLUMN packets SET STATISTICS 10000;

-- Fail the build when a table outgrows that: more than 3,000,000 rows (10000 x 300), or above 60,000 rows with
-- no such column.
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT c.relname, (xpath('/row/n/text()', query_to_xml(format('SELECT count(*) AS n FROM public.%I', c.relname), false, true, '')))[1]::text::bigint AS n,
                  (SELECT max(a.attstattarget) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS tgt
           FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' LOOP
    IF r.n > 300 * greatest(r.tgt, 200) THEN
      RAISE EXCEPTION 'fixture: % holds % rows, more than ANALYZE reads at its largest statistics target %; give it a full-sample column in 80_statistics.sql',
        r.relname, r.n, greatest(r.tgt, 200);
    END IF;
  END LOOP;
END $$;
