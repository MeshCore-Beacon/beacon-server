-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 50: channel messages at 1/20, staged in planguard_src.cm. A message is a channel packet; its
-- channel is drawn by volume (cm_channel) among the channels whose home site is one of the packet's sites, with
-- the testdata's home share, else among all channels. The packet then carries its channel's hash.
SELECT setseed(0.46);

SELECT planguard_src.mkcum('planguard_src.cm_channel', 'rank', 'n', 'c_cm_channel');
CREATE TABLE planguard_src.site_channels AS
SELECT home_iata AS iata, rank,
       sum(n) OVER (PARTITION BY home_iata ORDER BY rank ROWS UNBOUNDED PRECEDING)::float8 / sum(n) OVER (PARTITION BY home_iata) AS hi
FROM planguard_src.cm_channel WHERE home_iata IS NOT NULL AND n > 0;
CREATE INDEX ON planguard_src.site_channels (iata, hi);

CREATE TABLE planguard_src.cm0 AS
SELECT p.packet_hash, p.first_heard_at, s.sites, random() AS r_home, random() AS r_ch, random() AS r_site
FROM planguard_src.pk p JOIN planguard_src.pk_sites s ON s.packet_hash = p.packet_hash
WHERE p.channel_hash IS NOT NULL
ORDER BY md5(p.packet_hash::text)
LIMIT (SELECT round(sum(n) / 20.0) FROM planguard_src.cm_channel);

CREATE TABLE planguard_src.cm AS
SELECT 1000 + row_number() OVER (ORDER BY c.first_heard_at, c.packet_hash) AS id,
       coalesce(sc.rank, (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_cm_channel)[width_bucket(c.r_ch, (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_cm_channel))]) AS channel_id,
       c.packet_hash, 'user_' || floor(random() * 500)::int AS sender_name, c.first_heard_at - random() * interval '5 s' AS sent_at
FROM planguard_src.cm0 c
LEFT JOIN LATERAL (
  SELECT so.rank FROM planguard_src.site_channels so
  WHERE c.r_home < (SELECT sum(home_n)::float8 / sum(n) FROM planguard_src.cm_channel WHERE home_n IS NOT NULL)
    AND so.iata = c.sites[1 + floor(c.r_site * cardinality(c.sites))::int] AND so.hi > c.r_ch
  ORDER BY so.hi LIMIT 1) sc ON true;
DROP TABLE planguard_src.cm0;

UPDATE planguard_src.pk p SET channel_hash = ch.channel_hash
FROM planguard_src.cm m JOIN planguard_src.ch ch ON ch.id = m.channel_id
WHERE m.packet_hash = p.packet_hash;
