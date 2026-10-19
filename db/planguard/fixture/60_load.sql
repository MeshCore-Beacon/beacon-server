-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 60: insert the staged channels, packets, observations and messages once each, in time order, then
-- drop the staging tables. Every row references rows built in this build, so where the role may, the foreign-key
-- triggers are switched off for the load (session_replication_role = replica) and on again after it.
DO $$ BEGIN SET LOCAL session_replication_role = replica; EXCEPTION WHEN insufficient_privilege THEN NULL; END $$;
INSERT INTO channels (id, channel_hash, key_fingerprint, name, hashtag, is_hashtag, is_public, key_known, first_seen, last_seen, message_count)
SELECT ch.id, ch.channel_hash, ch.key_fingerprint, ch.name, ch.hashtag, ch.is_hashtag, ch.is_public, ch.key_known, ch.first_seen,
       ch.last_seen, coalesce(m.n, 0)
FROM planguard_src.ch LEFT JOIN (SELECT channel_id, count(*) n FROM planguard_src.cm GROUP BY 1) m ON m.channel_id = ch.id
ORDER BY ch.id;
SELECT setval('channels_id_seq', (SELECT max(id) FROM channels));

INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, transport_codes_present, scope_id, origin_pubkey,
                     raw_payload, raw_header, parsed_payload, decrypted, channel_hash, trace_tag, first_heard_at, last_heard_at,
                     observation_count)
SELECT packet_hash, payload_type, payload_version, route_type, transport_codes_present, scope_id, origin_pubkey, raw_payload,
       '\x01', NULL, false, channel_hash, trace_tag, first_heard_at, last_heard_at, observation_count
FROM planguard_src.pk ORDER BY last_heard_at, packet_hash;

INSERT INTO packet_observations (id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes,
                                 rssi, snr, payload_type)
SELECT id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes, rssi, snr, payload_type
FROM planguard_src.po ORDER BY id;
SELECT setval('packet_observations_id_seq', (SELECT max(id) FROM packet_observations));

INSERT INTO channel_messages (id, channel_id, packet_hash, sender_name, content, sent_at)
SELECT id, channel_id, packet_hash, sender_name, 'message ' || id, sent_at FROM planguard_src.cm ORDER BY id;
SELECT setval('channel_messages_id_seq', (SELECT max(id) FROM channel_messages));
DO $$ BEGIN SET LOCAL session_replication_role = origin; EXCEPTION WHEN insufficient_privilege THEN NULL; END $$;

DROP TABLE planguard_src.pk0, planguard_src.pk, planguard_src.po, planguard_src.cm, planguard_src.pk_sites, planguard_src.kr0,
  planguard_src.kr1;
