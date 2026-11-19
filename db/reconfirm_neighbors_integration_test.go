// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "testing"

func TestReconfirmNeighborsKeepsOnlyUniqueRegionalIdentitiesPostgres(t *testing.T) {
	ctx, pool, store := reconfirmPool(t)
	_, err := pool.Exec(ctx, `
CREATE TABLE node_neighbors (LIKE public.node_neighbors INCLUDING ALL);
TRUNCATE node_short_ids;
INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT md5(name)::uuid, region, decode(prefix, 'hex') FROM (VALUES
 ('unique','SEA','11223344'), ('same-three','SEA','11223355'), ('other-region','YOW','11223344'),
 ('collision-a','SEA','55667788'), ('collision-b','SEA','55667788')
) AS ids(name,region,prefix);
INSERT INTO node_neighbors (node_id,neighbor_id,iata)
SELECT md5('observer')::uuid,md5(name)::uuid,region FROM (VALUES
 ('unique','SEA'), ('same-three','SEA'), ('other-region','YOW'),
 ('other-region','SEA'), ('missing','SEA'), ('collision-a','SEA'), ('collision-b','SEA')
) AS edges(name,region);`)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReconfirmNeighbors(ctx); err != nil {
		t.Fatal(err)
	}
	var kept, expected int
	err = pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE
		(iata='SEA' AND neighbor_id IN (md5('unique')::uuid,md5('same-three')::uuid))
		OR (iata='YOW' AND neighbor_id=md5('other-region')::uuid))
		FROM node_neighbors`).Scan(&kept, &expected)
	if err != nil || kept != 3 || expected != 3 {
		t.Fatalf("kept %d edges, %d expected regional matches, err %v", kept, expected, err)
	}
}
