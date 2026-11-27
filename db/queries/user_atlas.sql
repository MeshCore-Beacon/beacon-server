-- name: EnsureUserAtlas :exec
-- Creates the empty Atlas row (revision 0) so a first write has a row to lock.
INSERT INTO user_atlas (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING;

-- name: LockUserAtlas :one
SELECT range, revision FROM user_atlas WHERE user_id = $1 FOR UPDATE;

-- name: ReadUserAtlas :many
-- The header and the pins in one statement, so they come from one snapshot. A user with no Atlas gets no rows.
SELECT a.range, a.revision, p.position, p.kind, p.public_key, p.name, p.tiles, p.view, p.telemetry_tiles
FROM user_atlas a LEFT JOIN user_atlas_pins p ON p.user_id = a.user_id
WHERE a.user_id = $1
ORDER BY p.position;

-- name: ListUserAtlasPins :many
SELECT position, kind, public_key, name, tiles, view, telemetry_tiles FROM user_atlas_pins WHERE user_id = $1 ORDER BY position;

-- name: DeleteUserAtlasPins :exec
DELETE FROM user_atlas_pins WHERE user_id = $1;

-- name: InsertUserAtlasPin :exec
INSERT INTO user_atlas_pins (user_id, position, kind, public_key, name, tiles, view, telemetry_tiles)
VALUES ($1, $2, $3, $4, $5, sqlc.narg(tiles), sqlc.narg(view), sqlc.narg(telemetry_tiles));

-- name: BumpUserAtlas :one
UPDATE user_atlas SET range = $2, revision = revision + 1, updated_at = now() WHERE user_id = $1 RETURNING revision;

-- name: NodeIDsByKeys :many
SELECT id, public_key FROM nodes WHERE public_key = ANY($1::bytea[]);

-- name: ObserverIDsByKeys :many
SELECT id, public_key FROM observers WHERE public_key = ANY($1::bytea[]);

-- name: ObserverKeysByIDs :many
SELECT id, public_key FROM observers WHERE id = ANY($1::uuid[]);
