-- name: EnsureUserAtlas :exec
-- Creates the empty Atlas row (revision 0) so a first write has a row to lock.
INSERT INTO user_atlas (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING;

-- name: LockUserAtlas :one
SELECT range, revision FROM user_atlas WHERE user_id = $1 FOR UPDATE;

-- name: GetUserAtlas :one
SELECT range, revision FROM user_atlas WHERE user_id = $1;

-- name: ListUserAtlasPins :many
SELECT position, kind, public_key, name, tiles, view FROM user_atlas_pins WHERE user_id = $1 ORDER BY position;

-- name: DeleteUserAtlasPins :exec
DELETE FROM user_atlas_pins WHERE user_id = $1;

-- name: InsertUserAtlasPin :exec
INSERT INTO user_atlas_pins (user_id, position, kind, public_key, name, tiles, view)
VALUES ($1, $2, $3, $4, $5, sqlc.narg(tiles), sqlc.narg(view));

-- name: BumpUserAtlas :one
UPDATE user_atlas SET range = $2, revision = revision + 1, updated_at = now() WHERE user_id = $1 RETURNING revision;

-- name: NodeIDsByKeys :many
SELECT id, public_key FROM nodes WHERE public_key = ANY($1::bytea[]);

-- name: ObserverIDsByKeys :many
SELECT id, public_key FROM observers WHERE public_key = ANY($1::bytea[]);

-- name: ObserverKeysByIDs :many
SELECT id, public_key FROM observers WHERE id = ANY($1::uuid[]);
