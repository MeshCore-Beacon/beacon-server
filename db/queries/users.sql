-- name: CreateUser :one
INSERT INTO users (username, display_name, email, pass_hash, provider, provider_subject, email_verified_at)
VALUES (sqlc.arg(username), sqlc.arg(display_name), sqlc.arg(email), sqlc.arg(pass_hash), sqlc.arg(provider),
        sqlc.narg(provider_subject), sqlc.narg(email_verified_at))
RETURNING *;

-- name: UserByID :one
SELECT * FROM users WHERE id = $1;

-- name: UserByUsername :one
SELECT * FROM users WHERE lower(username) = lower($1);

-- name: UserByEmail :one
SELECT * FROM users WHERE email = $1 AND email <> '';

-- name: UpdateUserDisplayName :execrows
UPDATE users SET display_name = $2 WHERE id = $1;

-- name: SetUserPassword :execrows
UPDATE users SET pass_hash = $2 WHERE id = $1;

-- name: SetUserEmailVerified :execrows
UPDATE users SET email_verified_at = $2 WHERE id = $1;

-- name: SetUserPendingEmail :execrows
UPDATE users SET pending_email = sqlc.narg(pending_email) WHERE id = sqlc.arg(id);

-- name: ApplyPendingEmail :execrows
-- Applies only the address the link was issued for ($2, lower case), so a later change of the pending address cannot ride on an old link.
-- The pending address was verified by the link that got us here, so the account counts as verified too.
UPDATE users SET email = pending_email, pending_email = NULL, email_verified_at = now()
WHERE id = $1 AND pending_email = $2;

-- name: ActivateUser :one
-- Completes a sign-up: the username and password hash of the verified sign-up replace the row's, the address counts
-- as verified, and every verify token and session of the account goes, in one statement. No row when the user is
-- already verified, disabled or gone.
WITH activated AS (
    UPDATE users SET username = sqlc.arg(username), display_name = sqlc.arg(username), pass_hash = sqlc.arg(pass_hash),
                     email_verified_at = sqlc.arg(verified_at)
    WHERE users.id = sqlc.arg(user_id) AND email_verified_at IS NULL AND disabled_at IS NULL
    RETURNING users.id
), dropped_tokens AS (
    DELETE FROM user_tokens WHERE kind = 'verify' AND user_id IN (SELECT id FROM activated)
), dropped_sessions AS (
    DELETE FROM user_sessions WHERE user_id IN (SELECT id FROM activated)
)
SELECT id FROM activated;

-- name: TouchUserLogin :execrows
UPDATE users SET last_login_at = $2 WHERE id = $1;

-- name: SetUserDisabled :execrows
UPDATE users SET disabled_at = sqlc.narg(disabled_at) WHERE id = sqlc.arg(id);

-- name: DeleteUser :execrows
DELETE FROM users WHERE id = $1;

-- name: ListUsers :many
-- search is a LIKE pattern with its wildcards already escaped, or empty for everyone. The cursor is the last row of
-- the previous page; the first page passes a far-future time and the max uuid.
SELECT * FROM users
WHERE (sqlc.arg(search)::text = '' OR lower(username) LIKE sqlc.arg(search)::text OR email LIKE sqlc.arg(search)::text)
  AND (created_at, id) < (sqlc.arg(after_created)::timestamptz, sqlc.arg(after_id)::uuid)
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(row_limit);

-- name: CreateUserSession :one
INSERT INTO user_sessions (user_id, token_hash, kind, label, expires_at, hard_expires_at, ip, user_agent)
VALUES (sqlc.arg(user_id), sqlc.arg(token_hash), sqlc.arg(kind), sqlc.arg(label), sqlc.arg(expires_at),
        sqlc.narg(hard_expires_at), sqlc.arg(ip), sqlc.arg(user_agent))
RETURNING id, user_id, kind, label, created_at, last_used_at, expires_at, hard_expires_at, ip, user_agent;

-- name: UserSessionByTokenHash :one
-- A session is live when it is not ended, not past either expiry, and its user is not disabled.
SELECT s.id AS session_id, s.user_id AS session_user_id, s.kind, s.label, s.created_at AS session_created_at,
       s.last_used_at, s.expires_at, s.hard_expires_at, s.ip, s.user_agent,
       u.id, u.username, u.display_name, u.email, u.pending_email, u.pass_hash, u.email_verified_at, u.provider,
       u.provider_subject, u.created_at, u.last_login_at, u.disabled_at
FROM user_sessions s JOIN users u ON u.id = s.user_id
WHERE s.token_hash = sqlc.arg(token_hash)
  AND s.ended_at IS NULL
  AND s.expires_at > sqlc.arg(now)
  AND (s.hard_expires_at IS NULL OR s.hard_expires_at > sqlc.arg(now))
  AND u.disabled_at IS NULL;

-- name: TouchUserSession :execrows
UPDATE user_sessions SET last_used_at = $2, expires_at = $3 WHERE id = $1 AND ended_at IS NULL;

-- name: ListUserSessions :many
SELECT id, user_id, kind, label, created_at, last_used_at, expires_at, hard_expires_at, ip, user_agent
FROM user_sessions
WHERE user_id = sqlc.arg(user_id) AND ended_at IS NULL AND expires_at > sqlc.arg(now)
  AND (hard_expires_at IS NULL OR hard_expires_at > sqlc.arg(now))
ORDER BY created_at DESC, id DESC;

-- name: EndUserSession :exec
UPDATE user_sessions SET ended_at = now() WHERE user_id = $1 AND id = $2 AND ended_at IS NULL;

-- name: EndOtherUserSessions :exec
UPDATE user_sessions SET ended_at = now() WHERE user_id = $1 AND id <> $2 AND ended_at IS NULL;

-- name: EndAllUserSessions :exec
UPDATE user_sessions SET ended_at = now() WHERE user_id = $1 AND ended_at IS NULL;

-- name: EndOldestAppSessions :exec
-- Ends the live app sessions beyond the newest keep.
UPDATE user_sessions SET ended_at = now()
WHERE id IN (
    SELECT s.id FROM user_sessions s
    WHERE s.user_id = sqlc.arg(user_id) AND s.kind = 'app' AND s.ended_at IS NULL AND s.expires_at > now()
      AND (s.hard_expires_at IS NULL OR s.hard_expires_at > now())
    ORDER BY s.created_at DESC, s.id DESC
    OFFSET sqlc.arg(keep)::int
);

-- name: CreateUserToken :exec
INSERT INTO user_tokens (token_hash, user_id, kind, data, expires_at)
VALUES (sqlc.arg(token_hash), sqlc.arg(user_id), sqlc.arg(kind), sqlc.narg(data), sqlc.arg(expires_at));

-- name: ConsumeUserToken :one
-- One statement, so two concurrent uses cannot both match used_at IS NULL.
UPDATE user_tokens SET used_at = sqlc.arg(now)
WHERE token_hash = sqlc.arg(token_hash) AND kind = sqlc.arg(kind) AND used_at IS NULL AND expires_at > sqlc.arg(now)
RETURNING user_id, data;

-- name: PeekUserToken :one
-- Who a live token belongs to, without using it.
SELECT user_id FROM user_tokens
WHERE token_hash = sqlc.arg(token_hash) AND kind = sqlc.arg(kind) AND used_at IS NULL AND expires_at > sqlc.arg(now);

-- name: LatestUserToken :one
-- The data of the user's newest unused token of a kind, even if it has expired (cleanup keeps it for a week), for
-- repeating a mail without minting from anything else.
SELECT data FROM user_tokens
WHERE user_id = sqlc.arg(user_id) AND kind = sqlc.arg(kind) AND used_at IS NULL
ORDER BY created_at DESC, expires_at DESC
LIMIT 1;

-- name: DeleteUserTokens :exec
DELETE FROM user_tokens WHERE user_id = $1 AND kind = $2;

-- name: DeleteExpiredUserSessions :exec
DELETE FROM user_sessions
WHERE ended_at < sqlc.arg(cutoff) OR expires_at < sqlc.arg(cutoff) OR hard_expires_at < sqlc.arg(cutoff);

-- name: DeleteExpiredUserTokens :exec
DELETE FROM user_tokens WHERE used_at < sqlc.arg(cutoff) OR expires_at < sqlc.arg(cutoff);

-- name: DeleteStaleUnverifiedUsers :exec
-- A sign-up nobody confirmed holds its username and address; once it is old and has no live verify link left it
-- is removed so both can be used again. A disabled account is never removed here.
DELETE FROM users u
WHERE u.email_verified_at IS NULL AND u.disabled_at IS NULL AND u.provider = 'local' AND u.created_at < sqlc.arg(cutoff)
  AND NOT EXISTS (
      SELECT 1 FROM user_tokens t
      WHERE t.user_id = u.id AND t.kind = 'verify' AND t.used_at IS NULL AND t.expires_at > sqlc.arg(now)
  );

-- name: ConsumeResetToken :one
-- Spends a live reset token only while the address it was mailed to is still the account's address, in one statement.
UPDATE user_tokens t SET used_at = sqlc.arg(now) FROM users u
WHERE t.token_hash = sqlc.arg(token_hash) AND t.kind = 'reset' AND t.used_at IS NULL AND t.expires_at > sqlc.arg(now)
  AND u.id = t.user_id AND t.data->>'email' = u.email
RETURNING t.user_id;

-- name: LockUserCredentials :one
-- Serializes session issuance and credential changes of one user until the transaction ends. NO KEY UPDATE keeps
-- the foreign-key checks of unrelated inserts (Atlas, requests) from waiting on it.
SELECT pass_hash, disabled_at FROM users WHERE id = $1 FOR NO KEY UPDATE;

-- name: RehashUserPassword :execrows
-- Stores the same password under a newer hash, only while the hash is still the one that was verified.
UPDATE users SET pass_hash = sqlc.arg(new_hash) WHERE id = sqlc.arg(id) AND pass_hash = sqlc.arg(old_hash);

-- name: UserSessionLive :one
SELECT EXISTS (
    SELECT 1 FROM user_sessions
    WHERE id = sqlc.arg(id) AND user_id = sqlc.arg(user_id) AND ended_at IS NULL AND expires_at > now()
      AND (hard_expires_at IS NULL OR hard_expires_at > now())
)::boolean;

-- name: DeleteUserIfHash :execrows
DELETE FROM users WHERE id = sqlc.arg(id) AND pass_hash = sqlc.arg(pass_hash);

-- name: UserByProviderSubject :one
SELECT * FROM users WHERE provider = sqlc.arg(provider) AND provider_subject = sqlc.arg(provider_subject);

-- name: RefreshPortalUser :one
UPDATE users SET username = sqlc.arg(username), display_name = sqlc.arg(display_name)
WHERE id = sqlc.arg(id) RETURNING *;

-- name: CreatePortalState :exec
INSERT INTO portal_states (cookie_hash, state_hash, verifier, return_path, expires_at)
VALUES (sqlc.arg(cookie_hash), sqlc.arg(state_hash), sqlc.arg(verifier), sqlc.arg(return_path), sqlc.arg(expires_at));

-- name: ConsumePortalState :one
-- One statement, so a state is used once; an expired one is deleted and reported as missing by the caller.
DELETE FROM portal_states WHERE cookie_hash = sqlc.arg(cookie_hash)
RETURNING state_hash, verifier, return_path, expires_at;

-- name: DeleteExpiredPortalStates :exec
DELETE FROM portal_states WHERE expires_at < sqlc.arg(cutoff);
