-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Portal users have no email address, so the address is unique only where there is one.
DROP INDEX idx_users_email;
CREATE UNIQUE INDEX idx_users_email ON users USING btree (email) WHERE email <> '';

-- A portal sign-in under way: the browser holds the cookie, the portal returns the state. The verifier is needed
-- in the clear for the token call; the rows live ten minutes and the callback deletes them.
CREATE TABLE portal_states (
    cookie_hash bytea PRIMARY KEY,
    state_hash bytea NOT NULL,
    verifier text NOT NULL,
    return_path text NOT NULL,
    expires_at timestamp with time zone NOT NULL
);
CREATE INDEX idx_portal_states_expires ON portal_states USING btree (expires_at);
