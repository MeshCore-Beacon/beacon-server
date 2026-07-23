-- Routes list orders by last_seen; index it (was seq-scanning ~150k rows per page).

CREATE INDEX idx_known_routes_last_seen ON known_routes(last_seen DESC);
