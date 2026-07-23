-- Scope stats count off scope_id, which had no index.

CREATE INDEX idx_packets_scope ON packets(scope_id) WHERE scope_id IS NOT NULL;
