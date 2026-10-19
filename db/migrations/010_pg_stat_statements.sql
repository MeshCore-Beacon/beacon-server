-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Statement statistics for GET /api/v1/admin/db/statements. Skipped with a notice where the
-- extension is not installed or this role may not create it; the endpoint then answers 503.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements') THEN
    BEGIN
      CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    EXCEPTION WHEN insufficient_privilege THEN
      RAISE NOTICE 'pg_stat_statements not created: %', SQLERRM;
    END;
  ELSE
    RAISE NOTICE 'pg_stat_statements is not available on this server';
  END IF;
END
$$;
