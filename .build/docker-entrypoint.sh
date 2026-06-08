#!/bin/sh
# Copyright 2026 Beacon Contributors
# SPDX-License-Identifier: agpl

set -e

if [ -n "$POSTGRES_DSN" ]; then
  echo "Running database migrations..."

  psql "$POSTGRES_DSN" -c "CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );"

  for f in /app/migrations/*.sql; do
    name=$(basename "$f")
    already=$(psql "$POSTGRES_DSN" -tAc "SELECT 1 FROM schema_migrations WHERE filename = '$name'")
    if [ "$already" != "1" ]; then
      echo "  Applying $name..."
      psql "$POSTGRES_DSN" -f "$f"
      psql "$POSTGRES_DSN" -c "INSERT INTO schema_migrations (filename) VALUES ('$name');"
    else
      echo "  Skipping $name (already applied)"
    fi
  done

  echo "Migrations complete."
fi

exec ./beacon "$@"
