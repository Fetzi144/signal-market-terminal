#!/bin/bash
set -e

export PYTHONPATH=/app:$PYTHONPATH

run_migrations="${RUN_MIGRATIONS:-true}"
case "${run_migrations,,}" in
  true|1|yes|on)
    echo "Running database migrations..."
    alembic upgrade head
    ;;
  *)
    echo "Skipping database migrations (RUN_MIGRATIONS=${RUN_MIGRATIONS:-false})"
    ;;
esac

echo "Starting application..."
exec "$@"
