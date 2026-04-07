#!/bin/bash
set -e

export PYTHONPATH=/app:$PYTHONPATH

echo "Running database migrations..."
alembic upgrade head

echo "Starting application..."
exec "$@"
