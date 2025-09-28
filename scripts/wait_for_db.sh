#!/usr/bin/env bash
set -e
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  echo "Waiting for Postgres in $POSTGRES_HOST:$POSTGRES_PORT..."
  sleep 2
done
exec "$@"