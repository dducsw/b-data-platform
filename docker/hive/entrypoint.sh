#!/bin/bash

set -e

echo "Starting Hive Metastore Service"

# Wait for PostgreSQL to be ready
until nc -z postgres 5432; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

echo "PostgreSQL is ready, starting Hive Metastore"

exec $HIVE_HOME/bin/hive --service metastore