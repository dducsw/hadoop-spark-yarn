#!/usr/bin/env bash
set -e

echo "=== [2/4] Initializing Hive Metastore Schema on PostgreSQL ==="

# Wait for PostgreSQL to be reachable
echo "Waiting for Hive Database (hive-db:5432)..."
while ! nc -z hive-db 5432; do
  sleep 2
done
echo "PostgreSQL is ready!"

# Check if schema already exists
if schematool -dbType postgres -info > /dev/null 2>&1; then
  echo "Hive Metastore Schema already initialized. Skipping."
else
  echo "Initializing fresh Hive Metastore Schema..."
  schematool -dbType postgres -initSchema --verbose
  echo "Hive Metastore Schema initialized successfully!"
fi
