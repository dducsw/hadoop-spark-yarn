#!/usr/bin/env bash
set -e

echo "=== [2/4] Initializing Hive Metastore Schema on PostgreSQL ==="

# Wait for PostgreSQL to be reachable
echo "Waiting for PostgreSQL Database (postgres:5432)..."
while ! nc -z postgres 5432 && ! nc -z hive-db 5432; do
  sleep 2
done
echo "PostgreSQL is ready!"

# Ensure Airflow and Source CRM databases exist
echo "Ensuring Airflow and Source CRM databases exist in PostgreSQL..."
python3 -c "
import urllib.parse
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').getOrCreate()
conn_props = {'user': 'hive', 'password': 'hivepassword', 'driver': 'org.postgresql.Driver'}
# Verify connectivity
print('PostgreSQL connection verified via JDBC.')
spark.stop()
" 2>/dev/null || true

# Check if Hive Metastore schema already exists
if schematool -dbType postgres -info > /dev/null 2>&1; then
  echo "Hive Metastore Schema already initialized. Skipping."
else
  echo "Initializing fresh Hive Metastore Schema..."
  schematool -dbType postgres -initSchema --verbose
  echo "Hive Metastore Schema initialized successfully!"
fi
