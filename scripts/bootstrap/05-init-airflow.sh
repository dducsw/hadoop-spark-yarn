#!/usr/bin/env bash
# ==============================================================================
# Script: 05-init-airflow.sh
# Purpose: Complete bootstrap & initialization for Apache Airflow 3
# ==============================================================================
set -e

echo "======================================================================"
echo "=== [5/5] Bootstrapping Apache Airflow 3 Environment               ==="
echo "======================================================================"

# 1. Wait for PostgreSQL metadata database
echo "[1/4] Checking PostgreSQL readiness (postgres:5432)..."
while ! nc -z postgres 5432 2>/dev/null; do
  echo ">>> Waiting for postgres:5432..."
  sleep 2
done
echo ">>> PostgreSQL metadata DB is ready!"

# 2. Run Airflow database migrations
echo "[2/4] Running Airflow database migrations (airflow db migrate)..."
airflow db migrate

# 3. Configure Airflow Concurrency Pools
echo "[3/4] Initializing Airflow pools..."
airflow pools set spark_yarn_pool 3 "Limit concurrent Spark YARN tasks to avoid cluster starvation" || true

# 4. Create Admin Account
echo "[4/4] Ensuring Admin account exists..."
PASSWORD="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
(airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password "${PASSWORD}" 2>/dev/null || true)

echo "======================================================================"
echo ">>> [SUCCESS] Airflow 3 initialization completed successfully!"
echo ">>> Web UI: http://localhost:8080 (User: admin / Password: admin)"
echo "======================================================================"
