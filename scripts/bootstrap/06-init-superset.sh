#!/usr/bin/env bash
# ==============================================================================
# Script: 06-init-superset.sh
# Purpose: Initialize database schema, admin user, roles, and ClickHouse driver
# ==============================================================================
set -e

echo "======================================================================"
echo "=== [6/6] Initializing Apache Superset                             ==="
echo "======================================================================"

# 1. Wait for PostgreSQL
echo "[1/5] Checking PostgreSQL readiness (postgres:5432)..."
while ! python -c "import socket; s = socket.socket(); s.connect(('postgres', 5432))" 2>/dev/null; do
  echo ">>> Waiting for postgres:5432..."
  sleep 2
done
echo ">>> PostgreSQL is ready!"

# 2. Install Drivers (ClickHouse & PostgreSQL)
echo "[2/5] Installing ClickHouse and PostgreSQL drivers..."
pip install --user --no-cache-dir clickhouse-connect psycopg2-binary

# 3. Database migrations
echo "[3/5] Upgrading Superset metadata database..."
superset db upgrade

# 4. Create admin user
echo "[4/5] Ensuring Admin account exists..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

# 5. Initialize roles and permissions
echo "[5/5] Setting up default roles and permissions..."
superset init

echo "======================================================================"
echo ">>> [SUCCESS] Apache Superset initialization completed successfully!"
echo ">>> Web UI: http://localhost:8089 (User: admin / Password: admin)"
echo "======================================================================"
