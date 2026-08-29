#!/usr/bin/env bash
# ***
# Script: sync_hdfs_to_clickhouse.sh
# Purpose: Native ClickHouse Ingestion from HDFS into MergeTree (Zero Spark)
# ***
set -e

CH_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASS="${CLICKHOUSE_PASSWORD:-clickhouse123}"

ch_exec() {
  curl -s -f -u "${CH_USER}:${CH_PASS}" "http://${CH_HOST}:${CH_PORT}/" --data-binary "$1"
}

echo " ** NATIVE SYNC: HDFS (hdfs://master:9000/data/products.csv) -> CLICKHOUSE **"

# 1. Create Database and Table if not exists
echo "[1/3] Ensuring Database & MergeTree table exist in ClickHouse..."
ch_exec "CREATE DATABASE IF NOT EXISTS analytics;"

ch_exec "
CREATE TABLE IF NOT EXISTS analytics.dim_products_olap (
    id UInt32,
    name String,
    price Decimal(18, 2),
    synced_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
"

# 2. Native Direct Ingestion from HDFS
echo "[2/3] Executing native ClickHouse ingestion from HDFS..."
ch_exec "
INSERT INTO analytics.dim_products_olap (id, name, price)
SELECT id, name, price
FROM hdfs('hdfs://master:9000/data/products.csv', 'CSVWithNames', 'id UInt32, name String, price Decimal(18,2)');
"

# 3. Output Result
echo "[3/3] Ingestion successful! Current table contents:"
ch_exec "SELECT id, name, price, synced_at FROM analytics.dim_products_olap FORMAT Pretty;"

echoe "** DONE **"
