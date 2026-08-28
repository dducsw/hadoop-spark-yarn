#!/usr/bin/env bash
set -e

echo "=== [4/4] Initializing Database & Tables in ClickHouse OLAP ==="

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse Server (clickhouse:8123)..."
while ! curl -s "http://clickhouse:8123/ping" | grep -q "Ok"; do
  sleep 2
done
echo "ClickHouse is READY!"

# Helper function for ClickHouse query with auth
ch_query() {
  curl -s -u "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-clickhouse123}" "http://clickhouse:8123/" --data-binary "$1"
}

# 1. Create Database analytics
echo "Creating database 'analytics' in ClickHouse..."
ch_query "CREATE DATABASE IF NOT EXISTS analytics;"

# 2. Create table fact_sales (MergeTree engine)
echo "Creating table 'analytics.fact_sales'..."
ch_query "
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    order_id UInt32,
    customer_id String,
    product_name String,
    category LowCardinality(String),
    quantity UInt32,
    unit_price Float64,
    total_amount Float64,
    order_date Date
) ENGINE = MergeTree()
ORDER BY (category, order_date, order_id);
"

# 3. Create table agg_category_sales
echo "Creating table 'analytics.agg_category_sales'..."
ch_query "
CREATE TABLE IF NOT EXISTS analytics.agg_category_sales (
    category String,
    total_orders UInt32,
    total_units_sold UInt32,
    revenue Float64,
    avg_order_value Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY category;
"

echo "ClickHouse tables in 'analytics' database:"
ch_query "SHOW TABLES IN analytics;"
echo "ClickHouse OLAP schema initialization complete!"
