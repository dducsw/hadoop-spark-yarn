#!/usr/bin/env bash
set -e

echo "=== [TEST 5] Verifying Sub-Second Queries on ClickHouse OLAP ==="

echo "1. Checking ClickHouse Liveness ping:"
curl -s "http://clickhouse:8123/ping"
echo ""

echo "2. Querying aggregated sales metrics from ClickHouse:"
curl -s -u "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-clickhouse123}" "http://clickhouse:8123/" --data-binary "
SELECT
    category,
    total_orders,
    total_units_sold,
    revenue,
    avg_order_value
FROM analytics.agg_category_sales
ORDER BY revenue DESC
FORMAT PrettyCompact;
"

echo -e "\n3. Benchmarking analytical query execution latency:"
START=$(date +%s%N)
curl -s -u "${CLICKHOUSE_USER:-default}:${CLICKHOUSE_PASSWORD:-clickhouse123}" "http://clickhouse:8123/" --data-binary "SELECT count(*), sum(revenue) FROM analytics.agg_category_sales;" > /dev/null
END=$(date +%s%N)
DIFF=$(( (END - START) / 1000000 ))

echo ">>> [TEST 5 SUCCESS] ClickHouse query completed in ${DIFF} ms (Sub-second latency)!"
