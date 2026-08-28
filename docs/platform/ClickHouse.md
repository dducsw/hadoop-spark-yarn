# ClickHouse OLAP Architecture

## 1. Overview
ClickHouse is an open-source, high-performance columnar database management system for real-time analytical reporting (OLAP).

- **Role**: Serving Layer / Speed Layer receiving Gold metrics from Spark and providing sub-10ms query responses for BI dashboards.
- **Engine**: `MergeTree()` family engines supporting high-throughput ingestion, columnar compression, and parallel processing.

## 2. Ports & Access
- **HTTP REST / Web Query Client**: `http://localhost:8123` (Interactive UI at `/play`)
- **Native TCP Protocol**: `9004` (mapped from container port 9000)

## 3. Useful Commands
```bash
# Liveness check
curl -s "http://localhost:8123/ping"

# Execute query via HTTP API
curl -s "http://localhost:8123/" --data-binary "
SELECT category, sum(revenue)
FROM analytics.agg_category_sales
GROUP BY category
FORMAT PrettyCompact;
"
```
