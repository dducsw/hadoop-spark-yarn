# Big Data Platform Operations Runbook

Step-by-step procedures for initializing, operating, testing, and troubleshooting the Big Data Platform.

---

## 1. System Prerequisites

- **Docker Engine**: >= 20.10
- **Docker Compose**: >= 2.0
- **Recommended Host Allocations (Docker Desktop / WSL2)**:
  - Memory: 8 GB - 12 GB RAM
  - CPU Cores: 4 - 6 vCPUs
  - Free Disk Space: 20 GB

---

## 2. Cluster Lifecycle Operations

### Building the Base Image
```bash
make build
# or: docker-compose build
```

### Starting the Cluster
```bash
make up
# or: docker-compose up -d
```
*Allow 30-45 seconds for ZooKeeper, PostgreSQL, NameNode, ResourceManager, Hive Metastore, and ClickHouse to become healthy.*

### Initializing the Platform (Automated & Manual)
The cluster automatically initializes HDFS system directories, uploads sample datasets, uploads Spark core JARs, and provisions ClickHouse OLAP schema in the background during `make up`.

To manually trigger or re-initialize the bootstrap pipeline at any time:
```bash
make bootstrap
```

### Stopping the Cluster
```bash
# Stop containers (preserves volume data):
make down

# Purge all containers, networks, and persistent data volumes:
make clean
```

---

## 3. Health & Status Verification

Inspect the health of all platform daemons across all containers:
```bash
make status
```

### Accessible Web UIs:
- **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870)
- **YARN ResourceManager UI**: [http://localhost:8088](http://localhost:8088)
- **MapReduce JobHistory UI**: [http://localhost:19888](http://localhost:19888)
- **Spark History Server UI**: [http://localhost:18080](http://localhost:18080)
- **HiveServer2 Web UI**: [http://localhost:10002](http://localhost:10002)
- **ClickHouse Web Query Client**: [http://localhost:8123/play](http://localhost:8123/play)

---

## 4. Running the Smoke Test Suite

Execute the 5-layer verification suite:
```bash
make test
```
The test runner validates:
1. **HDFS Storage**: Read, write, and delete operations against `hdfs://master:9000`.
2. **YARN MapReduce**: Distributed Pi estimation job across NodeManagers.
3. **PySpark on YARN**: Distributed DataFrame computation and Parquet write/read on HDFS.
4. **Spark SQL & Hive Catalog**: Shared catalog table creation and querying.
5. **ClickHouse OLAP**: Sub-10ms query execution and latency benchmarking.

---

## 5. Running Production Workflows

### Access the Master Shell
```bash
make master
# or: docker exec -it master bash
```

### A. Run Distributed PySpark WordCount on YARN
```bash
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/examples/wordcount.py
```

### B. Run End-to-End Lakehouse to OLAP Pipeline
```bash
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/examples/spark_to_clickhouse_etl.py
```

### C. Connect to Hive via Beeline
```bash
beeline -u "jdbc:hive2://master:10000/default" -n root -e "
  SHOW DATABASES;
  USE analytics_db;
  SHOW TABLES;
  SELECT * FROM fact_sales LIMIT 5;
"
```

### D. Query ClickHouse via HTTP REST API
```bash
curl -s "http://clickhouse:8123/" --data-binary "
  SELECT category, total_orders, revenue, avg_order_value
  FROM analytics.agg_category_sales
  ORDER BY revenue DESC
  FORMAT PrettyCompact;
"
```
