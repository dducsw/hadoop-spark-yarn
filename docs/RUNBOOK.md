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
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`)
- **ClickHouse Web Query Client**: [http://localhost:8123/play](http://localhost:8123/play)
- **PostgreSQL Multi-tenant**: Port `5432` (`metastore`, `source_crm`, `airflow`)

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

### A. PostgreSQL Multi-Tenant OLTP Database Operations
The unified `postgres` container hosts 3 isolated databases:
- `metastore` (User: `hive`, Pwd: `hivepassword`) - Hive Metastore catalog.
- `source_crm` (User: `hive`, Pwd: `hivepassword`) - Home Credit OLTP source database.
- `airflow` (User: `airflow`, Pwd: `airflowpassword`) - Airflow 3 metadata catalog.

#### 1. Inspect PostgreSQL databases via psql:
```bash
docker exec -it postgres psql -U hive -d source_crm -c "\dt"
```

#### 2. Seed OLTP Source Database from CSVs:
Run the high-speed binary copy seeding script:
```bash
# Executed from host (requires psycopg2 or run in docker container):
python scripts/ops/seed_postgres_from_csv.py --input-dir data/home-credit-default-risk --batch-size 10000

# Or execute inside master container:
docker exec -it master python /scripts/ops/seed_postgres_from_csv.py
```

---

### B. Running the Medallion Pipeline on YARN

All jobs adhere to standardized audit metadata: `_source_system`, `_processed_at`, `_batch_id`.

#### 1. Layer 1: Raw Ingestion (PostgreSQL JDBC -> HDFS Raw Parquet)
Supports **Full Load** (snapshot overwrite) and **Incremental Load** (watermarked append/merge):
```bash
# Full Load (e.g. application_train dimension):
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/src/jobs/raw/ingest_application_train.py

# Incremental Load (e.g. bureau_balance with watermark tracking):
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/src/jobs/raw/ingest_bureau_balance.py
```

#### 2. Layer 2: Stage Transformation (Data Cleansing & Typing)
```bash
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/src/jobs/stage/stage_bureau_balance.py
```

#### 3. Layer 3: Curated Analytics Marts (Star Schema Fact & Dim)
```bash
# Curated Dimension:
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/src/jobs/curated/dim_loan_product.py

# Curated Fact (Snapshot Mart):
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/src/jobs/curated/fact_monthly_bureau_snapshot.py
```

#### 4. Layer 4: Export to ClickHouse OLAP
```bash
spark-submit --master yarn \
             --deploy-mode client \
             /pipeline/src/jobs/export_clickhouse/export_fact_monthly_bureau_snapshot.py
```

---

### C. Connect to Hive via Beeline
```bash
beeline -u "jdbc:hive2://master:10000/default" -n root -e "
  SHOW DATABASES;
  USE lakehouse;
  SHOW TABLES;
  SELECT * FROM curated_fact_monthly_bureau_snapshot LIMIT 5;
"
```

---

### D. Query ClickHouse via HTTP REST API
```bash
curl -s "http://clickhouse:8123/" --data-binary "
  SELECT status, count(), sum(active_credits)
  FROM fintech.fact_monthly_bureau_snapshot
  GROUP BY status
  FORMAT PrettyCompact;
"
```

---

### E. Airflow Orchestration Operations
- Access Airflow Webserver: [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`).
- Trigger DAG via CLI:
```bash
docker exec -it airflow-scheduler airflow dags trigger fintech_medallion_pipeline
```
- List active DAGs:
```bash
docker exec -it airflow-scheduler airflow dags list
```
