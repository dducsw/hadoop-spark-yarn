# Big Data Platform Troubleshooting Guide

Common operational issues, diagnostic commands, and remediation strategies.

---

## 1. HDFS Storage & NameNode Issues

### Issue 1: NameNode stuck in SafeMode (`SafeMode: ON`)
- **Root Cause**: Replication threshold has not yet been satisfied during cluster startup.
- **Remediation**:
  ```bash
  docker exec -it master hdfs dfsadmin -safemode leave
  ```

### Issue 2: DataNodes not reporting to NameNode
- **Root Cause**: Network delay or hostname resolution failure between workers and master.
- **Diagnostic & Remediation**:
  ```bash
  # Check live DataNodes report
  docker exec -it master hdfs dfsadmin -report
  # Inspect worker logs
  docker logs worker1
  docker logs worker2
  ```

---

## 2. YARN & Resource Management Issues

### Issue 1: Spark application stuck in `ACCEPTED` state
- **Root Cause**: NodeManagers have insufficient allocated memory or vCores to spawn the ApplicationMaster container.
- **Remediation**:
  1. Verify running NodeManagers:
     ```bash
     docker exec -it master yarn node -list
     ```
  2. Reduce resource requests when submitting the job:
     ```bash
     spark-submit --master yarn --driver-memory 512m --executor-memory 512m ...
     ```

### Issue 2: Container killed due to Virtual Memory limits
- **Root Cause**: Docker and WSL2 kernel virtual memory accounting triggers false positives.
- **Remediation**: Handled automatically in `yarn-site.xml` by setting `yarn.nodemanager.vmem-check-enabled=false`.

---

## 3. Apache Hive & Spark SQL Interoperability

### Issue 1: `NoSuchMethodError: com.google.common.base.Preconditions.checkArgument`
- **Root Cause**: Guava version mismatch between Hive 3.1 and Hadoop 3.4.
- **Remediation**: Fixed automatically in `docker/base/Dockerfile` by replacing Hive's legacy `guava-19.0.jar` with Hadoop's modern Guava library.

### Issue 2: Hive Metastore cannot connect to PostgreSQL
- **Root Cause**: Unified `postgres` container initializing slower than the metastore thrift service.
- **Remediation**:
  ```bash
  # Verify database container health (named postgres, aliases: hive-db, airflow-db)
  docker ps --filter "name=postgres"
  # Test direct network connectivity from master
  docker exec -it master nc -zv postgres 5432
  ```

---

## 4. ClickHouse OLAP Issues

### Issue 1: Connection refused on port 8123
- **Root Cause**: ClickHouse server initializing or container not running.
- **Remediation**:
  ```bash
  docker logs clickhouse
  curl -s "http://localhost:8123/ping"
  ```

---

## 5. PostgreSQL Multi-Tenant Database Issues

### Issue 1: Missing database (`metastore`, `source_crm`, `airflow`) or role permissions
- **Root Cause**: PostgreSQL volume was initialized before multi-tenant init script was mounted, or container failed initial bootstrap.
- **Diagnostic**:
  ```bash
  docker exec -it postgres psql -U postgres -c "\l"
  ```
- **Remediation**:
  Re-run the initialization SQL script manually:
  ```bash
  docker exec -i postgres psql -U postgres < scripts/bootstrap/init-postgres-dbs.sql
  ```

### Issue 2: Port 5432 conflict on host machine
- **Root Cause**: Local PostgreSQL instance running on Windows host or leftover orphaned container (`airflow-db` or `hive-db`).
- **Remediation**:
  ```powershell
  # Check what process listens on 5432
  Get-NetTCPConnection -LocalPort 5432 | Select-Object OwningProcess, State
  # Stop leftover containers
  docker rm -f airflow-db hive-db
  ```

---

## 6. Spark on YARN & PostgreSQL JDBC Issues

### Issue 1: `java.lang.ClassNotFoundException: org.postgresql.Driver` on YARN executors
- **Root Cause**: PostgreSQL JDBC driver not distributed across worker NodeManagers.
- **Remediation**:
  Driver is cached centrally in HDFS at `hdfs://master:9000/spark-jars/` and linked via `spark.yarn.jars = hdfs://master:9000/spark-jars/*`. If missing, re-upload from master:
  ```bash
  docker exec -it master /scripts/bootstrap/03-upload-spark-jars.sh
  # Verify jar presence in HDFS
  docker exec -it master hdfs dfs -ls /spark-jars/postgresql-*.jar
  ```

---

## 7. Medallion Pipeline & Raw Ingestion Issues

### Issue 1: `SparkFileNotFoundException` during incremental load
- **Root Cause**: Spark lazy evaluation reading from and writing to the exact same HDFS directory during an overwrite/append operation.
- **Remediation**:
  Materialize existing dataset in memory before transforming and writing back:
  ```python
  existing_df = spark.read.parquet(target_path).checkpoint() # or .cache().count()
  ```

### Issue 2: Corrupted or Out-of-Sync Watermark File
- **Root Cause**: Raw ingestion job failed mid-flight after writing Parquet data but before committing `_watermarks/<table_name>.json`.
- **Diagnostic & Remediation**:
  ```bash
  # Check watermark content
  docker exec -it master hdfs dfs -cat /lakehouse/raw/_watermarks/<table_name>.json
  # Reset watermark to force full re-sync
  docker exec -it master hdfs dfs -rm -r /lakehouse/raw/_watermarks/<table_name>.json
  ```

### Issue 3: Schema Mismatch on Audit Columns (`_ingested_at` vs `_processed_at`)
- **Root Cause**: Legacy datasets created before standardization had divergent audit columns (`_ingested_at`, `_curated_at`, `_source_table`).
- **Remediation**:
  Clean the HDFS target path to regenerate data with the standardized schema (`_source_system`, `_processed_at`, `_batch_id`):
  ```bash
  docker exec -it master hdfs dfs -rm -r /lakehouse/curated/<table_name>
  ```

---

## 8. Apache Airflow 3 Orchestration Issues

### Issue 1: Airflow webserver / scheduler exits with database connection failure
- **Root Cause**: Airflow metadata database `airflow` has not run schema migrations.
- **Remediation**:
  ```bash
  docker compose run --rm airflow-init
  docker restart airflow-webserver airflow-scheduler airflow-dag-processor
  ```
