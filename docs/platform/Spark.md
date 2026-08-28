# Apache Spark on YARN Architecture

## 1. Overview
Apache Spark 3.5 is the unified analytics engine for large-scale distributed data processing, ETL pipelines, and SQL computations.

- **Deployment Mode**: `yarn-client` (interactive development) and `yarn-cluster` (scheduled production jobs).
- **Spark History Server**: Displays completed job metrics, DAG stages, tasks, and memory consumption.
- **Hive Catalog Integration**: Spark SQL queries shared Hive tables seamlessly via `enableHiveSupport()`.

## 2. Configuration & Ports
- **Config directory**: `config/spark/` (`spark-defaults.conf`, `spark-env.sh`)
- **Event Log Directory**: `hdfs://master:9000/spark-logs`
- **Spark History Server Web UI**: `http://localhost:18080`

## 3. Useful Commands
```bash
# Submit PySpark job to YARN
spark-submit --master yarn \
             --deploy-mode client \
             /jobs/spark_to_clickhouse_etl.py

# Launch interactive PySpark shell on YARN
pyspark --master yarn

# Launch interactive Spark SQL shell
spark-sql --master yarn
```
