# Fintech Credit Risk Data Pipeline (Hadoop - Spark - YARN)

> Enterprise Medallion Data Lakehouse architecture (Bronze -> Silver -> Gold) designed for credit scoring and loan portfolio risk analytics (Home Credit Default Risk dataset), running on a distributed **Apache Hadoop YARN & Apache Spark 3.5** cluster.

---

## 1. Directory Structure & Architecture

```text
pipeline/
├── config/                  # Environment parameters & HDFS path configs
│   └── raw_config.py        # Mapping of source CSVs, Hive table names, PKs
├── dags/                    # Workflow orchestration with Apache Airflow
│   └── demo_pipeline_dag.py # DAG orchestrating Spark jobs across the cluster
├── examples/                # Infrastructural reference implementations & demos
│   ├── spark_hive_etl.py    # Hive Metastore integration demo
│   ├── spark_to_clickhouse_etl.py # Syncing curated data into ClickHouse OLAP
│   └── wordcount.py         # Baseline cluster connectivity check
├── src/                     # Core pipeline source code
│   ├── common/              # Shared infrastructure modules (OOP Template Method)
│   │   ├── audit.py         # Centralized audit logging (pipeline_audit_log)
│   │   ├── base_spark_job.py# Root abstract lifecycle: Extract -> Validate -> Transform -> Audit -> Load
│   │   ├── base_raw_ingest.py # Base job for Raw (Bronze) layer
│   │   ├── base_stage_job.py  # Base job for Stage (Silver) layer (dedup, null filtering)
│   │   ├── base_curated_job.py# Base job for Curated (Gold) layer (feature engineering & marts)
│   │   ├── logger.py        # Standardized timestamped logger
│   │   ├── spark_session.py # Managed SparkSession optimized for YARN
│   │   └── watermark.py     # Watermark tracker for incremental pipelines
│   ├── jobs/                # 21 PySpark jobs organized by Medallion layer
│   │   ├── raw/             # 8 Ingestion jobs (CSV -> Bronze Parquet on HDFS)
│   │   ├── stage/           # 8 Cleaning jobs (Bronze -> Silver Parquet: explicit types, dedup)
│   │   └── curated/         # 13 Dimensional Modeling & Mart jobs (Silver -> Gold Parquet):
│   │       ├── curated_dim_*.py  # Conformed Dims & SCD4 (Customer, Loan Product, Delinquency Bucket,...)
│   │       ├── curated_fact_*.py # Constellation Facts (Loan Application, Monthly Loan Snapshot,...)
│   │       └── curated_obt_loan_portfolio_360.py # Wide 360 Mart for BI & ML serving
│   ├── schemas/             # Explicit PySpark schemas & Hive DDL strings
│   │   ├── raw_schemas.py   # Explicit DDL & HDFS locations for Raw
│   │   ├── stage_schemas.py # Explicit DDL & HDFS locations for Stage
│   │   └── curated_schemas.py # Explicit DDL & HDFS locations for Curated
│   └── sql/                 # Standalone SQL scripts for Metastore DDL initialization
│       ├── raw_tables.sql
│       ├── stage_tables.sql
│       └── curated_tables.sql
└── test/                    # Unit & regression test suite
    └── test_dimensional_modeling.py # Validates xxhash64 idempotency, Unknown row (-1), Decimal precision
```

---

## 2. Standardized Audit Metadata Specification

Every table across all layers (**Raw, Stage, Curated**) enforces a uniform 3-column technical audit trail at the end of its schema:

| Column Name | Data Type | Technical & Business Purpose |
| :--- | :--- | :--- |
| `_source_system` | `STRING` | Originating system identifier (`home_credit`, `credit_bureau`). |
| `_processed_at` | `TIMESTAMP` | Timestamp when the current layer processed the record (`F.current_timestamp()`). |
| `_batch_id` | `STRING` | Execution run identifier (`batch_YYYYMMDD_HHMMSS`), enabling data reconciliation, lineage, and partition rollbacks. |

> **Note**: Audit column injection is handled centrally inside `BaseSparkJob.run()`. It automatically strips stale audit columns from upstream layers and attaches fresh metadata prior to persisting to HDFS. Individual jobs require zero manual metadata boilerplate.

---

## 3. Dimensional Data Model (Gold Layer)

1. **Slowly Changing Dimension Type 4 (SCD4)**:
   - `credit_risk.dim_customer`: Current state snapshot (SCD1), enabling low-latency joins with Fact tables without filtering `is_current = true`.
   - `credit_risk.dim_customer_history`: Append-only audit table storing historical attribute changes alongside `change_id` and `effective_from`.
2. **Conformed Dimensions**:
   - `dim_delinquency_bucket`: Basel II / IFRS9 aligned delinquency bands (Current, 1-30, 31-60, 61-90, 91-120, 120+ DPD).
   - `dim_loan_product`, `dim_merchant_channel`, `dim_application_decision`, `dim_relative_time`, `dim_bureau_source`.
   - All dimensions implement a deterministic `Unknown (-1)` fallback row for missing or orphan foreign keys.
3. **Fact Constellation**:
   - `fact_loan_application`: Distinguishes between `amt_application` (requested amount) and `amt_credit` (approved credit).
   - `fact_monthly_loan_snapshot`: Monthly loan balance, credit limit, utilization ratio, and delinquency status.
   - `fact_installment_payment`: Granular installment payment records (identifying late payments and underpayments).
   - `fact_bureau_credit` & `fact_monthly_bureau_snapshot`: External bureau credit history and external monthly payment records.
4. **Wide Serving Mart**:
   - `obt_loan_portfolio_360`: One Big Table (OBT) joining customer demographics, loan details, and latest repayment metrics (`latest_balance`, `latest_utilization_ratio`, `latest_dpd`).

---

## 4. Pipeline Execution Guide on YARN

### 4.1. Cluster Startup (Docker Compose)

From the project root:
```bash
# Start all cluster services (Master, Worker1, Worker2, Hive DB, ClickHouse, Airflow)
docker compose up -d

# Verify container health
docker compose ps
```

Ensure `master`, `worker1`, and `worker2` are healthy and `Up`.

---

### 4.2. Option 1: Automated End-to-End Pipeline (Recommended)

Executes all 21 jobs sequentially on YARN (`raw` -> `stage` -> `curated dims` -> `curated facts` -> `obt 360`):

```bash
docker exec master bash /scripts/ops/run_full_pipeline_yarn.sh
```

---

### 4.3. Option 2: Layer-by-Layer Execution

Run individual layers for targeted processing or debugging:

#### Step 1: Raw Layer (Bronze Ingestion)
```bash
docker exec master bash -c '
export PYTHONPATH=/pipeline:${PYTHONPATH}
for job in ingest_application_train.py ingest_application_test.py ingest_bureau.py \
           ingest_bureau_balance.py ingest_pos_cash_balance.py ingest_credit_card_balance.py \
           ingest_installments_payments.py ingest_previous_application.py; do
    spark-submit --master yarn --deploy-mode client /pipeline/src/jobs/raw/${job}
done
'
```

#### Step 2: Stage Layer (Silver Cleaning & Deduplication)
```bash
docker exec master bash -c '
export PYTHONPATH=/pipeline:${PYTHONPATH}
for job in stage_application_train.py stage_application_test.py stage_bureau.py \
           stage_bureau_balance.py stage_pos_cash_balance.py stage_credit_card_balance.py \
           stage_installments_payments.py stage_previous_application.py; do
    spark-submit --master yarn --deploy-mode client /pipeline/src/jobs/stage/${job}
done
'
```

#### Step 3: Curated Layer (Gold Dimensions, Facts & Marts)
```bash
# Execute curated layer via dedicated runner:
docker exec master bash /scripts/ops/run_curated_yarn.sh
```

---

### 4.4. Option 3: Run a Single Job on YARN

Standard command template with memory and partition tuning:

```bash
docker exec master bash -c "
export PYTHONPATH=/pipeline:\${PYTHONPATH}
spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.default.parallelism=4 \
    --driver-memory 768m \
    --executor-memory 768m \
    /pipeline/src/jobs/curated/curated_obt_loan_portfolio_360.py
"
```

---

## 5. Monitoring & Data Verification

| Service | Web UI URL | Verification Role |
| :--- | :--- | :--- |
| **YARN ResourceManager** | [http://localhost:8088](http://localhost:8088) | Monitor running applications, vCores, and worker memory allocation |
| **HDFS NameNode** | [http://localhost:9870](http://localhost:9870) | Inspect Parquet files under `/raw`, `/stage`, `/curated`, and `/metadata` |
| **Spark History Server** | [http://localhost:18080](http://localhost:18080) | Inspect DAGs, shuffle read/write metrics, and stage durations |
| **Airflow Webserver** | [http://localhost:8080](http://localhost:8080) | Inspect pipeline DAGs and scheduled workflows |

### Quick Data Verification via PySpark:
```bash
# Verify row counts and audit schema on OBT 360 in HDFS
docker exec master python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName('VerifyData').getOrCreate()
df = spark.read.parquet('/curated/credit_risk/obt_loan_portfolio_360')
print('Row count:', df.count())
print('Columns:', df.columns)
df.select('sk_id_curr', 'portfolio_category', 'latest_balance', '_source_system', '_processed_at', '_batch_id').show(5, False)
spark.stop()
"
```
