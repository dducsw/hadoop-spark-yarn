# Fintech Credit Risk Data Pipeline Specification

Comprehensive technical specification for the Enterprise Big Data Warehouse and Lakehouse pipeline deployed on Apache Hadoop YARN, Apache Spark 3.5, Apache Hive Metastore, and ClickHouse OLAP.

---

## 1. Architecture Overview

The platform implements an **Enterprise Data Warehouse (DWH) & Lakehouse architecture** structured according to Inmon & Kimball best practices rather than generic lakehouse abstractions:

```
[PostgreSQL / CRM Source]
          │
          ▼
   1. RAW LANDING ZONE       (HDFS Parquet: /lakehouse/raw/*)
          │
          ▼
   2. STAGE / ODS LAYER      (Hive External: stage_credit_risk.*)
          │
          ▼
   3. CURATED CORE (DWH)     (Kimball Conformed Dims & Constellation Facts)
          │
          ▼
   4. DATA MART (OBT)        (Wide Denormalized Mart: obt_loan_portfolio_360)
          │
          ▼
   5. RECONCILIATION GATE    (Automated Balance Integrity & Data Quality Check)
          │
          ▼
   6. OLAP SERVING           (ClickHouse MergeTree with Atomic Staging Swap)
```

### Architectural Tenets:
1. **Separation of Compute and Storage**: HDFS acts as the distributed persistent storage engine; Spark runs on YARN for elastic batch computing.
2. **Strict Financial Data Types**: Monetary values (`amt_credit`, `amt_balance`, `amt_payment`) are strictly typed as `Decimal(18,2)` to prevent binary floating-point rounding inaccuracies.
3. **Audit Lineage & Determinism**: Every record written across all layers carries standardized audit columns:
   - `_source_system`: Source database/system tag.
   - `_processed_at`: Ingestion or transformation timestamp.
   - `_batch_id`: Deterministic execution identifier (`run_id` from Airflow).
4. **Idempotency & Zero-Downtime Serving**: All write operations utilize dynamic partition overwrite or atomic table swapping to ensure safe re-runs without duplicate records or service downtime.

---

## 2. Pipeline Layers & Transformations

### Layer 1: Raw Landing Zone (HDFS Parquet)
- **HDFS Location**: `/lakehouse/raw/credit_risk/<table_name>/`
- **Format**: Parquet with Snappy compression.
- **Ingestion Patterns**:
  - **Full Snapshot**: For slowly changing reference datasets (`application_train`, `application_test`, `previous_application`).
  - **Incremental Watermarking**: For high-volume transaction/monthly ledger feeds (`bureau_balance`, `pos_cash_balance`, `credit_card_balance`, `installments_payments`).
- **Metadata Management**: Watermarks and batch audit logs are atomically registered in PostgreSQL metadata database (`pipeline_metadata.watermarks`, `pipeline_metadata.audit_log`) using `ON CONFLICT DO UPDATE` UPSERT semantics.

### Layer 2: Stage / ODS (Operational Data Store)
- **Hive Database**: `stage_credit_risk`
- **Location**: `/stage/credit_risk/<table_name>/`
- **Core Transformations**:
  - Deduplication on source Natural Keys using Spark Window functions (`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`).
  - Explicit schema casting: Strings to `Decimal(18,2)`, dates to ISO strings / integers.
  - Basic quality cleansing: trimming strings, standardizing gender/marital codes.
  - Partition registration via `spark.catalog.recoverPartitions()` avoiding slow metastore locks.

### Layer 3: Curated Core - Dimensions (Kimball Bus Architecture)
- **Hive Database**: `credit_risk`
- **Location**: `/curated/credit_risk/dim_*/`
- **Surrogate Key Generation**: Deterministic 64-bit integer hashing using `xxhash64(natural_key)`:
  - Eliminates dependency on distributed auto-increment locks.
  - Enables embarrassingly parallel dimension lookups across cluster executors.
- **Default Unknown Record**: Every dimension table injects an unknown fallback row (`sk_... = -1`, description `"Unknown"`) to preserve referential integrity during outer joins.
- **Conformed Dimensions**:
  - `dim_customer`: Customer demographics, employment duration, income brackets.
  - `dim_loan_product`: Loan types, revolving credit indicators, contract groups.
  - `dim_merchant_channel`: Acquisition channels, seller industries, yield groups.
  - `dim_application_decision`: Approval/refusal status, cash loan purpose.
  - `dim_delinquency_bucket`: Standard Basel risk rating delinquency buckets (0-30, 31-60, 61-90, 91-120, 120+ DPD).
  - `dim_relative_time`: Relative month offsets for longitudinal credit history.
  - `dim_bureau_source`: External credit bureau credit types and active flags.

### Layer 4: Curated Core - Facts (Constellation Architecture)
- **Location**: `/curated/credit_risk/fact_*/`
- **Surrogate Key Resolution**: Fact extraction joins against dimension tables using Natural Keys to resolve 64-bit Surrogate Keys (`sk_customer_key`, `sk_product_key`, `sk_channel_key`, etc.).
- **Fact Tables**:
  - `fact_loan_application`: Transaction fact capturing application amounts, approved credit, and credit-to-annuity ratios.
  - `fact_monthly_loan_snapshot`: Periodic snapshot fact capturing historical monthly balance, credit card utilization, and Days Past Due (DPD).
  - `fact_installment_payment`: Accumulating/transaction fact capturing payment timeliness, installment deficits, and late penalties.
  - `fact_bureau_credit`: External credit bureau active balance, total credit limits, and debt amounts.
  - `fact_monthly_bureau_snapshot`: Monthly bureau status records tracking debt delinquency over time.

### Layer 5: Data Mart (One Big Table - OBT 360)
- **Location**: `/curated/credit_risk/obt_loan_portfolio_360/`
- **Purpose**: Pre-joined, wide denormalized analytics dataset optimized for Self-Service BI, Superset dashboards, and machine learning feature extraction without runtime join overhead.
- **Contents**: Combines loan application facts, customer dimension attributes, channel details, and the latest monthly risk/delinquency snapshot.

### Layer 5b: Financial Reconciliation & Data Quality Gate
- **Script**: `pipeline/src/jobs/curated/reconciliation_audit.py`
- **Execution**: Runs automatically immediately after OBT Mart generation and before OLAP ingestion.
- **Automated Assertions**:
  1. **Primary Key Integrity**: Asserts that `sk_id_curr` in OBT contains zero `NULL` values.
  2. **Financial Balance Reconciliation**: Verifies that total credit across OBT matches the raw stage application totals within a strict tolerance:
     $$\Delta(\sum \text{amt\_credit}) = \frac{|\sum \text{amt\_credit}_{\text{OBT}} - \sum \text{amt\_credit}_{\text{Stage}}|}{\sum \text{amt\_credit}_{\text{Stage}}} \le 0.01\%$$
  3. **Pipeline Circuit Breaker**: If discrepancy exceeds $0.01\%$, the job raises a non-zero exit code, immediately terminating the pipeline before corrupt data reaches the ClickHouse serving layer.

### Layer 6: OLAP Serving Layer (ClickHouse MergeTree)
- **Table**: `analytics.obt_loan_portfolio_360`
- **Script**: `scripts/ops/sync_hdfs_to_clickhouse.sh`
- **Zero-Downtime Serving Pattern**:
  1. Ingest HDFS Parquet directly into a temporary staging table: `analytics.obt_loan_portfolio_360_staging`.
  2. Verify row count of staging table ($N > 0$).
  3. Execute atomic table swap:
     ```sql
     EXCHANGE TABLES analytics.obt_loan_portfolio_360 AND analytics.obt_loan_portfolio_360_staging;
     ```
  4. Truncate staging table to release disk space.
  5. Dashboards and BI queries experience zero downtime and zero empty-table reads.

---

## 3. Storage Layout & Partitioning Strategy

| Table | Layer | HDFS Path | Partition Key | Sizing & Distribution |
| :--- | :--- | :--- | :--- | :--- |
| `raw_application_train` | Raw | `/lakehouse/raw/credit_risk/application_train/` | None (Unpartitioned) | 1-2 Parquet files (~150MB) |
| `raw_bureau_balance` | Raw | `/lakehouse/raw/credit_risk/bureau_balance/` | Date Watermark | Multi-partition Parquet |
| `stage_application_train` | Stage | `/stage/credit_risk/application_train/` | None | Cleaned, Decimal(18,2) |
| `dim_customer` | Curated | `/curated/credit_risk/dim_customer/` | None | Parquet, surrogate keyed |
| `fact_monthly_loan_snapshot` | Curated | `/curated/credit_risk/fact_monthly_loan_snapshot/` | `snapshot_month` | Monthly partitions |
| `obt_loan_portfolio_360` | Mart | `/curated/credit_risk/obt_loan_portfolio_360/` | None | Wide denormalized Parquet |
| `obt_loan_portfolio_360` | OLAP | ClickHouse Local Disk | `toYYYYMM(ingested_at)` | MergeTree, Primary Key: `(sk_id_curr, is_current_application)` |

---

## 4. Operational Execution

### Submitting Spark Jobs Manually:
```bash
# Execute Stage Application Train on YARN:
docker exec master bash -c "export BATCH_ID=manual_test && spark-submit --master yarn --deploy-mode client /pipeline/src/jobs/stage/stage_application_train.py"

# Execute OBT Mart on YARN:
docker exec master bash -c "export BATCH_ID=manual_test && spark-submit --master yarn --deploy-mode client /pipeline/src/jobs/curated/curated_obt_loan_portfolio_360.py"
```

### Running the End-to-End Automated Pipeline:
Orchestrated via Apache Airflow 3 (`risk_data_pipeline`). Refer to [ORCHESTRATION.md](ORCHESTRATION.md) for scheduler architecture and configuration.
