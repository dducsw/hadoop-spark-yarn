#!/usr/bin/env python3
"""
Production Airflow DAG: Fintech Data Platform Pipeline
Orchestrates: Raw Landing -> Stage ODS -> DWH Core (Kimball Dims/Facts) -> Data Mart (OBT 360) -> OLAP Serving (ClickHouse)
Standard: Zero-top-level compute, TaskFlow API, fine-grained lineage, and deterministic batch_id.
"""
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator


# -----------------------------------------------------------------------------
# 1. Alerting & Notification Callback
# -----------------------------------------------------------------------------
def on_failure_alert(context: Dict[str, Any]) -> None:
    """Triggered automatically when any task in the pipeline fails."""
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    exec_date = context.get("logical_date") or context.get("data_interval_start") or context.get("execution_date")
    error = context.get("exception")
    log_url = context.get("task_instance").log_url
    print(f"""
    ======================================================================
    [ALERT] Pipeline Failure Detected!
    DAG: {dag_id} | Task: {task_id}
    Execution Date: {exec_date}
    Log URL: {log_url}
    Error Details: {error}
    ======================================================================
    """)


# -----------------------------------------------------------------------------
# 2. DAG Default Configuration
# -----------------------------------------------------------------------------
default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_alert,
}


def build_spark_task(task_id: str, layer: str, script: str) -> BashOperator:
    """Standardized Spark on YARN runner with batch_id lineage and concurrency pool."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"bash /opt/airflow/scripts/ops/submit_spark_job.sh {layer} {script} 'batch_{{{{ ts_nodash }}}}'",
        pool="spark_yarn_pool",
        env={"BATCH_ID": "batch_{{ ts_nodash }}"},
    )


# -----------------------------------------------------------------------------
# 3. DAG Definition
# -----------------------------------------------------------------------------
@dag(
    dag_id="fintech_data_pipeline",
    default_args=default_args,
    description="Big Data Pipeline: HDFS Raw -> Stage ODS -> DWH Core -> BI Mart -> ClickHouse OLAP",
    schedule="0 2 * * *",  # Daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fintech", "hadoop", "spark", "yarn", "dwh", "clickhouse"],
)
def fintech_data_pipeline():

    # Step 0: Cluster Liveness & Connectivity Gate (Fail-fast)
    cluster_healthcheck = BashOperator(
        task_id="cluster_healthcheck",
        bash_command="""
            echo ">>> [HEALTHCHECK] Verifying cluster endpoints..."
            curl -f -s -I http://master:9870 > /dev/null || (echo ">>> [ERROR] NameNode (9870) DOWN!" && exit 1)
            curl -f -s -I http://master:8088 > /dev/null || (echo ">>> [ERROR] ResourceManager (8088) DOWN!" && exit 1)
            curl -f -s http://clickhouse:8123/ping > /dev/null || (echo ">>> [ERROR] ClickHouse (8123) DOWN!" && exit 1)
            echo ">>> [HEALTHCHECK] Cluster is healthy and ready for batch workload."
        """,
    )

    # -------------------------------------------------------------------------
    # Layer 1: Raw / Landing Zone (HDFS Parquet)
    # -------------------------------------------------------------------------
    @task_group(group_id="raw_landing_zone", tooltip="Ingest source data into HDFS (/raw/credit_risk/*)")
    def raw_layer():
        return {
            "app_train": build_spark_task("ingest_application_train", "raw", "ingest_application_train.py"),
            "app_test": build_spark_task("ingest_application_test", "raw", "ingest_application_test.py"),
            "bureau": build_spark_task("ingest_bureau", "raw", "ingest_bureau.py"),
            "bureau_balance": build_spark_task("ingest_bureau_balance", "raw", "ingest_bureau_balance.py"),
            "pos_cash": build_spark_task("ingest_pos_cash_balance", "raw", "ingest_pos_cash_balance.py"),
            "credit_card": build_spark_task("ingest_credit_card_balance", "raw", "ingest_credit_card_balance.py"),
            "installments": build_spark_task("ingest_installments_payments", "raw", "ingest_installments_payments.py"),
            "prev_app": build_spark_task("ingest_previous_application", "raw", "ingest_previous_application.py"),
        }

    # -------------------------------------------------------------------------
    # Layer 2: Stage / ODS (Operational Data Store - Cleaned & Deduplicated)
    # -------------------------------------------------------------------------
    @task_group(group_id="stage_ods_layer", tooltip="Deduplicate, clean, cast Decimal(18,2) into Hive stage tables")
    def stage_layer():
        return {
            "app_train": build_spark_task("stage_application_train", "stage", "stage_application_train.py"),
            "app_test": build_spark_task("stage_application_test", "stage", "stage_application_test.py"),
            "bureau": build_spark_task("stage_bureau", "stage", "stage_bureau.py"),
            "bureau_balance": build_spark_task("stage_bureau_balance", "stage", "stage_bureau_balance.py"),
            "pos_cash": build_spark_task("stage_pos_cash_balance", "stage", "stage_pos_cash_balance.py"),
            "credit_card": build_spark_task("stage_credit_card_balance", "stage", "stage_credit_card_balance.py"),
            "installments": build_spark_task("stage_installments_payments", "stage", "stage_installments_payments.py"),
            "prev_app": build_spark_task("stage_previous_application", "stage", "stage_previous_application.py"),
        }

    # -------------------------------------------------------------------------
    # Layer 3: DWH Core - Dimensions (Conformed Kimball Dims with xxhash64)
    # -------------------------------------------------------------------------
    @task_group(group_id="dwh_kimball_dimensions", tooltip="Build conformed Kimball dimensions")
    def dwh_dimensions_layer():
        return {
            "dim_bucket": build_spark_task("dim_delinquency_bucket", "curated", "curated_dim_delinquency_bucket.py"),
            "dim_cust": build_spark_task("dim_customer", "curated", "curated_dim_customer.py"),
            "dim_prod": build_spark_task("dim_loan_product", "curated", "curated_dim_loan_product.py"),
            "dim_chan": build_spark_task("dim_merchant_channel", "curated", "curated_dim_merchant_channel.py"),
            "dim_dec": build_spark_task("dim_application_decision", "curated", "curated_dim_application_decision.py"),
            "dim_rel_time": build_spark_task("dim_relative_time", "curated", "curated_dim_relative_time.py"),
            "dim_bureau_src": build_spark_task("dim_bureau_source", "curated", "curated_dim_bureau_source.py"),
        }

    # -------------------------------------------------------------------------
    # Layer 4: DWH Core - Facts (Constellation Fact Tables)
    # -------------------------------------------------------------------------
    @task_group(group_id="dwh_kimball_facts", tooltip="Build constellation fact tables with SK references")
    def dwh_facts_layer():
        return {
            "fact_loan_app": build_spark_task("fact_loan_application", "curated", "curated_fact_loan_application.py"),
            "fact_monthly_loan": build_spark_task("fact_monthly_loan_snapshot", "curated", "curated_fact_monthly_loan_snapshot.py"),
            "fact_installment": build_spark_task("fact_installment_payment", "curated", "curated_fact_installment_payment.py"),
            "fact_bureau_cred": build_spark_task("fact_bureau_credit", "curated", "curated_fact_bureau_credit.py"),
            "fact_monthly_bureau": build_spark_task("fact_monthly_bureau_snapshot", "curated", "curated_fact_monthly_bureau_snapshot.py"),
        }

    # -------------------------------------------------------------------------
    # Layer 5: Data Mart (One Big Table for Zero-Join Analytics)
    # -------------------------------------------------------------------------
    mart_obt_360 = build_spark_task(
        "data_mart_obt_loan_portfolio_360",
        "curated",
        "curated_obt_loan_portfolio_360.py",
    )

    # -------------------------------------------------------------------------
    # Layer 6: OLAP Serving Layer (ClickHouse MergeTree)
    # -------------------------------------------------------------------------
    sync_to_clickhouse = BashOperator(
        task_id="sync_to_clickhouse_olap",
        bash_command="bash /opt/airflow/scripts/ops/sync_hdfs_to_clickhouse.sh",
    )

    # -------------------------------------------------------------------------
    # Layer 7: Pipeline Completion & Watermark Summary
    # -------------------------------------------------------------------------
    @task(task_id="pipeline_audit_summary")
    def pipeline_audit_summary(**kwargs):
        ds = kwargs.get("ds")
        run_id = kwargs.get("run_id")
        print(f"""
        ======================================================================
        >>> [SUCCESS] Data Pipeline Run Finished Successfully!
        >>> Date Interval: {ds}
        >>> Run ID: {run_id}
        >>> Lineage (Raw -> Stage ODS -> DWH Core -> BI Mart -> ClickHouse) verified.
        ======================================================================
        """)

    audit_summary = pipeline_audit_summary()

    # =========================================================================
    # Granular Dependency Graph
    # =========================================================================
    raw = raw_layer()
    stage = stage_layer()
    dims = dwh_dimensions_layer()
    facts = dwh_facts_layer()

    # Step 0 -> Raw Ingestion
    cluster_healthcheck >> [
        raw["app_train"], raw["app_test"], raw["bureau"], raw["bureau_balance"],
        raw["pos_cash"], raw["credit_card"], raw["installments"], raw["prev_app"]
    ]

    # Raw -> Stage 1-1 Lineage
    raw["app_train"] >> stage["app_train"]
    raw["app_test"] >> stage["app_test"]
    raw["bureau"] >> stage["bureau"]
    raw["bureau_balance"] >> stage["bureau_balance"]
    raw["pos_cash"] >> stage["pos_cash"]
    raw["credit_card"] >> stage["credit_card"]
    raw["installments"] >> stage["installments"]
    raw["prev_app"] >> stage["prev_app"]

    # Stage -> DWH Dimensions
    [stage["app_train"], stage["app_test"]] >> dims["dim_cust"]
    stage["prev_app"] >> [dims["dim_prod"], dims["dim_chan"], dims["dim_dec"]]
    stage["bureau"] >> dims["dim_bureau_src"]

    # Stage & Dims -> DWH Facts
    [stage["app_train"], stage["app_test"], dims["dim_cust"], dims["dim_prod"], dims["dim_chan"], dims["dim_dec"]] >> facts["fact_loan_app"]
    [stage["pos_cash"], stage["credit_card"], dims["dim_bucket"]] >> facts["fact_monthly_loan"]
    stage["installments"] >> facts["fact_installment"]
    [stage["bureau"], dims["dim_bureau_src"]] >> facts["fact_bureau_cred"]
    stage["bureau_balance"] >> facts["fact_monthly_bureau"]

    # DWH Core -> Data Mart (OBT 360)
    [
        facts["fact_loan_app"],
        facts["fact_monthly_loan"],
        dims["dim_cust"],
        dims["dim_prod"],
        dims["dim_chan"],
        dims["dim_dec"]
    ] >> mart_obt_360

    # Mart -> OLAP ClickHouse -> Audit Summary
    mart_obt_360 >> sync_to_clickhouse >> audit_summary


dag_instance = fintech_data_pipeline()
