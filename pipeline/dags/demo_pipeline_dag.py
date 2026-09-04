#!/usr/bin/env python3
"""
Demo Airflow DAG: Big Data Medallion Pipeline Lifecycle
Orchestrates Raw Ingestion -> Stage Transformation -> Curated Feature Mart -> ClickHouse OLAP Serving
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def log_step_info(step_name: str, **kwargs):
    execution_date = kwargs.get("ds", datetime.now().strftime("%Y-%m-%d"))
    print(f"=== [AIRFLOW PIPELINE] Executing: {step_name} | Date: {execution_date} ===")


with DAG(
    dag_id="demo_bigdata_pipeline",
    default_args=default_args,
    description="Lightweight Demo DAG for Hadoop-Spark-Hive-ClickHouse Pipeline",
    schedule=None,  # Manual trigger for demo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["demo", "fintech", "lakehouse"],
) as dag:

    # 1. Connectivity & Cluster Health Check
    check_cluster_health = BashOperator(
        task_id="check_cluster_health",
        bash_command="""
            echo "Checking connectivity to Big Data cluster services..."
            curl -s -I http://master:9870 | head -n 1 || echo "NameNode Pinged"
            curl -s -I http://master:8088 | head -n 1 || echo "ResourceManager Pinged"
            curl -s http://clickhouse:8123/ping || echo "ClickHouse Pinged"
            echo "Cluster health check completed!"
        """,
    )

    # 2. Raw Ingest (Bronze Layer)
    task_raw_ingest = PythonOperator(
        task_id="demo_raw_ingest",
        python_callable=log_step_info,
        op_kwargs={"step_name": "Ingesting Raw Data into HDFS (/raw/credit_risk/*)"},
    )

    # 3. Stage Cleaning & Deduplication (Silver Layer)
    task_stage_processing = PythonOperator(
        task_id="demo_stage_processing",
        python_callable=log_step_info,
        op_kwargs={"step_name": "Cleaning, type casting & deduplicating source tables in Hive (stage_credit_risk)"},
    )

    # 4. Curated Marts & ML Features (Gold Layer)
    task_curated_analytics = PythonOperator(
        task_id="demo_curated_analytics",
        python_callable=log_step_info,
        op_kwargs={"step_name": "Building Conformed Dim/Fact (Kimball), OBT 360, Vintage Curves & ABT Features (credit_risk)"},
    )

    # 5. ClickHouse OLAP Export (Serving Layer - Native HDFS Ingestion)
    task_clickhouse_serving = BashOperator(
        task_id="demo_clickhouse_serving",
        bash_command="bash /opt/airflow/scripts/ops/sync_hdfs_to_clickhouse.sh \n",
    )

    # DAG Dependency Flow
    check_cluster_health >> task_raw_ingest >> task_stage_processing >> task_curated_analytics >> task_clickhouse_serving
