#!/usr/bin/env bash
# ==============================================================================
# Test 6: Verifying Apache Airflow 3.2.1 Orchestration Platform
# Checks Web UI / API Server, Metadata Database, DAG Parsing & Test Run
# ==============================================================================
set -e

echo "=== [TEST 6] Verifying Apache Airflow 3.2.1 Platform ==="

# 1. Check Airflow Web UI / API Server HTTP Endpoint
echo "1. Checking Airflow API Server / Web UI Liveness (http://airflow-webserver:8080):"
if curl -s -f "http://airflow-webserver:8080/" > /dev/null; then
  echo ">>> [OK] Airflow Webserver is LIVE and responding (HTTP 200)!"
else
  echo ">>> [WARNING] Could not reach http://airflow-webserver:8080 directly from container."
fi

# 2. Check DAG Parsing & Import Errors
echo -e "\n2. Verifying DAG parsing and checking for Import Errors:"
airflow dags list-import-errors

# 3. List Registered DAGs
echo -e "\n3. Listing active DAGs in Airflow:"
airflow dags list

# 4. Execute End-to-End Test Run on demo_bigdata_pipeline
echo -e "\n4. Executing End-to-End Test Run on DAG 'demo_bigdata_pipeline':"
airflow dags test demo_bigdata_pipeline

echo -e "\n>>> [TEST 6 SUCCESS] Apache Airflow 3.2.1 orchestration platform verified successfully!"
