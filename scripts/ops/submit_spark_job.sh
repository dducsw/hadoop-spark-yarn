#!/usr/bin/env bash
# ==============================================================================
# Script: submit_spark_job.sh
# Purpose: Unified Spark job submitter for Airflow & CLI across cluster
# ==============================================================================
set -e

LAYER="$1"
SCRIPT="$2"
BATCH_ID="${3:-${BATCH_ID:-batch_manual}}"

if [ -z "$LAYER" ] || [ -z "$SCRIPT" ]; then
    echo "Usage: $0 <layer> <script_name> [batch_id]"
    echo "Example: $0 raw ingest_application_train.py batch_20260904_020000"
    exit 1
fi

JOB_PATH="/pipeline/src/jobs/${LAYER}/${SCRIPT}"

echo "======================================================================"
echo ">>> [AIRFLOW-SPARK-RUNNER] Layer: ${LAYER^^} | Script: ${SCRIPT}"
echo ">>> Batch ID: ${BATCH_ID}"
echo ">>> Target Path: ${JOB_PATH}"
echo "======================================================================"

# 1. If spark-submit is directly available (running inside master container)
if command -v spark-submit > /dev/null 2>&1; then
    export PYTHONPATH="/pipeline:${PYTHONPATH}"
    export BATCH_ID="${BATCH_ID}"
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --executor-memory 1g \
        --conf spark.executor.memoryOverhead=384m \
        --conf spark.yarn.maxAppAttempts=1 \
        --conf spark.sql.shuffle.partitions=4 \
        --conf spark.default.parallelism=4 \
        "${JOB_PATH}"
# 2. If running from Airflow container with mounted docker socket (Docker-native)
elif command -v docker > /dev/null 2>&1 && [ -S /var/run/docker.sock ]; then
    echo ">>> Dispatching Spark job to master node via docker exec..."
    docker exec master bash -c \
        "export PYTHONPATH=/pipeline && export BATCH_ID=${BATCH_ID} && spark-submit --master yarn --deploy-mode client --executor-memory 1g --conf spark.executor.memoryOverhead=384m --conf spark.yarn.maxAppAttempts=1 --conf spark.sql.shuffle.partitions=4 --conf spark.default.parallelism=4 ${JOB_PATH}"
# 3. Fallback: Dispatch over SSH if available
elif command -v ssh > /dev/null 2>&1; then
    echo ">>> Dispatching Spark job to master node over SSH..."
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 root@master \
        "export PYTHONPATH=/pipeline && export BATCH_ID=${BATCH_ID} && spark-submit --master yarn --deploy-mode client --executor-memory 1g --conf spark.executor.memoryOverhead=384m --conf spark.yarn.maxAppAttempts=1 --conf spark.sql.shuffle.partitions=4 --conf spark.default.parallelism=4 ${JOB_PATH}"
else
    echo ">>> [ERROR] Neither docker socket nor ssh available to reach master node."
    exit 1
fi

echo ">>> [COMPLETED] ${SCRIPT} finished successfully."
