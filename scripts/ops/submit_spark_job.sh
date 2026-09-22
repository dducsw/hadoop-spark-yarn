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

DEPLOY_MODE="${SPARK_DEPLOY_MODE:-client}"
YARN_QUEUE="${SPARK_YARN_QUEUE:-etl}"
SPARK_EXTRA_ARGS="${SPARK_EXTRA_ARGS:-}"

SUBMIT_CMD="spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --queue ${YARN_QUEUE} ${SPARK_EXTRA_ARGS} ${JOB_PATH}"

# Detect SSH Identity key if available
SSH_IDENTITY_OPTS=""
if [ -f /config/ssh/id_rsa ]; then
    SSH_IDENTITY_OPTS="-i /config/ssh/id_rsa"
elif [ -f /home/airflow/.ssh/id_rsa ]; then
    SSH_IDENTITY_OPTS="-i /home/airflow/.ssh/id_rsa"
fi

# 1. If spark-submit is directly available (running inside master container)
if command -v spark-submit > /dev/null 2>&1; then
    export PYTHONPATH="/pipeline:${PYTHONPATH}"
    export BATCH_ID="${BATCH_ID}"
    ${SUBMIT_CMD}
# 2. Remote dispatch over SSH to master container (standard network execution)
elif command -v ssh > /dev/null 2>&1; then
    echo ">>> Dispatching Spark job to master node over SSH..."
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 ${SSH_IDENTITY_OPTS} root@master \
        "export PYTHONPATH=/pipeline && export BATCH_ID=${BATCH_ID} && ${SUBMIT_CMD}"
# 3. Fallback: Local dev Docker socket dispatch if available
elif command -v docker > /dev/null 2>&1 && [ -S /var/run/docker.sock ]; then
    echo ">>> Dispatching Spark job to master node via docker exec..."
    docker exec master bash -c \
        "export PYTHONPATH=/pipeline && export BATCH_ID=${BATCH_ID} && ${SUBMIT_CMD}"
else
    echo ">>> [ERROR] Neither spark-submit, ssh, nor docker socket available to execute job."
    exit 1
fi

echo ">>> [COMPLETED] ${SCRIPT} finished successfully."
