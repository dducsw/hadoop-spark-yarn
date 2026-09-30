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

# Absolute path to spark-submit. Non-interactive SSH sessions do not inherit the
# Docker image PATH, so relying on `spark-submit` being on PATH is unsafe.
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-$(command -v spark-submit 2>/dev/null || echo /opt/spark/bin/spark-submit)}"

# Environment required to submit against YARN. Again, sshd strips the container
# ENV, so these must be exported explicitly on the remote side.
RUNTIME_ENV="export PYTHONPATH=/pipeline && \
export BATCH_ID='${BATCH_ID}' && \
export HADOOP_HOME='${HADOOP_HOME:-/opt/hadoop}' && \
export SPARK_HOME='${SPARK_HOME:-/opt/spark}' && \
export HADOOP_CONF_DIR='${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}' && \
export YARN_CONF_DIR='${YARN_CONF_DIR:-/opt/hadoop/etc/hadoop}'"

# Forward credentials / connection settings that the jobs read via os.environ.
# Only variables that are actually set are exported, so a missing secret never
# injects an empty value into the remote session.
FORWARD_VARS="JDBC_URL JDBC_USER JDBC_PASSWORD POSTGRES_PASSWORD CLICKHOUSE_PASSWORD \
METADATA_PG_HOST METADATA_PG_PORT METADATA_PG_DB METADATA_PG_USER METADATA_PG_PASSWORD \
QUARANTINE_BASE_DIR PII_TOKENIZATION_SALT"
for _var in ${FORWARD_VARS}; do
    _val="${!_var:-}"
    if [ -n "${_val}" ]; then
        RUNTIME_ENV="${RUNTIME_ENV} && export ${_var}='${_val}'"
    fi
done

SUBMIT_CMD="${SPARK_SUBMIT_BIN} --master yarn --deploy-mode ${DEPLOY_MODE} --queue ${YARN_QUEUE} ${SPARK_EXTRA_ARGS} ${JOB_PATH}"

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
    export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
    export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
    export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"
    export YARN_CONF_DIR="${YARN_CONF_DIR:-/opt/hadoop/etc/hadoop}"
    ${SUBMIT_CMD}
# 2. Remote dispatch over SSH to master container (standard network execution)
elif command -v ssh > /dev/null 2>&1; then
    echo ">>> Dispatching Spark job to master node over SSH..."
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 ${SSH_IDENTITY_OPTS} root@master \
        "${RUNTIME_ENV} && ${SUBMIT_CMD}"
# 3. Fallback: Local dev Docker socket dispatch if available
elif command -v docker > /dev/null 2>&1 && [ -S /var/run/docker.sock ]; then
    echo ">>> Dispatching Spark job to master node via docker exec..."
    docker exec master bash -c "${RUNTIME_ENV} && ${SUBMIT_CMD}"
else
    echo ">>> [ERROR] Neither spark-submit, ssh, nor docker socket available to execute job."
    exit 1
fi

echo ">>> [COMPLETED] ${SCRIPT} finished successfully."
