#!/usr/bin/env bash
set -e

TABLE=${1:-"all"}
INPUT_DIR=${2:-"/data/home-credit-default-risk"}
OUTPUT_DIR=${3:-"/raw/credit_risk"}

echo "=== Ingesting Home Credit Risk Data to HDFS Raw Layer ==="
echo "Target table : $TABLE"
echo "Input Dir    : $INPUT_DIR"
echo "Output Dir   : $OUTPUT_DIR"

run_spark_job() {
  local job_script=$1
  echo ""
  echo ">>> Running spark-submit for: $job_script"
  spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1G \
    --executor-memory 1G \
    --executor-cores 1 \
    "/pipeline/src/jobs/raw/${job_script}" \
    --input-dir "$INPUT_DIR" \
    --output-dir "$OUTPUT_DIR"
}

if [ "$TABLE" = "all" ]; then
  TABLES=(
    "ingest_application_train.py"
    "ingest_application_test.py"
    "ingest_bureau.py"
    "ingest_bureau_balance.py"
    "ingest_pos_cash_balance.py"
    "ingest_credit_card_balance.py"
    "ingest_installments_payments.py"
    "ingest_previous_application.py"
  )
  for t in "${TABLES[@]}"; do
    run_spark_job "$t"
  done
else
  if [[ "$TABLE" != *.py ]]; then
    SCRIPT_NAME="ingest_${TABLE}.py"
  else
    SCRIPT_NAME="$TABLE"
  fi
  run_spark_job "$SCRIPT_NAME"
fi

echo ""
echo "=== All Ingestion Jobs Completed Successfully ==="
