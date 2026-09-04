#!/usr/bin/env bash
set -e

export PYTHONPATH="/pipeline:${PYTHONPATH}"

echo "======================================================================"
echo " Starting Full Fintech Data Pipeline on YARN Cluster"
echo " Master: yarn | Deploy Mode: client"
echo " Time: $(date -u)"
echo "======================================================================"

run_yarn_job() {
    local layer=$1
    local script=$2
    local full_path="/pipeline/src/jobs/${layer}/${script}"
    echo ""
    echo "----------------------------------------------------------------------"
    echo ">>> [${layer^^}] Running on YARN: ${script}"
    echo "----------------------------------------------------------------------"
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.yarn.maxAppAttempts=1 \
        --conf spark.sql.shuffle.partitions=4 \
        --conf spark.default.parallelism=4 \
        "${full_path}"
    echo ">>> Completed: ${script}"
}

echo ""
echo "=== STEP 1: RAW INGESTION (Bronze Layer) ==="
RAW_JOBS=(
    "ingest_application_train.py"
    "ingest_application_test.py"
    "ingest_bureau.py"
    "ingest_bureau_balance.py"
    "ingest_pos_cash_balance.py"
    "ingest_credit_card_balance.py"
    "ingest_installments_payments.py"
    "ingest_previous_application.py"
)
for job in "${RAW_JOBS[@]}"; do
    run_yarn_job "raw" "$job"
done

echo ""
echo "=== STEP 2: STAGE CLEANING & CASTING (Silver Layer) ==="
STAGE_JOBS=(
    "stage_application_train.py"
    "stage_application_test.py"
    "stage_bureau.py"
    "stage_bureau_balance.py"
    "stage_pos_cash_balance.py"
    "stage_credit_card_balance.py"
    "stage_installments_payments.py"
    "stage_previous_application.py"
)
for job in "${STAGE_JOBS[@]}"; do
    run_yarn_job "stage" "$job"
done

echo ""
echo "=== STEP 3: CURATED DIMENSIONS (Gold Layer - Conformed Dims) ==="
CURATED_DIMS=(
    "curated_dim_delinquency_bucket.py"
    "curated_dim_customer.py"
    "curated_dim_loan_product.py"
    "curated_dim_merchant_channel.py"
    "curated_dim_application_decision.py"
    "curated_dim_relative_time.py"
    "curated_dim_bureau_source.py"
)
for job in "${CURATED_DIMS[@]}"; do
    run_yarn_job "curated" "$job"
done

echo ""
echo "=== STEP 4: CURATED FACTS (Gold Layer - Constellation Facts) ==="
CURATED_FACTS=(
    "curated_fact_loan_application.py"
    "curated_fact_monthly_loan_snapshot.py"
    "curated_fact_installment_payment.py"
    "curated_fact_bureau_credit.py"
    "curated_fact_monthly_bureau_snapshot.py"
)
for job in "${CURATED_FACTS[@]}"; do
    run_yarn_job "curated" "$job"
done

echo ""
echo "=== STEP 5: CURATED BI MARTS & OBT 360 ==="
run_yarn_job "curated" "curated_obt_loan_portfolio_360.py"

echo ""
echo "======================================================================"
echo "ALL PIPELINE JOBS COMPLETED ON YARN SUCCESSFULLY!"
echo " Finished at: $(date -u)"
echo "======================================================================"