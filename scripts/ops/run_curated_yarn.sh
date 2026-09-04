#!/usr/bin/env bash
set -e

export PYTHONPATH="/pipeline:${PYTHONPATH}"

echo "======================================================================"
echo " Running Remaining Curated Layer Jobs on YARN Cluster"
echo " Master: yarn | Deploy Mode: client"
echo " Time: $(date -u)"
echo "======================================================================"

run_curated_job() {
    local script=$1
    local full_path="/pipeline/src/jobs/curated/${script}"
    echo ""
    echo "----------------------------------------------------------------------"
    echo ">>> [CURATED] Running on YARN: ${script}"
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
echo "=== STEP 1: CONSTELLATION FACTS ==="
REMAINING_FACTS=(
    "curated_fact_monthly_loan_snapshot.py"
    "curated_fact_installment_payment.py"
    "curated_fact_bureau_credit.py"
    "curated_fact_monthly_bureau_snapshot.py"
)
for job in "${REMAINING_FACTS[@]}"; do
    run_curated_job "$job"
done

echo ""
echo "=== STEP 2: OBT 360 ==="
run_curated_job "curated_obt_loan_portfolio_360.py"

echo ""
echo "======================================================================"
echo "ALL CURATED JOBS COMPLETED ON YARN SUCCESSFULLY!"
echo " Finished at: $(date -u)"
echo "======================================================================"