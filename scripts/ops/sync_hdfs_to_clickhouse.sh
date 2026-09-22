#!/usr/bin/env bash
# ==============================================================================
# Script: sync_hdfs_to_clickhouse.sh
# Purpose: Native ClickHouse Ingestion from HDFS for obt_loan_portfolio_360 (Zero Spark)
# ==============================================================================
set -e

CH_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASS="${CLICKHOUSE_PASSWORD:-clickhouse123}"
HDFS_SRC_PATH="hdfs://master:9000/curated/credit_risk/obt_loan_portfolio_360/*.parquet"

ch_exec() {
  curl -s -S -f -u "${CH_USER}:${CH_PASS}" "http://${CH_HOST}:${CH_PORT}/" --data-binary "$1"
}

echo "======================================================================"
echo " NATIVE SYNC: HDFS (${HDFS_SRC_PATH}) -> CLICKHOUSE (analytics.obt_loan_portfolio_360)"
echo "======================================================================"

# 1. Create Database
echo "[1/4] Ensuring Database 'analytics' exists in ClickHouse..."
ch_exec "CREATE DATABASE IF NOT EXISTS analytics;"

# 2. Create Target MergeTree Table
echo "[2/4] Ensuring MergeTree table 'obt_loan_portfolio_360' exists in ClickHouse..."
ch_exec "
CREATE TABLE IF NOT EXISTS analytics.obt_loan_portfolio_360 (
    sk_id_curr Int32 DEFAULT 0,
    sk_id_prev Nullable(Int32),
    is_current_application Bool DEFAULT false,
    target_default_flag Nullable(Int32),
    code_gender String DEFAULT '',
    flag_own_car String DEFAULT '',
    flag_own_realty String DEFAULT '',
    cnt_children Int32 DEFAULT 0,
    cnt_fam_members Int32 DEFAULT 0,
    amt_income_total Decimal(18, 2) DEFAULT 0,
    income_bracket String DEFAULT '',
    name_income_type String DEFAULT '',
    name_education_type String DEFAULT '',
    name_family_status String DEFAULT '',
    name_housing_type String DEFAULT '',
    occupation_type String DEFAULT '',
    organization_type String DEFAULT '',
    age_years Int32 DEFAULT 0,
    employed_years Int32 DEFAULT 0,
    name_contract_type String DEFAULT '',
    portfolio_category String DEFAULT '',
    product_group String DEFAULT '',
    is_revolving Bool DEFAULT false,
    channel_type String DEFAULT '',
    name_goods_category String DEFAULT '',
    name_seller_industry String DEFAULT '',
    name_yield_group String DEFAULT '',
    name_contract_status String DEFAULT '',
    code_reject_reason String DEFAULT '',
    name_client_type String DEFAULT '',
    amt_application Decimal(18, 2) DEFAULT 0,
    amt_credit Decimal(18, 2) DEFAULT 0,
    amt_annuity Decimal(18, 2) DEFAULT 0,
    amt_goods_price Decimal(18, 2) DEFAULT 0,
    amt_down_payment Decimal(18, 2) DEFAULT 0,
    rate_down_payment Decimal(8, 6) DEFAULT 0,
    rate_interest_primary Decimal(8, 6) DEFAULT 0,
    ext_source_1 Float32 DEFAULT 0,
    ext_source_2 Float32 DEFAULT 0,
    ext_source_3 Float32 DEFAULT 0,
    latest_balance Decimal(18, 2) DEFAULT 0,
    latest_credit_limit Decimal(18, 2) DEFAULT 0,
    latest_utilization_ratio Decimal(8, 6) DEFAULT 0,
    latest_dpd Int32 DEFAULT 0,
    latest_contract_status String DEFAULT '',
    latest_snapshot_month Int32 DEFAULT 0,
    _source_table String DEFAULT '',
    _curated_at Nullable(DateTime64(9)),
    synced_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(synced_at)
ORDER BY (sk_id_curr, coalesce(sk_id_prev, 0));
"

# Also ensure Staging table exists with identical schema
ch_exec "CREATE TABLE IF NOT EXISTS analytics.obt_loan_portfolio_360_staging AS analytics.obt_loan_portfolio_360;"

# 3. Ingest into Staging table & verify before Atomic Swap
echo "[3/4] Ingesting HDFS Parquet into staging table (production table stays live)..."
ch_exec "TRUNCATE TABLE analytics.obt_loan_portfolio_360_staging;"

ch_exec "
INSERT INTO analytics.obt_loan_portfolio_360_staging (
    sk_id_curr, sk_id_prev, is_current_application, target_default_flag,
    code_gender, flag_own_car, flag_own_realty, cnt_children, cnt_fam_members, amt_income_total,
    income_bracket,
    name_income_type, name_education_type, name_family_status, name_housing_type, occupation_type,
    organization_type, age_years, employed_years, name_contract_type, portfolio_category,
    product_group, is_revolving, channel_type, name_goods_category, name_seller_industry,
    name_yield_group, name_contract_status, code_reject_reason, name_client_type,
    amt_application, amt_credit, amt_annuity, amt_goods_price, amt_down_payment,
    rate_down_payment, rate_interest_primary, ext_source_1, ext_source_2, ext_source_3,
    latest_balance, latest_credit_limit, latest_utilization_ratio, latest_dpd,
    latest_contract_status, latest_snapshot_month, _source_table, _curated_at
)
SELECT
    sk_id_curr, sk_id_prev, is_current_application, target_default_flag,
    code_gender, flag_own_car, flag_own_realty, cnt_children, cnt_fam_members, amt_income_total,
    income_bracket,
    name_income_type, name_education_type, name_family_status, name_housing_type, occupation_type,
    organization_type, age_years, employed_years, name_contract_type, portfolio_category,
    product_group, is_revolving, channel_type, name_goods_category, name_seller_industry,
    name_yield_group, name_contract_status, code_reject_reason, name_client_type,
    amt_application, amt_credit, amt_annuity, amt_goods_price, amt_down_payment,
    rate_down_payment, rate_interest_primary, ext_source_1, ext_source_2, ext_source_3,
    latest_balance, latest_credit_limit, latest_utilization_ratio, latest_dpd,
    latest_contract_status, latest_snapshot_month, _source_table, _curated_at
FROM hdfs('${HDFS_SRC_PATH}', 'Parquet')
SETTINGS input_format_null_as_default = 1;
"

# Validate staging row count before committing swap
STAGING_COUNT=$(ch_exec "SELECT count() FROM analytics.obt_loan_portfolio_360_staging;" | tr -d '[:space:]')
echo ">>> Staging row count verified: ${STAGING_COUNT}"

if [ -z "$STAGING_COUNT" ] || [ "$STAGING_COUNT" -eq 0 ]; then
    echo ">>> [ERROR] Staging table contains 0 records! Aborting swap to protect production data."
    exit 1
fi

echo ">>> Performing zero-downtime atomic swap (EXCHANGE TABLES)..."
ch_exec "EXCHANGE TABLES analytics.obt_loan_portfolio_360 AND analytics.obt_loan_portfolio_360_staging;"

# 4. Verify loaded records
echo "[4/4] Ingestion and atomic swap finished! Summary stats from ClickHouse:"
ch_exec "
SELECT
    count() AS total_rows,
    countIf(target_default_flag = 1) AS default_count,
    round(countIf(target_default_flag = 1) * 100.0 / count(), 2) AS default_rate_pct,
    round(avg(amt_credit), 2) AS avg_credit_amount,
    max(synced_at) AS last_synced_at
FROM analytics.obt_loan_portfolio_360
FORMAT Vertical;
"
echo "======================================================================"
