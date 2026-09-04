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
    sk_id_curr Nullable(Int32),
    sk_id_prev Nullable(Int32),
    is_current_application Nullable(Bool),
    target_default_flag Nullable(Int32),
    code_gender Nullable(String),
    flag_own_car Nullable(String),
    flag_own_realty Nullable(String),
    cnt_children Nullable(Int32),
    cnt_fam_members Nullable(Int32),
    amt_income_total Nullable(Decimal(18, 2)),
    name_income_type Nullable(String),
    name_education_type Nullable(String),
    name_family_status Nullable(String),
    name_housing_type Nullable(String),
    occupation_type Nullable(String),
    organization_type Nullable(String),
    age_years Nullable(Int32),
    employed_years Nullable(Int32),
    name_contract_type Nullable(String),
    portfolio_category Nullable(String),
    product_group Nullable(String),
    is_revolving Nullable(Bool),
    channel_type Nullable(String),
    name_goods_category Nullable(String),
    name_seller_industry Nullable(String),
    name_yield_group Nullable(String),
    name_contract_status Nullable(String),
    code_reject_reason Nullable(String),
    name_client_type Nullable(String),
    amt_application Nullable(Decimal(18, 2)),
    amt_credit Nullable(Decimal(18, 2)),
    amt_annuity Nullable(Decimal(18, 2)),
    amt_goods_price Nullable(Decimal(18, 2)),
    amt_down_payment Nullable(Decimal(18, 2)),
    rate_down_payment Nullable(Decimal(8, 6)),
    rate_interest_primary Nullable(Decimal(8, 6)),
    ext_source_1 Nullable(Float32),
    ext_source_2 Nullable(Float32),
    ext_source_3 Nullable(Float32),
    latest_balance Nullable(Decimal(18, 2)),
    latest_credit_limit Nullable(Decimal(18, 2)),
    latest_utilization_ratio Nullable(Decimal(8, 6)),
    latest_dpd Nullable(Int32),
    latest_contract_status Nullable(String),
    latest_snapshot_month Nullable(Int32),
    _source_table Nullable(String),
    _curated_at Nullable(DateTime64(9)),
    synced_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (assumeNotNull(sk_id_curr), coalesce(sk_id_prev, 0));
"

# 3. Truncate for idempotent batch refresh and perform native HDFS ingestion
echo "[3/4] Truncating table for idempotent load and inserting from HDFS Parquet..."
ch_exec "TRUNCATE TABLE analytics.obt_loan_portfolio_360;"

ch_exec "
INSERT INTO analytics.obt_loan_portfolio_360 (
    sk_id_curr, sk_id_prev, is_current_application, target_default_flag,
    code_gender, flag_own_car, flag_own_realty, cnt_children, cnt_fam_members, amt_income_total,
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
    name_income_type, name_education_type, name_family_status, name_housing_type, occupation_type,
    organization_type, age_years, employed_years, name_contract_type, portfolio_category,
    product_group, is_revolving, channel_type, name_goods_category, name_seller_industry,
    name_yield_group, name_contract_status, code_reject_reason, name_client_type,
    amt_application, amt_credit, amt_annuity, amt_goods_price, amt_down_payment,
    rate_down_payment, rate_interest_primary, ext_source_1, ext_source_2, ext_source_3,
    latest_balance, latest_credit_limit, latest_utilization_ratio, latest_dpd,
    latest_contract_status, latest_snapshot_month, _source_table, _curated_at
FROM hdfs('${HDFS_SRC_PATH}', 'Parquet');
"

# 4. Verify loaded records
echo "[4/4] Ingestion finished! Summary stats from ClickHouse:"
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
