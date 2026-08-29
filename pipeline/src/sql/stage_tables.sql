-- ====================================================================
-- STAGE LAYER DDL SCHEMAS (hive.stage_credit_risk)
-- ====================================================================

CREATE DATABASE IF NOT EXISTS stage_credit_risk;

-- SCD Type 4: Current Customer Dimension (SCD1)
CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_customer (
    sk_id_curr INT,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children INT,
    cnt_fam_members INT,
    amt_income_total DECIMAL(18,2),
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    age_years INT,
    employed_years INT,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/dim_customer';

-- SCD Type 4: Historical Customer Profile Log
CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_customer_history (
    sk_id_curr INT,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children INT,
    cnt_fam_members INT,
    amt_income_total DECIMAL(18,2),
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    age_years INT,
    employed_years INT,
    _source_table STRING,
    _staged_at TIMESTAMP,
    snapshot_date STRING
)
USING PARQUET
PARTITIONED BY (snapshot_date)
LOCATION '/stage/credit_risk/dim_customer_history';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_loan_product (
    name_contract_type STRING,
    portfolio_category STRING,
    product_group STRING,
    is_revolving BOOLEAN,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/dim_loan_product';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_merchant_channel (
    channel_type STRING,
    name_type_suite STRING,
    name_goods_category STRING,
    name_seller_industry STRING,
    name_yield_group STRING,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/dim_merchant_channel';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_delinquency_bucket (
    sk_dpd_bucket_key INT,
    bucket_code STRING,
    bucket_name STRING,
    dpd_min INT,
    dpd_max INT,
    is_npl BOOLEAN,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/dim_delinquency_bucket';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_application_decision (
    name_contract_status STRING,
    code_reject_reason STRING,
    name_client_type STRING,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/dim_application_decision';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_dim_relative_time (
    sk_time_key INT,
    relative_day_offset INT,
    relative_month_offset INT,
    relative_period_bucket STRING,
    vintage_cohort_offset STRING,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/dim_relative_time';

-- Facts
CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_fact_loan_application (
    sk_id_curr INT,
    sk_id_prev INT,
    name_contract_type STRING,
    amt_application DECIMAL(18,2),
    amt_credit DECIMAL(18,2),
    amt_annuity DECIMAL(18,2),
    amt_goods_price DECIMAL(18,2),
    amt_down_payment DECIMAL(18,2),
    rate_down_payment DECIMAL(8,6),
    rate_interest_primary DECIMAL(8,6),
    ext_source_1 FLOAT,
    ext_source_2 FLOAT,
    ext_source_3 FLOAT,
    target_default_flag INT,
    is_current_application BOOLEAN,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/fact_loan_application';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_fact_installment_payment (
    sk_id_prev INT,
    sk_id_curr INT,
    num_instalment_version INT,
    num_instalment_number INT,
    days_instalment INT,
    days_entry_payment INT,
    amt_instalment DECIMAL(18,2),
    amt_payment DECIMAL(18,2),
    amt_underpayment DECIMAL(18,2),
    payment_delay_days INT,
    is_late_payment INT,
    is_underpaid INT,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/fact_installment_payment';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_fact_monthly_loan_snapshot (
    sk_id_prev INT,
    sk_id_curr INT,
    relative_month_offset INT,
    amt_balance DECIMAL(18,2),
    amt_credit_limit DECIMAL(18,2),
    credit_utilization_ratio DECIMAL(8,6),
    amt_drawings_current DECIMAL(18,2),
    amt_payment_current DECIMAL(18,2),
    cnt_instalment_total INT,
    cnt_instalment_future INT,
    sk_dpd INT,
    sk_dpd_def INT,
    contract_status STRING,
    _source_table STRING,
    _staged_at TIMESTAMP,
    loan_source_system STRING
)
USING PARQUET
PARTITIONED BY (loan_source_system)
LOCATION '/stage/credit_risk/fact_monthly_loan_snapshot';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_fact_bureau_credit (
    sk_id_bureau INT,
    sk_id_curr INT,
    credit_active_status STRING,
    credit_type STRING,
    days_credit INT,
    credit_day_overdue INT,
    days_credit_enddate INT,
    days_enddate_fact INT,
    cnt_credit_prolong INT,
    amt_credit_sum DECIMAL(18,2),
    amt_credit_sum_debt DECIMAL(18,2),
    amt_credit_sum_limit DECIMAL(18,2),
    amt_credit_sum_overdue DECIMAL(18,2),
    amt_credit_max_overdue DECIMAL(18,2),
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/fact_bureau_credit';

CREATE TABLE IF NOT EXISTS stage_credit_risk.stage_fact_monthly_bureau_snapshot (
    sk_id_bureau INT,
    relative_month_offset INT,
    bureau_status_raw STRING,
    is_closed INT,
    is_overdue INT,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '/stage/credit_risk/fact_monthly_bureau_snapshot';
