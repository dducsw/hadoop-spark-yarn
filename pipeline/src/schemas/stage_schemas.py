"""Spark SQL DDL Schema definitions for Stage layer (hive.stage_credit_risk)."""

STAGE_DB_NAME = "stage_credit_risk"
STAGE_HDFS_BASE = "/stage/credit_risk"

# SCD Type 4: Current Customer Dimension (SCD1)
STAGE_DIM_CUSTOMER_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_customer (
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
LOCATION '{STAGE_HDFS_BASE}/dim_customer'
"""

# SCD Type 4: Historical Customer Snapshot Log
STAGE_DIM_CUSTOMER_HISTORY_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_customer_history (
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
LOCATION '{STAGE_HDFS_BASE}/dim_customer_history'
"""

STAGE_DIM_LOAN_PRODUCT_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_loan_product (
    name_contract_type STRING,
    portfolio_category STRING,
    product_group STRING,
    is_revolving BOOLEAN,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/dim_loan_product'
"""

STAGE_DIM_MERCHANT_CHANNEL_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_merchant_channel (
    channel_type STRING,
    name_type_suite STRING,
    name_goods_category STRING,
    name_seller_industry STRING,
    name_yield_group STRING,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/dim_merchant_channel'
"""

STAGE_DIM_DELINQUENCY_BUCKET_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_delinquency_bucket (
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
LOCATION '{STAGE_HDFS_BASE}/dim_delinquency_bucket'
"""

STAGE_DIM_APPLICATION_DECISION_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_application_decision (
    name_contract_status STRING,
    code_reject_reason STRING,
    name_client_type STRING,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/dim_application_decision'
"""

STAGE_DIM_RELATIVE_TIME_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_dim_relative_time (
    sk_time_key INT,
    relative_day_offset INT,
    relative_month_offset INT,
    relative_period_bucket STRING,
    vintage_cohort_offset STRING,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/dim_relative_time'
"""

STAGE_FACT_LOAN_APPLICATION_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_fact_loan_application (
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
LOCATION '{STAGE_HDFS_BASE}/fact_loan_application'
"""

STAGE_FACT_INSTALLMENT_PAYMENT_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_fact_installment_payment (
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
LOCATION '{STAGE_HDFS_BASE}/fact_installment_payment'
"""

STAGE_FACT_MONTHLY_LOAN_SNAPSHOT_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_fact_monthly_loan_snapshot (
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
LOCATION '{STAGE_HDFS_BASE}/fact_monthly_loan_snapshot'
"""

STAGE_FACT_BUREAU_CREDIT_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_fact_bureau_credit (
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
LOCATION '{STAGE_HDFS_BASE}/fact_bureau_credit'
"""

STAGE_FACT_MONTHLY_BUREAU_SNAPSHOT_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_fact_monthly_bureau_snapshot (
    sk_id_bureau INT,
    relative_month_offset INT,
    bureau_status_raw STRING,
    is_closed INT,
    is_overdue INT,
    _source_table STRING,
    _staged_at TIMESTAMP
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/fact_monthly_bureau_snapshot'
"""

STAGE_ALL_DDLS = [
    STAGE_DIM_CUSTOMER_DDL,
    STAGE_DIM_CUSTOMER_HISTORY_DDL,
    STAGE_DIM_LOAN_PRODUCT_DDL,
    STAGE_DIM_MERCHANT_CHANNEL_DDL,
    STAGE_DIM_DELINQUENCY_BUCKET_DDL,
    STAGE_DIM_APPLICATION_DECISION_DDL,
    STAGE_DIM_RELATIVE_TIME_DDL,
    STAGE_FACT_LOAN_APPLICATION_DDL,
    STAGE_FACT_INSTALLMENT_PAYMENT_DDL,
    STAGE_FACT_MONTHLY_LOAN_SNAPSHOT_DDL,
    STAGE_FACT_BUREAU_CREDIT_DDL,
    STAGE_FACT_MONTHLY_BUREAU_SNAPSHOT_DDL,
]
