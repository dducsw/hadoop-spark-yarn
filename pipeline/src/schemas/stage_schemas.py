"""Spark SQL DDL Schema definitions for Stage (Silver) layer (hive.stage_credit_risk).

In accordance with Enterprise Medallion Lakehouse best practices:
Stage tables are cleaned, type-cast, deduplicated copies of source entities preserving Natural Keys.
Dimensional models (dim_*, fact_*) belong exclusively in the Curated (Gold) layer.
"""

STAGE_DB_NAME = "stage_credit_risk"
STAGE_HDFS_BASE = "/stage/credit_risk"

STAGE_APPLICATION_TRAIN_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_application_train (
    sk_id_curr INT,
    target INT,
    name_contract_type STRING,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children INT,
    amt_income_total DECIMAL(18,2),
    amt_credit DECIMAL(18,2),
    amt_annuity DECIMAL(18,2),
    amt_goods_price DECIMAL(18,2),
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    days_birth INT,
    days_employed INT,
    cnt_fam_members INT,
    ext_source_1 FLOAT,
    ext_source_2 FLOAT,
    ext_source_3 FLOAT,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/application_train'
"""

STAGE_APPLICATION_TEST_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_application_test (
    sk_id_curr INT,
    name_contract_type STRING,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children INT,
    amt_income_total DECIMAL(18,2),
    amt_credit DECIMAL(18,2),
    amt_annuity DECIMAL(18,2),
    amt_goods_price DECIMAL(18,2),
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    days_birth INT,
    days_employed INT,
    cnt_fam_members INT,
    ext_source_1 FLOAT,
    ext_source_2 FLOAT,
    ext_source_3 FLOAT,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/application_test'
"""

STAGE_BUREAU_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_bureau (
    sk_id_bureau INT,
    sk_id_curr INT,
    credit_active STRING,
    credit_currency STRING,
    days_credit INT,
    credit_day_overdue INT,
    days_credit_enddate INT,
    days_enddate_fact INT,
    amt_credit_max_overdue DECIMAL(18,2),
    cnt_credit_prolong INT,
    amt_credit_sum DECIMAL(18,2),
    amt_credit_sum_debt DECIMAL(18,2),
    amt_credit_sum_limit DECIMAL(18,2),
    amt_credit_sum_overdue DECIMAL(18,2),
    credit_type STRING,
    days_credit_update INT,
    amt_annuity DECIMAL(18,2),
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/bureau'
"""

STAGE_BUREAU_BALANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_bureau_balance (
    sk_id_bureau INT,
    months_balance INT,
    status STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/bureau_balance'
"""

STAGE_POS_CASH_BALANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_pos_cash_balance (
    sk_id_prev INT,
    sk_id_curr INT,
    months_balance INT,
    cnt_instalment INT,
    cnt_instalment_future INT,
    name_contract_status STRING,
    sk_dpd INT,
    sk_dpd_def INT,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/pos_cash_balance'
"""

STAGE_CREDIT_CARD_BALANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_credit_card_balance (
    sk_id_prev INT,
    sk_id_curr INT,
    months_balance INT,
    amt_balance DECIMAL(18,2),
    amt_credit_limit_actual DECIMAL(18,2),
    amt_drawings_atm_current DECIMAL(18,2),
    amt_drawings_current DECIMAL(18,2),
    amt_drawings_other_current DECIMAL(18,2),
    amt_drawings_pos_current DECIMAL(18,2),
    amt_inst_min_regularity DECIMAL(18,2),
    amt_payment_current DECIMAL(18,2),
    amt_payment_total_current DECIMAL(18,2),
    amt_receivable_principal DECIMAL(18,2),
    amt_recivable DECIMAL(18,2),
    amt_total_receivable DECIMAL(18,2),
    cnt_drawings_atm_current INT,
    cnt_drawings_current INT,
    cnt_drawings_other_current INT,
    cnt_drawings_pos_current INT,
    cnt_instalment_mature_cum INT,
    name_contract_status STRING,
    sk_dpd INT,
    sk_dpd_def INT,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/credit_card_balance'
"""

STAGE_PREVIOUS_APPLICATION_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_previous_application (
    sk_id_prev INT,
    sk_id_curr INT,
    name_contract_type STRING,
    amt_annuity DECIMAL(18,2),
    amt_application DECIMAL(18,2),
    amt_credit DECIMAL(18,2),
    amt_down_payment DECIMAL(18,2),
    amt_goods_price DECIMAL(18,2),
    rate_down_payment DECIMAL(8,6),
    rate_interest_primary DECIMAL(8,6),
    name_contract_status STRING,
    code_reject_reason STRING,
    name_client_type STRING,
    channel_type STRING,
    name_goods_category STRING,
    name_seller_industry STRING,
    name_yield_group STRING,
    cnt_payment INT,
    days_decision INT,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/previous_application'
"""

STAGE_INSTALLMENTS_PAYMENTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {STAGE_DB_NAME}.stage_installments_payments (
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
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{STAGE_HDFS_BASE}/installments_payments'
"""

STAGE_ALL_DDLS = [
    STAGE_APPLICATION_TRAIN_DDL,
    STAGE_APPLICATION_TEST_DDL,
    STAGE_BUREAU_DDL,
    STAGE_BUREAU_BALANCE_DDL,
    STAGE_POS_CASH_BALANCE_DDL,
    STAGE_CREDIT_CARD_BALANCE_DDL,
    STAGE_PREVIOUS_APPLICATION_DDL,
    STAGE_INSTALLMENTS_PAYMENTS_DDL,
]
