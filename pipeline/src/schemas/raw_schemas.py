"""Spark SQL DDL Schema definitions for Raw layer (hive.raw_credit_risk)."""

RAW_DB_NAME = "raw_credit_risk"
RAW_HDFS_BASE = "/raw/credit_risk"

RAW_APPLICATION_TRAIN_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_application_train (
    sk_id_curr STRING,
    target STRING,
    name_contract_type STRING,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children STRING,
    amt_income_total STRING,
    amt_credit STRING,
    amt_annuity STRING,
    amt_goods_price STRING,
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    days_birth STRING,
    days_employed STRING,
    cnt_fam_members STRING,
    ext_source_1 STRING,
    ext_source_2 STRING,
    ext_source_3 STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/application_train'
"""

RAW_APPLICATION_TEST_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_application_test (
    sk_id_curr STRING,
    name_contract_type STRING,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children STRING,
    amt_income_total STRING,
    amt_credit STRING,
    amt_annuity STRING,
    amt_goods_price STRING,
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    days_birth STRING,
    days_employed STRING,
    cnt_fam_members STRING,
    ext_source_1 STRING,
    ext_source_2 STRING,
    ext_source_3 STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/application_test'
"""

RAW_BUREAU_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_bureau (
    sk_id_bureau STRING,
    sk_id_curr STRING,
    credit_active STRING,
    credit_currency STRING,
    days_credit STRING,
    credit_day_overdue STRING,
    days_credit_enddate STRING,
    days_enddate_fact STRING,
    amt_credit_max_overdue STRING,
    cnt_credit_prolong STRING,
    amt_credit_sum STRING,
    amt_credit_sum_debt STRING,
    amt_credit_sum_limit STRING,
    amt_credit_sum_overdue STRING,
    credit_type STRING,
    days_credit_update STRING,
    amt_annuity STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/bureau'
"""

RAW_BUREAU_BALANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_bureau_balance (
    sk_id_bureau STRING,
    months_balance STRING,
    status STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/bureau_balance'
"""

RAW_POS_CASH_BALANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_pos_cash_balance (
    sk_id_prev STRING,
    sk_id_curr STRING,
    months_balance STRING,
    cnt_instalment STRING,
    cnt_instalment_future STRING,
    name_contract_status STRING,
    sk_dpd STRING,
    sk_dpd_def STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/pos_cash_balance'
"""

RAW_CREDIT_CARD_BALANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_credit_card_balance (
    sk_id_prev STRING,
    sk_id_curr STRING,
    months_balance STRING,
    amt_balance STRING,
    amt_credit_limit_actual STRING,
    amt_drawings_atm_current STRING,
    amt_drawings_current STRING,
    amt_drawings_other_current STRING,
    amt_drawings_pos_current STRING,
    amt_inst_min_regularity STRING,
    amt_payment_current STRING,
    amt_payment_total_current STRING,
    amt_receivable_principal STRING,
    amt_recivable STRING,
    amt_total_receivable STRING,
    cnt_drawings_atm_current STRING,
    cnt_drawings_current STRING,
    cnt_drawings_other_current STRING,
    cnt_drawings_pos_current STRING,
    cnt_instalment_mature_cum STRING,
    name_contract_status STRING,
    sk_dpd STRING,
    sk_dpd_def STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/credit_card_balance'
"""

RAW_PREVIOUS_APPLICATION_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_previous_application (
    sk_id_prev STRING,
    sk_id_curr STRING,
    name_contract_type STRING,
    amt_annuity STRING,
    amt_application STRING,
    amt_credit STRING,
    amt_down_payment STRING,
    amt_goods_price STRING,
    rate_down_payment STRING,
    rate_interest_primary STRING,
    name_contract_status STRING,
    code_reject_reason STRING,
    name_client_type STRING,
    channel_type STRING,
    name_goods_category STRING,
    name_seller_industry STRING,
    name_yield_group STRING,
    cnt_payment STRING,
    days_decision STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/previous_application'
"""

RAW_INSTALLMENTS_PAYMENTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_DB_NAME}.raw_installments_payments (
    sk_id_prev STRING,
    sk_id_curr STRING,
    num_instalment_version STRING,
    num_instalment_number STRING,
    days_instalment STRING,
    days_entry_payment STRING,
    amt_instalment STRING,
    amt_payment STRING,
    _source_system STRING,
    _processed_at TIMESTAMP,
    _batch_id STRING
)
USING PARQUET
LOCATION '{RAW_HDFS_BASE}/installments_payments'
"""

RAW_ALL_DDLS = [
    RAW_APPLICATION_TRAIN_DDL,
    RAW_APPLICATION_TEST_DDL,
    RAW_BUREAU_DDL,
    RAW_BUREAU_BALANCE_DDL,
    RAW_POS_CASH_BALANCE_DDL,
    RAW_CREDIT_CARD_BALANCE_DDL,
    RAW_PREVIOUS_APPLICATION_DDL,
    RAW_INSTALLMENTS_PAYMENTS_DDL,
]
