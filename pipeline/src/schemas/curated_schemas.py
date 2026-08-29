"""Spark SQL DDL Schema definitions for Curated (Gold) layer (hive.credit_risk).

Architecture:
1. ML & Feature Store Pillar:
   - obt_credit_risk_features (Master ABT for Model Training/Scoring)
   - agg_customer_* (Domain Feature Marts)
2. BI & Executive Analytics Pillar:
   - obt_loan_portfolio_360 (Wide 360-degree Flattened Table for Self-Service BI)
   - mart_application_underwriting_funnel (Underwriting Funnel & Channel Performance)
   - mart_portfolio_credit_quality_monthly (Monthly Portfolio Outstanding & NPL Ratio)
   - mart_repayment_collection_performance (Collection Discipline & Loss Shortfall)
   - agg_portfolio_delinquency_roll_rate (Delinquency Migration Roll-Rate Matrix)
   - agg_vintage_loss_curves (Cumulative Vintage Loss Curves by Cohort & MOB)
"""

CURATED_DB_NAME = "credit_risk"
CURATED_HDFS_BASE = "/curated/credit_risk"

# ==============================================================================
# PILLAR 1: MACHINE LEARNING & FEATURE STORE
# ==============================================================================

CURATED_OBT_CREDIT_RISK_FEATURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.obt_credit_risk_features (
    sk_id_curr INT,
    sk_customer_key BIGINT,
    target_default_flag INT,
    is_test_set BOOLEAN,
    code_gender STRING,
    flag_own_car STRING,
    flag_own_realty STRING,
    cnt_children INT,
    cnt_fam_members INT,
    amt_income_total DECIMAL(18,2),
    amt_credit DECIMAL(18,2),
    amt_annuity DECIMAL(18,2),
    amt_goods_price DECIMAL(18,2),
    name_contract_type STRING,
    name_income_type STRING,
    name_education_type STRING,
    name_family_status STRING,
    name_housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    age_years INT,
    employed_years INT,
    income_credit_ratio DECIMAL(8,6),
    annuity_income_ratio DECIMAL(8,6),
    credit_goods_price_ratio DECIMAL(8,6),
    credit_term_months INT,
    ext_source_1 FLOAT,
    ext_source_2 FLOAT,
    ext_source_3 FLOAT,
    ext_sources_mean FLOAT,
    ext_sources_std FLOAT,
    ext_sources_min FLOAT,
    ext_sources_max FLOAT,
    bureau_total_loans_count INT,
    bureau_active_loans_count INT,
    bureau_closed_loans_count INT,
    bureau_total_credit_sum DECIMAL(18,2),
    bureau_total_debt_sum DECIMAL(18,2),
    bureau_total_overdue_sum DECIMAL(18,2),
    bureau_max_overdue_amount DECIMAL(18,2),
    bureau_debt_credit_ratio DECIMAL(8,6),
    bureau_overdue_debt_ratio DECIMAL(8,6),
    bureau_monthly_delinquent_ratio DECIMAL(8,6),
    bureau_prolong_count_total INT,
    prev_total_applications_count INT,
    prev_approved_count INT,
    prev_refused_count INT,
    prev_canceled_count INT,
    prev_refusal_rate DECIMAL(8,6),
    prev_total_credit_applied DECIMAL(18,2),
    prev_total_credit_approved DECIMAL(18,2),
    prev_approval_amount_ratio DECIMAL(8,6),
    inst_total_repayments_count INT,
    inst_late_payments_count INT,
    inst_late_payment_ratio DECIMAL(8,6),
    inst_underpaid_payments_count INT,
    inst_total_underpayment_amt DECIMAL(18,2),
    inst_max_payment_delay_days INT,
    inst_mean_payment_delay_days FLOAT,
    inst_actual_scheduled_ratio_mean DECIMAL(8,6),
    pos_total_contracts_count INT,
    pos_delinquent_snapshots_count INT,
    pos_max_dpd INT,
    pos_mean_dpd FLOAT,
    cc_total_contracts_count INT,
    cc_total_drawings_amount DECIMAL(18,2),
    cc_mean_utilization_ratio DECIMAL(8,6),
    cc_max_utilization_ratio DECIMAL(8,6),
    cc_max_dpd INT,
    _source_table STRING,
    _curated_at TIMESTAMP
)
USING PARQUET
LOCATION '{CURATED_HDFS_BASE}/obt_credit_risk_features'
"""

CURATED_AGG_CUSTOMER_BUREAU_FEATURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.agg_customer_bureau_features (
    sk_id_curr INT,
    total_loans_count INT,
    active_loans_count INT,
    closed_loans_count INT,
    total_credit_sum DECIMAL(18,2),
    total_debt_sum DECIMAL(18,2),
    total_overdue_sum DECIMAL(18,2),
    max_overdue_amount DECIMAL(18,2),
    total_prolong_count INT,
    debt_to_credit_ratio DECIMAL(8,6),
    overdue_to_debt_ratio DECIMAL(8,6),
    historical_months_tracked INT,
    delinquent_months_count INT,
    bureau_delinquency_rate DECIMAL(8,6),
    _source_table STRING,
    _curated_at TIMESTAMP
)
USING PARQUET
LOCATION '{CURATED_HDFS_BASE}/agg_customer_bureau_features'
"""

CURATED_AGG_CUSTOMER_PREVIOUS_APPLICATION_FEATURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.agg_customer_previous_application_features (
    sk_id_curr INT,
    total_prev_applications INT,
    approved_applications_count INT,
    refused_applications_count INT,
    canceled_applications_count INT,
    refusal_rate DECIMAL(8,6),
    total_credit_applied DECIMAL(18,2),
    total_credit_approved DECIMAL(18,2),
    total_annuity_approved DECIMAL(18,2),
    mean_down_payment_rate DECIMAL(8,6),
    approval_credit_ratio DECIMAL(8,6),
    _source_table STRING,
    _curated_at TIMESTAMP
)
USING PARQUET
LOCATION '{CURATED_HDFS_BASE}/agg_customer_previous_application_features'
"""

CURATED_AGG_CUSTOMER_INSTALLMENT_FEATURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.agg_customer_installment_features (
    sk_id_curr INT,
    total_installments_count INT,
    late_installments_count INT,
    late_installment_rate DECIMAL(8,6),
    underpaid_installments_count INT,
    underpaid_installment_rate DECIMAL(8,6),
    total_scheduled_amount DECIMAL(18,2),
    total_actual_payment DECIMAL(18,2),
    total_underpayment_amount DECIMAL(18,2),
    max_payment_delay_days INT,
    mean_payment_delay_days FLOAT,
    payment_to_installment_ratio DECIMAL(8,6),
    _source_table STRING,
    _curated_at TIMESTAMP
)
USING PARQUET
LOCATION '{CURATED_HDFS_BASE}/agg_customer_installment_features'
"""

CURATED_AGG_CUSTOMER_MONTHLY_LOAN_FEATURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.agg_customer_monthly_loan_features (
    sk_id_curr INT,
    pos_total_snapshots INT,
    pos_delinquent_snapshots INT,
    pos_max_dpd INT,
    pos_mean_dpd FLOAT,
    cc_total_snapshots INT,
    cc_total_drawings DECIMAL(18,2),
    cc_mean_utilization DECIMAL(8,6),
    cc_max_utilization DECIMAL(8,6),
    cc_max_dpd INT,
    _source_table STRING,
    _curated_at TIMESTAMP
)
USING PARQUET
LOCATION '{CURATED_HDFS_BASE}/agg_customer_monthly_loan_features'
"""

# ==============================================================================
# PILLAR 2: BI & EXECUTIVE ANALYTICS MARTS
# ==============================================================================

# Wide 360-degree table for Self-Service BI (No JOINs required)
CURATED_OBT_LOAN_PORTFOLIO_360_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.obt_loan_portfolio_360 (
    -- Application & Target Keys
    sk_id_curr INT,
    sk_id_prev INT,
    is_current_application BOOLEAN,
    target_default_flag INT,

    -- Customer Profile Attributes (Text & Groupings)
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

    -- Product & Portfolio Attributes
    name_contract_type STRING,
    portfolio_category STRING,
    product_group STRING,
    is_revolving BOOLEAN,

    -- Merchant & Distribution Channel
    channel_type STRING,
    name_goods_category STRING,
    name_seller_industry STRING,
    name_yield_group STRING,

    -- Underwriting Decision Status
    name_contract_status STRING,
    code_reject_reason STRING,
    name_client_type STRING,

    -- Financial Amounts & Metrics
    amt_application DECIMAL(18,2),
    amt_credit DECIMAL(18,2),
    amt_annuity DECIMAL(18,2),
    amt_goods_price DECIMAL(18,2),
    amt_down_payment DECIMAL(18,2),
    rate_down_payment DECIMAL(8,6),
    rate_interest_primary DECIMAL(8,6),

    -- External Risk Scores
    ext_source_1 FLOAT,
    ext_source_2 FLOAT,
    ext_source_3 FLOAT,

    -- Lineage Metadata
    _source_table STRING,
    _curated_at TIMESTAMP
)
USING PARQUET
LOCATION '{CURATED_HDFS_BASE}/obt_loan_portfolio_360'
"""

# Underwriting Funnel & Channel Conversion Mart
CURATED_MART_APPLICATION_UNDERWRITING_FUNNEL_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.mart_application_underwriting_funnel (
    product_group STRING,
    channel_type STRING,
    name_contract_status STRING,
    name_client_type STRING,
    total_applications INT,
    total_applied_amount DECIMAL(18,2),
    total_approved_amount DECIMAL(18,2),
    approval_rate DECIMAL(8,6),
    refusal_rate DECIMAL(8,6),
    avg_credit_amount DECIMAL(18,2),
    _source_table STRING,
    _curated_at TIMESTAMP,
    vintage_quarter STRING
)
USING PARQUET
PARTITIONED BY (vintage_quarter)
LOCATION '{CURATED_HDFS_BASE}/mart_application_underwriting_funnel'
"""

# Monthly Portfolio Balance & NPL Ratio Mart
CURATED_MART_PORTFOLIO_CREDIT_QUALITY_MONTHLY_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.mart_portfolio_credit_quality_monthly (
    portfolio_category STRING,
    product_group STRING,
    bucket_code STRING,
    bucket_name STRING,
    is_npl BOOLEAN,
    active_contracts_count INT,
    total_principal_balance DECIMAL(18,2),
    total_overdue_balance DECIMAL(18,2),
    npl_balance_ratio DECIMAL(8,6),
    mean_utilization_ratio DECIMAL(8,6),
    _source_table STRING,
    _curated_at TIMESTAMP,
    relative_month_offset INT
)
USING PARQUET
PARTITIONED BY (relative_month_offset)
LOCATION '{CURATED_HDFS_BASE}/mart_portfolio_credit_quality_monthly'
"""

# Repayment Collection Performance & Cash Shortfall Mart
CURATED_MART_REPAYMENT_COLLECTION_PERFORMANCE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.mart_repayment_collection_performance (
    product_group STRING,
    name_yield_group STRING,
    scheduled_installments_count INT,
    scheduled_amount DECIMAL(18,2),
    collected_amount DECIMAL(18,2),
    collection_efficiency_ratio DECIMAL(8,6),
    late_payments_count INT,
    late_payment_volume_pct DECIMAL(8,6),
    underpayment_loss_amount DECIMAL(18,2),
    _source_table STRING,
    _curated_at TIMESTAMP,
    relative_month_offset INT
)
USING PARQUET
PARTITIONED BY (relative_month_offset)
LOCATION '{CURATED_HDFS_BASE}/mart_repayment_collection_performance'
"""

# Delinquency Roll-Rate Transition Matrix Mart
CURATED_AGG_PORTFOLIO_DELINQUENCY_ROLL_RATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.agg_portfolio_delinquency_roll_rate (
    loan_source_system STRING,
    product_group STRING,
    prior_dpd_bucket STRING,
    current_dpd_bucket STRING,
    active_accounts_count INT,
    total_balance DECIMAL(18,2),
    total_overdue_balance DECIMAL(18,2),
    roll_rate_percentage DECIMAL(8,6),
    _source_table STRING,
    _curated_at TIMESTAMP,
    relative_month_offset INT
)
USING PARQUET
PARTITIONED BY (relative_month_offset)
LOCATION '{CURATED_HDFS_BASE}/agg_portfolio_delinquency_roll_rate'
"""

# Cumulative Vintage Default Loss Curves Mart
CURATED_AGG_VINTAGE_LOSS_CURVES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CURATED_DB_NAME}.agg_vintage_loss_curves (
    product_group STRING,
    months_on_book INT,
    disbursed_loan_count INT,
    total_disbursed_amount DECIMAL(18,2),
    cumulative_default_count INT,
    cumulative_default_amount DECIMAL(18,2),
    cumulative_default_rate DECIMAL(8,6),
    _source_table STRING,
    _curated_at TIMESTAMP,
    origination_cohort STRING
)
USING PARQUET
PARTITIONED BY (origination_cohort)
LOCATION '{CURATED_HDFS_BASE}/agg_vintage_loss_curves'
"""

CURATED_ALL_DDLS = [
    # ML & Feature Store Pillar
    CURATED_OBT_CREDIT_RISK_FEATURES_DDL,
    CURATED_AGG_CUSTOMER_BUREAU_FEATURES_DDL,
    CURATED_AGG_CUSTOMER_PREVIOUS_APPLICATION_FEATURES_DDL,
    CURATED_AGG_CUSTOMER_INSTALLMENT_FEATURES_DDL,
    CURATED_AGG_CUSTOMER_MONTHLY_LOAN_FEATURES_DDL,

    # BI & Executive Analytics Pillar
    CURATED_OBT_LOAN_PORTFOLIO_360_DDL,
    CURATED_MART_APPLICATION_UNDERWRITING_FUNNEL_DDL,
    CURATED_MART_PORTFOLIO_CREDIT_QUALITY_MONTHLY_DDL,
    CURATED_MART_REPAYMENT_COLLECTION_PERFORMANCE_DDL,
    CURATED_AGG_PORTFOLIO_DELINQUENCY_ROLL_RATE_DDL,
    CURATED_AGG_VINTAGE_LOSS_CURVES_DDL,
]
