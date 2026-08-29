-- ====================================================================
-- RAW LAYER DDL SCHEMAS (hive.raw_credit_risk)
-- ====================================================================

CREATE DATABASE IF NOT EXISTS raw_credit_risk;

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_application_train
USING PARQUET
LOCATION '/raw/credit_risk/application_train';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_application_test
USING PARQUET
LOCATION '/raw/credit_risk/application_test';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_bureau
USING PARQUET
LOCATION '/raw/credit_risk/bureau';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_bureau_balance
USING PARQUET
LOCATION '/raw/credit_risk/bureau_balance';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_pos_cash_balance
USING PARQUET
LOCATION '/raw/credit_risk/pos_cash_balance';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_credit_card_balance
USING PARQUET
LOCATION '/raw/credit_risk/credit_card_balance';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_previous_application
USING PARQUET
LOCATION '/raw/credit_risk/previous_application';

CREATE TABLE IF NOT EXISTS raw_credit_risk.raw_installments_payments
USING PARQUET
LOCATION '/raw/credit_risk/installments_payments';
