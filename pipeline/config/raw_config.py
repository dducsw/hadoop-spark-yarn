"""Configuration for Home Credit Raw Ingestion."""

DATASETS = {
    "application_train": {
        "file_name": "application_train.csv",
        "raw_table": "raw_application_train",
        "primary_key": "SK_ID_CURR",
    },
    "application_test": {
        "file_name": "application_test.csv",
        "raw_table": "raw_application_test",
        "primary_key": "SK_ID_CURR",
    },
    "bureau": {
        "file_name": "bureau.csv",
        "raw_table": "raw_bureau",
        "primary_key": "SK_ID_BUREAU",
    },
    "bureau_balance": {
        "file_name": "bureau_balance.csv",
        "raw_table": "raw_bureau_balance",
        "primary_key": None,
    },
    "pos_cash_balance": {
        "file_name": "POS_CASH_balance.csv",
        "raw_table": "raw_pos_cash_balance",
        "primary_key": None,
    },
    "credit_card_balance": {
        "file_name": "credit_card_balance.csv",
        "raw_table": "raw_credit_card_balance",
        "primary_key": None,
    },
    "installments_payments": {
        "file_name": "installments_payments.csv",
        "raw_table": "raw_installments_payments",
        "primary_key": None,
    },
    "previous_application": {
        "file_name": "previous_application.csv",
        "raw_table": "raw_previous_application",
        "primary_key": "SK_ID_PREV",
    },
}

DEFAULT_BASE_INPUT_DIR = "/data/home-credit-default-risk"
DEFAULT_HDFS_RAW_DIR = "/raw/home_credit"
HIVE_RAW_DB = "raw_lakehouse"
