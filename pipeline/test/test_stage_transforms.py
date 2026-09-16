#!/usr/bin/env python3
"""
Unit tests for Stage job transformations.
Runnable via pytest and standard unittest without external cluster services.
"""
import os
import sys
import unittest
from decimal import Decimal

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PIPELINE_DIR = os.path.join(PROJECT_DIR, "pipeline")
sys.path.extend([PROJECT_DIR, PIPELINE_DIR])

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, FloatType, DoubleType


class TestStageTransforms(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("unit_tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_amt_credit_cast_to_decimal(self):
        """amt_credit must be Decimal(18,2) - financial accuracy."""
        raw = self.spark.createDataFrame([{
            "SK_ID_CURR": "1", "TARGET": "0",
            "AMT_CREDIT": "150000.555", "AMT_INCOME_TOTAL": "75000.0",
            "AMT_ANNUITY": "9000.0", "AMT_GOODS_PRICE": "135000.0",
            "NAME_CONTRACT_TYPE": "Cash loans", "CODE_GENDER": "M",
            "FLAG_OWN_CAR": "N", "FLAG_OWN_REALTY": "Y",
            "CNT_CHILDREN": "0", "NAME_INCOME_TYPE": "Working",
            "NAME_EDUCATION_TYPE": "Higher education",
            "NAME_FAMILY_STATUS": "Married", "NAME_HOUSING_TYPE": "House / apartment",
            "OCCUPATION_TYPE": "Laborers", "ORGANIZATION_TYPE": "Business Entity Type 3",
            "DAYS_BIRTH": "-9461", "DAYS_EMPLOYED": "-637",
            "CNT_FAM_MEMBERS": "1", "EXT_SOURCE_1": "0.08",
            "EXT_SOURCE_2": "0.26", "EXT_SOURCE_3": "0.13",
        }])
        from src.jobs.stage.stage_application_train import StageApplicationTrainJob
        job = StageApplicationTrainJob()
        result = job.clean_and_cast(raw)
        self.assertEqual(result.schema["amt_credit"].dataType, DecimalType(18, 2))

    def test_ext_source_is_float_not_decimal(self):
        """ext_source_* are risk scores - FloatType is appropriate."""
        raw = self.spark.createDataFrame([{
            "SK_ID_CURR": "1", "TARGET": "0",
            "AMT_CREDIT": "100000", "AMT_INCOME_TOTAL": "50000",
            "AMT_ANNUITY": "5000", "AMT_GOODS_PRICE": "90000",
            "NAME_CONTRACT_TYPE": "Cash loans", "CODE_GENDER": "F",
            "FLAG_OWN_CAR": "Y", "FLAG_OWN_REALTY": "N",
            "CNT_CHILDREN": "1", "NAME_INCOME_TYPE": "Commercial associate",
            "NAME_EDUCATION_TYPE": "Secondary", "NAME_FAMILY_STATUS": "Single / not married",
            "NAME_HOUSING_TYPE": "Rented apartment", "OCCUPATION_TYPE": "Core staff",
            "ORGANIZATION_TYPE": "School", "DAYS_BIRTH": "-12000",
            "DAYS_EMPLOYED": "-1200", "CNT_FAM_MEMBERS": "1",
            "EXT_SOURCE_1": "0.712", "EXT_SOURCE_2": "0.501", "EXT_SOURCE_3": "0.398",
        }])
        from src.jobs.stage.stage_application_train import StageApplicationTrainJob
        job = StageApplicationTrainJob()
        result = job.clean_and_cast(raw)
        self.assertEqual(result.schema["ext_source_2"].dataType, FloatType())

    def test_null_pk_filtered_and_dedup(self):
        """Null primary keys are excluded from clean output and duplicates are eliminated."""
        raw = self.spark.createDataFrame([
            {"SK_ID_CURR": None, "TARGET": "0", "AMT_CREDIT": "100000",
             "AMT_INCOME_TOTAL": "50000", "AMT_ANNUITY": "5000", "AMT_GOODS_PRICE": "90000",
             "NAME_CONTRACT_TYPE": "X", "CODE_GENDER": "M", "FLAG_OWN_CAR": "N",
             "FLAG_OWN_REALTY": "N", "CNT_CHILDREN": "0", "NAME_INCOME_TYPE": "X",
             "NAME_EDUCATION_TYPE": "X", "NAME_FAMILY_STATUS": "X",
             "NAME_HOUSING_TYPE": "X", "OCCUPATION_TYPE": "X", "ORGANIZATION_TYPE": "X",
             "DAYS_BIRTH": "-9000", "DAYS_EMPLOYED": "-500", "CNT_FAM_MEMBERS": "1",
             "EXT_SOURCE_1": "0.1", "EXT_SOURCE_2": "0.2", "EXT_SOURCE_3": "0.3"},
            {"SK_ID_CURR": "2", "TARGET": "1", "AMT_CREDIT": "200000",
             "AMT_INCOME_TOTAL": "80000", "AMT_ANNUITY": "10000", "AMT_GOODS_PRICE": "180000",
             "NAME_CONTRACT_TYPE": "Cash loans", "CODE_GENDER": "F", "FLAG_OWN_CAR": "Y",
             "FLAG_OWN_REALTY": "Y", "CNT_CHILDREN": "0", "NAME_INCOME_TYPE": "Working",
             "NAME_EDUCATION_TYPE": "Higher education", "NAME_FAMILY_STATUS": "Married",
             "NAME_HOUSING_TYPE": "House / apartment", "OCCUPATION_TYPE": "Managers",
             "ORGANIZATION_TYPE": "Government", "DAYS_BIRTH": "-11000",
             "DAYS_EMPLOYED": "-2000", "CNT_FAM_MEMBERS": "2",
             "EXT_SOURCE_1": "0.5", "EXT_SOURCE_2": "0.6", "EXT_SOURCE_3": "0.7"},
            {"SK_ID_CURR": "2", "TARGET": "1", "AMT_CREDIT": "200000",
             "AMT_INCOME_TOTAL": "80000", "AMT_ANNUITY": "10000", "AMT_GOODS_PRICE": "180000",
             "NAME_CONTRACT_TYPE": "Cash loans", "CODE_GENDER": "F", "FLAG_OWN_CAR": "Y",
             "FLAG_OWN_REALTY": "Y", "CNT_CHILDREN": "0", "NAME_INCOME_TYPE": "Working",
             "NAME_EDUCATION_TYPE": "Higher education", "NAME_FAMILY_STATUS": "Married",
             "NAME_HOUSING_TYPE": "House / apartment", "OCCUPATION_TYPE": "Managers",
             "ORGANIZATION_TYPE": "Government", "DAYS_BIRTH": "-11000",
             "DAYS_EMPLOYED": "-2000", "CNT_FAM_MEMBERS": "2",
             "EXT_SOURCE_1": "0.5", "EXT_SOURCE_2": "0.6", "EXT_SOURCE_3": "0.7"},
        ])
        from src.jobs.stage.stage_application_train import StageApplicationTrainJob
        job = StageApplicationTrainJob()
        # Mock route_quarantine in memory for unit test without HDFS
        job.route_quarantine = lambda df: None
        result = job.transform(raw)

        self.assertEqual(result.filter("sk_id_curr is null").count(), 0)
        self.assertEqual(result.count(), 1)
        self.assertIn("_processed_at", result.columns)
        self.assertIn("_batch_id", result.columns)

    def test_reconciliation_math(self):
        """Financial reconciliation math check."""
        stage_sum = Decimal("15000000.50")
        obt_sum = Decimal("15000000.50")
        diff = abs(stage_sum - obt_sum)
        pct = float(diff) / float(stage_sum) * 100.0
        self.assertLessEqual(pct, 0.01)

        obt_bad = Decimal("14000000.00")
        diff_bad = abs(stage_sum - obt_bad)
        pct_bad = float(diff_bad) / float(stage_sum) * 100.0
        self.assertGreater(pct_bad, 0.01)


if __name__ == "__main__":
    unittest.main()
