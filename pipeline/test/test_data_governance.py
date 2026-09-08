#!/usr/bin/env python3
"""Unit tests for Data Governance P0: PII Security & BaseStageJob DLQ Quarantine routing."""
import os
import shutil
import sys
import tempfile
import unittest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PIPELINE_DIR = os.path.join(PROJECT_DIR, "pipeline")
for p in [PIPELINE_DIR, PROJECT_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from src.common.security import mask_string, tokenize_id, mask_income_bracket
from src.common.base_stage_job import BaseStageJob


class TestDataGovernance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("TestDataGovernance")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.test_dir = tempfile.mkdtemp(prefix="dg_test_")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir, ignore_errors=True)

    def test_pii_security_masking(self):
        """Verify mask_string preserves prefix/suffix and masks middle characters."""
        df = self.spark.sql("""
            SELECT '0912345678' AS phone UNION ALL
            SELECT '12' AS phone UNION ALL
            SELECT CAST(NULL AS STRING) AS phone
        """)
        df_masked = df.withColumn("phone_masked", mask_string(F.col("phone"), keep_prefix=1, keep_suffix=1, mask_char="*"))
        rows = {r["phone"]: r["phone_masked"] for r in df_masked.collect()}

        self.assertEqual(rows["0912345678"], "0****8")
        self.assertEqual(rows["12"], "****")
        self.assertIsNone(rows[None])

    def test_pii_tokenization(self):
        """Verify tokenize_id provides deterministic salted SHA-256 tokens."""
        df = self.spark.sql("""
            SELECT 10001 AS sk_id_curr UNION ALL
            SELECT 10002 AS sk_id_curr
        """)
        df_token = df.withColumn("token", tokenize_id(F.col("sk_id_curr"), salt="test_salt"))
        rows = df_token.collect()

        self.assertEqual(len(rows[0]["token"]), 64)  # SHA-256 hex length
        self.assertNotEqual(rows[0]["token"], rows[1]["token"])

    def test_income_bracket_generalization(self):
        """Verify mask_income_bracket generalizes numbers into discrete brackets."""
        df = self.spark.sql("""
            SELECT 30000.0 AS income UNION ALL
            SELECT 75000.0 AS income UNION ALL
            SELECT 150000.0 AS income UNION ALL
            SELECT 350000.0 AS income UNION ALL
            SELECT 800000.0 AS income UNION ALL
            SELECT CAST(NULL AS DOUBLE) AS income
        """)
        df_bracket = df.withColumn("bracket", mask_income_bracket(F.col("income")))
        rows = [r["bracket"] for r in df_bracket.collect()]

        self.assertEqual(rows, ["<50k", "50k-100k", "100k-200k", "200k-500k", ">=500k", "Unknown"])

    def test_dlq_quarantine_routing(self):
        """Verify BaseStageJob routes Null PKs and Duplicates to Quarantine and keeps clean records."""
        quarantine_dir = os.path.join(self.test_dir, "quarantine").replace("\\", "/")

        job = BaseStageJob(
            table_name="test_clients",
            primary_key="client_id",
            dedup_cols=["client_id"],
            quarantine_base_dir=quarantine_dir,
        )

        # Input data: 1 clean (1), 1 duplicate (1), 1 null PK (None), 1 clean (2)
        df_in = self.spark.sql("""
            SELECT 1 AS client_id, 'Alice' AS name UNION ALL
            SELECT 1 AS client_id, 'Alice_Duplicate' AS name UNION ALL
            SELECT CAST(NULL AS INT) AS client_id, 'Bob_NullPK' AS name UNION ALL
            SELECT 2 AS client_id, 'Charlie' AS name
        """)

        df_clean = job.transform(df_in)
        clean_rows = df_clean.collect()

        # Check clean records
        self.assertEqual(len(clean_rows), 2)
        clean_ids = {r["client_id"] for r in clean_rows}
        self.assertEqual(clean_ids, {1, 2})
        self.assertIn("_processed_at", df_clean.columns)
        self.assertIn("_batch_id", df_clean.columns)

        # Check quarantine records and metrics
        self.assertEqual(job.rejected_count, 2)
        quarantine_path = f"{quarantine_dir}/test_clients"
        if os.path.exists(quarantine_path):
            df_quarantine = self.spark.read.parquet(quarantine_path)
            reasons = {r["name"]: r["_reject_reason"] for r in df_quarantine.collect()}
            self.assertEqual(reasons.get("Bob_NullPK"), "NULL_PRIMARY_KEY")
            self.assertEqual(reasons.get("Alice_Duplicate"), "DUPLICATE_RECORD")


if __name__ == "__main__":
    unittest.main()
