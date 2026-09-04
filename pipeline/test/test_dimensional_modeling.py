#!/usr/bin/env python3
"""Unit tests for Enterprise Dimensional Modeling: xxhash64 SK, fallback -1, and Decimal(18,2) precision."""
import os
import sys
import unittest
from decimal import Decimal

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType


class TestDimensionalModeling(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("TestDimensionalModeling")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_xxhash64_deterministic_and_idempotent(self):
        """Verify xxhash64 produces consistent, deterministic 64-bit keys without sequence locks."""
        df = self.spark.sql("""
            SELECT 100001 AS sk_id_curr UNION ALL
            SELECT 100002 AS sk_id_curr UNION ALL
            SELECT 100003 AS sk_id_curr
        """)

        df_sk1 = df.withColumn("sk_customer_key", F.xxhash64(F.col("sk_id_curr").cast("string")))
        df_sk2 = df.withColumn("sk_customer_key", F.xxhash64(F.col("sk_id_curr").cast("string")))

        rows1 = df_sk1.collect()
        rows2 = df_sk2.collect()

        self.assertEqual(len(rows1), 3)
        for r1, r2 in zip(rows1, rows2):
            self.assertEqual(r1["sk_customer_key"], r2["sk_customer_key"])
            self.assertIsInstance(r1["sk_customer_key"], int)
            self.assertEqual(df_sk1.schema["sk_customer_key"].dataType, LongType())

    def test_unknown_record_fallback_to_minus_one(self):
        """Verify unmapped/null foreign keys safely fallback to -1 without inner join data loss."""
        df_fact = self.spark.sql("""
            SELECT 101 AS sk_id_prev, 100001 AS sk_id_curr, 'Cash loans' AS name_contract_type UNION ALL
            SELECT 102 AS sk_id_prev, 999999 AS sk_id_curr, 'Cash loans' AS name_contract_type
        """)

        df_dim = self.spark.sql("""
            SELECT -1L AS sk_customer_key, -1 AS sk_id_curr, 'Unknown' AS code_gender UNION ALL
            SELECT xxhash64('100001') AS sk_customer_key, 100001 AS sk_id_curr, 'F' AS code_gender
        """)

        # Left join with broadcast and fallback -1
        df_joined = (
            df_fact.join(F.broadcast(df_dim), on="sk_id_curr", how="left")
            .withColumn("sk_customer_key", F.coalesce(F.col("sk_customer_key"), F.lit(-1).cast(LongType())))
        )

        results = {r["sk_id_curr"]: r["sk_customer_key"] for r in df_joined.collect()}
        self.assertNotEqual(results[100001], -1)
        self.assertEqual(results[999999], -1)

    def test_decimal_monetary_precision_preserved(self):
        """Verify amounts strictly retain Decimal(18,2) precision without float rounding errors."""
        df = self.spark.sql("SELECT CAST('1234567.89' AS DECIMAL(18,2)) AS amt_credit")
        row = df.collect()[0]
        self.assertEqual(row["amt_credit"], Decimal("1234567.89"))
        self.assertEqual(df.schema["amt_credit"].dataType, DecimalType(18, 2))


if __name__ == "__main__":
    unittest.main()
