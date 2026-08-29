#!/usr/bin/env python3
"""Stage Job: Build stage_dim_delinquency_bucket master lookup."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageDimDelinquencyBucketJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="dim_delinquency_bucket",
            primary_key="bucket_code",
            dedup_cols=["bucket_code"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
            source_table="lookup.standard_risk_rules",
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Constructs standardized delinquency aging buckets."""
        schema = StructType(
            [
                StructField("sk_dpd_bucket_key", IntegerType(), False),
                StructField("bucket_code", StringType(), False),
                StructField("bucket_name", StringType(), False),
                StructField("dpd_min", IntegerType(), False),
                StructField("dpd_max", IntegerType(), False),
                StructField("is_npl", BooleanType(), False),
            ]
        )

        buckets = [
            (0, "B0", "Current / 0 DPD", 0, 0, False),
            (1, "B1", "1-30 DPD", 1, 30, False),
            (2, "B2", "31-60 DPD", 31, 60, False),
            (3, "B3", "61-90 DPD", 61, 90, False),
            (4, "B4", "91-120 DPD", 91, 120, True),
            (5, "B5", "121-150 DPD", 121, 150, True),
            (6, "NPL", "150+ DPD / NPL", 151, 99999, True),
        ]

        return spark.createDataFrame(buckets, schema=schema)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        return df


def main():
    parser = argparse.ArgumentParser(description="Build stage_dim_delinquency_bucket")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageDimDelinquencyBucketJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
