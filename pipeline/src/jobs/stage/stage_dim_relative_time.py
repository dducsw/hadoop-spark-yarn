#!/usr/bin/env python3
"""Stage Job: Build stage_dim_relative_time master temporal offsets."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageDimRelativeTimeJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="dim_relative_time",
            primary_key="sk_time_key",
            dedup_cols=["sk_time_key"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
            source_table="system.relative_time_generator",
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Generates normalized monthly and daily relative historical offsets."""
        records = []
        # Generate offsets from 0 down to -120 months
        for m in range(0, -121, -1):
            day_offset = m * 30
            if m == 0:
                bucket = "Current / 0M"
            elif -3 <= m < 0:
                bucket = "1-3M Ago"
            elif -6 <= m < -3:
                bucket = "3-6M Ago"
            elif -12 <= m < -6:
                bucket = "6-12M Ago"
            elif -24 <= m < -12:
                bucket = "12-24M Ago"
            else:
                bucket = "24M+ Ago"

            cohort = f"M{m:+d}" if m != 0 else "M0"
            records.append((m, day_offset, m, bucket, cohort))

        schema = StructType(
            [
                StructField("sk_time_key", IntegerType(), False),
                StructField("relative_day_offset", IntegerType(), False),
                StructField("relative_month_offset", IntegerType(), False),
                StructField("relative_period_bucket", StringType(), False),
                StructField("vintage_cohort_offset", StringType(), False),
            ]
        )

        return spark.createDataFrame(records, schema=schema)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        return df


def main():
    parser = argparse.ArgumentParser(description="Build stage_dim_relative_time")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageDimRelativeTimeJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
