#!/usr/bin/env python3
"""Stage Job: Clean, deduplicate, and cast bureau_balance into stage_bureau_balance."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageBureauBalanceJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="bureau_balance",
            primary_key=None,
            dedup_cols=["sk_id_bureau", "months_balance"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Casts offsets to int, trims status code, and filters null composite keys."""
        df_renamed = df
        for col in df.columns:
            df_renamed = df_renamed.withColumnRenamed(col, col.lower())

        df_cleaned = (
            df_renamed
            .filter(F.col("sk_id_bureau").isNotNull() & F.col("months_balance").isNotNull())
            .withColumn("sk_id_bureau", F.col("sk_id_bureau").cast(IntegerType()))
            .withColumn("months_balance", F.col("months_balance").cast(IntegerType()))
            .withColumn("status", F.trim(F.coalesce(F.col("status"), F.lit("X"))))
        )
        return super().transform(df_cleaned)


def main():
    parser = argparse.ArgumentParser(description="Build stage_bureau_balance")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageBureauBalanceJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
