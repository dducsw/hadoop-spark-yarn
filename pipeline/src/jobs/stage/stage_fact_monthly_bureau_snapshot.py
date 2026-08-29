#!/usr/bin/env python3
"""Stage Job: Build stage_fact_monthly_bureau_snapshot from bureau_balance."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageFactMonthlyBureauSnapshotJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="fact_monthly_bureau_snapshot",
            primary_key=None,
            dedup_cols=["sk_id_bureau", "relative_month_offset"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )
        self.raw_path = os.path.join(raw_base_dir, "bureau_balance")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts monthly external bureau payment status history."""
        return spark.read.parquet(self.raw_path)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Standardizes status codes and generates closed/overdue flags."""
        return (
            df
            .withColumn("sk_id_bureau", F.col("SK_ID_BUREAU").cast(IntegerType()))
            .withColumn("relative_month_offset", F.col("MONTHS_BALANCE").cast(IntegerType()))
            .withColumn("bureau_status_raw", F.trim(F.coalesce(F.col("STATUS"), F.lit("X"))))
            .withColumn(
                "is_closed",
                F.when(F.col("bureau_status_raw") == "C", F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "is_overdue",
                F.when(F.col("bureau_status_raw").isin(["1", "2", "3", "4", "5"]), F.lit(1)).otherwise(F.lit(0)),
            )
            .select(
                "sk_id_bureau",
                "relative_month_offset",
                "bureau_status_raw",
                "is_closed",
                "is_overdue",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_fact_monthly_bureau_snapshot")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageFactMonthlyBureauSnapshotJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
