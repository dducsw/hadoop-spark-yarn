#!/usr/bin/env python3
"""Stage Job: Clean, deduplicate, and cast bureau into stage_bureau."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageBureauJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="bureau",
            primary_key="sk_id_bureau",
            dedup_cols=["sk_id_bureau"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2), normalizes snake_case, trims strings, and filters null PKs."""
        df_renamed = df
        for col in df.columns:
            df_renamed = df_renamed.withColumnRenamed(col, col.lower())

        df_cleaned = (
            df_renamed
            .withColumn("sk_id_bureau", F.col("sk_id_bureau").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("sk_id_curr").cast(IntegerType()))
            .withColumn("credit_active", F.trim(F.coalesce(F.col("credit_active"), F.lit("Unknown"))))
            .withColumn("credit_currency", F.trim(F.coalesce(F.col("credit_currency"), F.lit("currency 1"))))
            .withColumn("days_credit", F.col("days_credit").cast(IntegerType()))
            .withColumn("credit_day_overdue", F.col("credit_day_overdue").cast(IntegerType()))
            .withColumn("days_credit_enddate", F.col("days_credit_enddate").cast(IntegerType()))
            .withColumn("days_enddate_fact", F.col("days_enddate_fact").cast(IntegerType()))
            .withColumn("amt_credit_max_overdue", F.col("amt_credit_max_overdue").cast(DecimalType(18, 2)))
            .withColumn("cnt_credit_prolong", F.col("cnt_credit_prolong").cast(IntegerType()))
            .withColumn("amt_credit_sum", F.col("amt_credit_sum").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_sum_debt", F.col("amt_credit_sum_debt").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_sum_limit", F.col("amt_credit_sum_limit").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_sum_overdue", F.col("amt_credit_sum_overdue").cast(DecimalType(18, 2)))
            .withColumn("credit_type", F.trim(F.coalesce(F.col("credit_type"), F.lit("Unknown"))))
            .withColumn("days_credit_update", F.col("days_credit_update").cast(IntegerType()))
            .withColumn("amt_annuity", F.col("amt_annuity").cast(DecimalType(18, 2)))
        )
        return super().transform(df_cleaned)


def main():
    parser = argparse.ArgumentParser(description="Build stage_bureau")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageBureauJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
