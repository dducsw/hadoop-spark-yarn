#!/usr/bin/env python3
"""Stage Job: Clean, deduplicate, and cast POS_CASH_balance into stage_pos_cash_balance."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StagePosCashBalanceJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="pos_cash_balance",
            primary_key=None,
            dedup_cols=["sk_id_prev", "months_balance"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Casts counts & offsets to int, trims contract status, filters null composite keys."""
        df_renamed = df
        for col in df.columns:
            df_renamed = df_renamed.withColumnRenamed(col, col.lower())

        df_cleaned = (
            df_renamed
            .filter(F.col("sk_id_prev").isNotNull() & F.col("months_balance").isNotNull())
            .withColumn("sk_id_prev", F.col("sk_id_prev").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("sk_id_curr").cast(IntegerType()))
            .withColumn("months_balance", F.col("months_balance").cast(IntegerType()))
            .withColumn("cnt_instalment", F.col("cnt_instalment").cast(IntegerType()))
            .withColumn("cnt_instalment_future", F.col("cnt_instalment_future").cast(IntegerType()))
            .withColumn("name_contract_status", F.trim(F.coalesce(F.col("name_contract_status"), F.lit("Unknown"))))
            .withColumn("sk_dpd", F.col("sk_dpd").cast(IntegerType()))
            .withColumn("sk_dpd_def", F.col("sk_dpd_def").cast(IntegerType()))
        )
        return super().transform(df_cleaned)


def main():
    parser = argparse.ArgumentParser(description="Build stage_pos_cash_balance")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StagePosCashBalanceJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
