#!/usr/bin/env python3
"""Stage Job: Clean, deduplicate, and cast previous_application into stage_previous_application."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, FloatType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StagePreviousApplicationJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="previous_application",
            primary_key="sk_id_prev",
            dedup_cols=["sk_id_prev"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2), rates to Decimal(8,6), trims strings, and filters null PKs."""
        df_renamed = df
        for col in df.columns:
            df_renamed = df_renamed.withColumnRenamed(col, col.lower())

        df_cleaned = (
            df_renamed
            .withColumn("sk_id_prev", F.col("sk_id_prev").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("sk_id_curr").cast(IntegerType()))
            .withColumn("name_contract_type", F.trim(F.coalesce(F.col("name_contract_type"), F.lit("Unknown"))))
            .withColumn("amt_annuity", F.col("amt_annuity").cast(DecimalType(18, 2)))
            .withColumn("amt_application", F.col("amt_application").cast(DecimalType(18, 2)))
            .withColumn("amt_credit", F.col("amt_credit").cast(DecimalType(18, 2)))
            .withColumn("amt_down_payment", F.col("amt_down_payment").cast(DecimalType(18, 2)))
            .withColumn("amt_goods_price", F.col("amt_goods_price").cast(DecimalType(18, 2)))
            .withColumn("rate_down_payment", F.col("rate_down_payment").cast(DecimalType(8, 6)))
            .withColumn("rate_interest_primary", F.col("rate_interest_primary").cast(DecimalType(8, 6)))
            .withColumn("name_contract_status", F.trim(F.coalesce(F.col("name_contract_status"), F.lit("Unknown"))))
            .withColumn("code_reject_reason", F.trim(F.coalesce(F.col("code_reject_reason"), F.lit("XAP"))))
            .withColumn("name_client_type", F.trim(F.coalesce(F.col("name_client_type"), F.lit("Repeater"))))
            .withColumn("channel_type", F.trim(F.coalesce(F.col("channel_type"), F.lit("Unknown"))))
            .withColumn("name_goods_category", F.trim(F.coalesce(F.col("name_goods_category"), F.lit("XNA"))))
            .withColumn("name_seller_industry", F.trim(F.coalesce(F.col("name_seller_industry"), F.lit("XNA"))))
            .withColumn("name_yield_group", F.trim(F.coalesce(F.col("name_yield_group"), F.lit("XNA"))))
            .withColumn("cnt_payment", F.col("cnt_payment").cast(IntegerType()))
            .withColumn("days_decision", F.col("days_decision").cast(IntegerType()))
        )
        return super().transform(df_cleaned)


def main():
    parser = argparse.ArgumentParser(description="Build stage_previous_application")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StagePreviousApplicationJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
