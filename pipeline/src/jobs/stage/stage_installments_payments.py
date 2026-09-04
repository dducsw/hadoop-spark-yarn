#!/usr/bin/env python3
"""Stage Job: Clean, deduplicate, and cast installments_payments into stage_installments_payments."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageInstallmentsPaymentsJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="installments_payments",
            primary_key=None,
            dedup_cols=[
                "sk_id_prev",
                "num_instalment_version",
                "num_instalment_number",
                "days_instalment",
            ],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2), derives payment delays & underpayments, filters nulls."""
        df_renamed = df
        for col in df.columns:
            df_renamed = df_renamed.withColumnRenamed(col, col.lower())

        df_cleaned = (
            df_renamed
            .filter(F.col("sk_id_prev").isNotNull() & F.col("num_instalment_number").isNotNull())
            .withColumn("sk_id_prev", F.col("sk_id_prev").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("sk_id_curr").cast(IntegerType()))
            .withColumn("num_instalment_version", F.col("num_instalment_version").cast(IntegerType()))
            .withColumn("num_instalment_number", F.col("num_instalment_number").cast(IntegerType()))
            .withColumn("days_instalment", F.col("days_instalment").cast(IntegerType()))
            .withColumn("days_entry_payment", F.col("days_entry_payment").cast(IntegerType()))
            .withColumn("amt_instalment", F.col("amt_instalment").cast(DecimalType(18, 2)))
            .withColumn("amt_payment", F.coalesce(F.col("amt_payment"), F.lit(0)).cast(DecimalType(18, 2)))
            # Derived fields
            .withColumn(
                "amt_underpayment",
                F.when(F.col("amt_instalment") > F.col("amt_payment"), F.col("amt_instalment") - F.col("amt_payment"))
                .otherwise(F.lit(0))
                .cast(DecimalType(18, 2)),
            )
            .withColumn(
                "payment_delay_days",
                (F.col("days_entry_payment") - F.col("days_instalment")).cast(IntegerType()),
            )
            .withColumn(
                "is_late_payment",
                F.when(F.col("payment_delay_days") > 0, F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "is_underpaid",
                F.when(F.col("amt_underpayment") > 0, F.lit(1)).otherwise(F.lit(0)),
            )
        )
        return super().transform(df_cleaned)


def main():
    parser = argparse.ArgumentParser(description="Build stage_installments_payments")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageInstallmentsPaymentsJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
