#!/usr/bin/env python3
"""Stage Job: Build stage_fact_installment_payment from installments_payments."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageFactInstallmentPaymentJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="fact_installment_payment",
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
        self.raw_path = os.path.join(raw_base_dir, "installments_payments")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts installment payment transactions."""
        return spark.read.parquet(self.raw_path)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2), computes payment delays, and flags shortfalls."""
        return (
            df
            .withColumn("sk_id_prev", F.col("SK_ID_PREV").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("SK_ID_CURR").cast(IntegerType()))
            .withColumn("num_instalment_version", F.col("NUM_INSTALMENT_VERSION").cast(IntegerType()))
            .withColumn("num_instalment_number", F.col("NUM_INSTALMENT_NUMBER").cast(IntegerType()))
            .withColumn("days_instalment", F.col("DAYS_INSTALMENT").cast(IntegerType()))
            .withColumn("days_entry_payment", F.col("DAYS_ENTRY_PAYMENT").cast(IntegerType()))
            .withColumn("amt_instalment", F.col("AMT_INSTALMENT").cast(DecimalType(18, 2)))
            .withColumn("amt_payment", F.coalesce(F.col("AMT_PAYMENT"), F.lit(0)).cast(DecimalType(18, 2)))
            # Derived shortfall & delay metrics
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
            .select(
                "sk_id_prev",
                "sk_id_curr",
                "num_instalment_version",
                "num_instalment_number",
                "days_instalment",
                "days_entry_payment",
                "amt_instalment",
                "amt_payment",
                "amt_underpayment",
                "payment_delay_days",
                "is_late_payment",
                "is_underpaid",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_fact_installment_payment")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageFactInstallmentPaymentJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
