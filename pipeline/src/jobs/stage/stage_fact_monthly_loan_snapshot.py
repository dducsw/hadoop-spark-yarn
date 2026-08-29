#!/usr/bin/env python3
"""Stage Job: Build stage_fact_monthly_loan_snapshot from pos_cash & credit_card."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageFactMonthlyLoanSnapshotJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="fact_monthly_loan_snapshot",
            primary_key=None,
            dedup_cols=["sk_id_prev", "relative_month_offset", "loan_source_system"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
            partition_by=["loan_source_system"],
        )
        self.raw_pos_path = os.path.join(raw_base_dir, "pos_cash_balance")
        self.raw_cc_path = os.path.join(raw_base_dir, "credit_card_balance")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Aligns schemas between POS Cash loans and revolving Credit Card monthly records."""
        df_pos = (
            spark.read.parquet(self.raw_pos_path)
            .withColumn("loan_source_system", F.lit("POS_CASH"))
            .withColumn("amt_balance", F.lit(0).cast(DecimalType(18, 2)))
            .withColumn("amt_credit_limit", F.lit(0).cast(DecimalType(18, 2)))
            .withColumn("amt_drawings_current", F.lit(0).cast(DecimalType(18, 2)))
            .withColumn("amt_payment_current", F.lit(0).cast(DecimalType(18, 2)))
            .withColumn("_source_table", F.lit("raw_credit_risk.pos_cash_balance"))
            .withColumnRenamed("SK_ID_PREV", "sk_id_prev")
            .withColumnRenamed("SK_ID_CURR", "sk_id_curr")
            .withColumnRenamed("MONTHS_BALANCE", "relative_month_offset")
            .withColumnRenamed("CNT_INSTALMENT", "cnt_instalment_total")
            .withColumnRenamed("CNT_INSTALMENT_FUTURE", "cnt_instalment_future")
            .withColumnRenamed("SK_DPD", "sk_dpd")
            .withColumnRenamed("SK_DPD_DEF", "sk_dpd_def")
            .withColumnRenamed("NAME_CONTRACT_STATUS", "contract_status")
        )

        df_cc = (
            spark.read.parquet(self.raw_cc_path)
            .withColumn("loan_source_system", F.lit("CREDIT_CARD"))
            .withColumn("cnt_instalment_total", F.col("CNT_INSTALMENT_MATURE_CUM"))
            .withColumn("cnt_instalment_future", F.lit(0).cast(IntegerType()))
            .withColumn("_source_table", F.lit("raw_credit_risk.credit_card_balance"))
            .withColumnRenamed("SK_ID_PREV", "sk_id_prev")
            .withColumnRenamed("SK_ID_CURR", "sk_id_curr")
            .withColumnRenamed("MONTHS_BALANCE", "relative_month_offset")
            .withColumnRenamed("AMT_BALANCE", "amt_balance")
            .withColumnRenamed("AMT_CREDIT_LIMIT_ACTUAL", "amt_credit_limit")
            .withColumnRenamed("AMT_DRAWINGS_CURRENT", "amt_drawings_current")
            .withColumnRenamed("AMT_PAYMENT_CURRENT", "amt_payment_current")
            .withColumnRenamed("SK_DPD", "sk_dpd")
            .withColumnRenamed("SK_DPD_DEF", "sk_dpd_def")
            .withColumnRenamed("NAME_CONTRACT_STATUS", "contract_status")
        )

        cols = [
            "sk_id_prev",
            "sk_id_curr",
            "relative_month_offset",
            "loan_source_system",
            "amt_balance",
            "amt_credit_limit",
            "amt_drawings_current",
            "amt_payment_current",
            "cnt_instalment_total",
            "cnt_instalment_future",
            "sk_dpd",
            "sk_dpd_def",
            "contract_status",
            "_source_table",
        ]

        return df_pos.select(cols).unionByName(df_cc.select(cols))

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2) and derives credit utilization ratio."""
        return (
            df
            .withColumn("sk_id_prev", F.col("sk_id_prev").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("sk_id_curr").cast(IntegerType()))
            .withColumn("relative_month_offset", F.col("relative_month_offset").cast(IntegerType()))
            .withColumn("amt_balance", F.col("amt_balance").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_limit", F.col("amt_credit_limit").cast(DecimalType(18, 2)))
            .withColumn("amt_drawings_current", F.col("amt_drawings_current").cast(DecimalType(18, 2)))
            .withColumn("amt_payment_current", F.col("amt_payment_current").cast(DecimalType(18, 2)))
            .withColumn("cnt_instalment_total", F.col("cnt_instalment_total").cast(IntegerType()))
            .withColumn("cnt_instalment_future", F.col("cnt_instalment_future").cast(IntegerType()))
            .withColumn("sk_dpd", F.col("sk_dpd").cast(IntegerType()))
            .withColumn("sk_dpd_def", F.col("sk_dpd_def").cast(IntegerType()))
            .withColumn("contract_status", F.trim(F.coalesce(F.col("contract_status"), F.lit("Unknown"))))
            # Derived revolving utilization ratio
            .withColumn(
                "credit_utilization_ratio",
                F.when(
                    (F.col("amt_credit_limit").isNotNull()) & (F.col("amt_credit_limit") > 0),
                    (F.col("amt_balance") / F.col("amt_credit_limit")).cast(DecimalType(8, 6)),
                ).otherwise(F.lit(0).cast(DecimalType(8, 6))),
            )
            .select(
                "sk_id_prev",
                "sk_id_curr",
                "relative_month_offset",
                "loan_source_system",
                "amt_balance",
                "amt_credit_limit",
                "credit_utilization_ratio",
                "amt_drawings_current",
                "amt_payment_current",
                "cnt_instalment_total",
                "cnt_instalment_future",
                "sk_dpd",
                "sk_dpd_def",
                "contract_status",
                "_source_table",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_fact_monthly_loan_snapshot")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageFactMonthlyLoanSnapshotJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
