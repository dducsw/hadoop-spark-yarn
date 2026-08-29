#!/usr/bin/env python3
"""Stage Job: Build stage_fact_loan_application from applications & previous_application."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, FloatType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageFactLoanApplicationJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="fact_loan_application",
            primary_key=None,
            dedup_cols=["sk_id_curr", "sk_id_prev", "is_current_application"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )
        self.raw_train_path = os.path.join(raw_base_dir, "application_train")
        self.raw_test_path = os.path.join(raw_base_dir, "application_test")
        self.raw_prev_path = os.path.join(raw_base_dir, "previous_application")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts and aligns schema across current train/test and historical applications."""
        df_train = (
            spark.read.parquet(self.raw_train_path)
            .withColumn("sk_id_prev", F.lit(None).cast(IntegerType()))
            .withColumn("is_current_application", F.lit(True))
            .withColumn("AMT_APPLICATION", F.col("AMT_CREDIT"))
            .withColumn("AMT_DOWN_PAYMENT", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("RATE_DOWN_PAYMENT", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("RATE_INTEREST_PRIMARY", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("_source_table", F.lit("raw_credit_risk.application_train"))
        )

        df_test = (
            spark.read.parquet(self.raw_test_path)
            .withColumn("sk_id_prev", F.lit(None).cast(IntegerType()))
            .withColumn("TARGET", F.lit(None).cast(IntegerType()))
            .withColumn("is_current_application", F.lit(True))
            .withColumn("AMT_APPLICATION", F.col("AMT_CREDIT"))
            .withColumn("AMT_DOWN_PAYMENT", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("RATE_DOWN_PAYMENT", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("RATE_INTEREST_PRIMARY", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("_source_table", F.lit("raw_credit_risk.application_test"))
        )

        df_prev = (
            spark.read.parquet(self.raw_prev_path)
            .withColumn("TARGET", F.lit(None).cast(IntegerType()))
            .withColumn("is_current_application", F.lit(False))
            .withColumn("EXT_SOURCE_1", F.lit(None).cast(FloatType()))
            .withColumn("EXT_SOURCE_2", F.lit(None).cast(FloatType()))
            .withColumn("EXT_SOURCE_3", F.lit(None).cast(FloatType()))
            .withColumn("_source_table", F.lit("raw_credit_risk.previous_application"))
        )

        cols = [
            "SK_ID_CURR",
            "sk_id_prev",
            "NAME_CONTRACT_TYPE",
            "AMT_APPLICATION",
            "AMT_CREDIT",
            "AMT_ANNUITY",
            "AMT_GOODS_PRICE",
            "AMT_DOWN_PAYMENT",
            "RATE_DOWN_PAYMENT",
            "RATE_INTEREST_PRIMARY",
            "EXT_SOURCE_1",
            "EXT_SOURCE_2",
            "EXT_SOURCE_3",
            "TARGET",
            "is_current_application",
            "_source_table",
        ]

        df_curr = (
            df_train.select([c for c in cols if c in df_train.columns])
            .unionByName(df_test.select([c for c in cols if c in df_test.columns]), allowMissingColumns=True)
        )

        df_prev_sub = (
            df_prev
            .withColumnRenamed("SK_ID_PREV", "sk_id_prev")
            .select([c for c in cols if c in df_prev.columns or c == "sk_id_prev"])
        )

        return df_curr.unionByName(df_prev_sub, allowMissingColumns=True)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2), rates to Decimal(8,6), and flags."""
        return (
            df
            .withColumn("sk_id_curr", F.col("SK_ID_CURR").cast(IntegerType()))
            .withColumn("sk_id_prev", F.col("sk_id_prev").cast(IntegerType()))
            .withColumn("name_contract_type", F.trim(F.col("NAME_CONTRACT_TYPE")))
            .withColumn("amt_application", F.col("AMT_APPLICATION").cast(DecimalType(18, 2)))
            .withColumn("amt_credit", F.col("AMT_CREDIT").cast(DecimalType(18, 2)))
            .withColumn("amt_annuity", F.col("AMT_ANNUITY").cast(DecimalType(18, 2)))
            .withColumn("amt_goods_price", F.col("AMT_GOODS_PRICE").cast(DecimalType(18, 2)))
            .withColumn("amt_down_payment", F.col("AMT_DOWN_PAYMENT").cast(DecimalType(18, 2)))
            .withColumn("rate_down_payment", F.col("RATE_DOWN_PAYMENT").cast(DecimalType(8, 6)))
            .withColumn("rate_interest_primary", F.col("RATE_INTEREST_PRIMARY").cast(DecimalType(8, 6)))
            .withColumn("ext_source_1", F.col("EXT_SOURCE_1").cast(FloatType()))
            .withColumn("ext_source_2", F.col("EXT_SOURCE_2").cast(FloatType()))
            .withColumn("ext_source_3", F.col("EXT_SOURCE_3").cast(FloatType()))
            .withColumn("target_default_flag", F.col("TARGET").cast(IntegerType()))
            .select(
                "sk_id_curr",
                "sk_id_prev",
                "name_contract_type",
                "amt_application",
                "amt_credit",
                "amt_annuity",
                "amt_goods_price",
                "amt_down_payment",
                "rate_down_payment",
                "rate_interest_primary",
                "ext_source_1",
                "ext_source_2",
                "ext_source_3",
                "target_default_flag",
                "is_current_application",
                "_source_table",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_fact_loan_application")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageFactLoanApplicationJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
