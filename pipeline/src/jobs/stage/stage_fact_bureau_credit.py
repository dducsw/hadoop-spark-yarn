#!/usr/bin/env python3
"""Stage Job: Build stage_fact_bureau_credit from bureau."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageFactBureauCreditJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="fact_bureau_credit",
            primary_key="sk_id_bureau",
            dedup_cols=["sk_id_bureau"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )
        self.raw_path = os.path.join(raw_base_dir, "bureau")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts external credit bureau accounts."""
        return spark.read.parquet(self.raw_path)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Casts external debt balances and overdue amounts to Decimal(18,2)."""
        return (
            df
            .withColumn("sk_id_bureau", F.col("SK_ID_BUREAU").cast(IntegerType()))
            .withColumn("sk_id_curr", F.col("SK_ID_CURR").cast(IntegerType()))
            .withColumn("credit_active_status", F.trim(F.coalesce(F.col("CREDIT_ACTIVE"), F.lit("Unknown"))))
            .withColumn("credit_type", F.trim(F.coalesce(F.col("CREDIT_TYPE"), F.lit("Unknown"))))
            .withColumn("days_credit", F.col("DAYS_CREDIT").cast(IntegerType()))
            .withColumn("credit_day_overdue", F.col("CREDIT_DAY_OVERDUE").cast(IntegerType()))
            .withColumn("days_credit_enddate", F.col("DAYS_CREDIT_ENDDATE").cast(IntegerType()))
            .withColumn("days_enddate_fact", F.col("DAYS_ENDDATE_FACT").cast(IntegerType()))
            .withColumn("cnt_credit_prolong", F.col("CNT_CREDIT_PROLONG").cast(IntegerType()))
            .withColumn("amt_credit_sum", F.col("AMT_CREDIT_SUM").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_sum_debt", F.col("AMT_CREDIT_SUM_DEBT").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_sum_limit", F.col("AMT_CREDIT_SUM_LIMIT").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_sum_overdue", F.col("AMT_CREDIT_SUM_OVERDUE").cast(DecimalType(18, 2)))
            .withColumn("amt_credit_max_overdue", F.col("AMT_CREDIT_MAX_OVERDUE").cast(DecimalType(18, 2)))
            .select(
                "sk_id_bureau",
                "sk_id_curr",
                "credit_active_status",
                "credit_type",
                "days_credit",
                "credit_day_overdue",
                "days_credit_enddate",
                "days_enddate_fact",
                "cnt_credit_prolong",
                "amt_credit_sum",
                "amt_credit_sum_debt",
                "amt_credit_sum_limit",
                "amt_credit_sum_overdue",
                "amt_credit_max_overdue",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_fact_bureau_credit")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageFactBureauCreditJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
