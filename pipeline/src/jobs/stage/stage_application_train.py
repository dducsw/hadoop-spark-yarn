#!/usr/bin/env python3
"""Stage Job: Clean, deduplicate, and cast application_train into stage_application_train."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, FloatType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageApplicationTrainJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="application_train",
            primary_key="sk_id_curr",
            dedup_cols=["sk_id_curr"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Casts amounts to Decimal(18,2), rates to Float, trims strings, and filters null PKs."""
        # Standardize columns to lowercase
        df_renamed = df
        for col in df.columns:
            df_renamed = df_renamed.withColumnRenamed(col, col.lower())

        df_cleaned = (
            df_renamed
            .withColumn("sk_id_curr", F.col("sk_id_curr").cast(IntegerType()))
            .withColumn("target", F.col("target").cast(IntegerType()))
            .withColumn("name_contract_type", F.trim(F.coalesce(F.col("name_contract_type"), F.lit("Unknown"))))
            .withColumn("code_gender", F.trim(F.coalesce(F.col("code_gender"), F.lit("XNA"))))
            .withColumn("flag_own_car", F.trim(F.coalesce(F.col("flag_own_car"), F.lit("N"))))
            .withColumn("flag_own_realty", F.trim(F.coalesce(F.col("flag_own_realty"), F.lit("N"))))
            .withColumn("cnt_children", F.col("cnt_children").cast(IntegerType()))
            .withColumn("amt_income_total", F.col("amt_income_total").cast(DecimalType(18, 2)))
            .withColumn("amt_credit", F.col("amt_credit").cast(DecimalType(18, 2)))
            .withColumn("amt_annuity", F.col("amt_annuity").cast(DecimalType(18, 2)))
            .withColumn("amt_goods_price", F.col("amt_goods_price").cast(DecimalType(18, 2)))
            .withColumn("name_income_type", F.trim(F.coalesce(F.col("name_income_type"), F.lit("Unknown"))))
            .withColumn("name_education_type", F.trim(F.coalesce(F.col("name_education_type"), F.lit("Unknown"))))
            .withColumn("name_family_status", F.trim(F.coalesce(F.col("name_family_status"), F.lit("Unknown"))))
            .withColumn("name_housing_type", F.trim(F.coalesce(F.col("name_housing_type"), F.lit("Unknown"))))
            .withColumn("occupation_type", F.trim(F.coalesce(F.col("occupation_type"), F.lit("Unknown"))))
            .withColumn("organization_type", F.trim(F.coalesce(F.col("organization_type"), F.lit("Unknown"))))
            .withColumn("days_birth", F.col("days_birth").cast(IntegerType()))
            .withColumn("days_employed", F.col("days_employed").cast(IntegerType()))
            .withColumn("cnt_fam_members", F.col("cnt_fam_members").cast(IntegerType()))
            .withColumn("ext_source_1", F.col("ext_source_1").cast(FloatType()))
            .withColumn("ext_source_2", F.col("ext_source_2").cast(FloatType()))
            .withColumn("ext_source_3", F.col("ext_source_3").cast(FloatType()))
        )
        return super().transform(df_cleaned)


def main():
    parser = argparse.ArgumentParser(description="Build stage_application_train")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageApplicationTrainJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
