#!/usr/bin/env python3
"""Stage Job: Build stage_dim_customer from application_train & application_test."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, FloatType, IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageDimCustomerJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="dim_customer",
            primary_key="sk_id_curr",
            dedup_cols=["sk_id_curr"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
        )
        self.raw_train_path = os.path.join(raw_base_dir, "application_train")
        self.raw_test_path = os.path.join(raw_base_dir, "application_test")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts customer records from both train and test raw tables."""
        self.logger.info(f"Reading train from {self.raw_train_path} and test from {self.raw_test_path}")
        df_train = spark.read.parquet(self.raw_train_path)
        df_test = spark.read.parquet(self.raw_test_path)

        cols = [
            "SK_ID_CURR",
            "CODE_GENDER",
            "FLAG_OWN_CAR",
            "FLAG_OWN_REALTY",
            "CNT_CHILDREN",
            "AMT_INCOME_TOTAL",
            "NAME_INCOME_TYPE",
            "NAME_EDUCATION_TYPE",
            "NAME_FAMILY_STATUS",
            "NAME_HOUSING_TYPE",
            "REGION_POPULATION_RELATIVE",
            "DAYS_BIRTH",
            "DAYS_EMPLOYED",
            "DAYS_REGISTRATION",
            "DAYS_ID_PUBLISH",
            "OCCUPATION_TYPE",
            "CNT_FAM_MEMBERS",
            "ORGANIZATION_TYPE",
        ]

        df_train_sub = (
            df_train.select([c for c in cols if c in df_train.columns])
            .withColumn("_source_table", F.lit("raw_credit_risk.application_train"))
        )
        df_test_sub = (
            df_test.select([c for c in cols if c in df_test.columns])
            .withColumn("_source_table", F.lit("raw_credit_risk.application_test"))
        )

        return df_train_sub.unionByName(df_test_sub, allowMissingColumns=True)

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Standardizes types, derives age/employment years, and adds SCD metadata."""
        return (
            df
            .withColumn("sk_id_curr", F.col("SK_ID_CURR").cast(IntegerType()))
            .withColumn("code_gender", F.trim(F.coalesce(F.col("CODE_GENDER"), F.lit("XNA"))))
            .withColumn("flag_own_car", F.trim(F.coalesce(F.col("FLAG_OWN_CAR"), F.lit("N"))))
            .withColumn("flag_own_realty", F.trim(F.coalesce(F.col("FLAG_OWN_REALTY"), F.lit("N"))))
            .withColumn("cnt_children", F.col("CNT_CHILDREN").cast(IntegerType()))
            .withColumn("cnt_fam_members", F.col("CNT_FAM_MEMBERS").cast(IntegerType()))
            .withColumn("amt_income_total", F.col("AMT_INCOME_TOTAL").cast(DecimalType(18, 2)))
            .withColumn("name_income_type", F.trim(F.coalesce(F.col("NAME_INCOME_TYPE"), F.lit("Unknown"))))
            .withColumn("name_education_type", F.trim(F.coalesce(F.col("NAME_EDUCATION_TYPE"), F.lit("Unknown"))))
            .withColumn("name_family_status", F.trim(F.coalesce(F.col("NAME_FAMILY_STATUS"), F.lit("Unknown"))))
            .withColumn("name_housing_type", F.trim(F.coalesce(F.col("NAME_HOUSING_TYPE"), F.lit("Unknown"))))
            .withColumn("occupation_type", F.trim(F.coalesce(F.col("OCCUPATION_TYPE"), F.lit("Unknown"))))
            .withColumn("organization_type", F.trim(F.coalesce(F.col("ORGANIZATION_TYPE"), F.lit("Unknown"))))
            # Derived fields
            .withColumn(
                "age_years",
                F.floor(F.abs(F.col("DAYS_BIRTH")) / 365.25).cast(IntegerType()),
            )
            .withColumn(
                "employed_years",
                F.when(F.col("DAYS_EMPLOYED") > 0, 0)
                .otherwise(F.floor(F.abs(F.col("DAYS_EMPLOYED")) / 365.25))
                .cast(IntegerType()),
            )
            .select(
                "sk_id_curr",
                "code_gender",
                "flag_own_car",
                "flag_own_realty",
                "cnt_children",
                "cnt_fam_members",
                "amt_income_total",
                "name_income_type",
                "name_education_type",
                "name_family_status",
                "name_housing_type",
                "occupation_type",
                "organization_type",
                "age_years",
                "employed_years",
                "_source_table",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_dim_customer")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageDimCustomerJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
