#!/usr/bin/env python3
"""Stage Job: Build stage_dim_loan_product from application_train & previous_application."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageDimLoanProductJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="dim_loan_product",
            primary_key="name_contract_type",
            dedup_cols=["name_contract_type"],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
            source_table="raw_credit_risk.application_train,raw_credit_risk.previous_application",
        )
        self.raw_train_path = os.path.join(raw_base_dir, "application_train")
        self.raw_prev_path = os.path.join(raw_base_dir, "previous_application")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts unique contract types across active and historical loan applications."""
        df_train = spark.read.parquet(self.raw_train_path).select("NAME_CONTRACT_TYPE")
        df_prev = spark.read.parquet(self.raw_prev_path).select("NAME_CONTRACT_TYPE")
        return df_train.union(df_prev).filter(F.col("NAME_CONTRACT_TYPE").isNotNull()).distinct()

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Maps portfolio categories, product groupings, and revolving flags."""
        return (
            df
            .withColumn("name_contract_type", F.trim(F.col("NAME_CONTRACT_TYPE")))
            .withColumn(
                "portfolio_category",
                F.when(F.col("name_contract_type") == "Cash loans", F.lit("Unsecured Term"))
                .when(F.col("name_contract_type") == "Revolving loans", F.lit("Revolving Credit"))
                .when(F.col("name_contract_type") == "Consumer loans", F.lit("Secured POS"))
                .otherwise(F.lit("Other")),
            )
            .withColumn(
                "product_group",
                F.when(F.col("name_contract_type") == "Cash loans", F.lit("Personal Cash"))
                .when(F.col("name_contract_type") == "Revolving loans", F.lit("Credit Card"))
                .when(F.col("name_contract_type") == "Consumer loans", F.lit("Merchant POS Line"))
                .otherwise(F.lit("General Loan")),
            )
            .withColumn(
                "is_revolving",
                F.when(F.col("name_contract_type") == "Revolving loans", F.lit(True)).otherwise(F.lit(False)),
            )
            .select(
                "name_contract_type",
                "portfolio_category",
                "product_group",
                "is_revolving",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_dim_loan_product")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageDimLoanProductJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
