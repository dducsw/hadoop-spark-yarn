#!/usr/bin/env python3
"""Curated Job: Build conformed dim_loan_product from stage applications with xxhash64 SK."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimLoanProductJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        self.stage_base_dir = stage_base_dir
        source_path = os.path.join(stage_base_dir, "application_train")
        target_path = os.path.join(curated_base_dir, "dim_loan_product")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_loan_product",
            source_table=f"{stage_db}.stage_application_train,{stage_db}.stage_previous_application",
            target_table=f"{curated_db}.dim_loan_product",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_product_key",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts unique contract types across active and historical loan applications."""
        path_curr = os.path.join(self.stage_base_dir, "application_train")
        path_prev = os.path.join(self.stage_base_dir, "previous_application")
        df_curr = spark.read.parquet(path_curr).select("name_contract_type")
        df_prev = spark.read.parquet(path_prev).select("name_contract_type")
        return df_curr.union(df_prev).filter(F.col("name_contract_type").isNotNull()).distinct()

    def transform(self, df: DataFrame) -> DataFrame:
        """Generates deterministic surrogate key, maps portfolio category, and appends Unknown (-1)."""
        df_curated = (
            df
            .withColumn(
                "sk_product_key",
                (F.abs(F.xxhash64(F.col("name_contract_type"))) % 100000 + 1).cast(IntegerType()),
            )
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
                "sk_product_key",
                "name_contract_type",
                "portfolio_category",
                "product_group",
                "is_revolving",
            )
        )

        unknown_schema = "sk_product_key INT, name_contract_type STRING, portfolio_category STRING, product_group STRING, is_revolving BOOLEAN"
        unknown_row = [(-1, "Unknown", "Unknown", "Unknown", False)]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_curated.unionByName(unknown_df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_loan_product")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimLoanProductJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
