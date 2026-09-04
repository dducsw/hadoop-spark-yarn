#!/usr/bin/env python3
"""Curated Job: Build conformed dim_bureau_source from stage_bureau with xxhash64 SK."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimBureauSourceJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "bureau")
        target_path = os.path.join(curated_base_dir, "dim_bureau_source")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_bureau_source",
            source_table=f"{stage_db}.stage_bureau",
            target_table=f"{curated_db}.dim_bureau_source",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_bureau_source_key",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Extracting unique credit types from {self.source_path}")
        return spark.read.parquet(self.source_path).select("credit_type").filter(F.col("credit_type").isNotNull()).distinct()

    def transform(self, df: DataFrame) -> DataFrame:
        """Standardizes credit types, categorizes them, generates xxhash64 SK, and appends Unknown (-1)."""
        df_curated = (
            df
            .withColumn(
                "sk_bureau_source_key",
                (F.abs(F.xxhash64(F.coalesce(F.col("credit_type"), F.lit("")))) % 100000 + 1).cast(IntegerType()),
            )
            .withColumn(
                "credit_category",
                F.when(F.col("credit_type").isin("Credit card", "Revolving credit"), F.lit("Revolving"))
                .when(F.col("credit_type").isin("Consumer credit", "Microloan", "Cash loan (non-earmarked)"), F.lit("Consumer Loan"))
                .when(F.col("credit_type").isin("Mortgage", "Real estate loan"), F.lit("Mortgage"))
                .when(F.col("credit_type").isin("Car loan"), F.lit("Auto Loan"))
                .otherwise(F.lit("Other")),
            )
            .withColumn(
                "is_secured",
                F.when(F.col("credit_type").isin("Mortgage", "Real estate loan", "Car loan"), F.lit(True))
                .otherwise(F.lit(False)),
            )
            .select(
                "sk_bureau_source_key",
                "credit_type",
                "credit_category",
                "is_secured",
            )
        )

        unknown_schema = "sk_bureau_source_key INT, credit_type STRING, credit_category STRING, is_secured BOOLEAN"
        unknown_row = [(-1, "Unknown", "Unknown", False)]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_curated.unionByName(unknown_df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_bureau_source")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimBureauSourceJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
