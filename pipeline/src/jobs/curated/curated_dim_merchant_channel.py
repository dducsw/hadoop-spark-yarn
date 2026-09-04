#!/usr/bin/env python3
"""Curated Job: Build conformed dim_merchant_channel from stage_previous_application with xxhash64 SK."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimMerchantChannelJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "previous_application")
        target_path = os.path.join(curated_base_dir, "dim_merchant_channel")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_merchant_channel",
            source_table=f"{stage_db}.stage_previous_application",
            target_table=f"{curated_db}.dim_merchant_channel",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_channel_key",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Extracting unique Merchant Channels from {self.source_path}")
        cols = ["channel_type", "name_type_suite", "name_goods_category", "name_seller_industry", "name_yield_group"]
        return spark.read.parquet(self.source_path).select(cols).distinct()

    def transform(self, df: DataFrame) -> DataFrame:
        """Generates deterministic surrogate key and appends Unknown (-1) record."""
        df_curated = (
            df
            .withColumn(
                "sk_channel_key",
                (
                    F.abs(
                        F.xxhash64(
                            F.coalesce(F.col("channel_type"), F.lit("")),
                            F.coalesce(F.col("name_type_suite"), F.lit("")),
                            F.coalesce(F.col("name_goods_category"), F.lit("")),
                            F.coalesce(F.col("name_seller_industry"), F.lit("")),
                            F.coalesce(F.col("name_yield_group"), F.lit("")),
                        )
                    )
                    % 1000000
                    + 1
                ).cast(IntegerType()),
            )
            .select(
                "sk_channel_key",
                "channel_type",
                "name_type_suite",
                "name_goods_category",
                "name_seller_industry",
                "name_yield_group",
            )
        )

        unknown_schema = "sk_channel_key INT, channel_type STRING, name_type_suite STRING, name_goods_category STRING, name_seller_industry STRING, name_yield_group STRING"
        unknown_row = [(-1, "Unknown", "Unknown", "Unknown", "Unknown", "Unknown")]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_curated.unionByName(unknown_df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_merchant_channel")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimMerchantChannelJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
