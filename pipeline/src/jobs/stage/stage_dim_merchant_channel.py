#!/usr/bin/env python3
"""Stage Job: Build stage_dim_merchant_channel from previous_application."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageDimMerchantChannelJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="dim_merchant_channel",
            primary_key=None,
            dedup_cols=[
                "channel_type",
                "name_type_suite",
                "name_goods_category",
                "name_seller_industry",
                "name_yield_group",
            ],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
            source_table="raw_credit_risk.previous_application",
        )
        self.raw_prev_path = os.path.join(raw_base_dir, "previous_application")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts unique channel and merchant attribute combinations."""
        cols = [
            "CHANNEL_TYPE",
            "NAME_TYPE_SUITE",
            "NAME_GOODS_CATEGORY",
            "NAME_SELLER_INDUSTRY",
            "NAME_YIELD_GROUP",
        ]
        return spark.read.parquet(self.raw_prev_path).select(cols).distinct()

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Trims strings and imputes missing merchant attributes."""
        return (
            df
            .withColumn("channel_type", F.trim(F.coalesce(F.col("CHANNEL_TYPE"), F.lit("Unknown"))))
            .withColumn("name_type_suite", F.trim(F.coalesce(F.col("NAME_TYPE_SUITE"), F.lit("Unaccompanied"))))
            .withColumn("name_goods_category", F.trim(F.coalesce(F.col("NAME_GOODS_CATEGORY"), F.lit("XNA"))))
            .withColumn("name_seller_industry", F.trim(F.coalesce(F.col("NAME_SELLER_INDUSTRY"), F.lit("XNA"))))
            .withColumn("name_yield_group", F.trim(F.coalesce(F.col("NAME_YIELD_GROUP"), F.lit("XNA"))))
            .select(
                "channel_type",
                "name_type_suite",
                "name_goods_category",
                "name_seller_industry",
                "name_yield_group",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_dim_merchant_channel")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageDimMerchantChannelJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
