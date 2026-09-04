#!/usr/bin/env python3
"""Curated Job: Build conformed fact_bureau_credit from stage_bureau with xxhash64 surrogate keys."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedFactBureauCreditJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "bureau")
        target_path = os.path.join(curated_base_dir, "fact_bureau_credit")
        super().__init__(
            pipeline_layer="curated",
            table_name="fact_bureau_credit",
            source_table=f"{stage_db}.stage_bureau",
            target_table=f"{curated_db}.fact_bureau_credit",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_bureau_credit_key",
            write_mode=WriteMode.OVERWRITE,
        )
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Stage Bureau from {self.source_path}")
        return spark.read.parquet(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Looks up conformed dimension surrogate keys with fallback -1, generates xxhash64 fact PK."""
        spark = df.sparkSession

        # Read dim_customer & dim_bureau_source for lookup
        dim_cust = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_customer")).select("sk_id_curr", "sk_customer_key")
        dim_src = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_bureau_source")).select("credit_type", "sk_bureau_source_key")

        # 1. Join with dim_customer & dim_bureau_source
        df_joined = (
            df.join(F.broadcast(dim_cust), on="sk_id_curr", how="left")
            .withColumn("sk_customer_key", F.coalesce(F.col("sk_customer_key"), F.lit(-1).cast(LongType())))
            .join(F.broadcast(dim_src), on="credit_type", how="left")
            .withColumn("sk_bureau_source_key", F.coalesce(F.col("sk_bureau_source_key"), F.lit(-1).cast(IntegerType())))
            .withColumn("sk_time_key", F.floor(F.col("days_credit") / 30).cast(IntegerType()))
            .withColumnRenamed("credit_active", "credit_active_status")
        )

        # 2. Generate idempotent fact surrogate key using xxhash64
        df_curated = (
            df_joined
            .withColumn(
                "sk_bureau_credit_key",
                F.xxhash64(F.col("sk_id_bureau").cast(StringType())),
            )
            .select(
                "sk_bureau_credit_key",
                "sk_id_bureau",
                "sk_id_curr",
                "sk_customer_key",
                "sk_bureau_source_key",
                "sk_time_key",
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
        return df_curated


def main():
    parser = argparse.ArgumentParser(description="Build curated fact_bureau_credit")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedFactBureauCreditJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
