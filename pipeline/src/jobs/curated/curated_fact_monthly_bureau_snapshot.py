#!/usr/bin/env python3
"""Curated Job: Build conformed fact_monthly_bureau_snapshot from stage_bureau_balance with xxhash64 SK."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedFactMonthlyBureauSnapshotJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "bureau_balance")
        target_path = os.path.join(curated_base_dir, "fact_monthly_bureau_snapshot")
        super().__init__(
            pipeline_layer="curated",
            table_name="fact_monthly_bureau_snapshot",
            source_table=f"{stage_db}.stage_bureau_balance",
            target_table=f"{curated_db}.fact_monthly_bureau_snapshot",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_bureau_snapshot_key",
            write_mode=WriteMode.OVERWRITE,
        )
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Stage Bureau Balance from {self.source_path}")
        return spark.read.parquet(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Looks up parent fact_bureau_credit SK and maps delinquency bucket."""
        spark = df.sparkSession

        # Read parent fact_bureau_credit for lookup
        fact_bureau_path = os.path.join(self.curated_base_dir, "fact_bureau_credit")
        df_bureau = spark.read.parquet(fact_bureau_path).select("sk_id_bureau", "sk_bureau_credit_key")

        # 1. Join with fact_bureau_credit
        df_joined = (
            df.join(F.broadcast(df_bureau), on="sk_id_bureau", how="left")
            .withColumn("sk_bureau_credit_key", F.coalesce(F.col("sk_bureau_credit_key"), F.lit(-1).cast(LongType())))
            .withColumnRenamed("months_balance", "relative_month_offset")
            .withColumnRenamed("status", "bureau_status_raw")
        )

        # 2. Map bureau status to delinquency bucket & flags
        df_joined = (
            df_joined
            .withColumn(
                "sk_dpd_bucket_key",
                F.when(F.col("bureau_status_raw").isin("C", "0"), F.lit(0))
                .when(F.col("bureau_status_raw") == "1", F.lit(1))
                .when(F.col("bureau_status_raw") == "2", F.lit(2))
                .when(F.col("bureau_status_raw") == "3", F.lit(3))
                .when(F.col("bureau_status_raw") == "4", F.lit(4))
                .when(F.col("bureau_status_raw") == "5", F.lit(5))
                .otherwise(F.lit(-1))
                .cast(IntegerType()),
            )
            .withColumn("sk_time_key", F.col("relative_month_offset").cast(IntegerType()))
            .withColumn("is_closed", F.when(F.col("bureau_status_raw") == "C", F.lit(1)).otherwise(F.lit(0)))
            .withColumn("is_overdue", F.when(F.col("bureau_status_raw").isin("1", "2", "3", "4", "5"), F.lit(1)).otherwise(F.lit(0)))
        )

        # 3. Generate idempotent fact surrogate key using xxhash64
        df_curated = (
            df_joined
            .withColumn(
                "sk_bureau_snapshot_key",
                F.xxhash64(
                    F.col("sk_id_bureau").cast(StringType()),
                    F.col("relative_month_offset").cast(StringType()),
                ),
            )
            .select(
                "sk_bureau_snapshot_key",
                "sk_bureau_credit_key",
                "sk_dpd_bucket_key",
                "sk_time_key",
                "sk_id_bureau",
                "relative_month_offset",
                "bureau_status_raw",
                "is_closed",
                "is_overdue",
            )
        )
        return df_curated


def main():
    parser = argparse.ArgumentParser(description="Build curated fact_monthly_bureau_snapshot")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedFactMonthlyBureauSnapshotJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
