#!/usr/bin/env python3
"""Curated Job: Build conformed dim_delinquency_bucket with Unknown record fallback."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimDelinquencyBucketJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "dim_delinquency_bucket")
        target_path = os.path.join(curated_base_dir, "dim_delinquency_bucket")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_delinquency_bucket",
            source_table=f"{stage_db}.stage_dim_delinquency_bucket",
            target_table=f"{curated_db}.dim_delinquency_bucket",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_dpd_bucket_key",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Extracting Delinquency Buckets from {self.source_path}")
        try:
            return spark.read.parquet(self.source_path)
        except Exception:
            self.logger.info("Source stage table not found; bootstrapping standard Basel II / IFRS9 DPD buckets")
            schema = StructType([
                StructField("sk_dpd_bucket_key", IntegerType(), False),
                StructField("bucket_code", StringType(), False),
                StructField("bucket_name", StringType(), False),
                StructField("dpd_min", IntegerType(), False),
                StructField("dpd_max", IntegerType(), False),
                StructField("is_npl", BooleanType(), False),
            ])
            data = [
                (0, "DPD_0", "Current (0 DPD)", 0, 0, False),
                (1, "DPD_1_30", "Grace / Early 1-30", 1, 30, False),
                (2, "DPD_31_60", "Delinquent 31-60", 31, 60, False),
                (3, "DPD_61_90", "Delinquent 61-90", 61, 90, False),
                (4, "DPD_91_120", "NPL Substandard 91-120", 91, 120, True),
                (5, "DPD_121_150", "NPL Doubtful 121-150", 121, 150, True),
                (6, "DPD_150_PLUS", "NPL Loss >150", 151, 999999, True),
            ]
            return spark.createDataFrame(data, schema=schema)

    def transform(self, df: DataFrame) -> DataFrame:
        """Standardizes columns and appends Unknown (-1) record."""
        df_base = (
            df
            .select(
                F.col("sk_dpd_bucket_key").cast(IntegerType()),
                F.col("bucket_code").cast(StringType()),
                F.col("bucket_name").cast(StringType()),
                F.col("dpd_min").cast(IntegerType()),
                F.col("dpd_max").cast(IntegerType()),
                F.col("is_npl").cast(BooleanType()),
            )
        )

        unknown_schema = StructType([
            StructField("sk_dpd_bucket_key", IntegerType(), True),
            StructField("bucket_code", StringType(), True),
            StructField("bucket_name", StringType(), True),
            StructField("dpd_min", IntegerType(), True),
            StructField("dpd_max", IntegerType(), True),
            StructField("is_npl", BooleanType(), True),
        ])
        unknown_row = [(-1, "UNK", "Unknown DPD", -1, -1, False)]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_base.unionByName(unknown_df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_delinquency_bucket")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimDelinquencyBucketJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
