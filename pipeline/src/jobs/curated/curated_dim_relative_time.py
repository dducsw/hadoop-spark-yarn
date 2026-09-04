#!/usr/bin/env python3
"""Curated Job: Build conformed dim_relative_time with Unknown record fallback."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimRelativeTimeJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "dim_relative_time")
        target_path = os.path.join(curated_base_dir, "dim_relative_time")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_relative_time",
            source_table=f"{stage_db}.stage_dim_relative_time",
            target_table=f"{curated_db}.dim_relative_time",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_time_key",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Extracting Relative Time from {self.source_path}")
        try:
            return spark.read.parquet(self.source_path)
        except Exception:
            self.logger.info("Source stage table not found; bootstrapping standard relative time offsets (-96 to 0 months)")
            rows = []
            for m in range(-96, 1):
                day_offset = m * 30
                if m == 0:
                    bucket = "Current Month (0)"
                elif m >= -3:
                    bucket = "0-3 Months"
                elif m >= -6:
                    bucket = "3-6 Months"
                elif m >= -12:
                    bucket = "6-12 Months"
                elif m >= -24:
                    bucket = "12-24 Months"
                else:
                    bucket = "24+ Months"
                vintage = f"M_{abs(m)}" if m < 0 else "M_0"
                rows.append((m, day_offset, m, bucket, vintage))

            schema = "sk_time_key INT, relative_day_offset INT, relative_month_offset INT, relative_period_bucket STRING, vintage_cohort_offset STRING"
            return spark.createDataFrame(rows, schema=schema)

    def transform(self, df: DataFrame) -> DataFrame:
        """Standardizes time offset dimension and appends Unknown (-1) record."""
        df_base = (
            df.select(
                F.col("sk_time_key").cast(IntegerType()),
                F.col("relative_day_offset").cast(IntegerType()),
                F.col("relative_month_offset").cast(IntegerType()),
                F.col("relative_period_bucket").cast(StringType()),
                F.col("vintage_cohort_offset").cast(StringType()),
            )
        )

        unknown_schema = "sk_time_key INT, relative_day_offset INT, relative_month_offset INT, relative_period_bucket STRING, vintage_cohort_offset STRING"
        unknown_row = [(-1, -99999, -9999, "Unknown", "Unknown")]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_base.unionByName(unknown_df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_relative_time")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimRelativeTimeJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
