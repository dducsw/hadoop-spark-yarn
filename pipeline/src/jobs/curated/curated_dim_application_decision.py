#!/usr/bin/env python3
"""Curated Job: Build conformed dim_application_decision from stage_previous_application with xxhash64 SK."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimApplicationDecisionJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "previous_application")
        target_path = os.path.join(curated_base_dir, "dim_application_decision")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_application_decision",
            source_table=f"{stage_db}.stage_previous_application",
            target_table=f"{curated_db}.dim_application_decision",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_decision_key",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Extracting underwriting decisions from {self.source_path}")
        cols = ["name_contract_status", "code_reject_reason", "name_client_type"]
        return spark.read.parquet(self.source_path).select(cols).distinct()

    def transform(self, df: DataFrame) -> DataFrame:
        """Generates deterministic surrogate key and appends Unknown (-1) record."""
        df_curated = (
            df
            .withColumn(
                "sk_decision_key",
                (
                    F.abs(
                        F.xxhash64(
                            F.coalesce(F.col("name_contract_status"), F.lit("")),
                            F.coalesce(F.col("code_reject_reason"), F.lit("")),
                            F.coalesce(F.col("name_client_type"), F.lit("")),
                        )
                    )
                    % 100000
                    + 1
                ).cast(IntegerType()),
            )
            .select(
                "sk_decision_key",
                "name_contract_status",
                "code_reject_reason",
                "name_client_type",
            )
        )

        unknown_schema = "sk_decision_key INT, name_contract_status STRING, code_reject_reason STRING, name_client_type STRING"
        unknown_row = [(-1, "Unknown", "Unknown", "Unknown")]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_curated.unionByName(unknown_df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_application_decision")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimApplicationDecisionJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
