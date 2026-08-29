#!/usr/bin/env python3
"""Stage Job: Build stage_dim_application_decision from previous_application."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_stage_job import BaseStageJob


class StageDimApplicationDecisionJob(BaseStageJob):
    def __init__(
        self,
        raw_base_dir: str = "/raw/credit_risk",
        stage_base_dir: str = "/stage/credit_risk",
        raw_db: str = "raw_credit_risk",
        stage_db: str = "stage_credit_risk",
    ):
        super().__init__(
            table_name="dim_application_decision",
            primary_key=None,
            dedup_cols=[
                "name_contract_status",
                "code_reject_reason",
                "name_client_type",
            ],
            raw_base_dir=raw_base_dir,
            stage_base_dir=stage_base_dir,
            raw_db=raw_db,
            stage_db=stage_db,
            source_table="raw_credit_risk.previous_application",
        )
        self.raw_prev_path = os.path.join(raw_base_dir, "previous_application")

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts unique underwriting decision combinations."""
        cols = [
            "NAME_CONTRACT_STATUS",
            "CODE_REJECT_REASON",
            "NAME_CLIENT_TYPE",
        ]
        return spark.read.parquet(self.raw_prev_path).select(cols).distinct()

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Standardizes decision statuses and rejection codes."""
        return (
            df
            .withColumn("name_contract_status", F.trim(F.coalesce(F.col("NAME_CONTRACT_STATUS"), F.lit("Unknown"))))
            .withColumn("code_reject_reason", F.trim(F.coalesce(F.col("CODE_REJECT_REASON"), F.lit("XAP"))))
            .withColumn("name_client_type", F.trim(F.coalesce(F.col("NAME_CLIENT_TYPE"), F.lit("Unknown"))))
            .select(
                "name_contract_status",
                "code_reject_reason",
                "name_client_type",
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Build stage_dim_application_decision")
    parser.add_argument("--raw-dir", type=str, default="/raw/credit_risk")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--raw-db", type=str, default="raw_credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")

    args = parser.parse_args()

    StageDimApplicationDecisionJob(
        raw_base_dir=args.raw_dir,
        stage_base_dir=args.stage_dir,
        raw_db=args.raw_db,
        stage_db=args.stage_db,
    ).run()


if __name__ == "__main__":
    main()
