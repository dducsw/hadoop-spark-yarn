#!/usr/bin/env python3
"""Data Quality & Financial Reconciliation Gate for Fintech DWH Pipeline."""
import argparse
import os
import sys
from decimal import Decimal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from src.common.base_spark_job import BaseSparkJob, WriteMode


class ReconciliationAuditJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        curated_db: str = "credit_risk",
        max_discrepancy_pct: float = 0.01,
    ):
        self.stage_base_dir = stage_base_dir
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db
        self.max_discrepancy_pct = max_discrepancy_pct

        target_path = os.path.join(curated_base_dir, "reconciliation_audit")
        super().__init__(
            pipeline_layer="curated",
            table_name="reconciliation_audit",
            source_table="stage_application_train,stage_application_test,obt_loan_portfolio_360",
            target_table=f"{curated_db}.reconciliation_audit",
            source_path=os.path.join(curated_base_dir, "obt_loan_portfolio_360"),
            target_path=target_path,
            primary_key="check_name",
            write_mode=WriteMode.OVERWRITE,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Loads Stage application data and Curated OBT for financial reconciliation."""
        path_train = os.path.join(self.stage_base_dir, "application_train")
        path_test = os.path.join(self.stage_base_dir, "application_test")
        path_obt = os.path.join(self.curated_base_dir, "obt_loan_portfolio_360")

        self.logger.info("Extracting stage and mart datasets for reconciliation check...")
        df_train = spark.read.parquet(path_train)
        df_test = spark.read.parquet(path_test)
        df_obt = spark.read.parquet(path_obt)

        return self._build_audit_dataframe(spark, df_train, df_test, df_obt)

    def _build_audit_dataframe(
        self,
        spark: SparkSession,
        df_train: DataFrame,
        df_test: DataFrame,
        df_obt: DataFrame,
    ) -> DataFrame:
        # Single-pass aggregation per dataset: combines null PK check & sum(amt_credit)
        train_stats = df_train.select(
            F.count(F.when(F.col("sk_id_curr").isNull(), 1)).alias("null_pk"),
            F.coalesce(F.sum("amt_credit"), F.lit(0)).alias("sum_credit"),
        ).collect()[0]
        null_train_pk = train_stats["null_pk"]
        stage_train_sum = train_stats["sum_credit"] or Decimal("0.00")

        test_stats = df_test.select(
            F.count(F.when(F.col("sk_id_curr").isNull(), 1)).alias("null_pk"),
            F.coalesce(F.sum("amt_credit"), F.lit(0)).alias("sum_credit"),
        ).collect()[0]
        null_test_pk = test_stats["null_pk"]
        stage_test_sum = test_stats["sum_credit"] or Decimal("0.00")

        obt_stats = df_obt.select(
            F.count(F.when(F.col("sk_id_curr").isNull(), 1)).alias("null_pk"),
            F.coalesce(
                F.sum(F.when(F.col("is_current_application") == True, F.col("amt_credit"))),
                F.lit(0),
            ).alias("current_credit"),
        ).collect()[0]
        null_obt_pk = obt_stats["null_pk"]
        obt_current_credit = obt_stats["current_credit"] or Decimal("0.00")

        if null_train_pk > 0 or null_test_pk > 0 or null_obt_pk > 0:
            raise ValueError(
                f"Data Quality Violation: Null PK detected! Train: {null_train_pk}, Test: {null_test_pk}, OBT: {null_obt_pk}"
            )

        total_stage_credit = stage_train_sum + stage_test_sum
        diff = abs(total_stage_credit - obt_current_credit)
        pct_diff = (float(diff) / float(total_stage_credit) * 100.0) if total_stage_credit > 0 else 0.0

        self.logger.info(
            f"Financial Reconciliation Check: Stage Total Credit = {total_stage_credit:,.2f} | "
            f"OBT Current Credit = {obt_current_credit:,.2f} | Diff = {diff:,.2f} ({pct_diff:.4f}%)"
        )

        if pct_diff > self.max_discrepancy_pct:
            raise ValueError(
                f"Reconciliation Failed! Credit amount discrepancy {pct_diff:.4f}% exceeds limit {self.max_discrepancy_pct}%"
            )

        audit_schema = StructType([
            StructField("check_name", StringType(), False),
            StructField("source_value", DecimalType(18, 2), False),
            StructField("target_value", DecimalType(18, 2), False),
            StructField("difference", DecimalType(18, 2), False),
            StructField("discrepancy_pct", DoubleType(), False),
            StructField("passed", StringType(), False),
        ])

        audit_rows = [
            (
                "sum_amt_credit_reconciliation",
                total_stage_credit,
                obt_current_credit,
                diff,
                pct_diff,
                "PASSED",
            )
        ]

        return spark.createDataFrame(audit_rows, schema=audit_schema)

    def transform(self, df: DataFrame) -> DataFrame:
        return df


def main():
    parser = argparse.ArgumentParser(description="Run Financial Reconciliation & Data Quality Gate")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")
    parser.add_argument("--max-discrepancy-pct", type=float, default=0.01)

    args = parser.parse_args()

    ReconciliationAuditJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        curated_db=args.curated_db,
        max_discrepancy_pct=args.max_discrepancy_pct,
    ).run()


if __name__ == "__main__":
    main()
