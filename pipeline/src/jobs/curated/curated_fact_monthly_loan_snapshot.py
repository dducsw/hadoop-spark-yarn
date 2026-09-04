#!/usr/bin/env python3
"""Curated Job: Build conformed fact_monthly_loan_snapshot from stage POS and Credit Card balances."""
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


class CuratedFactMonthlyLoanSnapshotJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        self.stage_base_dir = stage_base_dir
        source_path = os.path.join(stage_base_dir, "pos_cash_balance")
        target_path = os.path.join(curated_base_dir, "fact_monthly_loan_snapshot")
        super().__init__(
            pipeline_layer="curated",
            table_name="fact_monthly_loan_snapshot",
            source_table=f"{stage_db}.stage_pos_cash_balance,{stage_db}.stage_credit_card_balance",
            target_table=f"{curated_db}.fact_monthly_loan_snapshot",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_snapshot_key",
            write_mode=WriteMode.DYNAMIC_PARTITION,
            partition_by=["loan_source_system"],
        )
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        """Unifies POS cash balance and revolving credit card balance into a single monthly contract grain."""
        path_pos = os.path.join(self.stage_base_dir, "pos_cash_balance")
        path_cc = os.path.join(self.stage_base_dir, "credit_card_balance")
        self.logger.info(f"Extracting monthly balances from {path_pos} and {path_cc}")

        df_pos = (
            spark.read.parquet(path_pos)
            .withColumn("loan_source_system", F.lit("POS_CASH"))
            .withColumn("amt_balance", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("amt_credit_limit", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("credit_utilization_ratio", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("amt_drawings_current", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("amt_payment_current", F.lit(None).cast(DecimalType(18, 2)))
            .withColumnRenamed("cnt_instalment", "cnt_instalment_total")
            .withColumnRenamed("name_contract_status", "contract_status")
        )

        df_cc = (
            spark.read.parquet(path_cc)
            .withColumn("loan_source_system", F.lit("CREDIT_CARD"))
            .withColumnRenamed("amt_credit_limit_actual", "amt_credit_limit")
            .withColumn(
                "credit_utilization_ratio",
                F.when(F.col("amt_credit_limit") > 0, F.col("amt_balance") / F.col("amt_credit_limit"))
                .otherwise(F.lit(0))
                .cast(DecimalType(8, 6)),
            )
            .withColumn("cnt_instalment_total", F.lit(None).cast(IntegerType()))
            .withColumn("cnt_instalment_future", F.lit(None).cast(IntegerType()))
            .withColumnRenamed("name_contract_status", "contract_status")
        )

        cols = [
            "sk_id_prev",
            "sk_id_curr",
            "months_balance",
            "amt_balance",
            "amt_credit_limit",
            "credit_utilization_ratio",
            "amt_drawings_current",
            "amt_payment_current",
            "cnt_instalment_total",
            "cnt_instalment_future",
            "sk_dpd",
            "sk_dpd_def",
            "contract_status",
            "loan_source_system",
        ]

        df_pos_sub = df_pos.select([c for c in cols if c in df_pos.columns])
        df_cc_sub = df_cc.select([c for c in cols if c in df_cc.columns])

        return df_pos_sub.unionByName(df_cc_sub)

    def transform(self, df: DataFrame) -> DataFrame:
        """Looks up conformed dimension surrogate keys with fallback -1, generates xxhash64 fact PK."""
        spark = df.sparkSession

        # Read dim_customer and dim_delinquency_bucket for lookup
        dim_cust = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_customer")).select("sk_id_curr", "sk_customer_key")
        dim_dpd_path = os.path.join(self.curated_base_dir, "dim_delinquency_bucket")
        try:
            dim_dpd = spark.read.parquet(dim_dpd_path).filter(F.col("sk_dpd_bucket_key") != -1).select("sk_dpd_bucket_key", "dpd_min", "dpd_max")
        except Exception:
            schema_dpd = "sk_dpd_bucket_key INT, dpd_min INT, dpd_max INT"
            data_dpd = [(0, 0, 0), (1, 1, 30), (2, 31, 60), (3, 61, 90), (4, 91, 120), (5, 121, 150), (6, 151, 999999)]
            dim_dpd = spark.createDataFrame(data_dpd, schema=schema_dpd)

        # 1. Join with dim_customer
        df_joined = (
            df.join(F.broadcast(dim_cust), on="sk_id_curr", how="left")
            .withColumn("sk_customer_key", F.coalesce(F.col("sk_customer_key"), F.lit(-1).cast(LongType())))
            .withColumnRenamed("months_balance", "relative_month_offset")
        )

        # 2. Map delinquency bucket by joining dim_delinquency_bucket
        clean_dpd = F.coalesce(df_joined["sk_dpd"], F.lit(0))
        df_joined = (
            df_joined
            .join(
                F.broadcast(dim_dpd),
                (clean_dpd >= dim_dpd["dpd_min"]) & (clean_dpd <= dim_dpd["dpd_max"]),
                how="left",
            )
            .withColumn("sk_dpd_bucket_key", F.coalesce(F.col("sk_dpd_bucket_key"), F.lit(-1).cast(IntegerType())))
            .withColumn("sk_time_key", F.col("relative_month_offset").cast(IntegerType()))
            .withColumn("sk_product_key", F.lit(-1).cast(IntegerType()))
        )

        # 3. Generate idempotent fact surrogate key using xxhash64
        df_curated = (
            df_joined
            .withColumn(
                "sk_snapshot_key",
                F.xxhash64(
                    F.col("sk_id_prev").cast(StringType()),
                    F.col("relative_month_offset").cast(StringType()),
                    F.coalesce(F.col("loan_source_system"), F.lit("UNKNOWN")),
                ),
            )
            .select(
                "sk_snapshot_key",
                "sk_id_prev",
                "sk_id_curr",
                "sk_customer_key",
                "sk_product_key",
                "sk_dpd_bucket_key",
                "sk_time_key",
                "relative_month_offset",
                "amt_balance",
                "amt_credit_limit",
                "credit_utilization_ratio",
                "amt_drawings_current",
                "amt_payment_current",
                "cnt_instalment_total",
                "cnt_instalment_future",
                "sk_dpd",
                "sk_dpd_def",
                "contract_status",
                "loan_source_system",
            )
        )
        return df_curated


def main():
    parser = argparse.ArgumentParser(description="Build curated fact_monthly_loan_snapshot")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedFactMonthlyLoanSnapshotJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
