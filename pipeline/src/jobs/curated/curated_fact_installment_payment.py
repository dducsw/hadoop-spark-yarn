#!/usr/bin/env python3
"""Curated Job: Build conformed fact_installment_payment with surrogate keys and delinquency bucket mapping."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedFactInstallmentPaymentJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(stage_base_dir, "installments_payments")
        target_path = os.path.join(curated_base_dir, "fact_installment_payment")
        super().__init__(
            pipeline_layer="curated",
            table_name="fact_installment_payment",
            source_table=f"{stage_db}.stage_installments_payments",
            target_table=f"{curated_db}.fact_installment_payment",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_installment_key",
            write_mode=WriteMode.DYNAMIC_PARTITION,
            partition_by=["is_revolving_installment"],
        )
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Stage Installments Payments from {self.source_path}")
        return spark.read.parquet(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Looks up conformed dimension surrogate keys with fallback -1, generates xxhash64 fact PK."""
        spark = df.sparkSession

        # Read dim_customer and dim_delinquency_bucket for lookup
        dim_cust_path = os.path.join(self.curated_base_dir, "dim_customer")
        df_dim_cust = spark.read.parquet(dim_cust_path).select("sk_id_curr", "sk_customer_key")

        dim_dpd_path = os.path.join(self.curated_base_dir, "dim_delinquency_bucket")
        try:
            dim_dpd = spark.read.parquet(dim_dpd_path).filter(F.col("sk_dpd_bucket_key") != -1).select("sk_dpd_bucket_key", "dpd_min", "dpd_max")
        except Exception:
            schema_dpd = "sk_dpd_bucket_key INT, dpd_min INT, dpd_max INT"
            data_dpd = [(0, 0, 0), (1, 1, 30), (2, 31, 60), (3, 61, 90), (4, 91, 120), (5, 121, 150), (6, 151, 999999)]
            dim_dpd = spark.createDataFrame(data_dpd, schema=schema_dpd)

        # 1. Join with dim_customer
        df_joined = (
            df.join(F.broadcast(df_dim_cust), on="sk_id_curr", how="left")
            .withColumn("sk_customer_key", F.coalesce(F.col("sk_customer_key"), F.lit(-1).cast(LongType())))
        )

        # 2. Map delinquency bucket by joining dim_delinquency_bucket
        clean_delay = F.coalesce(df_joined["payment_delay_days"], F.lit(0))
        df_joined = (
            df_joined
            .join(
                F.broadcast(dim_dpd),
                (clean_delay >= dim_dpd["dpd_min"]) & (clean_delay <= dim_dpd["dpd_max"]),
                how="left",
            )
            .withColumn("sk_dpd_bucket_key", F.coalesce(F.col("sk_dpd_bucket_key"), F.lit(-1).cast(IntegerType())))
            .withColumn(
                "sk_time_key",
                F.floor(F.col("days_instalment") / 30).cast(IntegerType()),
            )
            .withColumn("sk_product_key", F.lit(-1).cast(IntegerType()))
            .withColumn("is_revolving_installment", F.when(F.col("num_instalment_version") == 0, F.lit(True)).otherwise(F.lit(False)))
        )

        # 3. Generate idempotent fact surrogate key using xxhash64
        df_curated = (
            df_joined
            .withColumn(
                "sk_installment_key",
                F.xxhash64(
                    F.col("sk_id_prev").cast(StringType()),
                    F.col("num_instalment_version").cast(StringType()),
                    F.col("num_instalment_number").cast(StringType()),
                    F.col("days_instalment").cast(StringType()),
                ),
            )
            .select(
                "sk_installment_key",
                "sk_id_prev",
                "sk_id_curr",
                "sk_customer_key",
                "sk_product_key",
                "sk_dpd_bucket_key",
                "sk_time_key",
                "num_instalment_version",
                "num_instalment_number",
                "days_instalment",
                "days_entry_payment",
                "amt_instalment",
                "amt_payment",
                "amt_underpayment",
                "payment_delay_days",
                "is_late_payment",
                "is_underpaid",
                "is_revolving_installment",
            )
        )
        return df_curated


def main():
    parser = argparse.ArgumentParser(description="Build curated fact_installment_payment")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedFactInstallmentPaymentJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
