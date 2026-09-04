#!/usr/bin/env python3
"""Curated Job: Build conformed fact_loan_application from stage applications with dimension lookups and xxhash64 SK."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedFactLoanApplicationJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        self.stage_base_dir = stage_base_dir
        source_path = os.path.join(stage_base_dir, "application_train")
        target_path = os.path.join(curated_base_dir, "fact_loan_application")
        super().__init__(
            pipeline_layer="curated",
            table_name="fact_loan_application",
            source_table=f"{stage_db}.stage_application_train,{stage_db}.stage_previous_application",
            target_table=f"{curated_db}.fact_loan_application",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_application_key",
            write_mode=WriteMode.DYNAMIC_PARTITION,
            partition_by=["product_group"],
        )
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        """Extracts and unifies active applications (train/test) and historical previous applications."""
        path_train = os.path.join(self.stage_base_dir, "application_train")
        path_test = os.path.join(self.stage_base_dir, "application_test")
        path_prev = os.path.join(self.stage_base_dir, "previous_application")

        self.logger.info(f"Extracting applications from {path_train}, {path_test}, and {path_prev}")

        df_train = (
            spark.read.parquet(path_train)
            .withColumn("sk_id_prev", F.lit(None).cast(IntegerType()))
            .withColumn("is_current_application", F.lit(True))
            .withColumn("amt_application", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("amt_down_payment", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("rate_down_payment", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("rate_interest_primary", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("channel_type", F.lit("Unknown"))
            .withColumn("name_goods_category", F.lit("XNA"))
            .withColumn("name_seller_industry", F.lit("XNA"))
            .withColumn("name_yield_group", F.lit("XNA"))
            .withColumn("name_contract_status", F.lit("Approved"))
            .withColumn("code_reject_reason", F.lit("XAP"))
            .withColumn("name_client_type", F.lit("New"))
            .withColumn("days_decision", F.lit(0))
        )

        df_test = (
            spark.read.parquet(path_test)
            .withColumn("sk_id_prev", F.lit(None).cast(IntegerType()))
            .withColumn("target", F.lit(None).cast(IntegerType()))
            .withColumn("is_current_application", F.lit(True))
            .withColumn("amt_application", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("amt_down_payment", F.lit(None).cast(DecimalType(18, 2)))
            .withColumn("rate_down_payment", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("rate_interest_primary", F.lit(None).cast(DecimalType(8, 6)))
            .withColumn("channel_type", F.lit("Unknown"))
            .withColumn("name_goods_category", F.lit("XNA"))
            .withColumn("name_seller_industry", F.lit("XNA"))
            .withColumn("name_yield_group", F.lit("XNA"))
            .withColumn("name_contract_status", F.lit("Approved"))
            .withColumn("code_reject_reason", F.lit("XAP"))
            .withColumn("name_client_type", F.lit("New"))
            .withColumn("days_decision", F.lit(0))
        )

        df_prev = (
            spark.read.parquet(path_prev)
            .withColumn("target", F.lit(None).cast(IntegerType()))
            .withColumn("is_current_application", F.lit(False))
            .withColumn("ext_source_1", F.lit(None).cast(FloatType()))
            .withColumn("ext_source_2", F.lit(None).cast(FloatType()))
            .withColumn("ext_source_3", F.lit(None).cast(FloatType()))
        )

        cols = [
            "sk_id_curr",
            "sk_id_prev",
            "name_contract_type",
            "amt_application",
            "amt_credit",
            "amt_annuity",
            "amt_goods_price",
            "amt_down_payment",
            "rate_down_payment",
            "rate_interest_primary",
            "ext_source_1",
            "ext_source_2",
            "ext_source_3",
            "target",
            "is_current_application",
            "channel_type",
            "name_goods_category",
            "name_seller_industry",
            "name_yield_group",
            "name_contract_status",
            "code_reject_reason",
            "name_client_type",
            "days_decision",
        ]

        df_curr = (
            df_train.select([c for c in cols if c in df_train.columns])
            .unionByName(df_test.select([c for c in cols if c in df_test.columns]))
        )
        df_prev_sub = df_prev.select([c for c in cols if c in df_prev.columns])

        return df_curr.unionByName(df_prev_sub)

    def transform(self, df: DataFrame) -> DataFrame:
        """Looks up conformed dimension surrogate keys with fallback -1, generates xxhash64 fact PK."""
        spark = df.sparkSession

        # Read dimensions for lookup
        dim_cust = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_customer")).select("sk_id_curr", "sk_customer_key")
        dim_prod = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_loan_product")).select("name_contract_type", "sk_product_key", "product_group")
        dim_chan = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_merchant_channel")).select(
            "channel_type", "name_goods_category", "name_seller_industry", "name_yield_group", "sk_channel_key"
        )
        dim_dec = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_application_decision")).select(
            "name_contract_status", "code_reject_reason", "name_client_type", "sk_decision_key"
        )

        # 1. Join dimensions with broadcast
        df_joined = (
            df.join(F.broadcast(dim_cust), on="sk_id_curr", how="left")
            .withColumn("sk_customer_key", F.coalesce(F.col("sk_customer_key"), F.lit(-1).cast(LongType())))
            .join(F.broadcast(dim_prod), on="name_contract_type", how="left")
            .withColumn("sk_product_key", F.coalesce(F.col("sk_product_key"), F.lit(-1).cast(IntegerType())))
            .withColumn("product_group", F.coalesce(F.col("product_group"), F.lit("General Loan")))
            .join(F.broadcast(dim_chan), on=["channel_type", "name_goods_category", "name_seller_industry", "name_yield_group"], how="left")
            .withColumn("sk_channel_key", F.coalesce(F.col("sk_channel_key"), F.lit(-1).cast(IntegerType())))
            .join(F.broadcast(dim_dec), on=["name_contract_status", "code_reject_reason", "name_client_type"], how="left")
            .withColumn("sk_decision_key", F.coalesce(F.col("sk_decision_key"), F.lit(-1).cast(IntegerType())))
            .withColumn("sk_time_key", F.floor(F.coalesce(F.col("days_decision"), F.lit(0)) / 30).cast(IntegerType()))
        )

        # 2. Generate idempotent fact surrogate key using xxhash64
        df_curated = (
            df_joined
            .withColumn(
                "sk_application_key",
                F.xxhash64(
                    F.col("sk_id_curr").cast(StringType()),
                    F.coalesce(F.col("sk_id_prev").cast(StringType()), F.lit("-1")),
                    F.col("is_current_application").cast(StringType()),
                ),
            )
            .withColumnRenamed("target", "target_default_flag")
            .select(
                "sk_application_key",
                "sk_id_curr",
                "sk_id_prev",
                "sk_customer_key",
                "sk_product_key",
                "sk_channel_key",
                "sk_decision_key",
                "sk_time_key",
                "name_contract_type",
                "amt_application",
                "amt_credit",
                "amt_annuity",
                "amt_goods_price",
                "amt_down_payment",
                "rate_down_payment",
                "rate_interest_primary",
                "ext_source_1",
                "ext_source_2",
                "ext_source_3",
                "target_default_flag",
                "is_current_application",
                "product_group",
            )
        )
        return df_curated


def main():
    parser = argparse.ArgumentParser(description="Build curated fact_loan_application")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedFactLoanApplicationJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
