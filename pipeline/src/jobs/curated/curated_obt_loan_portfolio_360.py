#!/usr/bin/env python3
"""Curated Job: Build obt_loan_portfolio_360 wide denormalized table for Self-Service BI (Zero-JOIN)."""
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedObtLoanPortfolio360Job(BaseSparkJob):
    def __init__(
        self,
        curated_base_dir: str = "/curated/credit_risk",
        curated_db: str = "credit_risk",
    ):
        source_path = os.path.join(curated_base_dir, "fact_loan_application")
        target_path = os.path.join(curated_base_dir, "obt_loan_portfolio_360")
        super().__init__(
            pipeline_layer="curated",
            table_name="obt_loan_portfolio_360",
            source_table=f"{curated_db}.fact_loan_application",
            target_table=f"{curated_db}.obt_loan_portfolio_360",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_id_curr",
            write_mode=WriteMode.OVERWRITE,
        )
        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Fact Loan Application from {self.source_path}")
        return spark.read.parquet(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Joins fact_loan_application with conformed dimensions and latest monthly snapshot for 360-degree BI."""
        spark = df.sparkSession

        dim_cust = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_customer"))
        dim_prod = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_loan_product"))
        dim_chan = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_merchant_channel"))
        dim_dec = spark.read.parquet(os.path.join(self.curated_base_dir, "dim_application_decision"))

        # Load latest loan monthly snapshot if available for portfolio status
        fact_snapshot_path = os.path.join(self.curated_base_dir, "fact_monthly_loan_snapshot")
        try:
            df_snap = spark.read.parquet(fact_snapshot_path)
            w = Window.partitionBy("sk_id_prev").orderBy(F.col("relative_month_offset").desc())
            df_latest_snap = (
                df_snap
                .filter(F.col("sk_id_prev").isNotNull())
                .withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .select(
                    F.col("sk_id_prev"),
                    F.col("amt_balance").alias("latest_balance"),
                    F.col("amt_credit_limit").alias("latest_credit_limit"),
                    F.col("credit_utilization_ratio").alias("latest_utilization_ratio"),
                    F.col("sk_dpd").alias("latest_dpd"),
                    F.col("contract_status").alias("latest_contract_status"),
                    F.col("relative_month_offset").alias("latest_snapshot_month"),
                )
            )
        except Exception:
            schema_snap = "sk_id_prev INT, latest_balance DECIMAL(18,2), latest_credit_limit DECIMAL(18,2), latest_utilization_ratio DECIMAL(8,6), latest_dpd INT, latest_contract_status STRING, latest_snapshot_month INT"
            df_latest_snap = spark.createDataFrame([], schema=schema_snap)

        # Broadcast dimension joins + latest snapshot join
        audit_cols = ["_source_system", "_processed_at", "_batch_id", "_source_table", "_curated_at"]
        df_obt = (
            df
            .join(F.broadcast(dim_cust.drop(*audit_cols)), on="sk_customer_key", how="left")
            .join(F.broadcast(dim_prod.drop(*audit_cols)), on="sk_product_key", how="left")
            .join(F.broadcast(dim_chan.drop(*audit_cols)), on="sk_channel_key", how="left")
            .join(F.broadcast(dim_dec.drop(*audit_cols)), on="sk_decision_key", how="left")
            .join(df_latest_snap, on="sk_id_prev", how="left")
            .select(
                df["sk_id_curr"],
                df["sk_id_prev"],
                df["is_current_application"],
                df["target_default_flag"],
                F.coalesce(dim_cust["code_gender"], F.lit("Unknown")).alias("code_gender"),
                F.coalesce(dim_cust["flag_own_car"], F.lit("N")).alias("flag_own_car"),
                F.coalesce(dim_cust["flag_own_realty"], F.lit("N")).alias("flag_own_realty"),
                dim_cust["cnt_children"],
                dim_cust["cnt_fam_members"],
                dim_cust["amt_income_total"],
                F.coalesce(dim_cust["name_income_type"], F.lit("Unknown")).alias("name_income_type"),
                F.coalesce(dim_cust["name_education_type"], F.lit("Unknown")).alias("name_education_type"),
                F.coalesce(dim_cust["name_family_status"], F.lit("Unknown")).alias("name_family_status"),
                F.coalesce(dim_cust["name_housing_type"], F.lit("Unknown")).alias("name_housing_type"),
                F.coalesce(dim_cust["occupation_type"], F.lit("Unknown")).alias("occupation_type"),
                F.coalesce(dim_cust["organization_type"], F.lit("Unknown")).alias("organization_type"),
                dim_cust["age_years"],
                dim_cust["employed_years"],
                df["name_contract_type"],
                F.coalesce(dim_prod["portfolio_category"], F.lit("Other")).alias("portfolio_category"),
                F.coalesce(df["product_group"], F.lit("General Loan")).alias("product_group"),
                F.coalesce(dim_prod["is_revolving"], F.lit(False)).alias("is_revolving"),
                F.coalesce(dim_chan["channel_type"], F.lit("Unknown")).alias("channel_type"),
                F.coalesce(dim_chan["name_goods_category"], F.lit("XNA")).alias("name_goods_category"),
                F.coalesce(dim_chan["name_seller_industry"], F.lit("XNA")).alias("name_seller_industry"),
                F.coalesce(dim_chan["name_yield_group"], F.lit("XNA")).alias("name_yield_group"),
                F.coalesce(dim_dec["name_contract_status"], F.lit("Unknown")).alias("name_contract_status"),
                F.coalesce(dim_dec["code_reject_reason"], F.lit("Unknown")).alias("code_reject_reason"),
                F.coalesce(dim_dec["name_client_type"], F.lit("Unknown")).alias("name_client_type"),
                df["amt_application"],
                df["amt_credit"],
                df["amt_annuity"],
                df["amt_goods_price"],
                df["amt_down_payment"],
                df["rate_down_payment"],
                df["rate_interest_primary"],
                df["ext_source_1"],
                df["ext_source_2"],
                df["ext_source_3"],
                df_latest_snap["latest_balance"],
                df_latest_snap["latest_credit_limit"],
                df_latest_snap["latest_utilization_ratio"],
                df_latest_snap["latest_dpd"],
                F.coalesce(df_latest_snap["latest_contract_status"], F.lit("Unknown")).alias("latest_contract_status"),
                df_latest_snap["latest_snapshot_month"],
            )
        )
        return df_obt


def main():
    parser = argparse.ArgumentParser(description="Build curated obt_loan_portfolio_360")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedObtLoanPortfolio360Job(
        curated_base_dir=args.curated_dir,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
