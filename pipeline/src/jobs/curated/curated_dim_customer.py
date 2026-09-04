#!/usr/bin/env python3
"""Curated Job: Build conformed dim_customer from stage_application_train & test with xxhash64 SK and SCD2."""
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
    StructField,
    StructType,
    TimestampType,
)

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class CuratedDimCustomerJob(BaseSparkJob):
    def __init__(
        self,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
    ):
        self.stage_base_dir = stage_base_dir
        source_path = os.path.join(stage_base_dir, "application_train")
        target_path = os.path.join(curated_base_dir, "dim_customer")
        super().__init__(
            pipeline_layer="curated",
            table_name="dim_customer",
            source_table=f"{stage_db}.stage_application_train,{stage_db}.stage_application_test",
            target_table=f"{curated_db}.dim_customer",
            source_path=source_path,
            target_path=target_path,
            primary_key="sk_customer_key",
            write_mode=WriteMode.OVERWRITE,
        )

        self.curated_base_dir = curated_base_dir
        self.curated_db = curated_db

    def extract(self, spark: SparkSession) -> DataFrame:
        """Integrates customer records across active and test applications."""
        path_train = os.path.join(self.stage_base_dir, "application_train")
        path_test = os.path.join(self.stage_base_dir, "application_test")
        self.logger.info(f"Reading Stage applications from {path_train} and {path_test}")

        df_train = spark.read.parquet(path_train)
        df_test = spark.read.parquet(path_test)

        cols = [
            "sk_id_curr",
            "code_gender",
            "flag_own_car",
            "flag_own_realty",
            "cnt_children",
            "cnt_fam_members",
            "amt_income_total",
            "name_income_type",
            "name_education_type",
            "name_family_status",
            "name_housing_type",
            "occupation_type",
            "organization_type",
            "days_birth",
            "days_employed",
        ]

        df_train_sub = df_train.select([c for c in cols if c in df_train.columns])
        df_test_sub = df_test.select([c for c in cols if c in df_test.columns])

        return df_train_sub.unionByName(df_test_sub).dropDuplicates(["sk_id_curr"])

    def transform(self, df: DataFrame) -> DataFrame:
        """Derives age/employed years, generates xxhash64 surrogate key, adds SCD1 snapshot fields & Unknown row."""
        df_curated = (
            df
            .withColumn("sk_customer_key", F.xxhash64(F.col("sk_id_curr").cast(StringType())))
            .withColumn(
                "age_years",
                F.floor(F.abs(F.col("days_birth")) / 365.25).cast(IntegerType()),
            )
            .withColumn(
                "employed_years",
                F.when(F.col("days_employed") > 0, 0)
                .otherwise(F.floor(F.abs(F.col("days_employed")) / 365.25))
                .cast(IntegerType()),
            )
            .select(
                "sk_customer_key",
                "sk_id_curr",
                "code_gender",
                "flag_own_car",
                "flag_own_realty",
                "cnt_children",
                "cnt_fam_members",
                "amt_income_total",
                "name_income_type",
                "name_education_type",
                "name_family_status",
                "name_housing_type",
                "occupation_type",
                "organization_type",
                "age_years",
                "employed_years",
            )
        )

        unknown_schema = StructType([
            StructField("sk_customer_key", LongType(), True),
            StructField("sk_id_curr", IntegerType(), True),
            StructField("code_gender", StringType(), True),
            StructField("flag_own_car", StringType(), True),
            StructField("flag_own_realty", StringType(), True),
            StructField("cnt_children", IntegerType(), True),
            StructField("cnt_fam_members", IntegerType(), True),
            StructField("amt_income_total", DecimalType(18, 2), True),
            StructField("name_income_type", StringType(), True),
            StructField("name_education_type", StringType(), True),
            StructField("name_family_status", StringType(), True),
            StructField("name_housing_type", StringType(), True),
            StructField("occupation_type", StringType(), True),
            StructField("organization_type", StringType(), True),
            StructField("age_years", IntegerType(), True),
            StructField("employed_years", IntegerType(), True),
        ])
        unknown_row = [(-1, -1, "Unknown", "N", "N", 0, 1, None, "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", "Unknown", 0, 0)]
        unknown_df = df.sparkSession.createDataFrame(unknown_row, schema=unknown_schema)

        return df_curated.unionByName(unknown_df)

    def load(self, spark: SparkSession, df: DataFrame) -> int:
        """SCD4: Maintains current snapshot in dim_customer (SCD1) and appends changes to dim_customer_history."""
        history_path = os.path.join(self.curated_base_dir, "dim_customer_history")
        history_table = f"{self.curated_db}.dim_customer_history"

        # Change Detection against existing dim_customer
        df_real_incoming = df.filter(F.col("sk_id_curr") != -1)
        df_diff = None

        try:
            df_existing = spark.read.parquet(self.target_path)
            compare_cols = [
                "code_gender", "flag_own_car", "flag_own_realty", "cnt_children",
                "cnt_fam_members", "amt_income_total", "name_income_type",
                "name_education_type", "name_family_status", "name_housing_type",
                "occupation_type", "organization_type", "age_years", "employed_years"
            ]
            inc_hash = F.xxhash64(*[F.coalesce(F.col(f"inc.{c}").cast(StringType()), F.lit("")) for c in compare_cols])
            ext_hash = F.xxhash64(*[F.coalesce(F.col(f"ext.{c}").cast(StringType()), F.lit("")) for c in compare_cols])

            df_diff = (
                df_real_incoming.alias("inc")
                .join(df_existing.alias("ext"), on="sk_id_curr", how="left")
                .filter(F.col("ext.sk_id_curr").isNull() | (inc_hash != ext_hash))
                .select("inc.*")
            )
        except Exception:
            # Baseline initialization
            df_diff = df_real_incoming

        if df_diff is not None:
            now_ts = F.current_timestamp()
            df_history = (
                df_diff
                .withColumn("change_id", F.xxhash64(F.col("sk_customer_key").cast(StringType()), now_ts.cast(StringType())))
                .withColumn("effective_from", now_ts)
                .select(
                    "change_id",
                    "sk_customer_key",
                    "sk_id_curr",
                    "code_gender",
                    "flag_own_car",
                    "flag_own_realty",
                    "cnt_children",
                    "cnt_fam_members",
                    "amt_income_total",
                    "name_income_type",
                    "name_education_type",
                    "name_family_status",
                    "name_housing_type",
                    "occupation_type",
                    "organization_type",
                    "age_years",
                    "employed_years",
                    "effective_from",
                    "_source_system",
                    "_processed_at",
                    "_batch_id",
                )
            )
            df_history.write.mode("append").format("parquet").save(history_path)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {history_table} USING PARQUET LOCATION '{history_path}'")
            self.logger.info(f"SCD4: Recorded history changes to {history_table}")

        return super().load(spark, df)


def main():
    parser = argparse.ArgumentParser(description="Build curated dim_customer")
    parser.add_argument("--stage-dir", type=str, default="/stage/credit_risk")
    parser.add_argument("--curated-dir", type=str, default="/curated/credit_risk")
    parser.add_argument("--stage-db", type=str, default="stage_credit_risk")
    parser.add_argument("--curated-db", type=str, default="credit_risk")

    args = parser.parse_args()

    CuratedDimCustomerJob(
        stage_base_dir=args.stage_dir,
        curated_base_dir=args.curated_dir,
        stage_db=args.stage_db,
        curated_db=args.curated_db,
    ).run()


if __name__ == "__main__":
    main()
