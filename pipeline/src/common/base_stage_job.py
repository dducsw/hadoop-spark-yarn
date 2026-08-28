"""Base Stage (Silver) Cleaning & Standardizing Job template."""
import os, sys
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob


class BaseStageJob(BaseSparkJob):
    def __init__(self, table_name: str, primary_key: Optional[str] = None,
                 dedup_cols: Optional[List[str]] = None, raw_base_dir: str = "/raw/credit_risk",
                 stage_base_dir: str = "/stage/credit_risk", raw_db: str = "raw_credit_risk",
                 stage_db: str = "stage_credit_risk"):
        source_path = os.path.join(raw_base_dir, table_name)
        target_path = os.path.join(stage_base_dir, table_name)
        source_table = f"{raw_db}.raw_{table_name}"
        target_table = f"{stage_db}.stage_{table_name}"
        super().__init__(pipeline_layer="stage", table_name=table_name, source_table=source_table,
                         target_table=target_table, source_path=source_path, target_path=target_path, primary_key=primary_key)
        self.dedup_cols = dedup_cols or ([primary_key] if primary_key else None)

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Raw Parquet from {self.source_path}")
        return spark.read.parquet(self.source_path)

    def validate(self, df: DataFrame) -> None:
        super().validate(df)
        if self.primary_key and self.primary_key in df.columns:
            null_count = df.filter(F.col(self.primary_key).isNull()).count()
            if null_count > 0:
                self.logger.warning(f"Stage Warning: Dropping {null_count} rows with null PK '{self.primary_key}'")

    def clean_and_cast(self, df: DataFrame) -> DataFrame:
        """Override this method in table-specific stage jobs for custom type casts."""
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. Filter out null PKs if specified
        if self.primary_key and self.primary_key in df.columns:
            df = df.filter(F.col(self.primary_key).isNotNull())

        # 2. Deduplicate
        if self.dedup_cols:
            valid_cols = [c for c in self.dedup_cols if c in df.columns]
            if valid_cols:
                df = df.dropDuplicates(valid_cols)

        # 3. Custom type casting & cleaning
        df_cleaned = self.clean_and_cast(df)

        # 4. Add stage lineage timestamp
        return df_cleaned.withColumn("_staged_at", F.current_timestamp())
