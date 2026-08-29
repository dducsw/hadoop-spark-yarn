"""Base Curated (Gold) Feature Engineering & Aggregation Job template."""
import os, sys
from abc import abstractmethod
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.extend([SRC_DIR])

from src.common.base_spark_job import BaseSparkJob, WriteMode


class BaseCuratedJob(BaseSparkJob):
    def __init__(
        self,
        feature_name: str,
        stage_table_name: Optional[str] = None,
        stage_base_dir: str = "/stage/credit_risk",
        curated_base_dir: str = "/curated/credit_risk",
        stage_db: str = "stage_credit_risk",
        curated_db: str = "credit_risk",
        primary_key: str = "SK_ID_CURR",
        source_table: Optional[str] = None,
        write_mode: WriteMode = WriteMode.OVERWRITE,
        partition_by: Optional[List[str]] = None,
    ):
        stage_table = stage_table_name or feature_name
        source_path = os.path.join(stage_base_dir, stage_table)
        target_path = os.path.join(curated_base_dir, feature_name)
        resolved_source_table = source_table or f"{stage_db}.stage_{stage_table}"
        target_table = f"{curated_db}.{feature_name}"
        super().__init__(
            pipeline_layer="curated",
            table_name=feature_name,
            source_table=resolved_source_table,
            target_table=target_table,
            source_path=source_path,
            target_path=target_path,
            primary_key=primary_key,
            write_mode=write_mode,
            partition_by=partition_by,
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        self.logger.info(f"Reading Stage Parquet from {self.source_path}")
        return spark.read.parquet(self.source_path)

    @abstractmethod
    def build_features(self, df: DataFrame) -> DataFrame:
        """Abstract method: Subclasses implement feature aggregations (groupBy, sum, mean, etc.)."""
        pass

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. Execute business feature engineering / aggregations
        df_features = self.build_features(df)

        # 2. Add source table lineage metadata if not already set
        if "_source_table" not in df_features.columns:
            df_features = df_features.withColumn("_source_table", F.lit(self.source_table))

        # 3. Add curated lineage timestamp
        return df_features.withColumn("_curated_at", F.current_timestamp())
