"""Root BaseSparkJob template with unified lifecycle, audit, and watermark."""
import os, sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame, SparkSession

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
PROJECT_DIR = os.path.abspath(os.path.join(SRC_DIR, ".."))
sys.path.extend([SRC_DIR, PROJECT_DIR])

from src.common.audit import log_pipeline_execution
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.common.watermark import update_watermark


class BaseSparkJob(ABC):
    def __init__(self, pipeline_layer: str, table_name: str, source_table: str, target_table: str,
                 source_path: str, target_path: str, primary_key: Optional[str] = None):
        self.pipeline_layer = pipeline_layer
        self.table_name = table_name
        self.source_table = source_table
        self.target_table = target_table
        self.source_path = source_path.replace("\\", "/")
        self.target_path = target_path.replace("\\", "/")
        self.primary_key = primary_key
        self.logger = get_logger(f"{pipeline_layer.upper()}_{table_name}")

    @abstractmethod
    def extract(self, spark: SparkSession) -> DataFrame:
        """Step 1: Extract data from source layer."""
        pass

    def validate(self, df: DataFrame) -> None:
        """Step 2: Quality checks (overridable in subclasses)."""
        if df.rdd.isEmpty():
            raise ValueError(f"Job Failed: '{self.source_path}' is empty or unreadable!")

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Step 3: Transform, clean, or aggregate data."""
        pass

    def load(self, spark: SparkSession, df: DataFrame) -> int:
        """Step 4: Load to target HDFS Parquet & register Hive table."""
        self.logger.info(f"Writing to Parquet: {self.target_path}")
        df.write.mode("overwrite").format("parquet").save(self.target_path)
        db_name = self.target_table.split(".")[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.target_table} USING PARQUET LOCATION '{self.target_path}'")
        self.logger.info(f"Registered Hive table: {self.target_table}")
        return df.count()

    def run(self) -> None:
        """Execute full template lifecycle with automatic audit & watermark."""
        spark = get_spark_session(f"{self.pipeline_layer.capitalize()}_{self.table_name}")
        start_time = datetime.utcnow()
        status, error_msg, row_count, col_count = "FAILED", None, None, None
        try:
            df_in = self.extract(spark)
            col_count = len(df_in.columns)
            self.validate(df_in)
            df_out = self.transform(df_in)
            row_count = self.load(spark, df_out)
            status = "SUCCESS"
            self.logger.info(f"SUCCESS: Processed {row_count:,} rows ({col_count} cols) for {self.table_name}")
            update_watermark(spark, self.table_name, "execution_timestamp", start_time.strftime("%Y-%m-%d %H:%M:%S"), "SUCCESS")
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"FAILED: Error on {self.table_name}: {e}")
            raise
        finally:
            end_time = datetime.utcnow()
            log_pipeline_execution(spark, self.pipeline_layer, self.table_name, self.source_table, self.target_table,
                                   self.source_path, self.target_path, start_time, end_time, status, row_count, col_count, error_msg)
            spark.stop()
