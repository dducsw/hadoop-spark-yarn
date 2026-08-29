"""Root BaseSparkJob template with unified lifecycle, audit, and watermark."""
import os, sys
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
PROJECT_DIR = os.path.abspath(os.path.join(SRC_DIR, ".."))
sys.path.extend([SRC_DIR, PROJECT_DIR])

from src.common.audit import log_pipeline_execution
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.common.watermark import update_watermark


class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    DYNAMIC_PARTITION = "dynamic_partition"


class BaseSparkJob(ABC):
    def __init__(
        self,
        pipeline_layer: str,
        table_name: str,
        source_table: str,
        target_table: str,
        source_path: str,
        target_path: str,
        primary_key: Optional[str] = None,
        write_mode: WriteMode = WriteMode.OVERWRITE,
        partition_by: Optional[List[str]] = None,
    ):
        self.pipeline_layer = pipeline_layer
        self.table_name = table_name
        self.source_table = source_table
        self.target_table = target_table
        self.source_path = source_path.replace("\\", "/")
        self.target_path = target_path.replace("\\", "/")
        self.primary_key = primary_key
        self.write_mode = write_mode
        self.partition_by = partition_by or []
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
        """Step 4: Load to target HDFS Parquet & register Hive table using configured Write Strategy."""
        self.logger.info(
            f"Writing to Parquet: {self.target_path} | Mode: {self.write_mode.value} | Partitions: {self.partition_by}"
        )
        writer = df.write.format("parquet")

        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        if self.write_mode == WriteMode.DYNAMIC_PARTITION:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            writer = writer.mode("overwrite")
        else:
            writer = writer.mode(self.write_mode.value)

        writer.save(self.target_path)

        # Register Hive Metastore DDL
        self._register_hive_table(spark)
        return df.count()

    def _register_hive_table(self, spark: SparkSession) -> None:
        """Ensures Hive Database exists and Hive external/managed table is registered."""
        db_name = self.target_table.split(".")[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.target_table} "
            f"USING PARQUET LOCATION '{self.target_path}'"
        )
        if self.partition_by:
            spark.sql(f"MSCK REPAIR TABLE {self.target_table}")
        self.logger.info(f"Registered Hive table: {self.target_table}")

    def run(self) -> None:
        """Execute full template lifecycle with automatic audit & watermark."""
        spark = get_spark_session(f"{self.pipeline_layer.capitalize()}_{self.table_name}")
        start_time = datetime.now(timezone.utc)
        status, error_msg, row_count, col_count = "FAILED", None, None, None
        try:
            df_in = self.extract(spark)
            col_count = len(df_in.columns)
            self.validate(df_in)
            df_out = self.transform(df_in)
            row_count = self.load(spark, df_out)
            status = "SUCCESS"
            self.logger.info(f"SUCCESS: Processed {row_count:,} rows ({col_count} cols) for {self.table_name}")
            update_watermark(
                spark=spark,
                table_name=self.table_name,
                watermark_column="execution_timestamp",
                last_watermark_value=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                status="SUCCESS",
            )
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"FAILED: Error on {self.table_name}: {e}")
            raise
        finally:
            end_time = datetime.now(timezone.utc)
            log_pipeline_execution(
                spark=spark,
                pipeline_layer=self.pipeline_layer,
                table_name=self.table_name,
                source_table=self.source_table,
                target_table=self.target_table,
                source_path=self.source_path,
                target_path=self.target_path,
                start_time=start_time,
                end_time=end_time,
                status=status,
                row_count=row_count,
                column_count=col_count,
                error_message=error_msg,
            )
            spark.stop()
