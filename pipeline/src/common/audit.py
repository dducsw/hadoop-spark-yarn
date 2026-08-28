"""Pipeline Audit & Governance Logger."""
import uuid
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_SCHEMA = StructType(
    [
        StructField("job_id", StringType(), False),
        StructField("pipeline_layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), False),
        StructField("duration_sec", DoubleType(), False),
        StructField("row_count", LongType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

AUDIT_TABLE_NAME = "pipeline_audit_log"
AUDIT_DB_NAME = "metadata_db"
AUDIT_HDFS_LOCATION = "/metadata/pipeline_audit_log"


def log_pipeline_execution(
    spark: SparkSession,
    pipeline_layer: str,
    table_name: str,
    source_table: str,
    target_table: str,
    source_path: str,
    target_path: str,
    start_time: datetime,
    end_time: datetime,
    status: str,
    row_count: Optional[int] = None,
    column_count: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    """Logs job run metrics into a centralized HDFS/Hive audit table."""
    duration_sec = round((end_time - start_time).total_seconds(), 2)

    audit_row = [
        (
            str(uuid.uuid4()),
            pipeline_layer,
            table_name,
            source_table,
            target_table,
            source_path,
            target_path,
            start_time,
            end_time,
            float(duration_sec),
            int(row_count) if row_count is not None else None,
            int(column_count) if column_count is not None else None,
            status,
            str(error_message) if error_message else None,
        )
    ]

    try:
        audit_df = spark.createDataFrame(audit_row, schema=AUDIT_SCHEMA)

        # Append to HDFS Parquet
        (
            audit_df.write.mode("append")
            .format("parquet")
            .save(AUDIT_HDFS_LOCATION)
        )

        # Ensure Hive Metadata Table exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {AUDIT_DB_NAME}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_DB_NAME}.{AUDIT_TABLE_NAME} (
                job_id STRING,
                pipeline_layer STRING,
                table_name STRING,
                source_table STRING,
                target_table STRING,
                source_path STRING,
                target_path STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_sec DOUBLE,
                row_count BIGINT,
                column_count INT,
                status STRING,
                error_message STRING
            )
            USING PARQUET
            LOCATION '{AUDIT_HDFS_LOCATION}'
        """
        )
    except Exception as e:
        print(f"[AUDIT ERROR] Failed to write audit log: {e}")
