"""Pipeline Audit & Governance Logger backed by PostgreSQL / RDBMS."""
import os
import sys
import uuid
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

try:
    from .db_metadata import get_metadata_cursor
except (ImportError, ValueError):
    try:
        from src.common.db_metadata import get_metadata_cursor
    except ImportError:
        from db_metadata import get_metadata_cursor


def log_pipeline_execution(
    spark: Optional[SparkSession],
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
    rejected_count: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    """Logs job run metrics into PostgreSQL/RDBMS metadata table (zero HDFS small-files)."""
    duration_sec = round((end_time - start_time).total_seconds(), 2)
    job_id = str(uuid.uuid4())

    try:
        with get_metadata_cursor() as (cursor, backend):
            placeholder = "%s" if backend == "postgres" else "?"
            start_val = start_time.isoformat() if backend == "sqlite" and hasattr(start_time, "isoformat") else start_time
            end_val = end_time.isoformat() if backend == "sqlite" and hasattr(end_time, "isoformat") else end_time
            sql = f"""
                INSERT INTO pipeline_audit_log (
                    job_id, pipeline_layer, table_name, source_table, target_table,
                    source_path, target_path, start_time, end_time, duration_sec,
                    row_count, column_count, rejected_count, status, error_message
                ) VALUES ({', '.join([placeholder] * 15)})
            """
            cursor.execute(
                sql,
                (
                    job_id,
                    pipeline_layer,
                    table_name,
                    source_table,
                    target_table,
                    source_path,
                    target_path,
                    start_val,
                    end_val,
                    float(duration_sec),
                    int(row_count) if row_count is not None else None,
                    int(column_count) if column_count is not None else None,
                    int(rejected_count) if rejected_count is not None else 0,
                    status,
                    str(error_message) if error_message else None,
                ),
            )
    except Exception as e:
        print(f"[AUDIT ERROR] Failed to write audit log to database: {e}")
