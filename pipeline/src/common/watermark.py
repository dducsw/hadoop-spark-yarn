"""Pipeline Watermark State Management Module backed by PostgreSQL / RDBMS."""
import os
import sys
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import SparkSession

try:
    from src.common.db_metadata import get_metadata_cursor
except ImportError:
    from db_metadata import get_metadata_cursor


def get_watermark(spark: Optional[SparkSession], table_name: str) -> Optional[str]:
    """Retrieves the latest watermark value for a given table from metadata database."""
    try:
        with get_metadata_cursor() as (cursor, backend):
            placeholder = "%s" if backend == "postgres" else "?"
            cursor.execute(
                f"SELECT last_watermark_value FROM pipeline_watermark WHERE table_name = {placeholder} AND status = 'SUCCESS'",
                (table_name,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            return None
    except Exception as e:
        print(f"[WATERMARK ERROR] Failed to fetch watermark for {table_name}: {e}")
        return None


def update_watermark(
    spark: Optional[SparkSession],
    table_name: str,
    watermark_column: Optional[str] = "ingest_timestamp",
    last_watermark_value: Optional[str] = None,
    status: str = "SUCCESS",
) -> None:
    """Atomic UPSERT of watermark state for a table in metadata database (race-condition free)."""
    if last_watermark_value is None:
        last_watermark_value = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    now = datetime.now(timezone.utc)

    try:
        with get_metadata_cursor() as (cursor, backend):
            placeholder = "%s" if backend == "postgres" else "?"
            now_val = now.isoformat() if backend == "sqlite" else now
            sql = f"""
                INSERT INTO pipeline_watermark (
                    table_name, watermark_column, last_watermark_value, last_updated_at, status
                ) VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
                ON CONFLICT (table_name) DO UPDATE SET
                    watermark_column = EXCLUDED.watermark_column,
                    last_watermark_value = EXCLUDED.last_watermark_value,
                    last_updated_at = EXCLUDED.last_updated_at,
                    status = EXCLUDED.status;
            """
            cursor.execute(
                sql,
                (
                    table_name,
                    watermark_column or "N/A",
                    str(last_watermark_value),
                    now_val,
                    status,
                ),
            )
    except Exception as e:
        print(f"[WATERMARK ERROR] Failed to update watermark for {table_name}: {e}")
