"""Pipeline Watermark State Management Module."""
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

WATERMARK_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("watermark_column", StringType(), True),
        StructField("last_watermark_value", StringType(), True),
        StructField("last_updated_at", TimestampType(), False),
        StructField("status", StringType(), False),
    ]
)

WATERMARK_TABLE_NAME = "pipeline_watermark"
WATERMARK_DB_NAME = "metadata_db"
WATERMARK_HDFS_LOCATION = "/metadata/pipeline_watermark"


def get_watermark(spark: SparkSession, table_name: str) -> Optional[str]:
    """Retrieves the latest watermark value for a given table."""
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {WATERMARK_DB_NAME}")
        df = spark.table(f"{WATERMARK_DB_NAME}.{WATERMARK_TABLE_NAME}").filter(
            f"table_name = '{table_name}' AND status = 'SUCCESS'"
        )
        if df.rdd.isEmpty():
            return None
        return df.select("last_watermark_value").first()[0]
    except Exception:
        return None


def update_watermark(
    spark: SparkSession,
    table_name: str,
    watermark_column: Optional[str] = "ingest_timestamp",
    last_watermark_value: Optional[str] = None,
    status: str = "SUCCESS",
) -> None:
    """Updates/Upserts watermark state for a table in HDFS and Hive."""
    if last_watermark_value is None:
        last_watermark_value = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    now = datetime.utcnow()

    new_row = [
        (
            table_name,
            watermark_column or "N/A",
            str(last_watermark_value),
            now,
            status,
        )
    ]

    try:
        new_df: DataFrame = spark.createDataFrame(new_row, schema=WATERMARK_SCHEMA)

        # Upsert logic: read existing watermark table, filter out current table, union with new row
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {WATERMARK_DB_NAME}")
        try:
            existing_df = spark.table(f"{WATERMARK_DB_NAME}.{WATERMARK_TABLE_NAME}")
            updated_df = existing_df.filter(
                f"table_name != '{table_name}'"
            ).unionByName(new_df)
        except Exception:
            # Table doesn't exist yet
            updated_df = new_df

        # Overwrite watermark table with consolidated state
        (
            updated_df.write.mode("overwrite")
            .format("parquet")
            .save(WATERMARK_HDFS_LOCATION)
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {WATERMARK_DB_NAME}.{WATERMARK_TABLE_NAME} (
                table_name STRING,
                watermark_column STRING,
                last_watermark_value STRING,
                last_updated_at TIMESTAMP,
                status STRING
            )
            USING PARQUET
            LOCATION '{WATERMARK_HDFS_LOCATION}'
        """
        )
    except Exception as e:
        print(f"[WATERMARK ERROR] Failed to update watermark for {table_name}: {e}")
